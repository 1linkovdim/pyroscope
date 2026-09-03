package querier

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/common/model"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	ingestv1 "github.com/grafana/pyroscope/api/gen/proto/go/ingester/v1"
	typesv1 "github.com/grafana/pyroscope/api/gen/proto/go/types/v1"
	"github.com/grafana/pyroscope/v2/pkg/phlaredb/block"
	"github.com/grafana/pyroscope/v2/pkg/phlaredb/sharding"
	"github.com/grafana/pyroscope/v2/pkg/util"
	"github.com/grafana/pyroscope/v2/pkg/util/spanlogger"
)

type ResponseFromReplica[T any] struct {
	addr     string
	response T
}

type QueryReplicaFn[T any, Querier any] func(ctx context.Context, q Querier) (T, error)

type QueryReplicaWithHintsFn[T any, Querier any] func(ctx context.Context, q Querier, hint *ingestv1.Hints) (T, error)

type Closer interface {
	CloseRequest() error
	CloseResponse() error
}

type ClientFactory[T any] func(addr string) (T, error)

// cleanupResult, will make sure if the result was streamed, that we close the request and response
func cleanupStreams[Result any](result ResponseFromReplica[Result]) {
	if stream, ok := any(result.response).(interface {
		CloseRequest() error
	}); ok {
		if err := stream.CloseRequest(); err != nil {
			level.Warn(util.Logger).Log("msg", "failed to close request", "err", err)
		}
	}
	if stream, ok := any(result.response).(interface {
		CloseResponse() error
	}); ok {
		if err := stream.CloseResponse(); err != nil {
			level.Warn(util.Logger).Log("msg", "failed to close response", "err", err)
		}
	}
}

// forGivenReplicationSet runs f, in parallel, for given replica set.
// Under the hood it returns only enough responses to satisfy the quorum.
func forGivenReplicationSet[Result any, Querier any](ctx context.Context, clientFactory func(string) (Querier, error), replicationSet ring.ReplicationSet, f QueryReplicaFn[Result, Querier]) ([]ResponseFromReplica[Result], error) {
	results, err := ring.DoUntilQuorumWithoutSuccessfulContextCancellation(
		ctx,
		replicationSet,
		ring.DoUntilQuorumConfig{
			MinimizeRequests: true,
		},
		func(ctx context.Context, ingester *ring.InstanceDesc, _ context.CancelCauseFunc) (ResponseFromReplica[Result], error) {
			var res ResponseFromReplica[Result]
			client, err := clientFactory(ingester.Addr)
			if err != nil {
				return res, err
			}

			resp, err := f(ctx, client)
			if err != nil {
				return res, err
			}

			return ResponseFromReplica[Result]{ingester.Addr, resp}, nil
		},
		cleanupStreams[Result],
	)
	if err != nil {
		return nil, err
	}

	return results, err
}

// forGivenPlan runs f, in parallel, for given plan.
func forGivenPlan[Result any, Querier any](
	ctx context.Context,
	plan map[string]*blockPlanEntry,
	clientFactory func(string) (Querier, error),
	replicationSet ring.ReplicationSet, f QueryReplicaWithHintsFn[Result, Querier],
) ([]ResponseFromReplica[Result], error) {
	g, _ := errgroup.WithContext(ctx)

	var (
		idx    = 0
		result = make([]ResponseFromReplica[Result], len(plan))
	)

	for replica, planEntry := range plan {
		if !replicationSet.Includes(replica) {
			continue
		}
		var (
			i = idx
			r = replica
			h = planEntry.BlockHints
		)
		idx++
		g.Go(func() error {
			client, err := clientFactory(r)
			if err != nil {
				return err
			}

			resp, err := f(ctx, client, &ingestv1.Hints{Block: h})
			if err != nil {
				return err
			}

			result[i] = ResponseFromReplica[Result]{r, resp}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	result = result[:idx]

	return result, nil
}

type instanceType uint8

const (
	unknownInstanceType instanceType = iota
	ingesterInstance
	storeGatewayInstance
)

// map of block ID to replicas containing the block, when empty replicas, the
// block is already contained by a higher compaction level block in full.
type replicasPerBlockID struct {
	m             map[string][]string
	meta          map[string]*typesv1.BlockInfo
	instanceTypes map[string][]instanceType
	logger        log.Logger
}

func newReplicasPerBlockID(logger log.Logger) *replicasPerBlockID {
	return &replicasPerBlockID{
		m:             make(map[string][]string),
		meta:          make(map[string]*typesv1.BlockInfo),
		instanceTypes: make(map[string][]instanceType),
		logger:        logger,
	}
}

func (r *replicasPerBlockID) add(result []ResponseFromReplica[[]*typesv1.BlockInfo], t instanceType) {
	for _, replica := range result {
		// mark the replica's instance types (in single binary we can have the same replica have multiple types)
		r.instanceTypes[replica.addr] = append(r.instanceTypes[replica.addr], t)

		for _, block := range replica.response {
			// add block to map
			v, exists := r.m[block.Ulid]
			if exists && len(v) > 0 || !exists {
				r.m[block.Ulid] = append(r.m[block.Ulid], replica.addr)
			}

			// add block meta to map
			// note: we do override existing meta, as meta is immutable for all replicas
			r.meta[block.Ulid] = block
		}
	}
}

func shardFromBlock(m *typesv1.BlockInfo) (shard uint64, shardCount uint64, ok bool) {
	for _, lp := range m.Labels {
		if lp.Name != sharding.CompactorShardIDLabel {
			continue
		}

		shardID, shardCount, err := sharding.ParseShardIDLabelValue(lp.Value)
		if err == nil {
			return shardID, shardCount, true
		}
	}

	return 0, 0, false
}

func (r *replicasPerBlockID) removeBlock(ulid string) {
	delete(r.m, ulid)
	delete(r.meta, ulid)
}

// hasShardedBlocks reports whether any block carries a compactor shard label.
func (r *replicasPerBlockID) hasShardedBlocks() bool {
	for blockID := range r.m {
		if meta, ok := r.meta[blockID]; ok {
			if _, _, ok := shardFromBlock(meta); ok {
				return true
			}
		}
	}
	return false
}

// pruneIncompleteShardedBlocks drops sharded blocks for any window instant that
// is missing a shard. Only blocks at or above minLevel are counted towards
// completeness.
//
// It runs twice. First with minLevel = deduplicationLevel, before
// pruneSupersededBlocks, so that an incomplete set of deduplicated blocks is
// pruned while its lower-level ancestors are still around to serve as a
// fallback. Then again with minLevel = 0, after pruneSupersededBlocks, to check
// that whatever survived the collapse - which may include intermediate blocks
// kept as a last resort - still covers every shard.
func (r *replicasPerBlockID) pruneIncompleteShardedBlocks(minLevel int32) error {
	// Completeness is checked per sharding (shard count) and per time instant, not
	// by compaction level: a window's shards can legitimately sit at different
	// levels (a partial late re-merge advances only some), so keying on level would
	// falsely split a complete window and prune it. Different shard counts (e.g. a
	// shard-count reconfiguration) are separate shardings and checked independently.
	//
	// For each distinct window start we require every shard to have a trusted block
	// covering that instant. Coverage (minTime <= t < maxTime), rather than an exact
	// minTime match, keeps a wider block (a shard merged to a longer span) counted
	// for the later windows it overlaps, so sibling shards' blocks there are not
	// orphaned and silently pruned. Only blocks >= minLevel count:
	// intermediate lower blocks are preferentially dropped by
	// pruneSupersededBlocks, so they must not satisfy a shard here. They are also
	// never pruned here, which is what lets pruneSupersededBlocks keep them as a
	// last resort when their ancestors are gone.
	type shardedBlock struct {
		id      string
		shard   uint64
		minTime int64
		maxTime int64
	}
	byShardCount := make(map[uint64][]shardedBlock)
	for blockID := range r.m {
		meta, ok := r.meta[blockID]
		if !ok {
			return fmt.Errorf("meta missing for block id %s", blockID)
		}
		shard, shardCount, ok := shardFromBlock(meta)
		if !ok {
			continue
		}
		if meta.Compaction == nil || meta.Compaction.Level < minLevel {
			continue
		}
		byShardCount[shardCount] = append(byShardCount[shardCount],
			shardedBlock{id: blockID, shard: shard, minTime: meta.MinTime, maxTime: meta.MaxTime})
	}

	for shardCount, blocks := range byShardCount {
		// Distinct window starts, ascending: pruning an earlier incomplete window
		// first removes wide blocks that would otherwise mask a gap in a later one.
		starts := make([]int64, 0, len(blocks))
		seen := make(map[int64]struct{}, len(blocks))
		for _, b := range blocks {
			if _, ok := seen[b.minTime]; !ok {
				seen[b.minTime] = struct{}{}
				starts = append(starts, b.minTime)
			}
		}
		sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

		shardsSeen := make([]bool, shardCount)
		for _, t := range starts {
			for i := range shardsSeen {
				shardsSeen[i] = false
			}
			for _, b := range blocks {
				if _, live := r.m[b.id]; !live {
					continue // already pruned by an earlier incomplete window
				}
				if b.shard < shardCount && b.minTime <= t && t < b.maxTime {
					shardsSeen[b.shard] = true
				}
			}
			complete := true
			for _, s := range shardsSeen {
				if !s {
					complete = false
					break
				}
			}
			if complete {
				continue
			}

			// A shard is missing at this instant; dropping these can hide data (there
			// may be no lower-level fallback), so log rather than prune silently. We
			// prune the blocks that start here and fall back to the lower levels.
			var pruned int
			for _, b := range blocks {
				if b.minTime != t {
					continue
				}
				if _, live := r.m[b.id]; live {
					r.removeBlock(b.id)
					pruned++
				}
			}
			if pruned > 0 {
				level.Warn(r.logger).Log(
					"msg", "pruning incomplete sharded blocks from query plan; a shard is missing for this time window and its data will not be queried",
					"min_time", model.Time(t).Time().String(),
					"shards_expected", shardCount,
					"blocks_pruned", pruned,
				)
			}
		}
	}

	return nil
}

// pruneSupersededBlocks removes blocks whose data is fully contained in a block
// at or above the deduplication level, and collapses intermediate compaction
// artefacts when a complete lower-level fallback is still available.
//
// deduplicationLevel is the level at/above which a block is authoritative for
// its range (see block.DeduplicationLevel).
func (r *replicasPerBlockID) pruneSupersededBlocks(deduplicationLevel int32) error {
	// First pass: authoritative blocks supersede every ancestor they were built
	// from, including any intermediate blocks recorded as parents.
	for blockID := range r.m {
		meta, ok := r.meta[blockID]
		if !ok {
			return fmt.Errorf("meta missing for block id %s", blockID)
		}
		if meta.Compaction == nil || meta.Compaction.Level < deduplicationLevel {
			continue
		}
		for _, ancestor := range meta.Compaction.Parents {
			r.removeBlock(ancestor)
		}
		for _, ancestor := range meta.Compaction.Sources {
			r.removeBlock(ancestor)
		}
	}

	// Second pass: intermediate blocks (compacted at least once, but still below
	// the deduplication level – the split stage of split-and-merge compaction).
	// Reading them is more expensive than reading their ancestors: the split
	// stage fans a group out into shard_count blocks, which is typically
	// _significantly_ more blocks than it consumed, and they are not yet
	// deduplicated either way. So we prefer the ancestors – but only if they are
	// all still there. If any of them is gone, the intermediate block is the only
	// remaining copy of that data and dropping it loses the range entirely; keep
	// it instead and let the query deduplicate.
	for blockID := range r.m {
		meta, ok := r.meta[blockID]
		if !ok {
			return fmt.Errorf("meta missing for block id %s", blockID)
		}
		// Level < 2 means the block has not been compacted yet: it has no
		// ancestors to fall back to and is served as-is.
		if meta.Compaction == nil || meta.Compaction.Level < 2 || meta.Compaction.Level >= deduplicationLevel {
			continue
		}
		if r.ancestorsLive(meta) {
			r.removeBlock(blockID)
			continue
		}
		level.Warn(r.logger).Log(
			"msg", "querying intermediate compaction level block: its ancestors are no longer available",
			"block", blockID,
			"compaction_level", meta.Compaction.Level,
			"deduplication_level", deduplicationLevel,
		)
	}

	return nil
}

// ancestorsLive reports whether the block's ancestors are all still present and
// can therefore serve its range in its place. A block with no recorded
// ancestors has no known fallback.
func (r *replicasPerBlockID) ancestorsLive(meta *typesv1.BlockInfo) bool {
	if len(meta.Compaction.Sources) == 0 && len(meta.Compaction.Parents) == 0 {
		return false
	}
	for _, ancestor := range meta.Compaction.Sources {
		if _, ok := r.m[ancestor]; !ok {
			return false
		}
	}
	for _, ancestor := range meta.Compaction.Parents {
		if _, ok := r.m[ancestor]; !ok {
			return false
		}
	}
	return true
}

type blockPlanEntry struct {
	*ingestv1.BlockHints
	InstanceTypes []instanceType
}

type blockPlan map[string]*blockPlanEntry

func BlockHints(p blockPlan, replica string) (*ingestv1.BlockHints, error) {
	entry, ok := p[replica]
	if !ok && p != nil {
		return nil, fmt.Errorf("no hints found for replica %s", replica)
	}
	if entry == nil {
		return nil, nil
	}
	return entry.BlockHints, nil
}

func (p blockPlan) String() string {
	data, _ := json.Marshal(p)
	return string(data)
}

func (r *replicasPerBlockID) blockPlan(ctx context.Context) map[string]*blockPlanEntry {
	sp, _ := tracing.StartSpanFromContext(ctx, "blockPlan")
	defer sp.Finish()

	var (
		deduplicate             = false
		hash                    = xxhash.New()
		plan                    = make(map[string]*blockPlanEntry)
		smallestCompactionLevel = int32(0)
		shardCounts             = make(map[uint64]struct{})
	)

	sharded := r.hasShardedBlocks()

	// Depending on whether split sharding is used, the compaction level at
	// which the data gets deduplicated differs. The compactor uses the same
	// threshold, so the two stay in agreement.
	deduplicationLevel := block.DeduplicationLevel(sharded)

	// Prune incomplete sharded sets first, so that any lower-level ancestors
	// survive as a fallback, then collapse the surviving blocks per shard.
	if err := r.pruneIncompleteShardedBlocks(deduplicationLevel); err != nil {
		level.Warn(r.logger).Log("msg", "block planning failed to prune incomplete sharded blocks", "err", err)
		return nil
	}

	if err := r.pruneSupersededBlocks(deduplicationLevel); err != nil {
		level.Warn(r.logger).Log("msg", "block planning failed to prune superseded blocks", "err", err)
		return nil
	}

	// Re-check completeness over what survived: pruneSupersededBlocks may have
	// kept intermediate blocks that the first check did not count, and it may
	// have removed ancestors that were covering a shard.
	if err := r.pruneIncompleteShardedBlocks(0); err != nil {
		level.Warn(r.logger).Log("msg", "block planning failed to prune incomplete sharded blocks", "err", err)
		return nil
	}

	// now we go through all blocks and choose the replicas that we want to query
	for blockID, replicas := range r.m {
		// skip if we have no replicas, then block is already contained i an higher compaction level one
		if len(replicas) == 0 {
			continue
		}

		meta, ok := r.meta[blockID]
		if !ok {
			continue
		}
		// when we see a block with CompactionLevel less than the level at which data is deduplicated,
		// or a block without compaction section, we want the queriers to deduplicate
		if meta.Compaction == nil || meta.Compaction.Level < deduplicationLevel {
			deduplicate = true
		}

		// track the distinct shardings present in the plan (see below).
		if _, shardCount, ok := shardFromBlock(meta); ok {
			shardCounts[shardCount] = struct{}{}
		}

		// record the lowest compaction level
		if meta.Compaction != nil && (smallestCompactionLevel == 0 || meta.Compaction.Level < smallestCompactionLevel) {
			smallestCompactionLevel = meta.Compaction.Level
		}

		// only get store gateways replicas
		sgReplicas := lo.Filter(replicas, func(replica string, _ int) bool {
			instanceTypes, ok := r.instanceTypes[replica]
			if !ok {
				return false
			}
			for _, t := range instanceTypes {
				if t == storeGatewayInstance {
					return true
				}
			}
			return false
		})

		if len(sgReplicas) > 0 {
			// if we have store gateway replicas, we want to query them
			replicas = sgReplicas
		}

		// now select one replica, based on block id
		sort.Strings(replicas)
		hash.Reset()
		_, _ = hash.WriteString(blockID)
		hashIdx := int(hash.Sum64())
		if hashIdx < 0 {
			hashIdx = -hashIdx
		}
		selectedReplica := replicas[hashIdx%len(replicas)]

		// add block to plan
		p, exists := plan[selectedReplica]
		if !exists {
			p = &blockPlanEntry{
				BlockHints:    &ingestv1.BlockHints{},
				InstanceTypes: r.instanceTypes[selectedReplica],
			}
			plan[selectedReplica] = p
		}
		p.Ulids = append(p.Ulids, blockID)

		// set the selected replica
		r.m[blockID] = []string{selectedReplica}
	}

	// If more than one sharding scheme survived into the plan (e.g. an _of_2 and
	// an _of_4 set for overlapping data), they partition the same series
	// differently, so a series can appear in both. The sharded fast path assumes
	// each series is served by exactly one block, which only holds for a single
	// complete sharding - so force query-time deduplication to reconcile the
	// overlap instead of double-counting.
	if len(shardCounts) > 1 {
		deduplicate = true
	}

	// adapt the plan to make sure all replicas will deduplicate
	if deduplicate {
		for _, hints := range plan {
			hints.Deduplication = deduplicate
		}
	}

	var plannedIngesterBlocks, plannedStoreGatewayBlocks int
	for replica, blocks := range plan {
		instanceTypes, ok := r.instanceTypes[replica]
		if !ok {
			continue
		}
		for _, t := range instanceTypes {
			if t == storeGatewayInstance {
				plannedStoreGatewayBlocks += len(blocks.Ulids)
			}
			if t == ingesterInstance {
				plannedIngesterBlocks += len(blocks.Ulids)
			}
		}
	}

	sp.SetTag("deduplicate", deduplicate)
	sp.SetTag("smallest_compaction_level", smallestCompactionLevel)
	sp.SetTag("planned_blocks_ingesters", plannedIngesterBlocks)
	sp.SetTag("planned_blocks_store_gateways", plannedStoreGatewayBlocks)

	level.Debug(spanlogger.FromContext(ctx, r.logger)).Log(
		"msg", "block plan created",
		"deduplicate", deduplicate,
		"smallest_compaction_level", smallestCompactionLevel,
		"planned_blocks_ingesters", plannedIngesterBlocks,
		"planned_blocks_store_gateways", plannedStoreGatewayBlocks,
		"plan", blockPlan(plan),
	)

	return plan
}
