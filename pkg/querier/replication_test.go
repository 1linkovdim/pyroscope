package querier

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	typesv1 "github.com/grafana/pyroscope/api/gen/proto/go/types/v1"
	"github.com/grafana/pyroscope/v2/pkg/phlaredb/sharding"
)

type blockInfo struct {
	i typesv1.BlockInfo
}

func newBlockInfo(ulid string) *blockInfo {
	return &blockInfo{
		i: typesv1.BlockInfo{
			Ulid: ulid,
			Compaction: &typesv1.BlockCompaction{
				Level: 1,
			},
		},
	}
}

func (b *blockInfo) withMinTime(minT time.Time, d time.Duration) *blockInfo {
	b.i.MinTime = int64(model.TimeFromUnixNano(minT.UnixNano()))
	b.i.MaxTime = int64(model.TimeFromUnixNano(minT.Add(d).UnixNano()))
	return b
}

func (b *blockInfo) withCompactionLevel(i int32) *blockInfo {
	b.i.Compaction.Level = i
	return b
}

func (b *blockInfo) withCompactionSources(sources ...string) *blockInfo {
	b.i.Compaction.Sources = sources
	return b
}

func (b *blockInfo) withCompactionParents(parents ...string) *blockInfo {
	b.i.Compaction.Parents = parents
	return b
}

func (b *blockInfo) withLabelValue(k, v string) *blockInfo {
	b.i.Labels = append(b.i.Labels, &typesv1.LabelPair{
		Name:  k,
		Value: v,
	})
	return b
}

func (b *blockInfo) withCompactorShard(shard, shardsCount uint64) *blockInfo {
	return b.withLabelValue(
		sharding.CompactorShardIDLabel,
		sharding.FormatShardIDLabelValue(shard, shardsCount),
	)
}

func (b *blockInfo) info() *typesv1.BlockInfo {
	return &b.i
}

type validatorFunc func(t *testing.T, plan map[string]*blockPlanEntry)

func validatePlanBlockIDs(expBlockIDs ...string) validatorFunc {
	return func(t *testing.T, plan map[string]*blockPlanEntry) {
		var blockIDs []string
		for _, planEntry := range plan {
			blockIDs = append(blockIDs, planEntry.Ulids...)
		}
		sort.Strings(blockIDs)
		require.Equal(t, expBlockIDs, blockIDs)
	}
}

func validatePlanBlocksOnReplica(replica string, blocks ...string) validatorFunc {
	return func(t *testing.T, plan map[string]*blockPlanEntry) {
		planEntry, ok := plan[replica]
		require.True(t, ok, fmt.Sprintf("replica %s not found in plan", replica))
		for _, block := range blocks {
			require.Contains(t, planEntry.Ulids, block, "block %s not found in replica's %s plan", block, replica)
		}
	}
}

// validatePlanDeduplication asserts the query-time deduplication hint on every
// planned replica. A complete sharded set at/above the deduplication level is
// served directly (false); anything relying on a lower-level block is
// deduplicated at query time (true).
func validatePlanDeduplication(expected bool) validatorFunc {
	return func(t *testing.T, plan map[string]*blockPlanEntry) {
		require.NotEmpty(t, plan, "expected a non-empty plan to assert deduplication on")
		for replica, planEntry := range plan {
			require.Equal(t, expected, planEntry.Deduplication, "unexpected deduplication hint for replica %s", replica)
		}
	}
}

func Test_replicasPerBlockID_blockPlan(t *testing.T) {
	for _, tc := range []struct {
		name       string
		inputs     func(r *replicasPerBlockID)
		validators []validatorFunc
	}{
		{
			name: "single ingester",
			inputs: func(r *replicasPerBlockID) {
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "ingester-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("a").info(),
							newBlockInfo("b").info(),
							newBlockInfo("c").info(),
						},
					},
				}, ingesterInstance)
			},
			validators: []validatorFunc{
				validatePlanBlockIDs("a", "b", "c"),
				validatePlanBlocksOnReplica("ingester-0", "a", "b", "c"),
			},
		},
		{
			name: "two ingester with duplicated blocks",
			inputs: func(r *replicasPerBlockID) {
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "ingester-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("a").info(),
							newBlockInfo("d").info(),
						},
					},
					{
						addr: "ingester-1",
						response: []*typesv1.BlockInfo{
							newBlockInfo("b").info(),
							newBlockInfo("d").info(),
						},
					},
				}, ingesterInstance)
			},
			validators: []validatorFunc{
				validatePlanBlockIDs("a", "b", "d"),
				validatePlanBlocksOnReplica("ingester-0", "a", "d"),
				validatePlanBlocksOnReplica("ingester-1", "b"),
			},
		},
		{
			name: "prefer block on store-gateway over ingester",
			inputs: func(r *replicasPerBlockID) {
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "ingester-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("a").info(),
							newBlockInfo("b").info(),
						},
					},
				}, ingesterInstance)
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "store-gateway-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("a").info(),
						},
					},
				}, storeGatewayInstance)
			},
			validators: []validatorFunc{
				validatePlanBlockIDs("a", "b"),
				validatePlanBlocksOnReplica("store-gateway-0", "a"),
				validatePlanBlocksOnReplica("ingester-0", "b"),
			},
		},
		{
			name: "ignore incomplete shards",
			inputs: func(r *replicasPerBlockID) {
				t1, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
				t2, _ := time.Parse(time.RFC3339, "2021-01-01T01:00:00Z")
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "ingester-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("a").withMinTime(t1, time.Hour-time.Second).info(),
							newBlockInfo("b").withMinTime(t2, time.Hour-time.Second).info(),
						},
					},
				}, ingesterInstance)
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "store-gateway-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("a-1").
								withCompactionLevel(3).
								withCompactionSources("a").
								withCompactionParents("a").
								withCompactorShard(0, 2).
								withMinTime(t1, time.Hour-time.Second).
								info(),

							newBlockInfo("b-1").
								withCompactionLevel(3).
								withCompactionSources("b").
								withCompactionParents("b").
								withCompactorShard(0, 2).
								withMinTime(t2, time.Hour-(500*time.Millisecond)).info(),

							newBlockInfo("b-2").
								withCompactionLevel(3).
								withCompactionSources("b").
								withCompactionParents("b").
								withCompactorShard(1, 2).
								withMinTime(t2, time.Hour-time.Second).
								info(),
						},
					},
				}, storeGatewayInstance)
			},
			validators: []validatorFunc{
				validatePlanBlockIDs("a", "b-1", "b-2"),
				validatePlanBlocksOnReplica("store-gateway-0", "b-1"),
				validatePlanBlocksOnReplica("store-gateway-0", "b-2"),
				validatePlanBlocksOnReplica("ingester-0", "a"),
			},
		},
		{
			// Regression test for the mixed-level shard prune bug: a window whose
			// shards legitimately sit at DIFFERENT compaction levels (same minTime,
			// same shard count) must be treated as complete. With stacktracePartition
			// splitting, a small late-arriving increment re-merges only the shards it
			// touches, advancing those to a higher level while the rest lag. Here
			// shard 2 (of 4) stays at L3 while shards 0,1,3 advanced to L4. All four
			// shards are present, so the whole window must be queried - not pruned.
			name: "keep sharded window with shards at mixed compaction levels",
			inputs: func(r *replicasPerBlockID) {
				t1, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "store-gateway-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("s0").
								withCompactionLevel(4).
								withCompactorShard(0, 4).
								withMinTime(t1, 2*time.Hour).
								info(),
							newBlockInfo("s1").
								withCompactionLevel(4).
								withCompactorShard(1, 4).
								withMinTime(t1, 2*time.Hour).
								info(),
							// shard 2 lagged behind at level 3 (no late data touched it).
							newBlockInfo("s2").
								withCompactionLevel(3).
								withCompactorShard(2, 4).
								withMinTime(t1, time.Hour). // different maxTime, same minTime
								info(),
							newBlockInfo("s3").
								withCompactionLevel(4).
								withCompactorShard(3, 4).
								withMinTime(t1, 2*time.Hour).
								info(),
						},
					},
				}, storeGatewayInstance)
			},
			validators: []validatorFunc{
				// All four shards are present across the two levels, so the whole
				// window is queried. Pre-fix, the (level, minTime) grouping split
				// this into an L3 group (s2 only) and an L4 group (s0,s1,s3), judged
				// both incomplete, and returned an empty plan.
				validatePlanBlockIDs("s0", "s1", "s2", "s3"),
				// Every shard is at/above the deduplication level, so the complete
				// sharded set is served directly without query-time deduplication -
				// the #2586 optimisation, preserved across mixed levels.
				validatePlanDeduplication(false),
			},
		},
		{
			// Mixed-level window where one shard has BOTH its L3 block and the L4
			// block derived from it present at query time, while another shard lags
			// at L3 only. The completeness check (which counts level >= dedup) must
			// see both shards as present, and pruneSupersededBlocks must then
			// collapse shard 0's L3+L4 pair down to just the L4, leaving exactly one
			// block per shard: s0-l4 and s1-l3.
			name: "collapse L3+L4 duplicate of a shard within a mixed-level window",
			inputs: func(r *replicasPerBlockID) {
				t1, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "store-gateway-0",
						response: []*typesv1.BlockInfo{
							// shard 0: L4 derived from its L3 parent; both still present.
							newBlockInfo("s0-l4").
								withCompactionLevel(4).
								withCompactionSources("s0-l3").
								withCompactionParents("s0-l3").
								withCompactorShard(0, 2).
								withMinTime(t1, 2*time.Hour).
								info(),
							newBlockInfo("s0-l3").
								withCompactionLevel(3).
								withCompactorShard(0, 2).
								withMinTime(t1, time.Hour).
								info(),
							// shard 1: lagged at L3, no L4 derived yet.
							newBlockInfo("s1-l3").
								withCompactionLevel(3).
								withCompactorShard(1, 2).
								withMinTime(t1, time.Hour).
								info(),
						},
					},
				}, storeGatewayInstance)
			},
			validators: []validatorFunc{
				// Both shards present -> window kept; shard 0's L3 is superseded by
				// its L4 -> one block per shard remains.
				validatePlanBlockIDs("s0-l4", "s1-l3"),
			},
		},
		{
			// A window with a genuinely missing shard (absent at every level) must
			// still be pruned: shard 2 of 4 has no block at any level.
			name: "prune sharded window with a shard missing at all levels",
			inputs: func(r *replicasPerBlockID) {
				t1, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "ingester-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("fallback").withMinTime(t1, 2*time.Hour).info(),
						},
					},
				}, ingesterInstance)
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "store-gateway-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("s0").
								withCompactionLevel(4).
								withCompactorShard(0, 4).
								withMinTime(t1, 2*time.Hour).
								info(),
							newBlockInfo("s1").
								withCompactionLevel(3).
								withCompactorShard(1, 4).
								withMinTime(t1, time.Hour).
								info(),
							// shard 2 is missing entirely; shard 3 too.
							newBlockInfo("s3").
								withCompactionLevel(4).
								withCompactorShard(3, 4).
								withMinTime(t1, 2*time.Hour).
								info(),
						},
					},
				}, storeGatewayInstance)
			},
			validators: []validatorFunc{
				// Incomplete even across levels -> all sharded blocks pruned, only
				// the non-sharded fallback remains.
				validatePlanBlockIDs("fallback"),
				// The fallback is a low-level block, so query-time deduplication is
				// enabled.
				validatePlanDeduplication(true),
			},
		},
		{
			// Shard-count reconfiguration: the shard count was raised from 2 to 4
			// (e.g. compactor.split-and-merge-shards). The same window now holds a
			// complete old _of_2 scheme and a partial new _of_4 scheme, both fully
			// compacted to the SAME level and same minTime - so only the shard count
			// distinguishes them. Correct behaviour: the two schemes are counted for
			// completeness separately, the incomplete _of_4 scheme is pruned, and the
			// complete _of_2 scheme is served directly.
			//
			// This isolates the shardCount component of the grouping key: pre-fix,
			// keying on {level, minTime} pooled all four blocks into one group with
			// two different shard counts, tripping the shard-length-mismatch guard
			// and emptying the plan.
			name: "serve complete old sharding when a shard-count change leaves a partial new one",
			inputs: func(r *replicasPerBlockID) {
				t1, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "store-gateway-0",
						response: []*typesv1.BlockInfo{
							// complete old 2-shard scheme
							newBlockInfo("old0").withCompactionLevel(4).withCompactorShard(0, 2).withMinTime(t1, 2*time.Hour).info(),
							newBlockInfo("old1").withCompactionLevel(4).withCompactorShard(1, 2).withMinTime(t1, 2*time.Hour).info(),
							// partial new 4-shard scheme (shards 0 and 3 not yet produced)
							newBlockInfo("new1").withCompactionLevel(4).withCompactorShard(1, 4).withMinTime(t1, 2*time.Hour).info(),
							newBlockInfo("new2").withCompactionLevel(4).withCompactorShard(2, 4).withMinTime(t1, 2*time.Hour).info(),
						},
					},
				}, storeGatewayInstance)
			},
			validators: []validatorFunc{
				// The partial _of_4 scheme is pruned; the complete _of_2 scheme
				// survives and is served without query-time deduplication.
				validatePlanBlockIDs("old0", "old1"),
				validatePlanDeduplication(false),
			},
		},
		{
			// A window mid-merge: same shard count, but shard 0 already merged to
			// L3 while shard 1 is still only in an intermediate L2 block, and the
			// L1 ancestor is gone. The L2 block is not deduplicated and will be
			// dropped by pruneSupersededBlocks, so it must NOT count shard 1 as
			// present. Counting it would keep shard 0's L3 block and then drop the
			// L2 - silently serving only half the data. Correct behaviour: the
			// (trusted) sharded set is incomplete, so everything is pruned (a
			// transient empty result during compaction, rather than an under-count).
			name: "do not let an intermediate L2 block satisfy shard completeness",
			inputs: func(r *replicasPerBlockID) {
				t1, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "store-gateway-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("s0-l3").
								withCompactionLevel(3).
								withCompactionSources("s0-l2").
								withCompactorShard(0, 2).
								withMinTime(t1, time.Hour).
								info(),
							newBlockInfo("s0-l2").
								withCompactionLevel(2).
								withCompactorShard(0, 2).
								withMinTime(t1, time.Hour).
								info(),
							// shard 1 only exists as an intermediate L2 block.
							newBlockInfo("s1-l2").
								withCompactionLevel(2).
								withCompactorShard(1, 2).
								withMinTime(t1, time.Hour).
								info(),
						},
					},
				}, storeGatewayInstance)
			},
			validators: []validatorFunc{
				validatePlanBlockIDs(),
			},
		},
		{
			// Using a split-and-merge compactor, deduplication happens at level 3,
			// level 2 is intermediate step, where series distributed among shards
			// but not yet deduplicated.
			name: "ignore blocks which are sharded and in level 2",
			inputs: func(r *replicasPerBlockID) {
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "ingester-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("a").info(),
							newBlockInfo("b").info(),
						},
					},
				}, ingesterInstance)
				r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
					{
						addr: "store-gateway-0",
						response: []*typesv1.BlockInfo{
							newBlockInfo("a").info(),
							newBlockInfo("a-1").
								withCompactionLevel(2).
								withCompactionSources("a").
								withCompactionParents("a").
								withCompactorShard(0, 2).
								info(),

							newBlockInfo("a-2").
								withCompactionLevel(2).
								withCompactionSources("a").
								withCompactionParents("a").
								withCompactorShard(1, 2).
								info(),

							newBlockInfo("a-3").
								withCompactionLevel(3).
								withCompactionSources("a-2").
								withCompactorShard(0, 3).
								info(),
						},
					},
				}, storeGatewayInstance)
			},
			validators: []validatorFunc{
				validatePlanBlockIDs("a", "b"),
				validatePlanBlocksOnReplica("store-gateway-0", "a"),
				validatePlanBlocksOnReplica("ingester-0", "b"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newReplicasPerBlockID(log.NewNopLogger())
			tc.inputs(r)

			plan := r.blockPlan(context.TODO())
			for _, v := range tc.validators {
				v(t, plan)
			}
		})
	}
}

// The incident this fixes was silent: a fully-intact window returned zero with
// no log or metric. These tests pin down that pruning an incomplete sharded
// window now emits a WARN, and that a healthy mixed-level window does not.
func Test_pruneIncompleteShardedBlocks_logging(t *testing.T) {
	t1, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")

	const warnMsg = "a shard is missing for this time window"

	t.Run("warns when pruning an incomplete sharded window", func(t *testing.T) {
		var buf bytes.Buffer
		r := newReplicasPerBlockID(log.NewLogfmtLogger(&buf))
		// shard 0 of 2 present, shard 1 missing at every level, no fallback.
		r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
			{
				addr: "store-gateway-0",
				response: []*typesv1.BlockInfo{
					newBlockInfo("s0").withCompactionLevel(3).withCompactorShard(0, 2).withMinTime(t1, 2*time.Hour).info(),
				},
			},
		}, storeGatewayInstance)

		plan := r.blockPlan(context.TODO())
		require.Empty(t, plan)
		require.Contains(t, buf.String(), warnMsg, "expected a WARN when an incomplete window is pruned")
	})

	t.Run("does not warn for a complete mixed-level window", func(t *testing.T) {
		var buf bytes.Buffer
		r := newReplicasPerBlockID(log.NewLogfmtLogger(&buf))
		// both shards present, at different levels.
		r.add([]ResponseFromReplica[[]*typesv1.BlockInfo]{
			{
				addr: "store-gateway-0",
				response: []*typesv1.BlockInfo{
					newBlockInfo("s0").withCompactionLevel(4).withCompactorShard(0, 2).withMinTime(t1, 2*time.Hour).info(),
					newBlockInfo("s1").withCompactionLevel(3).withCompactorShard(1, 2).withMinTime(t1, time.Hour).info(),
				},
			},
		}, storeGatewayInstance)

		plan := r.blockPlan(context.TODO())
		require.NotEmpty(t, plan)
		require.NotContains(t, buf.String(), warnMsg, "must not warn for a complete window")
	})
}
