package querybackend

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tracing"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	metastorev1 "github.com/grafana/pyroscope/api/gen/proto/go/metastore/v1"
	queryv1 "github.com/grafana/pyroscope/api/gen/proto/go/query/v1"
	"github.com/grafana/pyroscope/v2/pkg/block"
	"github.com/grafana/pyroscope/v2/pkg/util"
)

// TODO(kolesnikovae): We have a procedural definition of our queries,
//  thus we have handlers. Instead, in order to enable pipelining and
//  reduce the boilerplate, we should define query execution plans.

const (
	// maxProfileIDsToLog is the maximum number of profile IDs to log in trace spans.
	maxProfileIDsToLog = 10
)

var (
	handlerMutex  = new(sync.RWMutex)
	queryHandlers = map[queryv1.QueryType]queryHandler{}
)

type queryHandler func(*queryContext, *queryv1.Query) (*queryv1.Report, error)

func registerQueryHandler(t queryv1.QueryType, h queryHandler) {
	handlerMutex.Lock()
	defer handlerMutex.Unlock()
	if _, ok := queryHandlers[t]; ok {
		panic(fmt.Sprintf("%s: handler already registered", t))
	}
	queryHandlers[t] = h
}

func getQueryHandler(t queryv1.QueryType) (queryHandler, error) {
	handlerMutex.RLock()
	defer handlerMutex.RUnlock()
	handler, ok := queryHandlers[t]
	if !ok {
		return nil, fmt.Errorf("unknown query type %s", t)
	}
	return handler, nil
}

var (
	depMutex          = new(sync.RWMutex)
	queryDependencies = map[queryv1.QueryType][]block.Section{}
)

func registerQueryDependencies(t queryv1.QueryType, deps ...block.Section) {
	depMutex.Lock()
	defer depMutex.Unlock()
	if _, ok := queryDependencies[t]; ok {
		panic(fmt.Sprintf("%s: dependencies already registered", t))
	}
	queryDependencies[t] = deps
}

func registerQueryType(
	qt queryv1.QueryType,
	rt queryv1.ReportType,
	q queryHandler,
	a aggregatorProvider,
	alwaysAggregate bool, // this option will always call the aggregate method for this report type, so it will also run when there is only one report
	deps ...block.Section,
) {
	registerQueryReportType(qt, rt)
	registerQueryHandler(qt, q)
	registerQueryDependencies(qt, deps...)
	registerAggregator(rt, a, alwaysAggregate)
}

type blockContext struct {
	ctx             context.Context
	log             log.Logger
	metrics         *metrics
	req             *request
	agg             *reportAggregator
	obj             *block.Object
	grp             *errgroup.Group
	execCollector   *blockExecutionCollector
	weightCollector *queryWeightCollector
	includeStripped bool
	timings         blockTimings
}

// blockTimings keeps per-dataset stage totals without a span for each stage.
// Query types within a dataset can run concurrently, so the totals are not
// necessarily additive with datasetWait or the block wall time.
type blockTimings struct {
	indexLookup    time.Duration
	datasetWait    time.Duration
	openNS         atomic.Int64
	openMaxNS      atomic.Int64
	objectOpenNS   atomic.Int64
	prefetchNS     atomic.Int64
	profilesOpenNS atomic.Int64
	tsdbOpenNS     atomic.Int64
	symbolsOpenNS  atomic.Int64
	queryNS        atomic.Int64
	queryMaxNS     atomic.Int64
	// Tree-query stages are accumulated once per dataset, not per profile row.
	treeEntriesNS        atomic.Int64
	treeScanNS           atomic.Int64
	treeScanMaxNS        atomic.Int64
	treeResolveNS        atomic.Int64
	treeResolveMaxNS     atomic.Int64
	treeSerializeNS      atomic.Int64
	treeFetchNS          atomic.Int64
	treeFetchMaxNS       atomic.Int64
	treeFetchWaitNS      atomic.Int64
	treeFetchWaitMaxNS   atomic.Int64
	treeBuildNS          atomic.Int64
	treeBuildMaxNS       atomic.Int64
	treeMergeWaitNS      atomic.Int64
	treeMergeNS          atomic.Int64
	treeQueries          atomic.Int64
	treeProfileRows      atomic.Int64
	treeCandidateSamples atomic.Int64
	treePartitions       atomic.Int64
	treeUniqueStacks     atomic.Int64
	treeOutputBytes      atomic.Int64
	aggregateNS          atomic.Int64
	closeNS              atomic.Int64
	queries              atomic.Int64
}

func maxAtomicDuration(dst *atomic.Int64, d time.Duration) {
	n := d.Nanoseconds()
	for old := dst.Load(); n > old; old = dst.Load() {
		if dst.CompareAndSwap(old, n) {
			return
		}
	}
}

func (b *blockContext) execute() error {
	startTime := time.Now()

	var span *tracing.Span
	span, b.ctx = tracing.StartSpanFromContext(b.ctx, "blockContext.execute")
	defer span.Finish()
	defer b.reportTimings(startTime)

	if idxs := b.datasetIndices(); len(idxs) > 0 {
		lookupStart := time.Now()
		err := b.lookupDatasets(idxs)
		b.timings.indexLookup = time.Since(lookupStart)
		if err != nil {
			if b.obj.IsNotExists(err) {
				level.Warn(b.log).Log("msg", "object not found", "err", err)
				return nil
			}
			return fmt.Errorf("failed to lookup datasets: %w", err)
		}
		// Only accumulate datasets resolved from the index lookup; Format0
		// datasets were already counted by the query frontend at planning time.
		b.weightCollector.addDatasets(b.obj.Metadata().Datasets)
	}

	md := b.obj.Metadata()
	for _, ds := range md.Datasets {
		datasetStart := time.Now()
		q := b.newQueryContext(ds)
		for _, query := range b.req.src.Query {
			q.grp.Go(util.RecoverPanic(func() error {
				return q.execute(query)
			}))
		}
		err := q.grp.Wait()
		b.timings.datasetWait += time.Since(datasetStart)
		if err != nil {
			return err
		}
	}

	if b.execCollector != nil {
		b.execCollector.record(&queryv1.BlockExecution{
			BlockId:           md.Id,
			StartTimeNs:       startTime.UnixNano(),
			EndTimeNs:         time.Now().UnixNano(),
			DatasetsProcessed: int64(len(md.Datasets)),
			Size:              md.Size,
			Shard:             md.Shard,
			CompactionLevel:   md.CompactionLevel,
		})
	}

	return nil
}

func (b *blockContext) reportTimings(start time.Time) {
	total := time.Since(start)
	md := b.obj.Metadata()
	var weight block.DatasetWeight
	for _, ds := range md.Datasets {
		weight.Add(block.WeightOf(ds))
	}
	if b.metrics != nil {
		observe := b.metrics.blockStageDuration.WithLabelValues
		observe("total").Observe(total.Seconds())
		for _, stage := range []struct {
			name     string
			duration time.Duration
		}{
			{"index_lookup", b.timings.indexLookup},
			{"dataset_wait", b.timings.datasetWait},
			{"dataset_open_sum", time.Duration(b.timings.openNS.Load())},
			{"dataset_open_max", time.Duration(b.timings.openMaxNS.Load())},
			{"object_open_sum", time.Duration(b.timings.objectOpenNS.Load())},
			{"dataset_prefetch_sum", time.Duration(b.timings.prefetchNS.Load())},
			{"profiles_open_sum", time.Duration(b.timings.profilesOpenNS.Load())},
			{"tsdb_open_sum", time.Duration(b.timings.tsdbOpenNS.Load())},
			{"symbols_open_sum", time.Duration(b.timings.symbolsOpenNS.Load())},
			{"query_sum", time.Duration(b.timings.queryNS.Load())},
			{"query_max", time.Duration(b.timings.queryMaxNS.Load())},
			{"tree_entries_sum", time.Duration(b.timings.treeEntriesNS.Load())},
			{"tree_scan_sum", time.Duration(b.timings.treeScanNS.Load())},
			{"tree_scan_max", time.Duration(b.timings.treeScanMaxNS.Load())},
			{"tree_resolve_sum", time.Duration(b.timings.treeResolveNS.Load())},
			{"tree_resolve_max", time.Duration(b.timings.treeResolveMaxNS.Load())},
			{"tree_serialize_sum", time.Duration(b.timings.treeSerializeNS.Load())},
			{"tree_partition_fetch_sum", time.Duration(b.timings.treeFetchNS.Load())},
			{"tree_partition_fetch_max", time.Duration(b.timings.treeFetchMaxNS.Load())},
			{"tree_partition_wait_sum", time.Duration(b.timings.treeFetchWaitNS.Load())},
			{"tree_partition_wait_max", time.Duration(b.timings.treeFetchWaitMaxNS.Load())},
			{"tree_build_sum", time.Duration(b.timings.treeBuildNS.Load())},
			{"tree_build_max", time.Duration(b.timings.treeBuildMaxNS.Load())},
			{"tree_merge_wait_sum", time.Duration(b.timings.treeMergeWaitNS.Load())},
			{"tree_merge_sum", time.Duration(b.timings.treeMergeNS.Load())},
			{"aggregate_sum", time.Duration(b.timings.aggregateNS.Load())},
			{"dataset_close_sum", time.Duration(b.timings.closeNS.Load())},
		} {
			if stage.duration > 0 {
				observe(stage.name).Observe(stage.duration.Seconds())
			}
		}
		if b.timings.treeQueries.Load() > 0 {
			observeItems := b.metrics.blockWorkItems.WithLabelValues
			observeItems("tree_queries").Observe(float64(b.timings.treeQueries.Load()))
			observeItems("profile_rows").Observe(float64(b.timings.treeProfileRows.Load()))
			observeItems("candidate_samples").Observe(float64(b.timings.treeCandidateSamples.Load()))
			observeItems("symbol_partitions").Observe(float64(b.timings.treePartitions.Load()))
			observeItems("unique_stacktrace_ids").Observe(float64(b.timings.treeUniqueStacks.Load()))
			b.metrics.blockWorkBytes.WithLabelValues("tree_output").Observe(float64(b.timings.treeOutputBytes.Load()))
		}
		observeBytes := b.metrics.blockWorkBytes.WithLabelValues
		observeBytes("profiles_estimate").Observe(float64(weight.ProfilesBytes))
		observeBytes("tsdb_estimate").Observe(float64(weight.TSDBBytes))
		observeBytes("symbols_estimate").Observe(float64(weight.SymbolsBytes))
	}
	if total < time.Second {
		return
	}
	traceID, _ := tracing.ExtractTraceID(b.ctx)
	level.Info(b.log).Log(
		"msg", "slow query backend block",
		"trace_id", traceID,
		"block_id", md.Id,
		"compaction_level", md.CompactionLevel,
		"datasets", len(md.Datasets),
		"queries", b.timings.queries.Load(),
		"tree_queries", b.timings.treeQueries.Load(),
		"profile_rows", b.timings.treeProfileRows.Load(),
		"candidate_samples", b.timings.treeCandidateSamples.Load(),
		"symbol_partitions", b.timings.treePartitions.Load(),
		"unique_stacktrace_ids", b.timings.treeUniqueStacks.Load(),
		"profiles_estimate_bytes", weight.ProfilesBytes,
		"tsdb_estimate_bytes", weight.TSDBBytes,
		"symbols_estimate_bytes", weight.SymbolsBytes,
		"tree_output_bytes", b.timings.treeOutputBytes.Load(),
		"total_ms", total.Milliseconds(),
		"index_lookup_ms", b.timings.indexLookup.Milliseconds(),
		"dataset_wait_ms", b.timings.datasetWait.Milliseconds(),
		"dataset_open_sum_ms", time.Duration(b.timings.openNS.Load()).Milliseconds(),
		"dataset_open_max_ms", time.Duration(b.timings.openMaxNS.Load()).Milliseconds(),
		"object_open_sum_ms", time.Duration(b.timings.objectOpenNS.Load()).Milliseconds(),
		"dataset_prefetch_sum_ms", time.Duration(b.timings.prefetchNS.Load()).Milliseconds(),
		"profiles_open_sum_ms", time.Duration(b.timings.profilesOpenNS.Load()).Milliseconds(),
		"tsdb_open_sum_ms", time.Duration(b.timings.tsdbOpenNS.Load()).Milliseconds(),
		"symbols_open_sum_ms", time.Duration(b.timings.symbolsOpenNS.Load()).Milliseconds(),
		"query_sum_ms", time.Duration(b.timings.queryNS.Load()).Milliseconds(),
		"query_max_ms", time.Duration(b.timings.queryMaxNS.Load()).Milliseconds(),
		"tree_entries_sum_ms", time.Duration(b.timings.treeEntriesNS.Load()).Milliseconds(),
		"tree_scan_sum_ms", time.Duration(b.timings.treeScanNS.Load()).Milliseconds(),
		"tree_scan_max_ms", time.Duration(b.timings.treeScanMaxNS.Load()).Milliseconds(),
		"tree_resolve_sum_ms", time.Duration(b.timings.treeResolveNS.Load()).Milliseconds(),
		"tree_resolve_max_ms", time.Duration(b.timings.treeResolveMaxNS.Load()).Milliseconds(),
		"tree_serialize_sum_ms", time.Duration(b.timings.treeSerializeNS.Load()).Milliseconds(),
		"tree_partition_fetch_sum_ms", time.Duration(b.timings.treeFetchNS.Load()).Milliseconds(),
		"tree_partition_fetch_max_ms", time.Duration(b.timings.treeFetchMaxNS.Load()).Milliseconds(),
		"tree_partition_wait_sum_ms", time.Duration(b.timings.treeFetchWaitNS.Load()).Milliseconds(),
		"tree_partition_wait_max_ms", time.Duration(b.timings.treeFetchWaitMaxNS.Load()).Milliseconds(),
		"tree_build_sum_ms", time.Duration(b.timings.treeBuildNS.Load()).Milliseconds(),
		"tree_build_max_ms", time.Duration(b.timings.treeBuildMaxNS.Load()).Milliseconds(),
		"tree_merge_wait_sum_ms", time.Duration(b.timings.treeMergeWaitNS.Load()).Milliseconds(),
		"tree_merge_sum_ms", time.Duration(b.timings.treeMergeNS.Load()).Milliseconds(),
		"aggregate_sum_ms", time.Duration(b.timings.aggregateNS.Load()).Milliseconds(),
		"dataset_close_sum_ms", time.Duration(b.timings.closeNS.Load()).Milliseconds(),
	)
}

// datasetIndices returns the Format1 (dataset_tsdb_index) pseudo-datasets
// in the block metadata that need to be resolved into concrete datasets
// before the query can be executed. It returns nil when no resolution
// is needed: either the metadata already lists explicit (Format0)
// datasets, or the query is index-only and can be served directly from
// the dataset_index TSDB section.
//
// Multiple Format1 datasets may be present in a single block when a
// segment writer covers more than one tenant: it emits a per-tenant
// dataset_index pseudo-dataset, and the metastore returns all matching
// pseudo-datasets to the query backend. In that case all of their
// indices must be looked up so the union of resolved datasets is
// considered.
func (b *blockContext) datasetIndices() []*metastorev1.Dataset {
	md := b.obj.Metadata()
	var indices []*metastorev1.Dataset
	for _, ds := range md.Datasets {
		if block.DatasetFormat(ds.Format) == block.DatasetFormat1 {
			indices = append(indices, ds)
		}
	}
	if len(indices) == 0 {
		// The block's metadata explicitly lists datasets to be queried.
		return nil
	}
	if len(indices) != len(md.Datasets) {
		// The metastore is expected to return a uniform set of datasets
		// (either all Format0 explicit datasets matched by service_name,
		// or all Format1 pseudo-datasets matched by __tenant_dataset__).
		// A mixed set is not expected; bail on the lookup so the query
		// runs against the explicitly-listed datasets only.
		return nil
	}

	// If the query only requires TSDB data, we can serve it directly
	// from each Format1 dataset's TSDB section (which is aliased to the
	// dataset_index) without resolving real datasets.
	s := (&queryContext{blockContext: b}).sections()
	indexOnly := len(s) == 1 && s[0] == block.SectionTSDB
	if indexOnly {
		oteltrace.SpanFromContext(b.ctx).SetAttributes(attribute.Bool("dataset_index_query_index_only", indexOnly))
		return nil
	}

	return indices
}

func (b *blockContext) lookupDatasets(indices []*metastorev1.Dataset) error {
	oteltrace.SpanFromContext(b.ctx).SetAttributes(attribute.Bool("dataset_index_query", true))
	oteltrace.SpanFromContext(b.ctx).SetAttributes(attribute.Int("dataset_index_count", len(indices)))

	// As query execution has not started yet,
	// we can safely open datasets.
	datasets := make([]*block.Dataset, len(indices))
	for i, ds := range indices {
		datasets[i] = block.NewDataset(ds, b.obj)
	}
	defer func() {
		for _, d := range datasets {
			_ = d.Close()
		}
	}()

	g, ctx := errgroup.WithContext(b.ctx)
	var md *metastorev1.BlockMeta
	g.Go(func() (err error) {
		md, err = b.obj.ReadMetadata(ctx)
		return err
	})
	for _, d := range datasets {
		g.Go(func() error {
			return d.Open(ctx, block.SectionDatasetIndex)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Each per-tenant dataset_index encodes its tenant's real datasets
	// using their global position within the block as the chunk
	// SeriesIndex (see segment writer and compaction). Therefore IDs
	// from different per-tenant indices are disjoint and a simple union
	// across tenants is correct.
	datasetIDs := make(map[uint32]struct{})
	for _, d := range datasets {
		ids, err := getSeriesIDs(d.Index(), b.req.matchers...)
		if err != nil {
			return err
		}
		for id := range ids {
			datasetIDs[id] = struct{}{}
		}
	}

	var j int
	for i := range md.Datasets {
		if _, ok := datasetIDs[uint32(i)]; ok {
			md.Datasets[j] = md.Datasets[i]
			j++
		}
	}
	md.Datasets = md.Datasets[:j]
	b.obj.SetMetadata(md)

	oteltrace.SpanFromContext(b.ctx).AddEvent("dataset tsdb index lookup complete")

	return nil
}

func (b *blockContext) newQueryContext(ds *metastorev1.Dataset) *queryContext {
	q := &queryContext{blockContext: b, ds: block.NewDataset(ds, b.obj, block.WithDatasetOpenObserver(b.recordDatasetOpen))}
	q.grp, q.ctx = errgroup.WithContext(b.ctx)
	return q
}

func (b *blockContext) recordDatasetOpen(stage block.DatasetOpenStage, d time.Duration) {
	switch stage {
	case block.DatasetOpenObject:
		b.timings.objectOpenNS.Add(d.Nanoseconds())
	case block.DatasetOpenPrefetch:
		b.timings.prefetchNS.Add(d.Nanoseconds())
	case block.DatasetOpenProfiles:
		b.timings.profilesOpenNS.Add(d.Nanoseconds())
	case block.DatasetOpenTSDB:
		b.timings.tsdbOpenNS.Add(d.Nanoseconds())
	case block.DatasetOpenSymbols:
		b.timings.symbolsOpenNS.Add(d.Nanoseconds())
	case block.DatasetOpenDatasetIndex:
		// Dataset-index opening is part of lookupDatasets, not query dataset open.
	}
}

type queryContext struct {
	*blockContext
	ctx context.Context
	grp *errgroup.Group
	ds  *block.Dataset
}

func (q *queryContext) execute(query *queryv1.Query) error {
	q.timings.queries.Add(1)
	var span *tracing.Span
	span, q.ctx = tracing.StartSpanFromContext(q.ctx, "executeQuery."+util.ToCamel(query.QueryType.String()))
	defer span.Finish()
	handle, err := getQueryHandler(query.QueryType)
	if err != nil {
		return err
	}

	openStart := time.Now()
	err = q.ds.Open(q.ctx, q.sections()...)
	openDuration := time.Since(openStart)
	q.timings.openNS.Add(openDuration.Nanoseconds())
	maxAtomicDuration(&q.timings.openMaxNS, openDuration)
	if err != nil {
		if q.obj.IsNotExists(err) {
			level.Warn(q.log).Log("msg", "object not found", "err", err)
			return nil
		}
		return fmt.Errorf("failed to initialize query context: %w", err)
	}
	defer func() {
		closeStart := time.Now()
		_ = q.ds.CloseWithError(err)
		q.timings.closeNS.Add(time.Since(closeStart).Nanoseconds())
	}()

	queryStart := time.Now()
	r, err := handle(q, query)
	queryDuration := time.Since(queryStart)
	q.timings.queryNS.Add(queryDuration.Nanoseconds())
	maxAtomicDuration(&q.timings.queryMaxNS, queryDuration)
	if err != nil {
		return err
	}
	if r != nil {
		r.ReportType = QueryReportType(query.QueryType)
		aggregateStart := time.Now()
		reportErr := q.agg.aggregateReport(r)
		q.timings.aggregateNS.Add(time.Since(aggregateStart).Nanoseconds())
		return reportErr
	}

	return nil
}

func (q *queryContext) sections() []block.Section {
	sections := make(map[block.Section]struct{}, 3)
	for _, qt := range q.req.src.Query {
		for _, s := range queryDependencies[qt.QueryType] {
			sections[s] = struct{}{}
		}
	}
	unique := make([]block.Section, 0, len(sections))
	for s := range sections {
		unique = append(unique, s)
	}
	return unique
}
