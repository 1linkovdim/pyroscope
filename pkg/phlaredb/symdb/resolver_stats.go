package symdb

import (
	"sync/atomic"
	"time"
)

// TreeStats records coarse resolver work for one tree query. Partition work can
// run concurrently and fetches can overlap profile scanning, so summed stage
// durations are not additive with the Tree call's wall time.
type TreeStats struct {
	partitions     atomic.Int64
	uniqueStackIDs atomic.Int64
	fetchNS        atomic.Int64
	fetchMaxNS     atomic.Int64
	fetchWaitNS    atomic.Int64
	fetchWaitMaxNS atomic.Int64
	buildNS        atomic.Int64
	buildMaxNS     atomic.Int64
	mergeWaitNS    atomic.Int64
	mergeNS        atomic.Int64
}

type TreeStatsSnapshot struct {
	Partitions     int64
	UniqueStackIDs int64
	Fetch          time.Duration
	FetchMax       time.Duration
	FetchWait      time.Duration
	FetchWaitMax   time.Duration
	Build          time.Duration
	BuildMax       time.Duration
	MergeWait      time.Duration
	Merge          time.Duration
}

func (s *TreeStats) Snapshot() TreeStatsSnapshot {
	return TreeStatsSnapshot{
		Partitions:     s.partitions.Load(),
		UniqueStackIDs: s.uniqueStackIDs.Load(),
		Fetch:          time.Duration(s.fetchNS.Load()),
		FetchMax:       time.Duration(s.fetchMaxNS.Load()),
		FetchWait:      time.Duration(s.fetchWaitNS.Load()),
		FetchWaitMax:   time.Duration(s.fetchWaitMaxNS.Load()),
		Build:          time.Duration(s.buildNS.Load()),
		BuildMax:       time.Duration(s.buildMaxNS.Load()),
		MergeWait:      time.Duration(s.mergeWaitNS.Load()),
		Merge:          time.Duration(s.mergeNS.Load()),
	}
}

func (s *TreeStats) recordFetch(d time.Duration) {
	s.fetchNS.Add(d.Nanoseconds())
	maxDuration(&s.fetchMaxNS, d)
}

func (s *TreeStats) recordFetchWait(d time.Duration) {
	s.fetchWaitNS.Add(d.Nanoseconds())
	maxDuration(&s.fetchWaitMaxNS, d)
}

func (s *TreeStats) recordBuild(d time.Duration) {
	s.buildNS.Add(d.Nanoseconds())
	maxDuration(&s.buildMaxNS, d)
}

func maxDuration(dst *atomic.Int64, d time.Duration) {
	n := d.Nanoseconds()
	for old := dst.Load(); n > old; old = dst.Load() {
		if dst.CompareAndSwap(old, n) {
			return
		}
	}
}
