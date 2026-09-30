package symdb

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTreeStatsConcurrentSnapshot(t *testing.T) {
	const partitions = 32
	stats := new(TreeStats)
	var wg sync.WaitGroup
	for i := 1; i <= partitions; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			stats.recordFetch(time.Duration(n) * time.Millisecond)
			stats.recordFetchWait(time.Duration(n) * time.Microsecond)
			stats.recordBuild(time.Duration(n) * time.Microsecond)
		}(i)
	}
	wg.Wait()
	got := stats.Snapshot()
	require.Equal(t, time.Duration(partitions)*time.Millisecond, got.FetchMax)
	require.Equal(t, time.Duration(partitions)*time.Microsecond, got.FetchWaitMax)
	require.Equal(t, time.Duration(partitions)*time.Microsecond, got.BuildMax)
	require.Equal(t, time.Duration(partitions*(partitions+1)/2)*time.Millisecond, got.Fetch)
}
