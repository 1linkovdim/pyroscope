package frontend

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"

	querierv1 "github.com/grafana/pyroscope/api/gen/proto/go/querier/v1"
	"github.com/grafana/pyroscope/v2/pkg/model"
	"github.com/grafana/pyroscope/v2/pkg/test/mocks/mockfrontend"
	"github.com/grafana/pyroscope/v2/pkg/util/connectgrpc"
	"github.com/grafana/pyroscope/v2/pkg/util/httpgrpc"
)

// wideTreeRoundTripper answers SelectMergeStacktraces sub-requests with a tree
// of `leaves` sibling leaves under the root, returned untruncated (maxNodes=-1)
// regardless of the per-query maxNodes. This mimics the real fan-out/merge: each
// shard truncates to the limit, but their union is much wider, so the final
// merged tree the frontend assembles is larger than the node limit.
func wideTreeRoundTripper(leaves int) *mockRoundTripper {
	return &mockRoundTripper{callback: func(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
		return connectgrpc.HandleUnary[querierv1.SelectMergeStacktracesRequest, querierv1.SelectMergeStacktracesResponse](ctx, req, func(ctx context.Context, req *connect.Request[querierv1.SelectMergeStacktracesRequest]) (*connect.Response[querierv1.SelectMergeStacktracesResponse], error) {
			s := new(model.FunctionNameTree)
			// Distinct, descending weights so truncation to the node limit pushes
			// the long tail of small leaves into an "other" bucket.
			for i := 0; i < leaves; i++ {
				s.InsertStack(int64(leaves-i), "root", model.FunctionName("leaf"+string(rune('a'+i))))
			}
			return connect.NewResponse(&querierv1.SelectMergeStacktracesResponse{
				Flamegraph: model.NewFlameGraph(s, -1),
			}), nil
		})
	}}
}

func flameGraphNodeCount(fg *querierv1.FlameGraph) int {
	var n int
	for _, l := range fg.GetLevels() {
		n += len(l.GetValues()) / 4
	}
	return n
}

func flameGraphHasName(fg *querierv1.FlameGraph, name string) bool {
	for _, n := range fg.GetNames() {
		if n == name {
			return true
		}
	}
	return false
}

// Test_Frontend_SelectMergeStacktraces_MaxNodesDefault is a regression test for
// the bug where an omitted maxNodes disabled truncation of the final merged
// flame graph: SelectMergeStacktraces built the response with
// c.Msg.GetMaxNodes() (0 when omitted) instead of the validated maxNodes, so
// the configured default (and the max ceiling) were bypassed and the full tree
// was returned. The default must be applied when the client omits maxNodes.
func Test_Frontend_SelectMergeStacktraces_MaxNodesDefault(t *testing.T) {
	const (
		defaultNodes = 4
		maxNodes     = 100_000
		leaves       = 32
	)

	limits := mockfrontend.NewMockLimits(t)
	limits.On("MaxFlameGraphNodesDefault", "test").Return(defaultNodes).Maybe()
	limits.On("MaxFlameGraphNodesMax", "test").Return(maxNodes).Maybe()
	limits.On("MaxQueryLookback", "test").Return(time.Hour * 24).Maybe()
	limits.On("MaxQueryLength", "test").Return(time.Hour).Maybe()
	limits.On("MaxQueryParallelism", "test").Return(100).Maybe()
	limits.On("QuerySplitDuration", "test").Return(time.Hour).Maybe()

	frontend := Frontend{limits: limits, GRPCRoundTripper: wideTreeRoundTripper(leaves)}
	ctx := user.InjectOrgID(context.Background(), "test")
	now := time.Now().UnixMilli()
	profileType := "memory:inuse_space:bytes:space:byte"

	newReq := func(maxNodes *int64) *connect.Request[querierv1.SelectMergeStacktracesRequest] {
		return connect.NewRequest(&querierv1.SelectMergeStacktracesRequest{
			ProfileTypeID: profileType,
			LabelSelector: "{}",
			Start:         now,
			End:           now + 1000,
			MaxNodes:      maxNodes,
		})
	}

	t.Run("omitted maxNodes applies the configured default", func(t *testing.T) {
		resp, err := frontend.SelectMergeStacktraces(ctx, newReq(nil))
		require.NoError(t, err)
		fg := resp.Msg.GetFlamegraph()
		require.NotNil(t, fg)
		// Truncation to the default must collapse the surplus leaves into "other"
		// and bound the node count, instead of returning the full wide tree.
		require.True(t, flameGraphHasName(fg, "other"),
			"expected an 'other' node from default truncation; got names=%v", fg.GetNames())
		require.LessOrEqual(t, flameGraphNodeCount(fg), defaultNodes+2,
			"node count must be bounded by the default max-nodes")
		require.Less(t, flameGraphNodeCount(fg), leaves,
			"omitted maxNodes must not return the full untruncated tree")
	})

	t.Run("explicit maxNodes above the max is rejected", func(t *testing.T) {
		n := int64(maxNodes + 1)
		_, err := frontend.SelectMergeStacktraces(ctx, newReq(&n))
		require.Error(t, err)
		require.ErrorContains(t, err, "max flamegraph nodes")
	})
}
