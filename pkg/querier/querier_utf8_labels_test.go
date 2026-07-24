package querier

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/pyroscope/v2/pkg/featureflags"
	"github.com/grafana/pyroscope/v2/pkg/tenant"
)

// stubLimits implements the querier Limits interface for tests.
type stubLimits struct {
	disableLabelSanitization bool
}

func (s stubLimits) QueryAnalysisSeriesEnabled(string) bool { return false }
func (s stubLimits) DisableLabelSanitization(string) bool   { return s.disableLabelSanitization }

func TestKeepUTF8LabelNames(t *testing.T) {
	withTenant := func() context.Context {
		return tenant.InjectTenantID(context.Background(), "tenant")
	}
	withCapability := func(ctx context.Context) context.Context {
		return featureflags.WithClientCapabilities(ctx, featureflags.ClientCapabilities{AllowUtf8LabelNames: true})
	}

	for _, tc := range []struct {
		name                     string
		ctx                      context.Context
		disableLabelSanitization bool
		want                     bool
	}{
		{
			name:                     "sanitization disabled preserves dotted names without capability",
			ctx:                      withTenant(),
			disableLabelSanitization: true,
			want:                     true,
		},
		{
			name:                     "sanitization enabled filters when no capability",
			ctx:                      withTenant(),
			disableLabelSanitization: false,
			want:                     false,
		},
		{
			name:                     "client capability preserves dotted names even when sanitization enabled",
			ctx:                      withCapability(withTenant()),
			disableLabelSanitization: false,
			want:                     true,
		},
		{
			name:                     "no tenant and no capability filters",
			ctx:                      context.Background(),
			disableLabelSanitization: true, // unreachable: tenant lookup fails, so this is ignored
			want:                     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := &Querier{limits: stubLimits{disableLabelSanitization: tc.disableLabelSanitization}}
			require.Equal(t, tc.want, q.keepUTF8LabelNames(tc.ctx))
		})
	}
}
