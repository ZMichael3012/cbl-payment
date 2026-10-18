package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-payment/internal/domain"
)

func TestFeeTotal(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		fee      int64
		gateway  int64
		network  int64
		fx       int64
		received int64
		want     int64
		wantErr  bool
	}{
		"components only":      {gateway: 100, network: 200, fx: 300, received: 100_000, want: 600},
		"aggregate only":       {fee: 500, received: 100_000, want: 500},
		"components with fee":  {fee: 600, gateway: 100, network: 200, fx: 300, received: 100_000, want: 600},
		"fee mismatch":         {fee: 500, gateway: 100, network: 200, fx: 300, received: 100_000, wantErr: true},
		"negative component":   {gateway: -1, received: 100_000, wantErr: true},
		"negative fee":         {fee: -1, received: 100_000, wantErr: true},
		"fee above received":   {fee: 100_001, received: 100_000, wantErr: true},
		"split above received": {gateway: 60_000, network: 60_000, received: 100_000, wantErr: true},
		"zero event":           {received: 100_000, want: 0},
		"exact received total": {fee: 100_000, received: 100_000, want: 100_000},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			total, err := domain.FeeTotal(tc.fee, tc.gateway, tc.network, tc.fx, tc.received)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, total)
		})
	}
}
