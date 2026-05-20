package compactor

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestConfig_ValidateRejectsBadValues verifies that each new coordinator-loop
// knob is validated when Enabled is true. The pre-existing
// MaxRunningCompactionTasks and Scheduler.Endpoint validations are exercised
// in compactor_test.go.
func TestConfig_ValidateRejectsBadValues(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr error
	}{
		{"polling interval zero", func(c *Config) { c.PollingInterval = 0 }, errInvalidPollingInterval},
		{"polling interval negative", func(c *Config) { c.PollingInterval = -1 }, errInvalidPollingInterval},
		{"index merge ttl zero", func(c *Config) { c.IndexMergeTaskTTL = 0 }, errInvalidIndexMergeTaskTTL},
		{"toc consolidate timeout zero", func(c *Config) { c.ToCConsolidateTimeout = 0 }, errInvalidToCConsolidateTimeout},
		{"max runs zero", func(c *Config) { c.MaxRunsPerTask = 0 }, errInvalidMaxRunsPerTask},
		{"max runs negative", func(c *Config) { c.MaxRunsPerTask = -1 }, errInvalidMaxRunsPerTask},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			cfg.RegisterFlags(flag.NewFlagSet("test", flag.ContinueOnError))
			cfg.Enabled = true                       // validation is gated on Enabled
			cfg.Scheduler.Endpoint = defaultEndpoint // satisfy existing scheduler-endpoint check
			tc.mutate(&cfg)
			err := cfg.Validate()
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestConfig_ValidateHappyPath_WithNewKnobs(t *testing.T) {
	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("test", flag.ContinueOnError))
	cfg.Enabled = true
	require.NoError(t, cfg.Validate())
}

func TestConfig_Validate_ConsolidationSLO(t *testing.T) {
	base := Config{
		Enabled:                   true,
		MaxRunningCompactionTasks: 1,
		PollingInterval:           5 * time.Minute,
		MaxRunsPerTask:            8,
		IndexMergeTaskTTL:         10 * time.Minute,
		ToCConsolidateTimeout:     30 * time.Second,
		ConsolidationSLO:          3 * time.Hour,
		PlanVersion:               1,
		Scheduler:                 SchedulerConfig{Endpoint: "/x"},
	}

	t.Run("valid 3h passes", func(t *testing.T) {
		require.NoError(t, base.Validate())
	})

	t.Run("zero fails when enabled", func(t *testing.T) {
		c := base
		c.ConsolidationSLO = 0
		require.ErrorIs(t, c.Validate(), errInvalidConsolidationSLO)
	})

	t.Run("negative fails when enabled", func(t *testing.T) {
		c := base
		c.ConsolidationSLO = -time.Second
		require.ErrorIs(t, c.Validate(), errInvalidConsolidationSLO)
	})

	t.Run("zero passes when disabled", func(t *testing.T) {
		c := base
		c.Enabled = false
		c.ConsolidationSLO = 0
		require.NoError(t, c.Validate())
	})

	t.Run("default after RegisterFlags is 3h", func(t *testing.T) {
		var c Config
		f := flag.NewFlagSet("test", flag.PanicOnError)
		c.RegisterFlags(f)
		require.NoError(t, f.Parse(nil))
		require.Equal(t, 3*time.Hour, c.ConsolidationSLO)
	})
}
