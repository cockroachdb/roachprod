package monitor

import (
	"context"
	"time"
)

type Options struct {
	Targets []string
	CheckFrequency time.Duration
}

type Monitor struct {
	opts Options
}

func MakeSQLUptimeMonitor(opts Options) *Monitor {
	return &Monitor{
		opts: opts,
	}
}

func (m *Monitor) Run(ctx context.Context) error {
	// TODO
	return nil
}
