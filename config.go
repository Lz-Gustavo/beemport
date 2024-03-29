package beemport

import "errors"

// ReduceInterval ...
type ReduceInterval int8

const (
	// Immediately log reduce takes place after each insertion on the log
	// structure. Not every 'Log()' call triggers an reduce, only those that
	// result in a state change (e.g. write operations).
	Immediately ReduceInterval = iota

	// Delayed log reduce executes the configured reduce algorithm only during
	// recovery, when a 'Recov()' call is invoked. This approach provides minimal
	// overhead during application's execution, but can incur in a longer recovery
	// on catastrophic fault scenarios (i.e. when all replicas fail together).
	Delayed

	// Interval log reduce acts similar to a checkpoint procedure, triggering
	// a reduce event after 'Period' commands. If a log interval is requested
	// (i.e. through 'Recov()' calls), the last reduced state is informed. If
	// no prior state is found (i.e. didnt reach 'Period' commands yet), a new
	// one is immediately executed.
	Interval
)

var (
	ErrNoFilename        = errors.New("invalid config: if persistent storage (i.e. Inmem == false), config.Fname must be provided")
	ErrNoLogPeriod       = errors.New("invalid config: if periodic reduce is set (i.e. Tick == Interval), a config.Period must be provided")
	ErrNoSecondFilename  = errors.New("invalid config: if parallel io is set (i.e. ParallelIO == true), config.SecondFname must be provided")
	ErrNoMeasureFilename = errors.New("invalid config: if latency measure is set (i.e. Measure == true), config.MeasureFname must be provided")
)

// LogConfig ...
type LogConfig struct {
	Inmem   bool
	KeepAll bool
	Sync    bool
	Tick    ReduceInterval
	Period  uint32
	Fname   string

	ParallelIO  bool
	SecondFname string

	Measure      bool
	MeasureFname string
}

// DefaultLogConfig ...
func DefaultLogConfig() *LogConfig {
	return &LogConfig{
		Inmem: true,
		Tick:  Delayed,
	}
}

// ValidateConfig ...
func (lc *LogConfig) ValidateConfig() error {
	if !lc.Inmem && lc.Fname == "" {
		return ErrNoFilename
	}
	if lc.Tick == Interval && lc.Period == 0 {
		return ErrNoLogPeriod
	}
	if lc.ParallelIO && lc.SecondFname == "" {
		return ErrNoSecondFilename
	}
	if lc.Measure && lc.MeasureFname == "" {
		return ErrNoMeasureFilename
	}
	return nil
}
