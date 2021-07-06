package retry

import "time"

// Operation represents a function to retry
type Operation func() error

// Represents an error at which the retrier should stop retrying
type irrecoverableError struct {
	error
}

// StopRetry function to call when an irrecoverable error arises
func StopRetry(err error) error {
	return irrecoverableError{error: err}
}

// Config retrier's configuration
type Config struct {
	InitialBackoff time.Duration
	Retries        int
	MaxBackoff     time.Duration
	Exp            int
}

// DefaultConfig default configuration for the retrier. Can be used to just override interesting values
var DefaultConfig = Config{
	InitialBackoff: 500 * time.Millisecond,
	Retries:        3,
	MaxBackoff:     5000 * time.Millisecond,
	Exp:            2,
}

// getNextBackoffPeriod calculates the next backoff period to wait
func getNextBackoffPeriod(attempt int, cfg Config) time.Duration {
	next := time.Duration(cfg.Exp*attempt) * cfg.InitialBackoff
	if next > cfg.MaxBackoff {
		next = cfg.MaxBackoff
	}
	return next
}

// Function retries an operation according to the provided config
func Function(fn Operation, config Config) error {
	var (
		err  error
		wait = config.InitialBackoff
	)

	for i := 0; i < config.Retries; i++ {
		err = fn()
		if err == nil {
			return nil
		}

		if ierr, ok := err.(irrecoverableError); ok {
			return ierr.error
		}

		time.Sleep(wait)
		wait = getNextBackoffPeriod(i, config)
	}

	return err
}
