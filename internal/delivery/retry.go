package delivery

import (
	"errors"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultBaseRetryDelay = 5 * time.Second
	DefaultMaxRetryDelay  = 15 * time.Minute
	DefaultMaxAttempts    = 8
)

const maxDurationSeconds = int64(
	time.Duration(1<<63-1) / time.Second,
)

var ErrInvalidRetryPolicy = errors.New("invalid retry policy")

func RandomJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}

	return time.Duration(
		rand.Int63n(int64(max) + 1),
	)
}

type RetryPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration

	Jitter func(time.Duration) time.Duration
}

func NewRetryPolicy(
	baseDelay time.Duration,
	maxDelay time.Duration,
	jitter func(time.Duration) time.Duration,
) (RetryPolicy, error) {
	if baseDelay <= 0 {
		return RetryPolicy{}, ErrInvalidRetryPolicy
	}

	if maxDelay < baseDelay {
		return RetryPolicy{}, ErrInvalidRetryPolicy
	}

	if jitter == nil {
		return RetryPolicy{}, ErrInvalidRetryPolicy
	}

	return RetryPolicy{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
		Jitter:    jitter,
	}, nil
}

func (p RetryPolicy) Backoff(attemptNumber int) time.Duration {
	if attemptNumber < 1 {
		attemptNumber = 1
	}

	delay := p.BaseDelay

	for range attemptNumber - 1 {
		if delay*2 > p.MaxDelay {
			delay = p.MaxDelay
			break
		}

		delay *= 2
	}

	if delay > p.MaxDelay {
		delay = p.MaxDelay
	}

	jitterLimit := delay / 4

	if remaining := p.MaxDelay - delay; jitterLimit > remaining {
		jitterLimit = remaining
	}

	if jitterLimit <= 0 {
		return delay
	}

	jitter := p.Jitter(jitterLimit)

	if jitter < 0 {
		jitter = 0
	}

	if jitter > jitterLimit {
		jitter = jitterLimit
	}

	return delay + jitter
}

func ParseRetryAfter(
	value string,
	now time.Time,
) (time.Duration, bool) {
	value = strings.TrimSpace(value)

	if value == "" {
		return 0, false
	}

	seconds, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		if seconds < 0 {
			return 0, false
		}

		if seconds > maxDurationSeconds {
			return 0, false
		}

		return time.Duration(seconds) * time.Second, true
	}

	retryTime, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}

	delay := retryTime.Sub(now)

	if delay < 0 {
		return 0, true
	}

	return delay, true
}

func (p RetryPolicy) NextDelay(
	attemptNumber int,
	statusCode int,
	retryAfter string,
	now time.Time,
) time.Duration {
	delay := p.Backoff(attemptNumber)

	if statusCode != http.StatusTooManyRequests && statusCode != http.StatusServiceUnavailable {
		return delay
	}

	retryAfterDelay, ok := ParseRetryAfter(
		retryAfter,
		now,
	)
	if !ok {
		return delay
	}

	if retryAfterDelay > delay {
		delay = retryAfterDelay
	}

	if delay > p.MaxDelay {
		delay = p.MaxDelay
	}

	return delay
}
