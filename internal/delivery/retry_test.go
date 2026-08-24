package delivery

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestRetryPolicyBackoff(t *testing.T) {
	policy, err := NewRetryPolicy(
		5*time.Second,
		15*time.Minute,
		func(time.Duration) time.Duration {
			return 0
		},
	)
	if err != nil {
		t.Fatalf("NewRetryPolicy() error = %v", err)
	}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 5 * time.Second},
		{attempt: 2, want: 10 * time.Second},
		{attempt: 3, want: 20 * time.Second},
		{attempt: 4, want: 40 * time.Second},
		{attempt: 5, want: 80 * time.Second},
		{attempt: 6, want: 160 * time.Second},
		{attempt: 7, want: 320 * time.Second},
		{attempt: 8, want: 640 * time.Second},
		{attempt: 20, want: 15 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(
			fmt.Sprintf("attempt_%d", tt.attempt),
			func(t *testing.T) {
				got := policy.Backoff(tt.attempt)

				if got != tt.want {
					t.Errorf(
						"Backoff(%d) = %v, want %v",
						tt.attempt,
						got,
						tt.want,
					)
				}
			},
		)
	}
}

func TestRetryPolicyBackoffAddsBoundedJitter(
	t *testing.T,
) {
	policy, err := NewRetryPolicy(
		20*time.Second,
		time.Minute,
		func(max time.Duration) time.Duration {
			return max
		},
	)
	if err != nil {
		t.Fatalf("NewRetryPolicy() error = %v", err)
	}

	got := policy.Backoff(1)

	// Base 20s + maximum 25% jitter of 5s.
	want := 25 * time.Second

	if got != want {
		t.Errorf("Backoff(1) = %v, want %v", got, want)
	}
}

func TestRetryPolicyClampsExcessiveJitter(t *testing.T) {
	policy, err := NewRetryPolicy(
		20*time.Second,
		time.Minute,
		func(time.Duration) time.Duration {
			return time.Hour
		},
	)
	if err != nil {
		t.Fatalf("NewRetryPolicy() error = %v", err)
	}

	got := policy.Backoff(1)

	if got != 25*time.Second {
		t.Errorf(
			"Backoff(1) = %v, want 25s",
			got,
		)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(
		2026,
		time.August,
		23,
		12,
		0,
		0,
		0,
		time.UTC,
	)

	tests := []struct {
		name  string
		value string
		want  time.Duration
		ok    bool
	}{
		{
			name:  "delta seconds",
			value: "120",
			want:  2 * time.Minute,
			ok:    true,
		},
		{
			name: "HTTP date",
			value: now.
				Add(3 * time.Minute).
				Format(http.TimeFormat),
			want: 3 * time.Minute,
			ok:   true,
		},
		{
			name: "past date",
			value: now.
				Add(-time.Minute).
				Format(http.TimeFormat),
			want: 0,
			ok:   true,
		},
		{
			name:  "invalid",
			value: "later",
			ok:    false,
		},
		{
			name:  "negative seconds",
			value: "-5",
			ok:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseRetryAfter(
				tt.value,
				now,
			)

			if ok != tt.ok {
				t.Fatalf(
					"ok = %v, want %v",
					ok,
					tt.ok,
				)
			}

			if got != tt.want {
				t.Errorf(
					"delay = %v, want %v",
					got,
					tt.want,
				)
			}
		})
	}
}

func TestRetryPolicyNextDelay(t *testing.T) {
	policy, err := NewRetryPolicy(
		5*time.Second,
		15*time.Minute,
		func(time.Duration) time.Duration {
			return 0
		},
	)
	if err != nil {
		t.Fatalf("NewRetryPolicy() error = %v", err)
	}

	now := time.Now().UTC()

	tests := []struct {
		name       string
		attempt    int
		status     int
		retryAfter string
		want       time.Duration
	}{
		{
			name:    "normal exponential retry",
			attempt: 3,
			status:  500,
			want:    20 * time.Second,
		},
		{
			name:       "429 honors larger Retry-After",
			attempt:    1,
			status:     429,
			retryAfter: "120",
			want:       2 * time.Minute,
		},
		{
			name:       "503 keeps larger backoff",
			attempt:    4,
			status:     503,
			retryAfter: "10",
			want:       40 * time.Second,
		},
		{
			name:       "500 ignores Retry-After",
			attempt:    1,
			status:     500,
			retryAfter: "120",
			want:       5 * time.Second,
		},
		{
			name:       "Retry-After is capped",
			attempt:    1,
			status:     429,
			retryAfter: "99999",
			want:       15 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := policy.NextDelay(
				tt.attempt,
				tt.status,
				tt.retryAfter,
				now,
			)

			if got != tt.want {
				t.Errorf(
					"NextDelay() = %v, want %v",
					got,
					tt.want,
				)
			}
		})
	}
}
