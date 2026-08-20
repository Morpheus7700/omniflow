package delivery

import (
	"context"
	"testing"
	"time"
)

type ctxKey string

// The property the whole package exists for. A SIGTERM cancels the consumer context while a message
// is mid-handoff; if that cancellation propagated, the DLQ produce and the offset commit would be
// abandoned together and the poison message would come back on restart to be re-processed from the
// top.
func TestContextSurvivesParentCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	ctx, done := Context(parent)
	defer done()

	cancel()

	select {
	case <-parent.Done():
	case <-time.After(time.Second):
		t.Fatal("parent did not cancel; the test is not exercising what it claims")
	}

	if err := ctx.Err(); err != nil {
		t.Fatalf("delivery context was cancelled with the parent: %v", err)
	}
}

// Values must still flow through, so a trace stays attached to the DLQ write.
func TestContextKeepsParentValues(t *testing.T) {
	const k ctxKey = "traceparent"
	parent := context.WithValue(context.Background(), k, "abc123")
	ctx, done := Context(parent)
	defer done()

	if got := ctx.Value(k); got != "abc123" {
		t.Fatalf("value did not survive: got %v, want abc123", got)
	}
}

// Bounded, or it is just the old unbounded call with extra steps.
func TestContextHasADeadline(t *testing.T) {
	ctx, done := Context(context.Background())
	defer done()

	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("delivery context has no deadline")
	}
	if remaining := time.Until(dl); remaining <= 0 || remaining > DefaultTimeout+time.Second {
		t.Fatalf("deadline %v is not within the expected window", remaining)
	}
}

func TestContextCancelStillWorks(t *testing.T) {
	ctx, done := Context(context.Background())
	done()
	if ctx.Err() == nil {
		t.Fatal("calling cancel did not cancel the delivery context")
	}
}

func TestTimeoutFromEnv(t *testing.T) {
	t.Run("absent uses default", func(t *testing.T) {
		t.Setenv("KAFKA_DELIVERY_TIMEOUT", "")
		if got := timeoutFromEnv(); got != DefaultTimeout {
			t.Fatalf("got %v, want %v", got, DefaultTimeout)
		}
	})
	t.Run("valid override", func(t *testing.T) {
		t.Setenv("KAFKA_DELIVERY_TIMEOUT", "3s")
		if got := timeoutFromEnv(); got != 3*time.Second {
			t.Fatalf("got %v, want 3s", got)
		}
	})
	// Falls back rather than refusing to start: taking the pipeline down over a mistyped timeout
	// that has a safe default would be the worse failure.
	for _, v := range []string{"banana", "0", "-5s", "10"} {
		t.Run("invalid falls back: "+v, func(t *testing.T) {
			t.Setenv("KAFKA_DELIVERY_TIMEOUT", v)
			if got := timeoutFromEnv(); got != DefaultTimeout {
				t.Fatalf("got %v, want the default %v", got, DefaultTimeout)
			}
		})
	}
}
