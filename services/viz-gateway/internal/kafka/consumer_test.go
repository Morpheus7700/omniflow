package kafka

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"omniflow/services/viz-gateway/internal/api"

	"github.com/twmb/franz-go/pkg/kgo"
)

// These tests pin the changefeed decode contract — the source of two prior production bugs:
//
//  1. reading projection columns from the TOP LEVEL of the payload when CockroachDB actually wraps
//     every row in an `after` envelope (which silently produced empty aggregate ids), and
//  2. losing precision on sequence_engine_key. CRDB emits INT8 columns as bare JSON numbers, and a
//     default encoding/json decode lands them in a float64 — which cannot represent a 19-digit HLC
//     key. The decoder therefore uses UseNumber() and the key is carried as a STRING end to end.
//
// Both are invisible to the happy-path E2E (it asserts the key arrives, and a corrupted key still
// arrives), so they are pinned here instead.

// realistic 19-digit HLC key: exactly the value the seeder printed in CI as SEED_SEQUENCE_ENGINE_KEY.
const hlcKey = "1784933243325766766"

// ─── SSE tap ─────────────────────────────────────────────────────────────────────────────────────

// sseTap runs a real SSEBroker with a real HTTP SSE client attached, so assertions are made against
// the bytes the frontend actually receives (id/event/data frames), not an internal mock.
type sseTap struct {
	broker *api.SSEBroker
	frames chan map[string]string
}

func newSSETap(t *testing.T) *sseTap {
	t.Helper()

	broker := api.NewSSEBroker()
	ctx, cancel := context.WithCancel(context.Background())
	go broker.Run(ctx)

	srv := httptest.NewServer(http.HandlerFunc(broker.StreamHandler))
	frames := make(chan map[string]string, 64)

	// The request must be individually cancellable. An SSE handler never returns on its own, and
	// httptest's Close() blocks until every in-flight request finishes — so tearing down the server
	// while this stream is open would deadlock.
	reqCtx, reqCancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("build SSE request: %v", err)
	}

	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()

		sc := bufio.NewScanner(resp.Body)
		frame := map[string]string{}
		for sc.Scan() {
			line := sc.Text()
			if line == "" { // blank line terminates an SSE frame
				if len(frame) > 0 {
					select {
					case frames <- frame:
					default:
					}
					frame = map[string]string{}
				}
				continue
			}
			if i := strings.Index(line, ": "); i > 0 {
				frame[line[:i]] = line[i+2:]
			}
		}
	}()

	// Teardown order matters: cancel the request so the handler can return, then close the server,
	// and only then stop Run — the handler's deferred deregistration sends on a channel that only a
	// live Run is reading.
	t.Cleanup(func() {
		reqCancel()
		srv.Close()
		cancel()
	})

	tap := &sseTap{broker: broker, frames: frames}
	tap.awaitRegistration(t)
	return tap
}

// awaitRegistration blocks until the HTTP client is actually registered with the broker. The broker
// drops broadcasts that arrive before a client connects, so without this barrier a "nothing was
// broadcast" assertion could pass for the wrong reason.
func (t2 *sseTap) awaitRegistration(t *testing.T) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for {
		t2.broker.Broadcast(api.SSEEvent{ID: "sync", Type: api.EventWatermark, Data: map[string]string{}})
		select {
		case f := <-t2.frames:
			if f["id"] == "sync" {
				t2.drain()
				return
			}
		case <-tick.C:
		case <-deadline:
			t.Fatal("SSE client never registered with the broker")
		}
	}
}

func (t2 *sseTap) drain() {
	for {
		select {
		case <-t2.frames:
		default:
			return
		}
	}
}

// expectFrame waits for one SSE frame.
func (t2 *sseTap) expectFrame(t *testing.T) map[string]string {
	t.Helper()
	select {
	case f := <-t2.frames:
		return f
	case <-time.After(3 * time.Second):
		t.Fatal("expected an SSE frame, got none")
		return nil
	}
}

// expectSilence asserts the handler dropped the record instead of projecting it.
func (t2 *sseTap) expectSilence(t *testing.T) {
	t.Helper()
	select {
	case f := <-t2.frames:
		t.Fatalf("expected no SSE frame, got %v", f)
	case <-time.After(250 * time.Millisecond):
	}
}

func newTestConsumer(tap *sseTap) *Consumer {
	// client is only touched by Start/MarkCommitRecords, never by the handlers under test.
	return &Consumer{client: nil, broker: tap.broker}
}

func record(topic, payload string) *kgo.Record {
	return &kgo.Record{Topic: topic, Value: []byte(payload)}
}

func dataField(t *testing.T, frame map[string]string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(frame["data"]), &out); err != nil {
		t.Fatalf("data is not JSON: %v (raw=%q)", err, frame["data"])
	}
	return out
}

// ─── p2p.completed.v1 ────────────────────────────────────────────────────────────────────────────

func TestHandleP2PCompletedUnwrapsAfterEnvelope(t *testing.T) {
	tap := newSSETap(t)
	c := newTestConsumer(tap)

	// The shape CockroachDB's orchestrator_outbox changefeed actually emits. Note
	// sequence_engine_key is an UNQUOTED number — INT8 columns are not strings on the wire.
	c.handleP2PCompleted(record("omniflow.p2p.completed.v1", `{
	  "after": {
	    "aggregate_id": "5f05b9d0-e173-43cc-b52a-627671f55508",
	    "event_type": "NodeTransition",
	    "sequence_engine_key": `+hlcKey+`,
	    "trace_parent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	    "occurred_at": "2026-07-24T22:47:23.123456789Z"
	  },
	  "updated": "1784933243325766766.0000000000",
	  "mvcc_timestamp": "1784933243325766766.0000000000"
	}`))

	frame := tap.expectFrame(t)
	if frame["event"] != string(api.EventMovement) {
		t.Errorf("event = %q, want %q", frame["event"], api.EventMovement)
	}
	// The SSE id IS the HLC key — the frontend orders the timeline on it.
	if frame["id"] != hlcKey {
		t.Errorf("SSE id = %q, want %q", frame["id"], hlcKey)
	}

	data := dataField(t, frame)
	if got := data["sequence_engine_key"]; got != hlcKey {
		t.Errorf("sequence_engine_key = %v (%T), want the exact string %q — precision was lost", got, got, hlcKey)
	}
	if got := data["aggregate_id"]; got != "5f05b9d0-e173-43cc-b52a-627671f55508" {
		t.Errorf("aggregate_id = %v, want the value from inside after{}", got)
	}
	if got := data["status"]; got != "NodeTransition" {
		t.Errorf("status = %v, want NodeTransition", got)
	}
	if got := data["trace_parent"]; got != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Errorf("trace_parent = %v, want the W3C traceparent from after{}", got)
	}
}

// The original bug: projection columns read from the top level. Such a payload must be dropped,
// never projected with empty/zero fields.
func TestHandleP2PCompletedIgnoresTopLevelFields(t *testing.T) {
	tap := newSSETap(t)
	c := newTestConsumer(tap)

	c.handleP2PCompleted(record("omniflow.p2p.completed.v1", `{
	  "aggregate_id": "5f05b9d0-e173-43cc-b52a-627671f55508",
	  "event_type": "NodeTransition",
	  "sequence_engine_key": `+hlcKey+`
	}`))

	tap.expectSilence(t)
}

func TestHandleP2PCompletedDropsNonProjectableRecords(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"tombstone / delete", `{"after": null}`},
		{"missing sequence key", `{"after": {"aggregate_id": "abc", "event_type": "NodeTransition"}}`},
		{"empty sequence key", `{"after": {"aggregate_id": "abc", "sequence_engine_key": ""}}`},
		{"malformed json", `{"after": `},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tap := newSSETap(t)
			newTestConsumer(tap).handleP2PCompleted(record("omniflow.p2p.completed.v1", tc.payload))
			tap.expectSilence(t)
		})
	}
}

func TestHandleP2PCompletedEmitsWatermark(t *testing.T) {
	tap := newSSETap(t)
	c := newTestConsumer(tap)

	c.handleP2PCompleted(record("omniflow.p2p.completed.v1",
		`{"resolved": "1784933243325766766.0000000000"}`))

	frame := tap.expectFrame(t)
	if frame["event"] != string(api.EventWatermark) {
		t.Errorf("event = %q, want %q", frame["event"], api.EventWatermark)
	}
	if frame["id"] != "1784933243325766766.0000000000" {
		t.Errorf("id = %q, want the resolved timestamp", frame["id"])
	}
	if got := dataField(t, frame)["resolved_ts"]; got != "1784933243325766766.0000000000" {
		t.Errorf("resolved_ts = %v, want the resolved timestamp", got)
	}
}

// ─── inventory fact topics ───────────────────────────────────────────────────────────────────────

func TestHandleInventoryMovementProjectsFacts(t *testing.T) {
	tap := newSSETap(t)
	c := newTestConsumer(tap)

	c.handleInventoryMovement(record("omniflow.inventory.fact_inventory_movement", `{
	  "after": {
	    "event_id": "evt-42",
	    "sequence_engine_key": `+hlcKey+`,
	    "trace_parent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	    "occurred_at": "2026-07-24T22:47:23.123456789Z",
	    "fifo_total_value": 60.00
	  }
	}`))

	frame := tap.expectFrame(t)
	if frame["id"] != hlcKey {
		t.Errorf("SSE id = %q, want %q", frame["id"], hlcKey)
	}

	data := dataField(t, frame)
	if got := data["aggregate_id"]; got != "evt-42" {
		t.Errorf("aggregate_id = %v, want evt-42", got)
	}
	metrics, ok := data["metrics"].(map[string]any)
	if !ok {
		t.Fatalf("metrics missing or wrong shape: %v", data["metrics"])
	}
	if got := metrics["value"]; got != 60.0 {
		t.Errorf("metrics.value = %v, want 60", got)
	}
}

// ─── pure decode helpers ─────────────────────────────────────────────────────────────────────────

// The precision guarantee in isolation. A plain json.Unmarshal into interface{} rounds this key to
// 1784933243325766656; UseNumber must keep all 19 digits.
func TestParsePayloadPreservesInt64Precision(t *testing.T) {
	payload, err := parsePayload([]byte(`{"after":{"sequence_engine_key":` + hlcKey + `}}`))
	if err != nil {
		t.Fatalf("parsePayload: %v", err)
	}
	after := payload["after"].(map[string]any)

	num, ok := after["sequence_engine_key"].(json.Number)
	if !ok {
		t.Fatalf("sequence_engine_key decoded as %T, want json.Number (UseNumber is not enabled)", after["sequence_engine_key"])
	}
	if num.String() != hlcKey {
		t.Errorf("sequence_engine_key = %s, want %s", num.String(), hlcKey)
	}

	// Pin the failure mode this guards against: a default decode really does corrupt this key.
	// If Go ever stopped rounding here, the guarantee above would be untested and we want to know.
	var lossy map[string]any
	if err := json.Unmarshal([]byte(`{"k":`+hlcKey+`}`), &lossy); err != nil {
		t.Fatal(err)
	}
	f, ok := lossy["k"].(float64)
	if !ok {
		t.Fatalf("default decode gave %T, expected float64", lossy["k"])
	}
	if strconv.FormatInt(int64(f), 10) == hlcKey {
		t.Error("float64 preserved the HLC key exactly; this test no longer guards anything")
	}
}

func TestExtractString(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"string passes through", "abc", "abc"},
		{"json.Number keeps every digit", json.Number(hlcKey), hlcKey},
		{"nil / absent key", nil, ""},
		{"float64 is rejected rather than silently rounded", float64(1.78e18), ""},
		{"bool", true, ""},
		{"nested object", map[string]any{"a": 1}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractString(tc.in); got != tc.want {
				t.Errorf("extractString(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseFloat(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want float64
	}{
		{"json.Number", json.Number("60.00"), 60},
		{"negative", json.Number("-12.5"), -12.5},
		{"string is not coerced", "60.00", 0},
		{"nil", nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseFloat(tc.in); got != tc.want {
				t.Errorf("parseFloat(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
