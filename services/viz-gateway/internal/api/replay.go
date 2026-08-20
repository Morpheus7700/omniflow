package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"omniflow/services/viz-gateway/internal/repository"
)

type ReplayHandler struct {
	repo *repository.ReplayRepository
}

func NewReplayHandler(repo *repository.ReplayRepository) *ReplayHandler {
	return &ReplayHandler{repo: repo}
}

func (h *ReplayHandler) HandleReplay(w http.ResponseWriter, r *http.Request) {
	// Parse errors used to be discarded into `_`, so `?from_seq=banana` silently became 0 and
	// widened the query to everything. Bad input now says so instead of quietly returning more data
	// than was asked for — the failure mode of a validation bug that widens a range is that nobody
	// notices it working.
	fromSeq, err := parseSeq(r.URL.Query().Get("from_seq"), 0)
	if err != nil {
		http.Error(w, "from_seq must be a non-negative integer", http.StatusBadRequest)
		return
	}
	toSeq, err := parseSeq(r.URL.Query().Get("to_seq"), 0)
	if err != nil {
		http.Error(w, "to_seq must be a non-negative integer", http.StatusBadRequest)
		return
	}
	// 0 means "no upper bound"; the SQL treats $2 = 0 as open-ended, so it is passed through rather
	// than turned into max-uint64 (which does not fit the signed INT8 column the key is stored in).

	limit, err := parseLimit(r.URL.Query().Get("limit"))
	if err != nil {
		http.Error(w, "limit must be an integer between 1 and "+strconv.Itoa(maxReplayLimit), http.StatusBadRequest)
		return
	}

	events, err := h.repo.GetMovements(r.Context(), fromSeq, toSeq, limit)
	if err != nil {
		// The detail goes to the log, not to the browser. This previously returned err.Error(),
		// which hands a caller the raw pgx text — table names, column names, and the SQLSTATE.
		slog.Error("replay query failed", "from_seq", fromSeq, "to_seq", toSeq, "error", err)
		http.Error(w, "failed to load movements", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(events); err != nil {
		// Nothing can be sent to the client now — 200 and the Content-Type are already on the wire,
		// and part of the body may be too. Logging is the only honest option, and it matters: a
		// truncated JSON array reaches the dashboard as a short replay, not as an error.
		slog.Error("encoding replay response failed",
			"from_seq", fromSeq, "to_seq", toSeq, "events", len(events), "error", err)
	}
}

const (
	// defaultReplayLimit is what a caller gets when they do not ask. Chosen to fill a dashboard
	// view generously without ever being the reason this service allocates a large response.
	defaultReplayLimit = 500

	// maxReplayLimit is the ceiling a caller may request. It exists so "give me everything" is not
	// expressible: a single request should never be able to drain the projection history, whether
	// the caller is a curious dashboard or someone enumerating the table.
	maxReplayLimit = 5000
)

// parseSeq parses a sequence-key bound, returning def for an absent value.
func parseSeq(s string, def uint64) (uint64, error) {
	if s == "" {
		return def, nil
	}
	return strconv.ParseUint(s, 10, 64)
}

// parseLimit parses ?limit=, defaulting when absent and refusing anything outside 1..maxReplayLimit.
//
// An out-of-range limit is rejected rather than clamped. Clamping would answer a request for 100000
// rows with 5000 and no indication, which reads to the caller as "that is all the data there is" —
// a wrong answer is worse than an error.
func parseLimit(s string) (int, error) {
	if s == "" {
		return defaultReplayLimit, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	if n < 1 || n > maxReplayLimit {
		return 0, fmt.Errorf("limit %d out of range 1..%d", n, maxReplayLimit)
	}
	return n, nil
}
