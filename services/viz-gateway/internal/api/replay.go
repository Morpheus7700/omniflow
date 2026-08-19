package api

import (
	"encoding/json"
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
	fromStr := r.URL.Query().Get("from_seq")
	toStr := r.URL.Query().Get("to_seq")

	fromSeq, _ := strconv.ParseUint(fromStr, 10, 64)
	toSeq, _ := strconv.ParseUint(toStr, 10, 64)

	if toSeq == 0 {
		toSeq = 1<<64 - 1 // max uint64
	}

	events, err := h.repo.GetMovements(r.Context(), fromSeq, toSeq)
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
