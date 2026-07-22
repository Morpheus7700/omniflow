package api

import (
	"encoding/json"
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}
