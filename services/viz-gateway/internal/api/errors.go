package api

import (
	"encoding/json"
	"net/http"
)

// errorBody is the one error shape every JSON endpoint returns. The replay endpoint answered
// text/plain via http.Error while the probes answered JSON, so a client had to sniff the
// Content-Type to know how to read a failure. `code` is stable and machine-readable; `message`
// is for a human and never carries a database error or a stack.
type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	var body errorBody
	body.Error.Code = code
	body.Error.Message = message
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body) // the status is already on the wire; nothing to do on failure
}
