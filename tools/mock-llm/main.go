// Command mock-llm is a deterministic stand-in for the LiteLLM/Gemini gateway, used ONLY by the
// end-to-end harness. It lets the LOCKED CommBot classification path run in CI without a real model
// or spend. It intentionally implements just enough of the OpenAI-style chat/completions contract
// that CommBot's llm.LiteLLMGateway parses (see services/commbot/.../llm/gateway.go): a POST to
// /chat/completions returning one choice whose message content is a bare intent integer.
//
// It is NOT part of the shipped system and never runs in production.
package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
)

// chatResponse mirrors the exact subset CommBot's gateway decodes. The classifier prompt instructs
// the model to output ONLY the integer intent ID; parseIntent then rejects anything non-integer or
// out of range. "1" == INTENT_INVOICE_SUBMISSION.
type chatResponse struct {
	Choices []struct {
		Message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func main() {
	addr := env("MOCK_LLM_ADDR", ":4000")
	// The canned classification. Overridable so a test can drive a different branch.
	intent := env("MOCK_LLM_INTENT", "1")

	mux := http.NewServeMux()
	mux.HandleFunc("/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		slog.Info("mock-llm classify", "remote", r.RemoteAddr)
		var resp chatResponse
		resp.Choices = make([]struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
		}, 1)
		resp.Choices[0].Message.Role = "assistant"
		resp.Choices[0].Message.Content = intent

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	slog.Info("mock-llm listening", "addr", addr, "intent", intent)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("mock-llm exited", "error", err)
		os.Exit(1)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
