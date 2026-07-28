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
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
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
	Usage struct {
		PromptTokens     uint32 `json:"prompt_tokens"`
		CompletionTokens uint32 `json:"completion_tokens"`
	} `json:"usage"`
}

// Canned purchase orders for the drafting agent.
//
// goodPO is internally consistent: every extended_amount equals quantity x unit_price and
// total_amount equals their sum.
const goodPO = `{"vendor_id":"VENDOR-MOCK-1","currency":"USD","lines":[` +
	`{"sku":"SKU-1001","description":"Widget, 10mm","quantity":"100","unit_price":"2.5000","extended_amount":"250.0000"},` +
	`{"sku":"SKU-2002","description":"Gasket, nitrile","quantity":"40","unit_price":"1.2500","extended_amount":"50.0000"}` +
	`],"total_amount":"300.0000"}`

// badArithmeticPO is the governance test fixture: plausible, confidently formatted, and WRONG.
// The lines sum to 300.00 but it claims 30.00 — the class of error a model actually makes, and the
// one that costs real money if it is taken on trust. The orchestrator must refuse it.
const badArithmeticPO = `{"vendor_id":"VENDOR-MOCK-1","currency":"USD","lines":[` +
	`{"sku":"SKU-1001","description":"Widget, 10mm","quantity":"100","unit_price":"2.5000","extended_amount":"250.0000"},` +
	`{"sku":"SKU-2002","description":"Gasket, nitrile","quantity":"40","unit_price":"1.2500","extended_amount":"50.0000"}` +
	`],"total_amount":"30.0000"}`

// hugePO breaches the autonomous ceiling, forcing the human-approval branch.
const hugePO = `{"vendor_id":"VENDOR-MOCK-1","currency":"USD","lines":[` +
	`{"sku":"SKU-9999","description":"Industrial press","quantity":"1","unit_price":"250000.0000","extended_amount":"250000.0000"}` +
	`],"total_amount":"250000.0000"}`

func main() {
	addr := env("MOCK_LLM_ADDR", ":4000")
	// The canned classification. Overridable so a test can drive a different branch.
	intent := env("MOCK_LLM_INTENT", "1")

	mux := http.NewServeMux()
	// Which canned PO the drafting agent receives. Overridable so a failtest can drive the
	// rejection branches without needing a real model to misbehave on cue.
	poMode := env("MOCK_LLM_PO_MODE", "good")

	mux.HandleFunc("/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))

		// Two callers share this endpoint: CommBot classifies intent and expects a bare integer,
		// while the drafting agent expects a PO object. Route on the prompt rather than on a second
		// port, so compose wiring stays a single LITELLM_URL.
		if strings.Contains(string(body), "purchase order") {
			content := goodPO
			switch poMode {
			case "bad_arithmetic":
				content = badArithmeticPO
			case "over_ceiling":
				content = hugePO
			case "malformed":
				content = "I am afraid I cannot help with that request."
			}
			slog.Info("mock-llm draft_po", "remote", r.RemoteAddr, "mode", poMode)
			writeChat(w, content, 320, 180)
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

// writeChat emits an OpenAI-shaped completion with usage, which the drafting agent reads to record
// real token counts against its budget rather than trusting its own pre-call estimate.
func writeChat(w http.ResponseWriter, content string, promptToks, completionToks uint32) {
	var resp chatResponse
	resp.Choices = make([]struct {
		Message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
	}, 1)
	resp.Choices[0].Message.Role = "assistant"
	resp.Choices[0].Message.Content = content
	resp.Usage.PromptTokens = promptToks
	resp.Usage.CompletionTokens = completionToks

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
