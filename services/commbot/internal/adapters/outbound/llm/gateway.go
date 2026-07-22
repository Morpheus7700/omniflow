package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"omniflow/services/commbot/internal/core/domain"

	"golang.org/x/time/rate"
)

// ErrRateLimited is chained under domain.ErrTransient so the consumer can retry.
var ErrRateLimited = errors.New("AI gateway rate limit exceeded")

const (
	maxFetchSize   = 32 * 1024 // ceiling per quarantined URI
	maxLLMRespSize = 8 * 1024  // ceiling on gateway response body
)

const classifierSystemPrompt = `You are a strict classifier. Classify the vendor email into exactly one ID: ` +
	`1 (Invoice), 2 (Dispute), 3 (PO Inquiry), 4 (General). Output ONLY the integer, nothing else.`

type LiteLLMGateway struct {
	fetchClient  *http.Client // short timeout for object-store fetches
	llmClient    *http.Client // longer timeout for completions
	baseURL      string
	apiKey       string
	limiter      *rate.Limiter       // client-side Denial-of-Wallet ceiling (proactive, not reactive)
	allowedHosts map[string]struct{} // SSRF allowlist for vendor-supplied quarantine URIs
}

func NewLiteLLMGateway(baseURL, apiKey string, rps float64, burst int, allowedObjectStoreHosts []string) *LiteLLMGateway {
	hosts := make(map[string]struct{}, len(allowedObjectStoreHosts))
	for _, h := range allowedObjectStoreHosts {
		hosts[strings.ToLower(h)] = struct{}{}
	}
	return &LiteLLMGateway{
		fetchClient:  &http.Client{Timeout: 5 * time.Second},
		llmClient:    &http.Client{Timeout: 30 * time.Second},
		baseURL:      strings.TrimRight(baseURL, "/"),
		apiKey:       apiKey,
		limiter:      rate.NewLimiter(rate.Limit(rps), burst),
		allowedHosts: hosts,
	}
}

func (g *LiteLLMGateway) ClassifyIntent(ctx context.Context, secureSubjectURI, secureBodyURI string) (domain.Intent, error) {
	subject, err := g.fetchAndSanitize(ctx, secureSubjectURI)
	if err != nil {
		return domain.IntentUnspecified, err
	}
	body, err := g.fetchAndSanitize(ctx, secureBodyURI)
	if err != nil {
		return domain.IntentUnspecified, err
	}

	// Proactive rate limiting: block (honoring ctx) rather than flood the gateway and rack up spend.
	if err := g.limiter.Wait(ctx); err != nil {
		return domain.IntentUnspecified, fmt.Errorf("%w: rate limiter: %v", domain.ErrTransient, err)
	}
	return g.callGateway(ctx, subject, body)
}

func (g *LiteLLMGateway) fetchAndSanitize(ctx context.Context, raw string) (string, error) {
	if err := g.validateQuarantineURI(raw); err != nil {
		// Attacker-controlled URI to a non-allowlisted host is terminal (never retry a hostile input).
		return "", fmt.Errorf("%w: unsafe quarantine URI: %v", domain.ErrTerminal, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return "", fmt.Errorf("%w: build request: %v", domain.ErrTerminal, err)
	}
	resp, err := g.fetchClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: fetch: %v", domain.ErrTransient, err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotFound, resp.StatusCode == http.StatusForbidden, resp.StatusCode == http.StatusGone:
		return "", fmt.Errorf("%w: object store status %d", domain.ErrTerminal, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return "", fmt.Errorf("%w: object store status %d", domain.ErrTransient, resp.StatusCode)
	}

	buf, err := io.ReadAll(io.LimitReader(resp.Body, maxFetchSize))
	if err != nil {
		return "", fmt.Errorf("%w: read body: %v", domain.ErrTransient, err)
	}
	return sanitize(string(buf)), nil
}

// validateQuarantineURI enforces the zero-trust boundary: https only, host on the object-store
// allowlist, and no resolution to loopback/private/link-local ranges (blocks cloud-metadata SSRF).
// NOTE: this resolves-then-fetches (a TOCTOU/DNS-rebind window remains). For hard guarantees, pin
// the resolved IP into a custom net.Dialer.Control on fetchClient.
func (g *LiteLLMGateway) validateQuarantineURI(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Scheme != "https" {
		return fmt.Errorf("scheme %q not allowed", u.Scheme)
	}
	host := strings.ToLower(u.Hostname())
	if _, ok := g.allowedHosts[host]; !ok {
		return fmt.Errorf("host %q not in object-store allowlist", host)
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("resolve %q: %w", host, err)
	}
	for _, ip := range ips {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
			return fmt.Errorf("host %q resolves to non-public IP %s", host, ip)
		}
	}
	return nil
}

// sanitize is defense-in-depth ONLY — it is not a prompt-injection defense by itself.
// Real controls: untrusted content stays in the user role (never the system prompt); NeMo
// Guardrails performs semantic scanning at the gateway edge; and model output is parsed as
// untrusted (see parseIntent).
func sanitize(raw string) string {
	return strings.NewReplacer(
		"<|endoftext|>", "",
		"<|im_start|>", "",
		"<|im_end|>", "",
		"<system>", "",
		"</system>", "",
	).Replace(raw)
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model       string        `json:"model"`
	Temperature float64       `json:"temperature"`
	Messages    []chatMessage `json:"messages"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

func (g *LiteLLMGateway) callGateway(ctx context.Context, subject, body string) (domain.Intent, error) {
	reqBody, err := json.Marshal(chatRequest{
		Model:       "gemini-3-flash",
		Temperature: 0,
		Messages: []chatMessage{
			{Role: "system", Content: classifierSystemPrompt},
			{Role: "user", Content: fmt.Sprintf("Subject: %s\nBody: %s", subject, body)},
		},
	})
	if err != nil {
		return domain.IntentUnspecified, fmt.Errorf("%w: marshal: %v", domain.ErrTerminal, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.baseURL+"/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		return domain.IntentUnspecified, fmt.Errorf("%w: build request: %v", domain.ErrTerminal, err)
	}
	req.Header.Set("Authorization", "Bearer "+g.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.llmClient.Do(req)
	if err != nil {
		return domain.IntentUnspecified, fmt.Errorf("%w: gateway call: %v", domain.ErrTransient, err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return domain.IntentUnspecified, errors.Join(domain.ErrTransient, ErrRateLimited)
	case resp.StatusCode >= 500:
		return domain.IntentUnspecified, fmt.Errorf("%w: gateway status %d", domain.ErrTransient, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return domain.IntentUnspecified, fmt.Errorf("%w: gateway status %d", domain.ErrTerminal, resp.StatusCode)
	}

	var out chatResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxLLMRespSize)).Decode(&out); err != nil {
		return domain.IntentUnspecified, fmt.Errorf("%w: decode response: %v", domain.ErrTransient, err)
	}
	if len(out.Choices) == 0 {
		return domain.IntentUnspecified, fmt.Errorf("%w: empty choices", domain.ErrTerminal)
	}
	return parseIntent(out.Choices[0].Message.Content)
}

// parseIntent treats the model output as untrusted: strict integer, strictly in range.
func parseIntent(s string) (domain.Intent, error) {
	s = strings.TrimSpace(s)
	n, err := strconv.Atoi(s)
	if err != nil {
		return domain.IntentUnspecified, fmt.Errorf("%w: non-integer classifier output %q", domain.ErrTerminal, truncate(s, 64))
	}
	switch domain.Intent(n) {
	case domain.IntentInvoiceSubmission, domain.IntentPaymentDispute,
		domain.IntentPurchaseOrderInquiry, domain.IntentGeneralSupport:
		return domain.Intent(n), nil
	default:
		return domain.IntentUnspecified, fmt.Errorf("%w: out-of-range intent %d", domain.ErrTerminal, n)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
