package domain

import (
	"context"
)

// LLMGateway defines the port for securely classifying vendor intents.
// By passing URIs instead of raw text, we ensure the core domain never
// holds potentially hostile prompt-injection payloads in memory longer than needed,
// and the LLM adapter handles the secure retrieval and sanitization.
type LLMGateway interface {
	ClassifyIntent(ctx context.Context, secureSubjectURI, secureBodyURI string) (Intent, error)
}

// Publisher defines the port for routing the classified event to the next DAG stage.
type Publisher interface {
	PublishClassifiedEmail(ctx context.Context, email *VendorEmail) error
}
