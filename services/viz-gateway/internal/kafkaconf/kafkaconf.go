// Package kafkaconf turns the environment into franz-go client options, in one place, for every
// binary that talks to Kafka.
//
// This is a verbatim copy of internal/platform/kafkaconf in the root module. viz-gateway is its own
// Go module and cannot import the root module's internal packages; the drift test in each module
// keeps the two copies identical (see TestMatchesRootModule).
//
// Before it existed each service built its client from a bare SeedBrokers call: there was no way
// to point any of them at a broker that required TLS or SASL, which is every broker outside this
// repo's compose file. Plaintext remains the default so the proof stack is unchanged; a secured
// broker is a matter of setting variables, not editing five main functions.
//
// Environment contract:
//
//	KAFKA_BROKERS          comma-separated seed brokers (required; KAFKA_BOOTSTRAP is honoured as a
//	                       deprecated alias so existing deployments keep working)
//	KAFKA_TLS              "true" to dial with TLS (system roots)
//	KAFKA_TLS_CA           path to a PEM bundle to trust instead of / in addition to system roots
//	KAFKA_SASL_MECHANISM   PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 (empty = no SASL)
//	KAFKA_SASL_USERNAME    with KAFKA_SASL_PASSWORD; both required when a mechanism is set
//
// A mechanism without TLS is refused: SASL/PLAIN over plaintext sends the password in the clear,
// and SCRAM without TLS is still open to downgrade — an operator who wants that has to be unable
// to get it by accident.
package kafkaconf

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Getter is the environment lookup, injectable for tests.
type Getter func(key string) string

// Brokers returns the seed broker list from KAFKA_BROKERS, falling back to the deprecated
// KAFKA_BOOTSTRAP. An empty result is an error: a consumer that silently defaults to
// localhost:9092 inside a container connects to nothing and reports itself healthy.
func Brokers(get Getter) ([]string, error) {
	raw := get("KAFKA_BROKERS")
	if raw == "" {
		raw = get("KAFKA_BOOTSTRAP")
	}
	var out []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("KAFKA_BROKERS is not set")
	}
	return out, nil
}

// Options returns the transport options — seed brokers, and TLS/SASL when configured. Callers
// append their own consumer or producer options.
func Options(get Getter) ([]kgo.Opt, error) {
	brokers, err := Brokers(get)
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...)}

	useTLS := strings.EqualFold(get("KAFKA_TLS"), "true")
	if useTLS {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if ca := get("KAFKA_TLS_CA"); ca != "" {
			// Cleaned for gosec G304: an operator-supplied path from the environment, read once at
			// boot — the same trust level as the DSN beside it.
			pem, err := os.ReadFile(filepath.Clean(ca))
			if err != nil {
				return nil, fmt.Errorf("read KAFKA_TLS_CA %q: %w", ca, err)
			}
			pool, err := x509.SystemCertPool()
			if err != nil || pool == nil {
				pool = x509.NewCertPool()
			}
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("KAFKA_TLS_CA %q contains no usable certificates", ca)
			}
			cfg.RootCAs = pool
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	}

	mech := strings.ToUpper(strings.TrimSpace(get("KAFKA_SASL_MECHANISM")))
	if mech == "" {
		return opts, nil
	}
	if !useTLS {
		return nil, fmt.Errorf("KAFKA_SASL_MECHANISM=%s requires KAFKA_TLS=true", mech)
	}
	user, pass := get("KAFKA_SASL_USERNAME"), get("KAFKA_SASL_PASSWORD")
	if user == "" || pass == "" {
		return nil, fmt.Errorf("KAFKA_SASL_MECHANISM=%s requires KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD", mech)
	}
	switch mech {
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: user, Pass: pass}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()))
	default:
		return nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM %q (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", mech)
	}
	return opts, nil
}

// FromEnv is Options over the process environment.
func FromEnv() ([]kgo.Opt, error) { return Options(os.Getenv) }
