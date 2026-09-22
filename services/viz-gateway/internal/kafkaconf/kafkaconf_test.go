package kafkaconf

import (
	"strings"
	"testing"
)

func getter(m map[string]string) Getter { return func(k string) string { return m[k] } }

func TestBrokers(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want []string
		err  bool
	}{
		{"required", map[string]string{}, nil, true},
		{"split and trimmed", map[string]string{"KAFKA_BROKERS": " a:9092, b:9092 ,"}, []string{"a:9092", "b:9092"}, false},
		{"deprecated alias honoured", map[string]string{"KAFKA_BOOTSTRAP": "k:29092"}, []string{"k:29092"}, false},
		{"KAFKA_BROKERS wins over alias", map[string]string{"KAFKA_BROKERS": "new:1", "KAFKA_BOOTSTRAP": "old:1"}, []string{"new:1"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Brokers(getter(tc.env))
			if (err != nil) != tc.err {
				t.Fatalf("err = %v, want error=%v", err, tc.err)
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("brokers = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOptions(t *testing.T) {
	base := map[string]string{"KAFKA_BROKERS": "kafka:29092"}
	with := func(kv ...string) map[string]string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for i := 0; i+1 < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}
	cases := []struct {
		name    string
		env     map[string]string
		wantN   int // number of options: brokers(1) + tls(1) + sasl(1)
		wantErr string
	}{
		{"plaintext default", base, 1, ""},
		{"tls only", with("KAFKA_TLS", "true"), 2, ""},
		{"tls + scram-256", with("KAFKA_TLS", "true", "KAFKA_SASL_MECHANISM", "SCRAM-SHA-256", "KAFKA_SASL_USERNAME", "u", "KAFKA_SASL_PASSWORD", "p"), 3, ""},
		{"tls + scram-512 case-insensitive", with("KAFKA_TLS", "true", "KAFKA_SASL_MECHANISM", "scram-sha-512", "KAFKA_SASL_USERNAME", "u", "KAFKA_SASL_PASSWORD", "p"), 3, ""},
		{"tls + plain", with("KAFKA_TLS", "true", "KAFKA_SASL_MECHANISM", "PLAIN", "KAFKA_SASL_USERNAME", "u", "KAFKA_SASL_PASSWORD", "p"), 3, ""},
		{"sasl without tls refused", with("KAFKA_SASL_MECHANISM", "PLAIN", "KAFKA_SASL_USERNAME", "u", "KAFKA_SASL_PASSWORD", "p"), 0, "requires KAFKA_TLS=true"},
		{"sasl without credentials refused", with("KAFKA_TLS", "true", "KAFKA_SASL_MECHANISM", "PLAIN"), 0, "KAFKA_SASL_USERNAME"},
		{"unknown mechanism refused", with("KAFKA_TLS", "true", "KAFKA_SASL_MECHANISM", "GSSAPI", "KAFKA_SASL_USERNAME", "u", "KAFKA_SASL_PASSWORD", "p"), 0, "unsupported"},
		{"missing CA file refused", with("KAFKA_TLS", "true", "KAFKA_TLS_CA", "/nonexistent/ca.pem"), 0, "read KAFKA_TLS_CA"},
		{"no brokers refused", map[string]string{"KAFKA_TLS": "true"}, 0, "KAFKA_BROKERS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := Options(getter(tc.env))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(opts) != tc.wantN {
				t.Fatalf("got %d options, want %d", len(opts), tc.wantN)
			}
		})
	}
}
