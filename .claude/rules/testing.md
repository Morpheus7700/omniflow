# Testing

- The test suite is the enforcement mechanism in this repo. Before changing a load-bearing file, the
  question is "which test proves I did not break it" — see `.github/CODEOWNERS` for the mapping.
- Run the affected package, not the whole suite: `go test ./services/p2p-orchestrator/internal/core/...`.
- Verify behavior, not implementation. Don't assert call counts when output values would do.
- Table-driven tests are the Go norm and are expected here; loops over cases are fine. What is not
  fine is a loop that hides which case failed — name subtests with `t.Run(tc.name, ...)`.
- Prefer real implementations. Mock only at system boundaries (network, filesystem, clock, randomness).
  Integration tests run against a real CockroachDB, not a fake.
- Flaky test? Fix it or delete it. Never retry to make it pass. Determinism tests (e.g. Kahn ordering
  over 50 randomised runs) exist precisely to catch flakiness — don't weaken them.
- Never assert nothing: no bare `err == nil` as the only check when the output value is the point.
- A change no test objects to is either safe or a gap in the suite. Chase the second case.
