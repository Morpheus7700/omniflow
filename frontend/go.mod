// This file exists to keep Go OUT of frontend/. It declares no dependencies and builds nothing.
//
// `go build ./...` walks every subdirectory of the root module. It skips `testdata` and names
// beginning with `.` or `_`, but it does NOT skip `node_modules` — so any npm package that happens
// to ship a `.go` file becomes a package of the `omniflow` module. One already does: `flatted`
// ships `golang/pkg/flatted/flatted.go`, which put third-party Go into `go list ./...`, `go vet`,
// and every local gosec run.
//
// The effect was a local/CI divergence rather than a broken build, and that is the part worth
// fixing. CI installs npm dependencies AFTER the Go steps, so `node_modules` does not exist when it
// runs `go build ./...` — 25 packages there, 26 on any machine where the dashboard has been
// installed. A future npm dependency shipping Go that does not compile, or that gosec dislikes,
// would fail locally and stay green in CI, which is the most confusing shape a failure can take.
// CLAUDE.md calls a local pass a fast pre-check; a pre-check over a different package set is not
// one.
//
// A nested go.mod makes its whole subtree a separate module, which the parent's `./...` skips. That
// is the supported mechanism, and it is why this file has a module path that is deliberately not
// importable and no Go source next to it.
module omniflow-frontend-no-go-code-here

go 1.25
