## What changed

<!-- One paragraph. The commit body carries the why; this is the reviewer's orientation. -->

## Which test proves it

<!-- The question this repo asks instead of "may I touch this file". Name the test, proof script
     or CI job that would go red if this change were wrong. For a load-bearing path,
     .github/CODEOWNERS names the guard. If nothing would catch it, add the test in this PR. -->

## Checklist

- [ ] `make check` passes locally (build, vet, race tests, lint, gofmt, frontend lint/typecheck)
- [ ] If compose, scripts, Dockerfiles or a consumer changed: a boot proof ran (`make e2e` or a `failtest_*.sh`)
- [ ] `docs/kb/` updated if this invalidates a note (and `05-gotchas.md` if something bit)
- [ ] No new dependency without a line in `.github/dependabot.yml` covering its source
- [ ] No locked decision re-litigated (see `CLAUDE.md`)
