# 07 · The Gate (current critical path)

Everything in [[04-progress-ledger]] is committed (`d351b96`) but **unproven** — no CI has run. The
one thing standing between "built" and "proven" is three **human-only** steps. Neither Claude nor
Antigravity can do them (captcha, personal GitHub auth, license signup).

## Why blocked (verified 2026-07-23)
- **No git remote** — the repo has never been connected to GitHub.
- **`gh` CLI not installed** on this machine, no auth.
- **`CRDB_LICENSE` is human-gated** — free signup at cockroachlabs.com; only the human can set repo
  secrets.

## The exact steps (human runs these; `!` prefix runs in-session)
```bash
# 1. Free CockroachDB Enterprise license (browser): copy the license string + org
#    https://www.cockroachlabs.com/get-cockroachdb/enterprise/

# 2. Auth + create repo + push:
gh auth login
gh repo create omniflow --public --source=. --remote=origin --push
#    (no gh? create in browser, then: git remote add origin <url> && git push -u origin master)

# 3. Set CI secrets:
gh secret set CRDB_LICENSE --body "<license-string>"
gh secret set CRDB_ORG     --body "<org-name>"
```

## Then
GitHub Actions runs `build` + `e2e` + `failtest-resume` + `failtest-exactly-once` +
`failtest-restatement` + `security`. Watch the four E2E/failtest jobs. Paste results (green or red) to
the Sentinel:
- **Green** → the "Make It Real" milestone is genuinely proven. Move to README/SCOPE.md.
- **Red** → the jobs are split so a failure names its invariant; the Sentinel patches from the logs.
  First suspects are always the [[05-gotchas]] class (a serialization or timing detail only CI reveals).

## Safe to know
No secrets are in the committed tree — compose reads `CRDB_LICENSE` from env. The commit is reversible
with `git reset --soft HEAD~1` if the human wants to restructure before pushing.

Related: [[04-progress-ledger]] · [[06-build-and-test]]
