# 07 · The Gate (current critical path)

Everything in [[04-progress-ledger]] is committed but **unproven** — no CI has run. As of 2026-07-24
the only remaining blocker is **GitHub auth + push** (one human step). The CockroachDB license is **no
longer required** (see below).

## Status (2026-07-24)
- **`gh` CLI installed** (v2.96.0, on Machine PATH). Only `gh auth login` remains — interactive, so a
  human runs it once (authenticates gh for the whole Windows user, incl. Antigravity's terminal).
- **No git remote yet** — repo has never been connected to GitHub.
- **`CRDB_LICENSE` is now OPTIONAL.** Under CockroachDB v24.3+ licensing a single-node cluster
  (`start-single-node`, which is what compose runs) needs no license key. crdb-init + the scripts + CI
  were re-plumbed to run license-free and **fail loudly** (never skip) if changefeed creation actually
  needs a key. So the license signup is dropped unless CI proves otherwise.

## The exact steps (human auths; then Claude or Antigravity can push)
```bash
# 1. Auth (human, one-time, interactive browser):
gh auth login

# 2. Create the public repo + push (no secrets needed):
gh repo create omniflow --public --source=. --remote=origin --push
#    (no gh? create in browser, then: git remote add origin <url> && git push -u origin master)

# 3. (Optional) ONLY if CI shows changefeeds need a license — grab a FREE key and set secrets:
#    https://www.cockroachlabs.com/get-cockroachdb/enterprise/
#    gh secret set CRDB_LICENSE --body "<license-string>"
#    gh secret set CRDB_ORG     --body "<org-name>"
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
