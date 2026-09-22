# Vendored fonts

These are the **latin subset** woff2 files Google Fonts serves, committed so that building the
frontend image needs no network access to a third party.

`next/font/google` downloads font files at **build** time. That made every Docker image build depend
on reaching `fonts.gstatic.com`, and it failed a CI boot job — `Module not found: Can't resolve
'@vercel/turbopack-next/internal/font/google/font'`, one error per face — minutes after the same
`npm run build` had succeeded on the runner. A build that can fail for a reason outside the
repository is not reproducible, and an air-gapped build could never have worked.

| File | Family | Covers |
|---|---|---|
| `newsreader-variable.woff2` | Newsreader | weights 400–500, upright (variable font) |
| `newsreader-variable-italic.woff2` | Newsreader | weights 400–500, italic (variable font) |
| `public-sans-variable.woff2` | Public Sans | weights 400–500 (variable font) |
| `ibm-plex-mono-400.woff2` | IBM Plex Mono | weight 400 |
| `ibm-plex-mono-500.woff2` | IBM Plex Mono | weight 500 |

Newsreader and Public Sans are variable fonts: Google returns byte-identical files for 400 and 500,
so one file per style covers the range. IBM Plex Mono ships static weights, so it needs two.

## Licence

All three families are licensed under the **SIL Open Font License 1.1**, which permits
redistribution of the font files alongside this notice. See `OFL.txt`.

- Newsreader — Production Type
- Public Sans — the United States Web Design System
- IBM Plex Mono — IBM

## Refreshing them

`scripts/fetch-fonts.py` at the repo root re-downloads the same subset. Run it only when a family
is added or changed — a routine refresh would churn binary files for no benefit.
