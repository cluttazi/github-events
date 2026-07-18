# ingestion/github_archive

Seeded generator for a GH-Archive-style NDJSON event feed.

## Why a generator instead of real GH-Archive files

The template convention this repo follows is **no real data, ever**: every
run must be deterministic and CI-provable offline. The generator emits the
same six event types (`PushEvent`, `PullRequestEvent`, `IssuesEvent`,
`WatchEvent`, `ForkEvent`, `ReleaseEvent`) with the same envelope shape and
hourly file naming (`YYYY-MM-DD-H.ndjson`) as gharchive.org, so pointing the
bronze loader at a directory of real archive files would require no code
change — only a conscious decision to break determinism.

## Why the payloads carry full repo objects

The event envelope's `repo` object is minimal (id, name, url) — exactly as
in the real feed. Repo detail (owner, language, license, star/fork counts)
only appears inside `payload.pull_request.base.repo` and `payload.forkee`,
which is why the raw vault loads `sat_repo_profile`/`sat_repo_stats` from
those payload paths and nothing else.

## Determinism contract

One seeded `random.Random` + seeded Faker + a logical clock (fixed start,
random step per event) drive everything. Same seed ⇒ byte-identical files.
The `--corrupt-pct` fraction of lines is damaged *after* serialization; the
bronze quarantine count must equal the generator's corrupt count, and the
integration tests assert exactly that.
