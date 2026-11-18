## [unreleased]

### 🚀 Features

- *(notes)* Link a handwritten study note to its published typed source post (#41)
- *(logs)* Add journald logger and trusted LAN stream
- *(storage)* Add v2 relational schema for notes, publication, sync, projects, tags and files (#18)
- *(schemas)* Add note document v2 families and nested item validation (#19)
- *(schemas)* Store stroke samples as integer columns (#19)
- *(domain)* Validate editor placement rules with shared rule fixtures (#20)
- *(domain)* Let ink-only groups overlap like free ink (#20)
- *(domain)* Check the invisible placement grid of 8 page units for structured blocks and compare collisions on occupied cells (#20)
- *(domain)* Save note revisions with crash-safe reconciliation (#21)
- *(notes)* Add the owner working-copy API with manual sync, conflicts and account binding (#22)
- *(notes)* Publish Original, Typed or both as independent public snapshots (#23)
- *(projects)* Add owner projects, note memberships and the public project projection (#24)
- *(discovery)* Add public note search, author-balanced shuffled browsing, authors and projects (#25)
- *(files)* Add managed file references with FSM deduplication and safe last-reference cleanup (#27)
- *(files)* Stage uploads in the configurable UPLOAD_STAGING_DIR instead of the system temp folder (#27)
- *(admin)* Add administrative note, project and file operations with the central access level (#26)
- *(instant)* Validate v2 room documents with atomic edits, item revisions and fixed expiry (#29)
- *(instant)* Accept CSV table and PDF preview cards on room files in Instant rooms (#29)
- *(instant)* Reserve temporary file metadata, verify binary uploads and preserve expiry quotas (#30)
- *(models)* Register model releases with server CLI commands, keep one default with rollback and serve signed-in manifests without private fields (#31)
- *(models)* Add account-bound model downloads that stream the shared release artifact from FSM with resumable HTTP Range reads (#32)
- *(transcriptions)* Add server-owned transcription runs with real stage events over SSE, one local result per run, stale source checks and accepted Typed source revisions (#33)
- *(training)* Store mandatory correction feedback of transcription runs as validated, untrusted training input with admin CLI export and removal (#34)

### 🐛 Bug Fixes

- *(training)* Add `training review`, `approve`, and `flag` CLI commands, default `training export` to approved examples, and enforce per-account Redis rate and concurrency limits on feedback (#34)
- *(models)* Mark rolled-back releases as withdrawn, exclude them from listing and downloads, and add `models withdraw <version>` (#31)
- *(files)* Accept any file type as a file card in Notes and Instant and serve unverified types as attachment with nosniff (#27)
- *(links)* Bound public-page previews and validate pinned destinations on every redirect (#28)

### 💼 Other

- Back-merge v1.1.0 into feature/main

## [1.1.0] - 2026-10-27

### 🚀 Features

- Remove Instant Share items added by mistake (#14)
- Merge Instant Share item removal (#14)
- Cite researched sources and embed public Reports charts in demo content (#11)
- Merge researched demo content with Reports embeds (#11)
- Return one linked author per article (#15)
- Merge linked article author (#15)

### 💼 Other

- Promote release/v1.0.0 to development
- Promote feature/main to development for v1.1.0

## [1.0.0] - 2026-10-26

### 🚀 Features

- Add relational storage and isolated service foundation (#2)
- Merge storage foundation (#2)
- Validate document bodies against published schema versions (#3)
- Merge content guardrails (#3)
- Publish owned multilingual articles through validated APIs (#4)
- Merge multilingual publishing (#4)
- Scope read caches by locale and invalidation epoch (#5)
- Merge read caching (#5)
- Add provider previews and scoped article images (#6)
- Merge article media integration (#6)
- Register informational waitlist requests idempotently (#7)
- Merge public waitlist (#7)
- Add expiring rooms and conflict-aware shared items (#8)
- Merge temporary room state (#8)
- Stream scoped temporary files with atomic per-IP reservations (#9)
- Merge temporary file transfers (#9)
- Retain cleanup ownership until temporary files are deleted (#10)
- Merge expiring file cleanup (#10)
- Add API-driven researched demonstration content (#11)
- Merge explicit demonstration population (#11)
- Merge backend integration verification (#13)

### 🧪 Testing

- Verify storage recovery, private caching and streamed transfers (#13)

### ⚙️ Miscellaneous Tasks

- Initialise repository root
- Add project baseline and ignore local context (#1)
- Merge repository bootstrap (#1)
- Separate explicit schema migrations from service installation (#12)
- Merge explicit operations scripts (#12)
