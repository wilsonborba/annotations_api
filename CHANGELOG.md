## [unreleased]

### 🚀 Features

- *(logs)* Add journald logger and trusted LAN stream
- *(storage)* Add v2 relational schema for notes, publication, sync, projects, tags and files (#18)
- *(schemas)* Add note document v2 families and nested item validation (#19)
- *(domain)* Validate editor placement rules with shared rule fixtures (#20)
- *(domain)* Save note revisions with crash-safe reconciliation (#21)
- *(notes)* Add the owner working-copy API with manual sync, conflicts and account binding (#22)
- *(notes)* Publish Original, Typed or both as independent public snapshots (#23)
- *(projects)* Add owner projects, note memberships and the public project projection (#24)
- *(discovery)* Add public note search, author-balanced shuffled browsing, authors and projects (#25)
- *(files)* Add managed file references with FSM deduplication and safe last-reference cleanup (#27)
- *(admin)* Add administrative note, project and file operations with the central access level (#26)
- *(instant)* Add validated atomic v2 room editing (#29)
- *(instant)* Merge atomic v2 room editing (#29)
- *(instant)* Add verified two-stage temporary uploads (#30)
- *(instant)* Merge two-stage temporary uploads (#30)
- *(models)* Register releases and serve signed-in manifests (#31)
- *(models)* Merge release registry and signed-in manifests (#31)
- *(models)* Add account-bound model downloads with resumable range reads (#32)
- *(models)* Merge account-bound model downloads (#32)
- *(transcriptions)* Add server-owned runs, stage events and local result submission (#33)
- *(transcriptions)* Merge server-owned transcription runs and stage events (#33)
- *(training)* Store correction feedback as validated, untrusted training input (#34)
- *(training)* Merge validated correction feedback and admin export and removal (#34)
- *(notes)* Link a handwritten study note to its published typed source post (#41)
- *(content-jobs)* Add durable optional Cortex processing jobs (#35)
- *(tasks)* Add owner task card routes through per-user Plane Slim containers (#36)
- *(annotations_api#37)* Add created-for-note Reports reference accounting, reuse preview, and retryable cascade deletion
- *(#35)* Add server-side Notes to Certifications prepared study handoff
- *(notes)* Complete H3 prepared certification handoff
- *(notes)* Collect grouped original text and verify full H3 prepared handoff
- *(notes)* Carry prepared generation config and guard non-text original extraction
- *(notes)* Verify mixed original text subset extraction and non-default config handoff
- *(notes)* Attach canonical external_ref origin to prepared study sources

### 🐛 Bug Fixes

- *(schemas)* Store stroke samples as integer columns (#19)
- *(domain)* Let ink-only groups overlap like free ink (#20)
- *(files)* Stage uploads in the configurable UPLOAD_STAGING_DIR (#27)
- *(links)* Harden public page preview fetching (#28)
- *(links)* Merge bounded public link previews (#28)
- *(models)* Stop rollback toggling and keep metrics out of manifests (#31)
- *(domain)* Check the invisible placement grid for structured blocks (#20)
- *(instant)* Accept CSV table and PDF preview cards in rooms (#29)
- *(files)* Accept any file type as a file card and serve unverified types as attachment (#27)
- *(models)* Mark rolled-back releases as withdrawn and add models withdraw CLI (#31)
- *(training)* Add review/approve/flag CLI, approved-only default export, and per-account Redis limits (#34)
- *(content-jobs)* Resume unfinished jobs on startup and store validated drafts in CouchDB (#35)
- *(discovery)* Sync x-display-name header and return author_name on public notes, authors, and projects (#25)
- *(tasks)* Use 10-hex Plane project identifier with collision retry (#36)
- *(schemas)* Hold at least 30 dense handwritten pages in one note (#19)
- Persist explicit Reports card identity
- *(#37)* Enforce server creation provenance, owner-scoped warnings, and explicit reuse confirm
- *(#37)* Add server creation receipts, 200 idempotent replay, and canonical owner metadata
- *(#37)* Mint opaque server creation receipts and enforce request_digest conflict
- *(reports)* Merge explicit note card identity (#43)
- *(reports)* Serialize note creation and deletion outcomes
- *(reports)* Integrate reviewed note creation and deletion handling (#37)
- *(#35)* Persist synchronous temporary Cortex 201 results without polling history
- Guard content job cancellation during Cortex start
- *(notes)* Align prepared selection PDF page cap and text-only passages with Cortex

### 💼 Other

- Back-merge v1.1.0 into feature/main
- Add Annotations API log stream (#17)
- Add v2 relational schema (#18)
- Add note document v2 schema and nested validation (#19)
- Store stroke samples as integer columns (#19)
- Validate editor placement rules with shared rule fixtures (#20)
- Let ink-only groups overlap like free ink (#20)
- Add crash-safe note revision saves (#21)
- Add the owner working-copy notes API (#22)
- Publish Original, Typed or both as independent public snapshots (#23)
- Add owner projects, note memberships and the public project projection (#24)
- Add public note search, author-balanced shuffled browsing, authors and projects (#25)
- Add managed file references with FSM deduplication and safe last-reference cleanup (#27)
- Stage uploads in the configurable UPLOAD_STAGING_DIR (#27)
- Add administrative note, project and file operations with the central access level (#26)
- Check the invisible placement grid for structured blocks (#20)
- Accept CSV table and PDF preview cards in Instant rooms (#29)
- Accept any file type as a file card and serve unverified types as attachment (#27)
- Mark rolled-back releases as withdrawn and add models withdraw (#31)
- Add training review step, approved-only export, and per-account limits (#34)
- Link handwritten study notes to published typed source posts (#41)
- Add durable optional Cortex processing jobs (#35)
- Resume unfinished content jobs on startup and store drafts in CouchDB (#35)
- Sync gateway display name and return author_name on public responses (#25)
- Add owner task card routes through Plane Slim adapter (#36)
- Verify v2 gateway route access classes (#38)
- Fix Plane project identifier length and collision retry (#36)
- Forward author waitlist to central gateway and drop local table (#16)
- *(schemas)* Compact stroke samples and higher note limits (#19)
- *(schemas)* Compact samples in all fixtures and tests (#19)
- Hold at least 30 dense handwritten pages in one note (#19)
- Review preserved synchronous results and cancellation admission
- Review note-owned prepared certification handoff

### 🚜 Refactor

- *(waitlist)* Forward signups to central gateway waitlist and drop local table (#16)

### 📚 Documentation

- *(changelog)* Record backend logging changes
- *(changelog)* Record v2 relational schema
- *(changelog)* Record note document v2 schema
- *(changelog)* Record compact stroke encoding
- *(changelog)* Record editor placement rules
- *(changelog)* Record ink-only group rule
- *(changelog)* Record crash-safe note saves
- *(changelog)* Record the owner notes API
- *(changelog)* Record note publication
- *(changelog)* Record owner projects
- *(changelog)* Record public discovery
- *(changelog)* Record managed files
- *(changelog)* Record upload staging folder
- *(changelog)* Record administrative operations
- *(changelog)* Record Reports card identity fix (#43)
- *(changelog)* Record reviewed Reports cleanup integration (#37)

### 🧪 Testing

- *(gateway)* Verify v2 route access classes and cutover exclusions (#38)
- *(reports)* Cover lost create response, digest 409, and concurrent duplicates (#37)

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
