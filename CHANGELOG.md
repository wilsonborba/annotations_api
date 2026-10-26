## [unreleased]

### 🚀 Features

- *(logs)* Add journald logger and trusted LAN stream

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
