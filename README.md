# Annotations API

Rust service for Asodya multilingual study notes and temporary browser sharing.
The external gateway prefix is `https://api.asodya.com/apps/annotations/v1`.
The trusted LAN service listens on `127.0.0.1:8111` by default. External permanent
writes, private JSON and schema management require the existing gateway admin
credential or an administrator session with CSRF. The service adds no LAN login.

## Storage and architecture

`lib/core` holds settings, `dal` contains adapters, `domain` owns validation and
business rules, and `presentation` exposes HTTP routes. PostgreSQL contains
normalised owners, channels, topics, languages, post metadata and schema fields.
There are no SQL JSON/JSONB/array columns. CouchDB stores article bodies only after
validation against an administrator-published schema. Published schema versions
are immutable. Redis caches public reads and holds temporary rooms, IP quotas and
retryable cleanup ownership. FSM stores bytes; it does not store article content.

A post belongs to one owner and can appear in multiple channels. Its translations
share a post ID; each available translation has a language, title, summary and
validated body. Interface language is independent of content language. Additional
language catalog entries are possible without a schema change. Normal accounts
cannot publish in this release. No frontend editor or administration UI exists.

## Setup and operations

1. Create the dedicated PostgreSQL database `asodya_annotations` and a least-privilege
   application role using the server's existing administrative process. Register an
   `annotations` FSM app/key and ensure its upload configuration permits documents.
2. Copy `.env.example` to canonical `.env` and set PostgreSQL, CouchDB, Redis and FSM
   credentials locally. Never copy them into Git, issue bodies or browser builds.
3. Run `scripts/migrate.sh --dry-run`, then `scripts/migrate.sh --yes`. These operations
   create schema and the dedicated `annotations_documents` CouchDB/index only.
   SQL migration files contain no application population. Rerunning is supported.
4. Run `scripts/install.sh` as the service user. It builds and restarts the systemd
   user service, without migrations or population. Check `/health` and `/ready`.
5. Explicitly publish optional labelled research examples with
   `python3 scripts/populate_demo.py --base-url http://127.0.0.1:8111 --yes`.
   For gateway calls, set `ANNOTATIONS_ADMIN_KEY` in the invoking environment.
   `--dry-run` resolves the embedded public Reports cards and writes nothing. Each chart
   caption states whether the card shows live data or sample data.
   Existing published article schemas are reused. Examples are not Wilson's own
   studies and contain no invented experimental measurements.

## API usage

The gateway's central `/docs` owns complete external documentation. Public reads:
`GET /catalog`, `/posts?q=&topic=&channel=&lang=&offset=`, `/posts/{slug}?lang=&channel=`,
`/links/preview?url=`, and `/media/{key}`. `POST /waitlist` accepts email/message and
returns a duplicate-safe registration result; it grants no publishing permission.

Administrative operations are `PUT /admin/catalog`, `POST /admin/schemas`,
`POST /admin/schemas/{name}/versions/{version}/publish`, `PUT /admin/posts/{id}`,
`POST /admin/posts/{id}/publish`, `GET /admin/posts/{id}`, and `POST /admin/images?owner_id=`
(multipart `file`). A default `article-body` schema can declare a required `blocks`
array. See `scripts/populate_demo.py` for an executable create/translate/publish
example. Backend defaults control typography; formatting parameters are optional.
Supported blocks: heading, paragraph, quote, list, image, link, source, YouTube,
external PDF, and an individual public Reports embed. Persistent PDFs remain links;
persistent image uploads use FSM. Private JSON is never placed in the public cache.

Temporary rooms use `PUT /instant/{code}`, `GET /instant/{code}`,
`POST /instant/{code}/items` and multipart `POST /instant/{code}/files?size={bytes}`.
Edits of existing items require the current revision; conflicts return 409.
Files are read only through `/instant/{code}/files/{id}` while the room is live.
`DELETE /instant/{code}/items/{id}` removes an item added by mistake; for files it also
deletes the stored bytes and releases the uploader's allowance. Removal never extends the room.
The fixed lifetime is 900 seconds from creation. Limits are 50,000,000 bytes per
file and 50,000,000 active uploaded bytes per uploader IP across rooms. Quota
reservations are atomic and cleanup retries until FSM deletion succeeds. IP quota
is released after cleanup; a storage outage can delay release. Public JSON and
file responses use `no-store`; permanent media is excluded from temporary cleanup.
Only a loopback gateway can supply the backend's forwarded client IP. Anyone who
knows a room code can read/edit it during its lifetime; it is a temporary exchange,
not private document storage. Choose a code that others cannot casually guess.

## Isolated verification

`cargo fmt --check`, `cargo test`, and `cargo clippy --all-targets -- -D warnings`.
Tests use SQLite plus isolated memory adapters for CouchDB/Redis/FSM, not production.
`cargo run --example browser_fixture` starts a debug-only fixture on localhost:18111
without reading `.env`. Populate it with the explicit demo command above, changing
the base URL to `http://127.0.0.1:18111`. The fixture is ephemeral and must not be
installed or exposed as a production service. Real storage provisioning and live
acceptance are tracked separately in issues #12 and #13.
