# Annotations API

Annotations API is the Rust backend for Asodya notes, multilingual publications and
temporary browser sharing. The Asodya application gateway exposes its external routes.

## Status

This repository currently contains the project baseline. Application implementation
is tracked in the [milestones](https://github.com/wilsonborba/annotations_api/milestones).
There is no runnable service or provisioned Annotations database yet.

## Architecture

The service separates configuration (`core`), storage adapters (`dal`), business rules
(`domain`) and HTTP presentation. PostgreSQL holds normalised catalog and schema data;
CouchDB stores validated article bodies; Redis caches reads and holds temporary rooms.
FSM stores file bytes through a backend adapter.

The gateway owns external authentication. The trusted internal service does not add
an independent authentication system. Permanent publishing and schema administration
remain restricted externally; temporary room routes are public.

## Development

The linked issues define service setup, isolated SQLite-based verification, explicit
schema-only migrations, API-driven content population and deployment. Setup and usage
commands will be added when their executable targets exist.
