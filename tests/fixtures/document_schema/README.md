# Note document schema fixtures

Canonical cases for the v2 note document validator (`lib/domain/note_documents.rs`).
The frontend can copy this folder and must give the same verdict for every case.

## Folders

- `families/`: the admin draft payloads for `POST /admin/schemas`. Publish each one
  with `POST /admin/schemas/{name}/versions/1/publish`. The registry rows check the
  root fields and basic types. The validator code checks every nested item.
- `valid/`: documents that pass.
- `invalid/`: documents that fail with one stable error code.

## Case file format

One JSON object per file:

| Key | Meaning |
| --- | --- |
| `description` | Short text about the case. |
| `family` | `note-original`, `note-typed` or `instant-room`. |
| `schema_version` | The schema version of the document. |
| `files` | Managed file ids that exist: ready files of the owner, or files of the Instant room. |
| `limits` | Optional. Lower limits for this case only: `max_items`, `max_samples_per_stroke`, `max_nodes_per_stroke`, `max_document_bytes`. |
| `document` | The document body. |
| `document_text` | Used instead of `document` when strict JSON cannot hold the case (for example `1e999`). Parse this text first. |
| `expect` | `{"valid": true}` or `{"valid": false, "code": "<code>", "path": "<path>"}`. |

`path` names the first failing location, for example `items[3].samples[0].x`.
An empty path means the whole document. Checks run in document order, so the first
error is stable.

## Units

Positions and sizes use logical page units (1 unit is 1 logical pixel at zoom 1.0).
`page.width` comes from the document. Sample `t` is integer milliseconds from the
stroke start. Pressure is 0 to 1, tilt is 0 to pi/2 radians, and both can be absent
or null. Angles are radians. Colors are `#RRGGBBAA`. `captured_at` is Unix seconds.

## Error codes

`invalid_json`, `non_finite_number`, `document_too_large`, `too_many_items`,
`too_many_samples`, `too_many_nodes`, `schema_not_published`,
`unsupported_schema_version`, `invalid_type`, `missing_field`, `unknown_field`,
`unknown_kind`, `kind_not_allowed`, `invalid_variant`, `invalid_id`,
`invalid_reference`, `duplicate_id`, `out_of_bounds`, `out_of_range`, `invalid_size`,
`invalid_color`, `invalid_url`, `markup_not_allowed`, `invalid_text`, `empty_array`,
`invalid_count`, `invalid_sample_order`, `missing_reference`, `missing_file`,
`group_cycle`, `multiple_groups`, `storage_unavailable`.

`schema_not_published` and `storage_unavailable` depend on server state. They have
Rust tests, not case files.
