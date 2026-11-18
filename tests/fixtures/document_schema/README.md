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

`path` names the first failing location, for example `items[3].samples.data`.
An empty path means the whole document. Checks run in document order, so the first
error is stable.

## Units and stroke encoding

Positions and sizes use logical page units (1 unit is 1 logical pixel at zoom 1.0).
`page.width` comes from the document. Angles are radians. Colors are `#RRGGBBAA`.
`captured_at` is Unix seconds.

Stroke samples are compact columns (annotations#19): a sample count and the columns as
base64url (no padding) bytes:

```json
"samples": {"count": 2, "data": "AQOgnAGgBuDUA9gEABDoByiwCRQ"}
```

The example holds x 10000, 10400; y 30000, 30300; t 0, 8; pressure 500, 520; tilt
600, 610. The bytes are:

1. byte 0: format version, 1;
2. byte 1: flags, bit 0 when a pressure column follows, bit 1 for tilt;
3. the columns x, y, t, then pressure and tilt when flagged, `count` values each. Every
   value is the zigzag-encoded difference to the value before it in its column (the
   first one to 0), written as an unsigned LEB128 varint. Nothing follows the last
   column. Test vector: x 1, 0; y -1, 63; t 0, 8 gives `AQACAQGAAQAQ`.

The units:

- `x`, `y`: 1/100 of a page unit (10000 is 100.0 units). Inside the page width (or the
  typed handwriting box), `y` at or above 0.
- `t`: milliseconds from the stroke start. Starts at 0 and never decreases.
- `pressure`: optional column, 0 to 1000. `tilt`: optional column, 0 to 1571
  milliradians (pi/2). A missing column means the device gave no value.

Data that cannot be read (not base64url, another version, unknown flags, too short or
too long for `count`) is `invalid_samples` at `samples.data`; the range and order checks
of the values also name `samples.data`.

Editable nodes (`x`, `y`) and their `in` and `out` handles of strokes and shape paths
also use integer 1/100 page units. Other shapes and blocks use page units.

## Error codes

`invalid_json`, `non_finite_number`, `document_too_large`, `too_many_items`,
`too_many_samples`, `invalid_samples`, `too_many_nodes`, `schema_not_published`,
`unsupported_schema_version`, `invalid_type`, `missing_field`, `unknown_field`,
`unknown_kind`, `kind_not_allowed`, `invalid_variant`, `invalid_id`,
`invalid_reference`, `duplicate_id`, `out_of_bounds`, `out_of_range`, `invalid_size`,
`invalid_color`, `invalid_url`, `markup_not_allowed`, `invalid_text`, `empty_array`,
`invalid_count`, `invalid_sample_order`, `missing_reference`, `missing_file`,
`group_cycle`, `multiple_groups`, `storage_unavailable`.

`schema_not_published` and `storage_unavailable` depend on server state. They have
Rust tests, not case files.
