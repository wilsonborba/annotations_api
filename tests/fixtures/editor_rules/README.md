# Editor placement rule fixtures

Canonical cases for the editor placement rules (`lib/domain/editor_rules.rs`).
The app keeps a copy of this folder and must give the same verdict, the same error
code and the same item list for every case.

## Case file format

One JSON object per file:

| Key | Meaning |
| --- | --- |
| `description` | Short text about the case. |
| `rule_id` | The rule that the case tests (see below). |
| `family` | `note-original`, `instant-room` or `note-typed`. |
| `schema_version` | The schema version of the document. |
| `document` | The stored document before the operation. It is valid. |
| `operation` | Optional. The operation to apply to a working copy of `document`. Without it, the case checks `document` itself. |
| `expected` | `valid` or `invalid`. |
| `error` | Only for `invalid`: `{"code": "<code>", "items": ["<id>", ...]}`. |

`error.items` names the conflicting item ids in document order of the result. For an
invalid case, the rule of the error is `rule_id`.

File references are not checked here: the document schema fixtures
(`tests/fixtures/document_schema/`) cover them.

## Check order

1. Apply the operation to a copy of `document`. The stored document never changes.
2. Check the result with the note document schema (see the document schema fixtures).
3. Check the placement rules on the result (`note-original` and `instant-room` only):
   first the grid of every structured block in document order, then the collisions.
4. Report the first failure. A failure rejects the whole operation. The server never
   moves other items to make space.

## Rules

| `rule_id` | Rule | Codes |
| --- | --- | --- |
| `grid` | Structured blocks sit on whole cells of the placement grid. | `off_grid` |
| `block_overlap` | Two structured blocks must not overlap. | `block_overlap` |
| `group_overlap` | A group that contains a structured block collides with the outside rectangle of all its members. | `group_overlap` |
| `free_ink_overlap` | Free strokes, shapes and ink-only groups may overlap anything. Valid cases only. | none |
| `page_bounds` | Items stay inside the page width and at or below the page top. | `out_of_bounds` |
| `operation` | The operation is well formed and names existing items. | `invalid_operation`, `missing_item`, `immutable_field` |
| `document_schema` | The result passes the note document schema. | every document schema code |

Structured blocks: `text`, `image`, `file_card`, `link_card`, `website_card`,
`youtube_card`, `csv_table`, `pdf_preview`, `reports_embed`, `task_card`, `quiz_card`.

### Placement grid

- The editor has an invisible placement grid (B2.1). One cell is 8 page units.
- Every structured block, grouped or not, has `x`, `y` and `width` on whole cells
  (multiples of 8). `csv_table` and `reports_embed` set their own height, so their
  `height` is on whole cells too. The other structured kinds take a measured or
  proportional height (text, cards, images, PDF and YouTube previews), so their
  height can be any positive number.
- Free strokes and shapes never snap: their points, sizes and line widths are free.
- `off_grid` names the block and the path of the first value that is not a whole
  cell, for example `items[2].width`.

### Geometry

- The server checks stored geometry only. It never measures text. The app measures
  text with its text engine and stores the resulting block size.
- A block rectangle is `x`, `y`, `width`, `height` in page units.
- Collisions compare occupied cells: each collision rectangle (block or group) is
  widened outward to whole cells. A text block with a measured height of 81 occupies
  the cells down to 88, and a group whose ink starts at 85 occupies the cells from 80.
- An overlap is an intersection with a positive area of the occupied cells. Shared
  edges and corners are allowed.
- A stroke rectangle covers every stored point: all samples, all nodes and their
  `in` and `out` handles (1/100 page units, divided by 100). A shape rectangle is
  `center` plus and minus `radius` (circle), `x`, `y`, `width`, `height` (rectangle),
  the three points (triangle), or the nodes and handles (path). Line widths are not
  added.
- A group rectangle is the outside rectangle of all its members, ink and nested
  groups included.

### Collision units

- A structured group is a group that contains a structured block, directly or in a
  nested group. A group of only strokes and shapes (an ink-only group) behaves like
  free ink: it is never a unit and overlaps anything.
- Top level: every structured block and every structured group that is not a member
  of a group. Free strokes, shapes and ink-only groups are not units.
- Inside each group: its direct structured members and nested structured groups.
  Ink members and nested ink-only groups only make the group rectangle larger.
- Units of one level are compared in pairs, in document order: first unit 0 with
  1, 2, ..., then unit 1 with 2, 3, .... The top level comes first, then the groups
  in document order. The first overlapping pair is the error.
- If one unit of the pair is a group, the code is `group_overlap`. Otherwise it is
  `block_overlap`. `items` lists the two unit ids in document order.

### Page bounds

The document schema rejects positions outside the page with `out_of_bounds`. In
these cases its rule is `page_bounds` and `items` names the top-level item of the
failing path. Every other schema error has the rule `document_schema` and names the
same item when the path starts with `items[<n>]`.

## Operations

Operations change `note-original` and `instant-room` documents. A `note-typed`
document has no placement operations (`invalid_operation`).

```json
{"op": "add", "item": {"id": "t2", "kind": "text", "x": 0, "y": 0, "width": 16, "height": 10, "spans": []}}
{"op": "replace", "item": {"id": "t2", "kind": "text", "x": 8, "y": 0, "width": 16, "height": 10, "spans": []}}
{"op": "update", "id": "t2", "changes": {"x": 24, "title": null}}
{"op": "delete", "id": "t2"}
{"op": "page", "page": {"width": 800, "background": {"kind": "ruled", "spacing": 32, "visibility": 0.5}}}
{"op": "batch", "steps": [{"op": "delete", "id": "t1"}, {"op": "update", "id": "t2", "changes": {"x": 0}}]}
```

- `add` appends the item at the end of `items`.
- `replace` swaps the item with the same id. The kind must stay the same.
- `update` merges the top-level fields of `changes` into the item. A `null` value
  removes the field. `id` and `kind` cannot change.
- `delete` removes the named item only. Group members are not removed with the group.
- `page` replaces the page settings (`width` and `background`). Items do not move; an
  item outside the new page width gives `out_of_bounds`.
- `batch` applies its steps in order and checks only the final result. It is all or
  nothing. A batch cannot contain a batch and cannot be empty. Moving a group means a
  batch that moves every member.

Operation errors (rule `operation`):

- `invalid_operation`: unknown `op`, unknown or missing step field, wrong type, empty
  or nested batch, or an operation on a `note-typed` document. `items` is empty.
- `missing_item`: the step names an id that does not exist. `items` is that id.
- `immutable_field`: the step changes `id` or `kind`. `items` is the item id.
