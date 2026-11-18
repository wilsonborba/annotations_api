//! Editor placement rules, checked again on the server for every document that
//! reaches it (Instant rooms, synced notes and published notes).
//!
//! The server checks stored geometry only. It never measures text layout: the app
//! stores the block size that its text engine measured. Rules:
//! - `block_overlap`: two structured blocks must not overlap. An overlap is an
//!   intersection with a positive area, so shared edges are allowed.
//! - `group_overlap`: a group that contains a structured block collides with the
//!   outside rectangle of all its members. A group of free ink and shapes only
//!   behaves like free ink.
//! - `free_ink_overlap`: free strokes, shapes and ink-only groups may overlap anything.
//! - `page_bounds`: items stay inside the page width, at or below the page top.
//! - `grid`: structured blocks sit on the invisible placement grid of
//!   [`GRID_CELL`] page units. Their `x`, `y` and `width` are whole cells, and so is
//!   the `height` of the kinds whose height the user sets ([`AREA_KINDS`]). Ink and
//!   shape points never snap. Collisions use the occupied cells: each collision
//!   rectangle is widened outward to whole cells.
//!
//! An operation is applied to a working copy first. The whole operation is then
//! rejected if the result breaks a rule. Neighbors are never moved to make space.
use crate::{
    core::{Error, GRID_CELL, NoteLimits},
    domain::{
        note_documents::{self, DocumentError, Family, FileRef},
        stroke_samples,
    },
};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Structured kinds whose height the user sets (B3.1: the CSV table area and the
/// Reports display area). Other structured kinds take a measured or proportional
/// height, so only their position and width are on the grid.
pub const AREA_KINDS: [&str; 2] = ["csv_table", "reports_embed"];

/// Rule ids. They are stable and shared with the app fixtures.
pub const RULES: [&str; 7] = [
    "grid",
    "block_overlap",
    "group_overlap",
    "free_ink_overlap",
    "page_bounds",
    "operation",
    "document_schema",
];

/// A rejected edit. `rule` names the broken rule, `code` is the stable error code
/// and `items` names the conflicting item ids in document order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuleError {
    pub rule: &'static str,
    pub code: &'static str,
    pub path: String,
    pub items: Vec<String>,
}
impl RuleError {
    fn new(rule: &'static str, code: &'static str, path: impl Into<String>) -> Self {
        Self {
            rule,
            code,
            path: path.into(),
            items: vec![],
        }
    }
    fn with_items(mut self, items: Vec<String>) -> Self {
        self.items = items;
        self
    }
    pub fn message(&self) -> &'static str {
        match self.code {
            "off_grid" => "Structured blocks must sit on whole cells of the placement grid",
            "block_overlap" => "Structured blocks must not overlap",
            "group_overlap" => {
                "The outside rectangle of a group must not overlap another block or group"
            }
            "invalid_operation" => "The operation is not valid",
            "missing_item" => "The operation names an item that does not exist",
            "immutable_field" => "The id and the kind of an item cannot change",
            code => DocumentError::new(code, "").message(),
        }
    }
    /// A schema error of the working copy. `out_of_bounds` belongs to `page_bounds`.
    fn document(e: DocumentError, document: &Value) -> Self {
        let rule = if e.code == "out_of_bounds" {
            "page_bounds"
        } else {
            "document_schema"
        };
        let items = item_at(document, &e.path).into_iter().collect();
        Self::new(rule, e.code, e.path).with_items(items)
    }
}
impl From<RuleError> for Error {
    fn from(e: RuleError) -> Self {
        let mut text = format!("{}: {} (rule {}", e.code, e.message(), e.rule);
        if !e.items.is_empty() {
            text.push_str(&format!("; items {}", e.items.join(", ")));
        }
        text.push(')');
        if !e.path.is_empty() {
            text.push_str(&format!(" at {}", e.path));
        }
        Error::bad(text)
    }
}
type R<T> = std::result::Result<T, RuleError>;

/// The id of the top-level item that a path such as `items[3].samples.x[2]` names.
fn item_at(document: &Value, path: &str) -> Option<String> {
    let rest = path.strip_prefix("items[")?;
    let index: usize = rest[..rest.find(']')?].parse().ok()?;
    document["items"][index]["id"].as_str().map(str::to_string)
}

/// The result of an accepted edit: the new document and its file references.
#[derive(Debug)]
pub struct Edited {
    pub document: Value,
    pub files: Vec<FileRef>,
}

/// Apply `operation` (if any) to a working copy of `document`, then check the
/// schema and the placement rules. The stored document is never changed here.
pub fn check_edit(
    family: Family,
    version: i64,
    document: &Value,
    operation: Option<&Value>,
    limits: &NoteLimits,
) -> R<Edited> {
    let document = match operation {
        Some(op) => apply_operation(family, document, op, limits)?,
        None => document.clone(),
    };
    let files = note_documents::check_document(family, version, &document, limits)
        .map_err(|e| RuleError::document(e, &document))?;
    if family != Family::Typed {
        check_placement(&document)?;
    }
    Ok(Edited { document, files })
}

// Operations -----------------------------------------------------------------

fn op_fail<T>(code: &'static str, path: impl Into<String>) -> R<T> {
    Err(RuleError::new("operation", code, path))
}

/// Apply one operation to a copy of a `note-original` or `instant-room` document.
///
/// Steps: `{"op":"add","item":{..}}`, `{"op":"replace","item":{..}}`,
/// `{"op":"update","id":"..","changes":{..}}` (top-level merge, null removes a field),
/// `{"op":"delete","id":".."}`, `{"op":"page","page":{..}}` (replaces the page
/// settings: width and background) and `{"op":"batch","steps":[..]}` (applied in
/// order, all or nothing). The operation touches only the items that it names.
pub fn apply_operation(
    family: Family,
    document: &Value,
    operation: &Value,
    limits: &NoteLimits,
) -> R<Value> {
    if family == Family::Typed {
        return op_fail("invalid_operation", "operation");
    }
    let mut copy = document.clone();
    if copy.get("items").and_then(Value::as_array).is_none() {
        return Err(RuleError::new("document_schema", "missing_field", "items"));
    }
    let op = step_obj(operation, "operation")?;
    if op_kind(op, "operation")? == "batch" {
        allow(op, &["op", "steps"], "operation")?;
        let steps = op
            .get("steps")
            .and_then(Value::as_array)
            .ok_or_else(|| RuleError::new("operation", "invalid_operation", "operation.steps"))?;
        if steps.is_empty() || steps.len() > limits.max_items {
            return op_fail("invalid_operation", "operation.steps");
        }
        for (i, s) in steps.iter().enumerate() {
            let path = format!("operation.steps[{i}]");
            let step = step_obj(s, &path)?;
            apply_document_step(&mut copy, step, &path)?;
        }
    } else {
        apply_document_step(&mut copy, op, "operation")?;
    }
    Ok(copy)
}

/// One step on the working copy: page settings, or one item step.
fn apply_document_step(copy: &mut Value, step: &Map<String, Value>, path: &str) -> R<()> {
    if op_kind(step, path)? == "page" {
        allow(step, &["op", "page"], path)?;
        copy["page"] = Value::Object(target(step, "page", path)?.clone());
        return Ok(());
    }
    let items = copy
        .get_mut("items")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| RuleError::new("document_schema", "missing_field", "items"))?;
    apply_step(items, step, path)
}

fn step_obj<'a>(v: &'a Value, path: &str) -> R<&'a Map<String, Value>> {
    v.as_object()
        .ok_or_else(|| RuleError::new("operation", "invalid_operation", path))
}
fn op_kind<'a>(step: &'a Map<String, Value>, path: &str) -> R<&'a str> {
    match step.get("op").and_then(Value::as_str) {
        Some(k @ ("add" | "replace" | "update" | "delete" | "page" | "batch")) => Ok(k),
        _ => op_fail("invalid_operation", format!("{path}.op")),
    }
}
fn allow(step: &Map<String, Value>, keys: &[&str], path: &str) -> R<()> {
    let mut unknown: Vec<&String> = step
        .keys()
        .filter(|k| !keys.contains(&k.as_str()))
        .collect();
    unknown.sort();
    match unknown.first() {
        Some(k) => op_fail("invalid_operation", format!("{path}.{k}")),
        None => Ok(()),
    }
}
fn target<'a>(step: &'a Map<String, Value>, key: &str, path: &str) -> R<&'a Map<String, Value>> {
    step.get(key)
        .and_then(Value::as_object)
        .ok_or_else(|| RuleError::new("operation", "invalid_operation", format!("{path}.{key}")))
}
fn find(items: &[Value], id: &str, path: &str) -> R<usize> {
    items
        .iter()
        .position(|it| it["id"].as_str() == Some(id))
        .ok_or_else(|| {
            RuleError::new("operation", "missing_item", path).with_items(vec![id.to_string()])
        })
}

fn apply_step(items: &mut Vec<Value>, step: &Map<String, Value>, path: &str) -> R<()> {
    match op_kind(step, path)? {
        "add" => {
            allow(step, &["op", "item"], path)?;
            items.push(Value::Object(target(step, "item", path)?.clone()));
        }
        "replace" => {
            allow(step, &["op", "item"], path)?;
            let item = target(step, "item", path)?;
            let id_path = format!("{path}.item.id");
            let Some(id) = item.get("id").and_then(Value::as_str) else {
                return op_fail("invalid_operation", id_path);
            };
            let i = find(items, id, &id_path)?;
            if items[i].get("kind") != item.get("kind") {
                return op_fail("immutable_field", format!("{path}.item.kind"))
                    .map_err(|e| e.with_items(vec![id.to_string()]));
            }
            items[i] = Value::Object(item.clone());
        }
        "update" => {
            allow(step, &["op", "id", "changes"], path)?;
            let id = step_id(step, path)?;
            let changes = target(step, "changes", path)?;
            let i = find(items, id, &format!("{path}.id"))?;
            let mut keys: Vec<&String> = changes.keys().collect();
            keys.sort();
            if let Some(k) = keys.iter().find(|k| ["id", "kind"].contains(&k.as_str())) {
                return op_fail("immutable_field", format!("{path}.changes.{k}"))
                    .map_err(|e| e.with_items(vec![id.to_string()]));
            }
            let Some(item) = items[i].as_object_mut() else {
                return Err(RuleError::new(
                    "document_schema",
                    "invalid_type",
                    format!("items[{i}]"),
                ));
            };
            for (k, v) in changes {
                if v.is_null() {
                    item.remove(k);
                } else {
                    item.insert(k.clone(), v.clone());
                }
            }
        }
        "delete" => {
            allow(step, &["op", "id"], path)?;
            let id = step_id(step, path)?;
            let i = find(items, id, &format!("{path}.id"))?;
            items.remove(i);
        }
        // A batch inside a batch is not allowed; page steps are handled above.
        _ => return op_fail("invalid_operation", format!("{path}.op")),
    }
    Ok(())
}
fn step_id<'a>(step: &'a Map<String, Value>, path: &str) -> R<&'a str> {
    step.get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| RuleError::new("operation", "invalid_operation", format!("{path}.id")))
}

// Placement ------------------------------------------------------------------

/// An axis-aligned rectangle in page units.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Rect {
    x0: f64,
    y0: f64,
    x1: f64,
    y1: f64,
}
impl Rect {
    /// Positive-area intersection. Shared edges and corners are not an overlap.
    fn overlaps(&self, o: &Rect) -> bool {
        self.x1.min(o.x1) > self.x0.max(o.x0) && self.y1.min(o.y1) > self.y0.max(o.y0)
    }
    fn union(&self, o: &Rect) -> Rect {
        Rect {
            x0: self.x0.min(o.x0),
            y0: self.y0.min(o.y0),
            x1: self.x1.max(o.x1),
            y1: self.y1.max(o.y1),
        }
    }
    /// The occupied cells: the rectangle widened outward to whole grid cells.
    fn cells(&self) -> Rect {
        Rect {
            x0: (self.x0 / GRID_CELL).floor() * GRID_CELL,
            y0: (self.y0 / GRID_CELL).floor() * GRID_CELL,
            x1: (self.x1 / GRID_CELL).ceil() * GRID_CELL,
            y1: (self.y1 / GRID_CELL).ceil() * GRID_CELL,
        }
    }
    fn of_points(points: impl IntoIterator<Item = (f64, f64)>) -> Option<Rect> {
        points.into_iter().fold(None, |acc, (x, y)| {
            let p = Rect {
                x0: x,
                y0: y,
                x1: x,
                y1: y,
            };
            Some(acc.map_or(p, |r: Rect| r.union(&p)))
        })
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Role {
    Block,
    Group,
    Ink,
}

/// Check the placement rules on a schema-valid `note-original` or `instant-room`
/// document. The first conflict in document order is reported.
pub fn check_placement(document: &Value) -> R<()> {
    let empty = vec![];
    let items = document["items"].as_array().unwrap_or(&empty);
    let index: HashMap<&str, usize> = items
        .iter()
        .enumerate()
        .filter_map(|(i, it)| it["id"].as_str().map(|id| (id, i)))
        .collect();
    let role = |it: &Value| match it["kind"].as_str() {
        Some("group") => Role::Group,
        Some("stroke" | "shape") => Role::Ink,
        _ => Role::Block,
    };
    // Every structured block, grouped or not, sits on whole grid cells.
    for (i, it) in items.iter().enumerate() {
        if role(it) == Role::Block {
            check_grid(it, i)?;
        }
    }
    // Direct members of each group, in document order.
    let mut members: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut grouped = vec![false; items.len()];
    for (g, it) in items.iter().enumerate() {
        if role(it) != Role::Group {
            continue;
        }
        let mut list: Vec<usize> = it["members"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|m| m.as_str().and_then(|m| index.get(m).copied()))
            .collect();
        list.sort_unstable();
        for m in &list {
            grouped[*m] = true;
        }
        members.insert(g, list);
    }
    let mut rects: Vec<Option<Rect>> = vec![None; items.len()];
    for i in 0..items.len() {
        rect(items, &members, &mut rects, i, 0);
    }
    // Collisions compare occupied cells, so a block with a measured height and a
    // group that holds ink take every cell that they touch.
    let rects: Vec<Option<Rect>> = rects.iter().map(|r| r.map(|r| r.cells())).collect();
    // A group collides only when it contains a structured block (at any depth). A
    // group of free ink and shapes only behaves like free ink.
    let mut structured: Vec<Option<bool>> = vec![None; items.len()];
    for i in 0..items.len() {
        has_block(items, &members, &mut structured, i, 0);
    }
    let collides = |i: &usize| role(&items[*i]) != Role::Ink && structured[*i] == Some(true);
    let top: Vec<usize> = (0..items.len())
        .filter(|i| !grouped[*i] && collides(i))
        .collect();
    check_level(items, &rects, &top, &|i| role(&items[i]))?;
    // Inside each group, its structured members and nested groups collide too.
    let mut groups: Vec<&usize> = members.keys().collect();
    groups.sort_unstable();
    for g in groups {
        let level: Vec<usize> = members[g].iter().copied().filter(|i| collides(i)).collect();
        check_level(items, &rects, &level, &|i| role(&items[i]))?;
    }
    Ok(())
}

/// The grid rule of one structured block at `items[i]` (schema-valid geometry).
fn check_grid(it: &Value, i: usize) -> R<()> {
    let mut keys = vec!["x", "y", "width"];
    if it["kind"].as_str().is_some_and(|k| AREA_KINDS.contains(&k)) {
        keys.push("height");
    }
    for k in keys {
        let v = it[k].as_f64().unwrap_or_default();
        if v.rem_euclid(GRID_CELL) != 0.0 {
            let id = it["id"].as_str().unwrap_or_default().to_string();
            return Err(
                RuleError::new("grid", "off_grid", format!("items[{i}].{k}")).with_items(vec![id]),
            );
        }
    }
    Ok(())
}

fn check_level(
    items: &[Value],
    rects: &[Option<Rect>],
    units: &[usize],
    role: &dyn Fn(usize) -> Role,
) -> R<()> {
    for (n, &a) in units.iter().enumerate() {
        let Some(ra) = rects[a] else { continue };
        for &b in &units[n + 1..] {
            let Some(rb) = rects[b] else { continue };
            if ra.overlaps(&rb) {
                let code = if role(a) == Role::Group || role(b) == Role::Group {
                    "group_overlap"
                } else {
                    "block_overlap"
                };
                let id = |i: usize| items[i]["id"].as_str().unwrap_or_default().to_string();
                return Err(RuleError::new(code, code, format!("items[{b}]"))
                    .with_items(vec![id(a), id(b)]));
            }
        }
    }
    Ok(())
}

/// Whether an item is a structured block or a group that contains one. Cached.
fn has_block(
    items: &[Value],
    members: &HashMap<usize, Vec<usize>>,
    cache: &mut Vec<Option<bool>>,
    i: usize,
    depth: usize,
) -> bool {
    if let Some(v) = cache[i] {
        return v;
    }
    let v = match items[i]["kind"].as_str() {
        Some("stroke" | "shape") => false,
        Some("group") if depth <= items.len() => members.get(&i).is_some_and(|list| {
            list.iter()
                .any(|m| has_block(items, members, cache, *m, depth + 1))
        }),
        Some("group") => false,
        _ => true,
    };
    cache[i] = Some(v);
    v
}

/// The rectangle of an item: block geometry, every stored point of ink, or the
/// outside rectangle of all members of a group. Cached in `rects`.
fn rect(
    items: &[Value],
    members: &HashMap<usize, Vec<usize>>,
    rects: &mut Vec<Option<Rect>>,
    i: usize,
    depth: usize,
) -> Option<Rect> {
    if rects[i].is_some() || depth > items.len() {
        return rects[i];
    }
    let it = &items[i];
    let num = |v: &Value| v.as_f64().unwrap_or(0.0);
    let r = match it["kind"].as_str() {
        Some("group") => members.get(&i).and_then(|list| {
            list.iter()
                .filter_map(|m| rect(items, members, rects, *m, depth + 1))
                .reduce(|a, b| a.union(&b))
        }),
        Some("stroke") => {
            // A schema-valid stroke always decodes; the samples cover the line.
            let s = &it["samples"];
            let columns = stroke_samples::decode(
                s["count"].as_u64().unwrap_or(0) as usize,
                s["data"].as_str().unwrap_or_default(),
            )
            .unwrap_or_default();
            let samples = columns
                .x
                .iter()
                .zip(&columns.y)
                .map(|(x, y)| (*x as f64 / 100.0, *y as f64 / 100.0));
            Rect::of_points(samples.chain(node_points(&it["nodes"])))
        }
        Some("shape") => match it["shape"].as_str() {
            Some("circle") => {
                let (cx, cy, r) = (
                    num(&it["center"]["x"]),
                    num(&it["center"]["y"]),
                    num(&it["radius"]),
                );
                Rect::of_points([(cx - r, cy - r), (cx + r, cy + r)])
            }
            Some("triangle") => Rect::of_points(
                it["points"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|p| (num(&p["x"]), num(&p["y"]))),
            ),
            Some("path") => Rect::of_points(node_points(&it["nodes"])),
            _ => block_rect(it),
        },
        _ => block_rect(it),
    };
    rects[i] = r;
    r
}
fn block_rect(it: &Value) -> Option<Rect> {
    let (x, y) = (it["x"].as_f64()?, it["y"].as_f64()?);
    let (w, h) = (it["width"].as_f64()?, it["height"].as_f64()?);
    Some(Rect {
        x0: x,
        y0: y,
        x1: x + w,
        y1: y + h,
    })
}
/// Nodes and their curve handles, stored in 1/100 page units.
fn node_points(nodes: &Value) -> Vec<(f64, f64)> {
    let mut out = vec![];
    for n in nodes.as_array().into_iter().flatten() {
        for p in [n, &n["in"], &n["out"]] {
            if let (Some(x), Some(y)) = (p["x"].as_f64(), p["y"].as_f64()) {
                out.push((x / 100.0, y / 100.0));
            }
        }
    }
    out
}
