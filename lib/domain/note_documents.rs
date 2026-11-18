//! Validation of v2 note documents before any CouchDB write.
//!
//! A published schema version alone cannot teach the server a new item kind:
//! every family version is also known here, and every nested item is checked.
use crate::{
    core::{Error, NoteLimits},
    dal::Store,
    domain::schemas,
};
use regex::Regex;
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
};

/// Document families. Each published version needs validator support here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    Original,
    Typed,
    Instant,
}
impl Family {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "note-original" => Some(Self::Original),
            "note-typed" => Some(Self::Typed),
            "instant-room" => Some(Self::Instant),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Original => "note-original",
            Self::Typed => "note-typed",
            Self::Instant => "instant-room",
        }
    }
    /// Schema versions that this validator code knows.
    pub fn supports(self, version: i64) -> bool {
        version == 1
    }
}

/// A rejected document. `code` is stable and machine-readable. `path` names the
/// offending location (for example `items[3].samples[0].x`); it never carries content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentError {
    pub code: &'static str,
    pub path: String,
}
impl DocumentError {
    pub fn new(code: &'static str, path: impl Into<String>) -> Self {
        Self {
            code,
            path: path.into(),
        }
    }
    pub fn message(&self) -> &'static str {
        match self.code {
            "invalid_json" => "The document is not valid JSON",
            "non_finite_number" => "Numbers must be finite",
            "document_too_large" => "The document is larger than the allowed size",
            "too_many_items" => "The document has more items than allowed",
            "too_many_samples" => "The stroke has more samples than allowed",
            "too_many_nodes" => "The stroke or path has more nodes than allowed",
            "schema_not_published" => "A registered published schema version is required",
            "unsupported_schema_version" => "No validator exists for this schema version",
            "invalid_type" => "The value has the wrong type",
            "missing_field" => "A required field is missing",
            "unknown_field" => "The field is not allowed here",
            "unknown_kind" => "The item kind is not known for this schema version",
            "kind_not_allowed" => "The item kind is not allowed in this document family",
            "invalid_variant" => "The value is not one of the allowed variants",
            "invalid_id" => "Ids use 1 to 64 letters, digits, hyphens or underscores",
            "invalid_reference" => {
                "References use 1 to 128 letters, digits, hyphens or underscores"
            }
            "duplicate_id" => "Item ids must be unique in the document",
            "out_of_bounds" => "The position is outside the page",
            "out_of_range" => "The number is outside the allowed range",
            "invalid_size" => "Sizes must be greater than zero",
            "invalid_color" => "Colors use the #RRGGBBAA format",
            "invalid_url" => "Only HTTP(S) links without credentials are supported",
            "markup_not_allowed" => "Text must not contain HTML or scripts",
            "invalid_text" => "Text contains a control character",
            "empty_array" => "The list must not be empty",
            "invalid_count" => "The list has the wrong number of entries",
            "invalid_sample_order" => "Sample times start at 0 and never decrease",
            "missing_reference" => "The referenced item does not exist in the document",
            "missing_file" => "The referenced file is not available to this owner",
            "group_cycle" => "Groups must not contain themselves",
            "multiple_groups" => "An item belongs to at most one group",
            "storage_unavailable" => "Storage is unavailable",
            _ => "The document is invalid",
        }
    }
}
impl From<DocumentError> for Error {
    fn from(e: DocumentError) -> Self {
        if e.code == "storage_unavailable" {
            return Error::unavailable();
        }
        let at = if e.path.is_empty() {
            String::new()
        } else {
            format!(" at {}", e.path)
        };
        Error::bad(format!("{}: {}{at}", e.code, e.message()))
    }
}
type R<T> = std::result::Result<T, DocumentError>;
fn fail<T>(code: &'static str, path: impl Into<String>) -> R<T> {
    Err(DocumentError::new(code, path))
}

/// Where file references must exist.
pub enum FileScope<'a> {
    /// Ready managed files of this owner (`files` table).
    Owner(&'a str),
    /// Files uploaded to the current Instant room.
    Room(&'a HashSet<String>),
}

/// A managed file id found in a document, with its path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRef {
    pub file_id: String,
    pub path: String,
}

/// Item kinds per family (version 1).
pub const ORIGINAL_KINDS: [&str; 14] = [
    "text",
    "image",
    "file_card",
    "link_card",
    "website_card",
    "youtube_card",
    "csv_table",
    "pdf_preview",
    "reports_embed",
    "task_card",
    "quiz_card",
    "group",
    "stroke",
    "shape",
];
pub const INSTANT_KINDS: [&str; 12] = [
    "text",
    "image",
    "file_card",
    "link_card",
    "website_card",
    "youtube_card",
    "csv_table",
    "pdf_preview",
    "reports_embed",
    "group",
    "stroke",
    "shape",
];
pub const TYPED_KINDS: [&str; 15] = [
    "heading",
    "paragraph",
    "list",
    "table",
    "figure",
    "handwriting",
    "file_card",
    "link_card",
    "website_card",
    "youtube_card",
    "csv_table",
    "pdf_preview",
    "reports_embed",
    "task_card",
    "quiz_card",
];

/// Parse raw request bytes. Use this before `check_document` when raw bytes exist,
/// so that the byte limit and non-finite numbers (for example `1e999`) give stable codes.
pub fn parse_document(raw: &[u8], limits: &NoteLimits) -> R<Value> {
    if raw.len() > limits.max_document_bytes {
        return fail("document_too_large", "");
    }
    serde_json::from_slice(raw).map_err(|e| {
        if e.to_string().starts_with("number out of range") {
            DocumentError::new("non_finite_number", "")
        } else {
            DocumentError::new("invalid_json", "")
        }
    })
}

/// Validate a document against the published registry entry and the item validator,
/// then check that every file reference exists in `files`. Writes nothing.
pub async fn validate_document(
    store: &Store,
    name: &str,
    version: Option<i64>,
    files: FileScope<'_>,
    body: &Value,
    limits: &NoteLimits,
) -> R<i64> {
    validate_document_files(store, name, version, files, body, limits)
        .await
        .map(|(version, _)| version)
}

/// Like `validate_document`, and also return the file references of the document.
pub async fn validate_document_files(
    store: &Store,
    name: &str,
    version: Option<i64>,
    files: FileScope<'_>,
    body: &Value,
    limits: &NoteLimits,
) -> R<(i64, Vec<FileRef>)> {
    let family =
        Family::parse(name).ok_or_else(|| DocumentError::new("unsupported_schema_version", ""))?;
    let (version, fields) = schemas::published_fields(store, name, version)
        .await
        .map_err(|e| {
            if e.0.is_server_error() {
                DocumentError::new("storage_unavailable", "")
            } else {
                DocumentError::new("schema_not_published", "")
            }
        })?;
    // Order: size and item validation first, so that registry shape errors keep
    // the precise item codes; registry fields then confirm the root structure.
    let refs = check_document(family, version, body, limits)?;
    if let Err((code, path)) = schemas::check_fields(&fields, body) {
        return fail(code, path);
    }
    match files {
        FileScope::Room(ids) => {
            if let Some(r) = refs.iter().find(|r| !ids.contains(&r.file_id)) {
                return fail("missing_file", r.path.clone());
            }
        }
        FileScope::Owner(owner) => {
            let mut known = HashSet::new();
            for r in &refs {
                if known.contains(&r.file_id) {
                    continue;
                }
                let row = sqlx::query(
                    "SELECT file_id FROM files WHERE file_id=$1 AND owner_id=$2 AND status='ready'",
                )
                .bind(&r.file_id)
                .bind(owner)
                .fetch_optional(&store.db)
                .await
                .map_err(|e| {
                    tracing::error!(error=%e,"File reference check failed");
                    DocumentError::new("storage_unavailable", "")
                })?;
                if row.is_none() {
                    return fail("missing_file", r.path.clone());
                }
                known.insert(r.file_id.clone());
            }
        }
    }
    Ok((version, refs))
}

/// Pure structural validation of one document. Returns the file references to check.
pub fn check_document(
    family: Family,
    version: i64,
    body: &Value,
    limits: &NoteLimits,
) -> R<Vec<FileRef>> {
    if !family.supports(version) {
        return fail("unsupported_schema_version", "");
    }
    let bytes = serde_json::to_vec(body)
        .map(|v| v.len())
        .unwrap_or(usize::MAX);
    if bytes > limits.max_document_bytes {
        return fail("document_too_large", "");
    }
    let mut ctx = Ctx {
        family,
        limits,
        ids: HashMap::new(),
        files: vec![],
        groups: vec![],
        items: 0,
    };
    let root = Obj::new(body, "")?;
    match family {
        Family::Original | Family::Instant => {
            root.allow(&["page", "items"])?;
            let page = Obj::new(root.req("page")?, "page")?;
            let width = check_page(&page)?;
            let items = root.arr("items")?;
            ctx.count(items.len(), "items")?;
            let area = Area {
                width,
                height: None,
            };
            for (i, item) in items.iter().enumerate() {
                ctx.paper_item(item, &format!("items[{i}]"), area)?;
            }
            ctx.check_groups()?;
        }
        Family::Typed => {
            root.allow(&["blocks"])?;
            let blocks = root.arr("blocks")?;
            ctx.count(blocks.len(), "blocks")?;
            for (i, block) in blocks.iter().enumerate() {
                ctx.typed_block(block, &format!("blocks[{i}]"))?;
            }
        }
    }
    Ok(ctx.files)
}

fn check_page(page: &Obj) -> R<f64> {
    page.allow(&["width", "background"])?;
    let width = page.num("width")?;
    if width <= 0.0 {
        return fail("invalid_size", page.at("width"));
    }
    let bg = Obj::new(page.req("background")?, &page.at("background"))?;
    let kind = bg.variant("kind", &["blank", "ruled", "grid", "dots"])?;
    if kind == "blank" {
        bg.allow(&["kind"])?;
    } else {
        bg.allow(&["kind", "spacing", "visibility"])?;
        if bg.num("spacing")? <= 0.0 {
            return fail("invalid_size", bg.at("spacing"));
        }
        bg.range("visibility", 0.0, 1.0)?;
    }
    Ok(width)
}

/// The drawing area of free ink: the page (open downward) or a typed handwriting box.
#[derive(Clone, Copy)]
struct Area {
    width: f64,
    height: Option<f64>,
}
impl Area {
    /// A point in integer 1/100 page units.
    fn hundredths(&self, x: i64, y: i64, path: &str) -> R<()> {
        self.point(x as f64 / 100.0, y as f64 / 100.0, path)
    }
    fn point(&self, x: f64, y: f64, path: &str) -> R<()> {
        if x < 0.0 || x > self.width || y < 0.0 || self.height.is_some_and(|h| y > h) {
            return fail("out_of_bounds", path);
        }
        Ok(())
    }
}

/// A group: id, path and members with their paths.
type Group = (String, String, Vec<(String, String)>);
struct Ctx<'a> {
    family: Family,
    limits: &'a NoteLimits,
    ids: HashMap<String, String>,
    files: Vec<FileRef>,
    groups: Vec<Group>,
    items: usize,
}
impl Ctx<'_> {
    fn count(&mut self, n: usize, path: &str) -> R<()> {
        self.items += n;
        if self.items > self.limits.max_items {
            return fail("too_many_items", path);
        }
        Ok(())
    }
    fn id(&mut self, item: &Obj) -> R<()> {
        let id = item.str("id")?;
        if !token(id, 64) {
            return fail("invalid_id", item.at("id"));
        }
        if self.ids.insert(id.to_string(), item.path.clone()).is_some() {
            return fail("duplicate_id", item.at("id"));
        }
        Ok(())
    }
    fn file(&mut self, item: &Obj, key: &str) -> R<()> {
        let id = item.str(key)?;
        if !token(id, 64) {
            return fail("invalid_reference", item.at(key));
        }
        self.files.push(FileRef {
            file_id: id.to_string(),
            path: item.at(key),
        });
        Ok(())
    }

    /// An item of `note-original` or `instant-room`.
    fn paper_item(&mut self, value: &Value, path: &str, area: Area) -> R<()> {
        let item = Obj::new(value, path)?;
        let kind = item.str("kind")?;
        if !ORIGINAL_KINDS.contains(&kind) {
            return fail("unknown_kind", item.at("kind"));
        }
        if self.family == Family::Instant && !INSTANT_KINDS.contains(&kind) {
            return fail("kind_not_allowed", item.at("kind"));
        }
        self.id(&item)?;
        match kind {
            "stroke" => return self.stroke(&item, area),
            "shape" => return self.shape(&item, area),
            "group" => {
                item.allow(&["id", "kind", "members"])?;
                let members = item.arr("members")?;
                if members.len() < 2 {
                    return fail("invalid_count", item.at("members"));
                }
                let mut list = vec![];
                for (i, m) in members.iter().enumerate() {
                    let p = format!("{}[{i}]", item.at("members"));
                    let m = m
                        .as_str()
                        .ok_or_else(|| DocumentError::new("invalid_type", p.clone()))?;
                    if !token(m, 64) {
                        return fail("invalid_id", p);
                    }
                    list.push((m.to_string(), p));
                }
                let id = item.str("id")?.to_string();
                self.groups.push((id, item.path.clone(), list));
                return Ok(());
            }
            _ => {}
        }
        // Structured block: geometry inside the page width, y at or above 0.
        let x = item.num("x")?;
        let y = item.num("y")?;
        let w = item.num("width")?;
        let h = item.num("height")?;
        if w <= 0.0 {
            return fail("invalid_size", item.at("width"));
        }
        if h <= 0.0 {
            return fail("invalid_size", item.at("height"));
        }
        if x < 0.0 || x + w > area.width {
            return fail("out_of_bounds", item.at("x"));
        }
        if y < 0.0 {
            return fail("out_of_bounds", item.at("y"));
        }
        const GEOMETRY: [&str; 6] = ["id", "kind", "x", "y", "width", "height"];
        if kind == "text" {
            item.allow_with(&GEOMETRY, &["spans"])?;
            return spans(&item, "spans");
        }
        if kind == "image" {
            item.allow_with(&GEOMETRY, &["file_id"])?;
            return self.file(&item, "file_id");
        }
        self.card(&item, kind, &GEOMETRY)
    }

    /// Cards shared by every family. `base` lists the keys of the container.
    fn card(&mut self, item: &Obj, kind: &str, base: &[&str]) -> R<()> {
        match kind {
            "file_card" | "csv_table" | "pdf_preview" => {
                item.allow_with(base, &["file_id"])?;
                self.file(item, "file_id")
            }
            "link_card" => {
                item.allow_with(base, &["url", "title", "site_name"])?;
                url(item, "url", false)?;
                opt_text(item, "title")?;
                opt_text(item, "site_name")
            }
            "website_card" => {
                item.allow_with(
                    base,
                    &[
                        "url",
                        "title",
                        "site_name",
                        "description",
                        "preview_file_id",
                    ],
                )?;
                url(item, "url", false)?;
                for k in ["title", "site_name", "description"] {
                    opt_text(item, k)?;
                }
                if item.opt("preview_file_id").is_some() {
                    self.file(item, "preview_file_id")?;
                }
                Ok(())
            }
            "youtube_card" => {
                item.allow_with(base, &["url", "title", "preview_file_id"])?;
                url(item, "url", true)?;
                opt_text(item, "title")?;
                if item.opt("preview_file_id").is_some() {
                    self.file(item, "preview_file_id")?;
                }
                Ok(())
            }
            "reports_embed" => {
                let source = item.variant("source", &["link", "upload"])?;
                if self.family == Family::Instant && source != "link" {
                    return fail("kind_not_allowed", item.at("source"));
                }
                reference(item, "report_id")?;
                if item.variant("mode", &["live", "snapshot"])? == "snapshot" {
                    item.allow_with(
                        base,
                        &["report_id", "mode", "source", "snapshot_id", "captured_at"],
                    )?;
                    reference(item, "snapshot_id")?;
                    if item.int("captured_at")? <= 0 {
                        return fail("out_of_range", item.at("captured_at"));
                    }
                } else {
                    item.allow_with(base, &["report_id", "mode", "source"])?;
                }
                Ok(())
            }
            "task_card" => {
                item.allow_with(base, &["task_id"])?;
                reference(item, "task_id")
            }
            "quiz_card" => {
                item.allow_with(base, &["study_id", "quiz_id"])?;
                if item.opt("study_id").is_none() && item.opt("quiz_id").is_none() {
                    return fail("missing_field", item.at("quiz_id"));
                }
                for k in ["study_id", "quiz_id"] {
                    if item.opt(k).is_some() {
                        reference(item, k)?;
                    }
                }
                Ok(())
            }
            _ => fail("unknown_kind", item.at("kind")),
        }
    }

    fn ink(&mut self, item: &Obj, area: Area) -> R<()> {
        item.variant("kind", &["stroke", "shape"])?;
        if item.str("kind")? == "stroke" {
            self.stroke(item, area)
        } else {
            self.shape(item, area)
        }
    }

    fn stroke(&mut self, item: &Obj, area: Area) -> R<()> {
        let tool = item.variant("tool", &["round", "flat", "brush", "highlighter"])?;
        const KEYS: [&str; 9] = [
            "id",
            "kind",
            "tool",
            "brush_version",
            "width",
            "color",
            "opacity",
            "samples",
            "nodes",
        ];
        if tool == "flat" {
            item.allow_with(&KEYS, &["tip_angle"])?;
            item.range("tip_angle", -std::f64::consts::PI, std::f64::consts::PI)?;
        } else {
            item.allow(&KEYS)?;
        }
        if item.int("brush_version")? < 1 {
            return fail("out_of_range", item.at("brush_version"));
        }
        if item.num("width")? <= 0.0 {
            return fail("invalid_size", item.at("width"));
        }
        color(item, "color")?;
        opacity(item)?;
        // Samples are stored column by column as integers: x and y in 1/100 page
        // units, t in ms from the stroke start, pressure 0 to 1000, tilt in mrad.
        let samples = Obj::new(item.req("samples")?, &item.at("samples"))?;
        samples.allow(&["x", "y", "t", "pressure", "tilt"])?;
        let xs = samples.ints("x")?;
        if xs.is_empty() {
            return fail("empty_array", samples.at("x"));
        }
        if xs.len() > self.limits.max_samples_per_stroke {
            return fail("too_many_samples", item.at("samples"));
        }
        let column = |key: &str, required: bool| -> R<Option<Vec<i64>>> {
            if !required && samples.opt(key).is_none() {
                return Ok(None);
            }
            let values = samples.ints(key)?;
            if values.len() != xs.len() {
                return fail("invalid_count", samples.at(key));
            }
            Ok(Some(values))
        };
        let ys = column("y", true)?.unwrap_or_default();
        let ts = column("t", true)?.unwrap_or_default();
        for (i, (x, y)) in xs.iter().zip(&ys).enumerate() {
            area.hundredths(*x, *y, &format!("{}[{i}]", samples.at("x")))?;
        }
        for (i, t) in ts.iter().enumerate() {
            if (i == 0 && *t != 0) || (i > 0 && *t < ts[i - 1]) {
                return fail("invalid_sample_order", format!("{}[{i}]", samples.at("t")));
            }
        }
        for (key, max) in [("pressure", PRESSURE_MAX), ("tilt", TILT_MAX)] {
            if let Some(values) = column(key, false)?
                && let Some(i) = values.iter().position(|v| *v < 0 || *v > max)
            {
                return fail("out_of_range", format!("{}[{i}]", samples.at(key)));
            }
        }
        self.nodes(item, "nodes", area, Some(xs.len()))
    }

    /// Editable nodes and handles use integer 1/100 page units.
    fn nodes(&self, item: &Obj, key: &str, area: Area, samples: Option<usize>) -> R<()> {
        let nodes = item.arr(key)?;
        if nodes.is_empty() {
            return fail("empty_array", item.at(key));
        }
        if nodes.len() > self.limits.max_nodes_per_stroke {
            return fail("too_many_nodes", item.at(key));
        }
        for (i, n) in nodes.iter().enumerate() {
            let n = Obj::new(n, &format!("{}[{i}]", item.at(key)))?;
            if samples.is_some() {
                n.allow(&["x", "y", "in", "out", "mode", "corner", "sample"])?;
            } else {
                n.allow(&["x", "y", "in", "out", "mode", "corner"])?;
            }
            area.hundredths(n.int("x")?, n.int("y")?, &n.at("x"))?;
            n.variant("mode", &["independent", "smooth", "symmetric"])?;
            n.bool("corner")?;
            for h in ["in", "out"] {
                if let Some(v) = n.opt(h) {
                    let handle = Obj::new(v, &n.at(h))?;
                    handle.allow(&["x", "y"])?;
                    handle.int("x")?;
                    handle.int("y")?;
                }
            }
            if let (Some(count), Some(_)) = (samples, n.opt("sample")) {
                let s = n.int("sample")?;
                if s < 0 || s as usize >= count {
                    return fail("out_of_range", n.at("sample"));
                }
            }
        }
        Ok(())
    }

    fn shape(&mut self, item: &Obj, area: Area) -> R<()> {
        let shape = item.variant("shape", &["circle", "rectangle", "triangle", "path"])?;
        const STYLE: [&str; 7] = [
            "id",
            "kind",
            "shape",
            "color",
            "line_width",
            "opacity",
            "fill",
        ];
        if item.num("line_width")? <= 0.0 {
            return fail("invalid_size", item.at("line_width"));
        }
        color(item, "color")?;
        if item.opt("fill").is_some() {
            color(item, "fill")?;
        }
        opacity(item)?;
        match shape {
            "circle" => {
                item.allow_with(&STYLE, &["center", "radius"])?;
                let c = Obj::new(item.req("center")?, &item.at("center"))?;
                c.allow(&["x", "y"])?;
                let r = item.num("radius")?;
                if r <= 0.0 {
                    return fail("invalid_size", item.at("radius"));
                }
                let (x, y) = (c.num("x")?, c.num("y")?);
                area.point(x - r, y - r, &item.at("center"))?;
                area.point(x + r, y + r, &item.at("center"))
            }
            "rectangle" => {
                item.allow_with(&STYLE, &["x", "y", "width", "height"])?;
                let (x, y) = (item.num("x")?, item.num("y")?);
                let (w, h) = (item.num("width")?, item.num("height")?);
                if w <= 0.0 {
                    return fail("invalid_size", item.at("width"));
                }
                if h <= 0.0 {
                    return fail("invalid_size", item.at("height"));
                }
                area.point(x, y, &item.at("x"))?;
                area.point(x + w, y + h, &item.at("x"))
            }
            "triangle" => {
                item.allow_with(&STYLE, &["points"])?;
                let points = item.arr("points")?;
                if points.len() != 3 {
                    return fail("invalid_count", item.at("points"));
                }
                for (i, p) in points.iter().enumerate() {
                    let p = Obj::new(p, &format!("{}[{i}]", item.at("points")))?;
                    p.allow(&["x", "y"])?;
                    area.point(p.num("x")?, p.num("y")?, &p.at("x"))?;
                }
                Ok(())
            }
            _ => {
                item.allow_with(&STYLE, &["nodes", "closed"])?;
                item.bool("closed")?;
                self.nodes(item, "nodes", area, None)
            }
        }
    }

    /// Every group member exists, no item is in two groups and groups have no cycle.
    fn check_groups(&self) -> R<()> {
        let mut owner: HashMap<&str, &str> = HashMap::new();
        for (id, _, members) in &self.groups {
            for (m, p) in members {
                if !self.ids.contains_key(m) {
                    return fail("missing_reference", p.clone());
                }
                if m == id {
                    return fail("group_cycle", p.clone());
                }
                if owner.insert(m, id).is_some() {
                    return fail("multiple_groups", p.clone());
                }
            }
        }
        // Each item has at most one parent, so a cycle shows as a parent chain that
        // returns to its start.
        for (id, path, _) in &self.groups {
            let mut cur = id.as_str();
            let mut steps = 0;
            while let Some(parent) = owner.get(cur) {
                if *parent == id {
                    return fail("group_cycle", path.clone());
                }
                cur = parent;
                steps += 1;
                if steps > self.groups.len() {
                    return fail("group_cycle", path.clone());
                }
            }
        }
        Ok(())
    }

    /// A block of `note-typed`.
    fn typed_block(&mut self, value: &Value, path: &str) -> R<()> {
        let block = Obj::new(value, path)?;
        let kind = block.str("kind")?;
        if !TYPED_KINDS.contains(&kind) {
            return fail("unknown_kind", block.at("kind"));
        }
        self.id(&block)?;
        const BASE: [&str; 2] = ["id", "kind"];
        match kind {
            "heading" => {
                block.allow_with(&BASE, &["level", "spans"])?;
                let level = block.int("level")?;
                if !(1..=6).contains(&level) {
                    return fail("out_of_range", block.at("level"));
                }
                spans(&block, "spans")
            }
            "paragraph" => {
                block.allow_with(&BASE, &["spans"])?;
                spans(&block, "spans")
            }
            "list" => {
                block.allow_with(&BASE, &["ordered", "items"])?;
                block.bool("ordered")?;
                let items = block.arr("items")?;
                if items.is_empty() {
                    return fail("empty_array", block.at("items"));
                }
                for (i, it) in items.iter().enumerate() {
                    let it = Obj::new(it, &format!("{}[{i}]", block.at("items")))?;
                    it.allow(&["spans"])?;
                    spans(&it, "spans")?;
                }
                Ok(())
            }
            "table" => {
                block.allow_with(&BASE, &["header", "rows"])?;
                if block.opt("header").is_some() {
                    block.bool("header")?;
                }
                let rows = block.arr("rows")?;
                if rows.is_empty() {
                    return fail("empty_array", block.at("rows"));
                }
                let mut width = None;
                for (i, row) in rows.iter().enumerate() {
                    let row = Obj::new(row, &format!("{}[{i}]", block.at("rows")))?;
                    row.allow(&["cells"])?;
                    let cells = row.arr("cells")?;
                    if cells.is_empty() {
                        return fail("empty_array", row.at("cells"));
                    }
                    if *width.get_or_insert(cells.len()) != cells.len() {
                        return fail("invalid_count", row.at("cells"));
                    }
                    for (j, c) in cells.iter().enumerate() {
                        let c = Obj::new(c, &format!("{}[{j}]", row.at("cells")))?;
                        c.allow(&["spans"])?;
                        spans(&c, "spans")?;
                    }
                }
                Ok(())
            }
            "figure" => {
                block.allow_with(&BASE, &["file_id", "caption"])?;
                self.file(&block, "file_id")?;
                if block.opt("caption").is_some() {
                    spans(&block, "caption")?;
                }
                Ok(())
            }
            "handwriting" => {
                block.allow_with(&BASE, &["width", "height", "ink"])?;
                let (w, h) = (block.num("width")?, block.num("height")?);
                if w <= 0.0 {
                    return fail("invalid_size", block.at("width"));
                }
                if h <= 0.0 {
                    return fail("invalid_size", block.at("height"));
                }
                let ink = block.arr("ink")?;
                if ink.is_empty() {
                    return fail("empty_array", block.at("ink"));
                }
                self.count(ink.len(), &block.at("ink"))?;
                let area = Area {
                    width: w,
                    height: Some(h),
                };
                for (i, v) in ink.iter().enumerate() {
                    let it = Obj::new(v, &format!("{}[{i}]", block.at("ink")))?;
                    self.id(&it)?;
                    self.ink(&it, area)?;
                }
                Ok(())
            }
            _ => self.card(&block, kind, &BASE),
        }
    }
}

/// Pressure is stored as 0 to 1000; tilt as 0 to pi/2 in milliradians.
const PRESSURE_MAX: i64 = 1000;
const TILT_MAX: i64 = 1571;

static HEX_COLOR: LazyLock<Regex> = LazyLock::new(|| Regex::new("^#[0-9A-Fa-f]{8}$").unwrap());
static MARKUP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)</?[a-z!?][^<>]*>|<!--|javascript:").unwrap());

fn token(s: &str, max: usize) -> bool {
    !s.is_empty()
        && s.len() <= max
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
fn reference(item: &Obj, key: &str) -> R<()> {
    if !token(item.str(key)?, 128) {
        return fail("invalid_reference", item.at(key));
    }
    Ok(())
}
fn color(item: &Obj, key: &str) -> R<()> {
    if !HEX_COLOR.is_match(item.str(key)?) {
        return fail("invalid_color", item.at(key));
    }
    Ok(())
}
fn opacity(item: &Obj) -> R<()> {
    let o = item.num("opacity")?;
    if o <= 0.0 || o > 1.0 {
        return fail("out_of_range", item.at("opacity"));
    }
    Ok(())
}
fn plain_text(s: &str, path: String) -> R<()> {
    if MARKUP.is_match(s) {
        return fail("markup_not_allowed", path);
    }
    if s.chars().any(|c| c.is_control() && c != '\n' && c != '\t') {
        return fail("invalid_text", path);
    }
    Ok(())
}
fn opt_text(item: &Obj, key: &str) -> R<()> {
    if item.opt(key).is_some() {
        plain_text(item.str(key)?, item.at(key))?;
    }
    Ok(())
}
fn url(item: &Obj, key: &str, youtube: bool) -> R<()> {
    let parsed = schemas::validate_url(item.str(key)?)
        .map_err(|_| DocumentError::new("invalid_url", item.at(key)))?;
    if youtube
        && !matches!(
            parsed.host_str(),
            Some("youtube.com" | "www.youtube.com" | "m.youtube.com" | "youtu.be")
        )
    {
        return fail("invalid_url", item.at(key));
    }
    Ok(())
}
/// Rich text: plain text runs with bold, italic, strikethrough, color and size.
fn spans(item: &Obj, key: &str) -> R<()> {
    for (i, s) in item.arr(key)?.iter().enumerate() {
        let s = Obj::new(s, &format!("{}[{i}]", item.at(key)))?;
        s.allow(&["text", "bold", "italic", "strikethrough", "color", "size"])?;
        plain_text(s.str("text")?, s.at("text"))?;
        for flag in ["bold", "italic", "strikethrough"] {
            if s.opt(flag).is_some() {
                s.bool(flag)?;
            }
        }
        if s.opt("color").is_some() {
            color(&s, "color")?;
        }
        if s.opt("size").is_some() && s.num("size")? <= 0.0 {
            return fail("invalid_size", s.at("size"));
        }
    }
    Ok(())
}

/// A JSON object with its path, for strict field access.
struct Obj<'a> {
    map: &'a Map<String, Value>,
    path: String,
}
impl<'a> Obj<'a> {
    fn new(value: &'a Value, path: &str) -> R<Self> {
        match value.as_object() {
            Some(map) => Ok(Self {
                map,
                path: path.to_string(),
            }),
            None => fail("invalid_type", path),
        }
    }
    fn at(&self, key: &str) -> String {
        if self.path.is_empty() {
            key.to_string()
        } else {
            format!("{}.{key}", self.path)
        }
    }
    fn allow(&self, keys: &[&str]) -> R<()> {
        self.allow_with(keys, &[])
    }
    fn allow_with(&self, keys: &[&str], more: &[&str]) -> R<()> {
        let mut unknown: Vec<&String> = self
            .map
            .keys()
            .filter(|k| !keys.contains(&k.as_str()) && !more.contains(&k.as_str()))
            .collect();
        unknown.sort();
        match unknown.first() {
            Some(k) => fail("unknown_field", self.at(k)),
            None => Ok(()),
        }
    }
    /// Optional value; JSON null counts as absent.
    fn opt(&self, key: &str) -> Option<&'a Value> {
        self.map.get(key).filter(|v| !v.is_null())
    }
    fn req(&self, key: &str) -> R<&'a Value> {
        self.opt(key)
            .ok_or_else(|| DocumentError::new("missing_field", self.at(key)))
    }
    fn str(&self, key: &str) -> R<&'a str> {
        self.req(key)?
            .as_str()
            .ok_or_else(|| DocumentError::new("invalid_type", self.at(key)))
    }
    fn bool(&self, key: &str) -> R<bool> {
        self.req(key)?
            .as_bool()
            .ok_or_else(|| DocumentError::new("invalid_type", self.at(key)))
    }
    fn arr(&self, key: &str) -> R<&'a Vec<Value>> {
        self.req(key)?
            .as_array()
            .ok_or_else(|| DocumentError::new("invalid_type", self.at(key)))
    }
    fn num(&self, key: &str) -> R<f64> {
        let n = self
            .req(key)?
            .as_f64()
            .ok_or_else(|| DocumentError::new("invalid_type", self.at(key)))?;
        if !n.is_finite() {
            return fail("non_finite_number", self.at(key));
        }
        Ok(n)
    }
    fn int(&self, key: &str) -> R<i64> {
        self.req(key)?
            .as_i64()
            .ok_or_else(|| DocumentError::new("invalid_type", self.at(key)))
    }
    /// An array of integers. A float or another type is `invalid_type` at its index.
    fn ints(&self, key: &str) -> R<Vec<i64>> {
        self.arr(key)?
            .iter()
            .enumerate()
            .map(|(i, v)| {
                v.as_i64().ok_or_else(|| {
                    DocumentError::new("invalid_type", format!("{}[{i}]", self.at(key)))
                })
            })
            .collect()
    }
    fn range(&self, key: &str, min: f64, max: f64) -> R<f64> {
        let n = self.num(key)?;
        if n < min || n > max {
            return fail("out_of_range", self.at(key));
        }
        Ok(n)
    }
    fn variant(&self, key: &str, allowed: &[&str]) -> R<&'a str> {
        let v = self.str(key)?;
        if !allowed.contains(&v) {
            return fail("invalid_variant", self.at(key));
        }
        Ok(v)
    }
}
