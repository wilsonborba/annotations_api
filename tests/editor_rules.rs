use annotations_api::{
    core::NOTE_LIMITS,
    domain::{
        editor_rules::{self, RULES, RuleError},
        note_documents::Family,
    },
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashSet},
    path::Path,
};

const STRUCTURED: [&str; 11] = [
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
];

fn fixtures() -> Vec<(String, Value)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/editor_rules");
    let mut out: Vec<(String, Value)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .map(|p| {
            let text = std::fs::read_to_string(&p).unwrap();
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                serde_json::from_str(&text).unwrap(),
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn run(f: &Value) -> Result<editor_rules::Edited, RuleError> {
    let family = Family::parse(f["family"].as_str().unwrap()).unwrap();
    editor_rules::check_edit(
        family,
        f["schema_version"].as_i64().unwrap(),
        &f["document"],
        f.get("operation"),
        &NOTE_LIMITS,
    )
}

/// Ids that an operation names. Every other item must stay as it was.
fn named(op: &Value, out: &mut HashSet<String>) {
    for s in op["steps"].as_array().into_iter().flatten() {
        named(s, out);
    }
    for id in [&op["id"], &op["item"]["id"]] {
        if let Some(id) = id.as_str() {
            out.insert(id.to_string());
        }
    }
}

#[test]
fn every_fixture_gives_its_expected_verdict() {
    let mut failures = vec![];
    for (file, f) in fixtures() {
        let got = run(&f);
        let rule = f["rule_id"].as_str().unwrap();
        match (f["expected"].as_str().unwrap(), &got) {
            ("valid", Ok(edited)) => {
                // Neighbors are never moved: items that the operation does not name
                // keep their content and their order.
                if let Some(op) = f.get("operation") {
                    let mut ids = HashSet::new();
                    named(op, &mut ids);
                    let keep = |d: &Value| -> Vec<Value> {
                        d["items"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .filter(|it| !ids.contains(it["id"].as_str().unwrap()))
                            .cloned()
                            .collect()
                    };
                    if keep(&f["document"]) != keep(&edited.document) {
                        failures.push(format!(
                            "{file}: an item that the operation does not name changed"
                        ));
                    }
                }
            }
            ("invalid", Err(e)) => {
                let items: Vec<String> = f["error"]["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect();
                if e.rule != rule || e.code != f["error"]["code"] || e.items != items {
                    failures.push(format!("{file}: got {e:?}, expected {rule} {}", f["error"]));
                }
            }
            (expected, _) => failures.push(format!("{file}: expected {expected}, got {got:?}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn fixtures_cover_every_rule_kind_and_operation() {
    let all = fixtures();
    let mut missing = vec![];
    let mut seen = BTreeSet::new();
    let mut overlap_kinds = BTreeSet::new();
    let mut steps = BTreeSet::new();
    for (file, f) in &all {
        let rule = f["rule_id"].as_str().unwrap();
        assert!(RULES.contains(&rule), "{file}: unknown rule {rule}");
        let verdict = f["expected"].as_str().unwrap();
        assert!(
            verdict == "valid" || verdict == "invalid",
            "{file}: verdict {verdict}"
        );
        seen.insert(format!("{rule}:{verdict}"));
        seen.insert(format!("family:{}", f["family"].as_str().unwrap()));
        if verdict == "invalid" {
            seen.insert(format!("code:{}", f["error"]["code"].as_str().unwrap()));
        }
        if f["error"]["code"] == "block_overlap" {
            for it in f["document"]["items"].as_array().unwrap() {
                overlap_kinds.insert(it["kind"].as_str().unwrap().to_string());
            }
        }
        if let Some(op) = f.get("operation") {
            steps.insert(op["op"].as_str().unwrap().to_string());
            for s in op["steps"].as_array().into_iter().flatten() {
                steps.insert(s["op"].as_str().unwrap_or_default().to_string());
            }
        }
    }
    for need in [
        "block_overlap:valid",
        "block_overlap:invalid",
        "group_overlap:valid",
        "group_overlap:invalid",
        "free_ink_overlap:valid",
        "page_bounds:valid",
        "page_bounds:invalid",
        "operation:valid",
        "operation:invalid",
        "document_schema:invalid",
        "family:note-original",
        "family:instant-room",
        "family:note-typed",
        "code:block_overlap",
        "code:group_overlap",
        "code:out_of_bounds",
        "code:missing_item",
        "code:invalid_operation",
        "code:immutable_field",
    ] {
        if !seen.contains(need) {
            missing.push(need.to_string());
        }
    }
    for k in STRUCTURED {
        if !overlap_kinds.contains(k) {
            missing.push(format!("block_overlap with {k}"));
        }
    }
    for s in ["add", "replace", "update", "delete", "batch"] {
        if !steps.contains(s) {
            missing.push(format!("operation {s}"));
        }
    }
    // Cases that the issue names.
    for name in [
        "block-touching-edges-every-kind",
        "block-touching-corner",
        "group-member-resize-inside",
        "group-overlap-member-resize",
        "group-overlap-nested-member-resize",
        "group-overlap-grouping-creates-conflict",
        "page-bounds-block-too-wide",
        "block-overlap-op-add",
    ] {
        if !all.iter().any(|(f, _)| f == &format!("{name}.json")) {
            missing.push(name.to_string());
        }
    }
    assert!(missing.is_empty(), "missing fixtures: {missing:?}");
}

#[test]
fn errors_name_the_rule_and_the_items() {
    let doc = json!({"page":{"width":500,"background":{"kind":"blank"}},"items":[
        {"id":"t1","kind":"text","x":0,"y":0,"width":100,"height":100,"spans":[]}]});
    let op = json!({"op":"add","item":{"id":"t2","kind":"text","x":50,"y":50,"width":100,"height":100,"spans":[]}});
    let e =
        editor_rules::check_edit(Family::Original, 1, &doc, Some(&op), &NOTE_LIMITS).unwrap_err();
    let core: annotations_api::core::Error = e.into();
    assert_eq!(core.0, axum::http::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        core.1,
        "block_overlap: Structured blocks must not overlap (rule block_overlap; items t1, t2) at items[1]"
    );
    // The stored document is unchanged and an accepted edit returns the working copy.
    assert_eq!(doc["items"].as_array().unwrap().len(), 1);
    let ok = json!({"op":"add","item":{"id":"t2","kind":"text","x":100,"y":0,"width":100,"height":100,"spans":[]}});
    let edited =
        editor_rules::check_edit(Family::Original, 1, &doc, Some(&ok), &NOTE_LIMITS).unwrap();
    assert_eq!(edited.document["items"][1]["id"], "t2");
    let e = editor_rules::check_edit(
        Family::Original,
        1,
        &doc,
        Some(&json!({"op":"update","id":"t1","changes":{"x":450}})),
        &NOTE_LIMITS,
    )
    .unwrap_err();
    let core: annotations_api::core::Error = e.into();
    assert_eq!(
        core.1,
        "out_of_bounds: The position is outside the page (rule page_bounds; items t1) at items[0].x"
    );
}

/// A full page of touching blocks at the item limit stays fast enough for a request.
#[test]
fn a_document_at_the_item_limit_is_checked() {
    let n = NOTE_LIMITS.max_items;
    let items: Vec<Value> = (0..n)
        .map(|i| {
            json!({"id":format!("t{i}"),"kind":"text","x":(i % 10) as f64 * 100.0,"y":(i / 10) as f64 * 40.0,"width":100,"height":40,"spans":[]})
        })
        .collect();
    let doc = json!({"page":{"width":1000,"background":{"kind":"blank"}},"items":items});
    let start = std::time::Instant::now();
    let ok = editor_rules::check_edit(Family::Original, 1, &doc, None, &NOTE_LIMITS);
    println!("{n} blocks checked in {:?}", start.elapsed());
    assert!(ok.is_ok(), "{:?}", ok.err());
}
