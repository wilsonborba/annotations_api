//! Note capacity (annotations_api#19): a 30-page dense handwritten note.
use annotations_api::{
    core::NOTE_LIMITS,
    domain::note_documents::{self, Family},
};
use serde_json::{Value, json};

/// One stroke like the editor capture: 84 samples at 240 Hz, 5 fitted nodes.
fn stroke(i: usize) -> Value {
    let samples = 84;
    let x0 = 40.0 + (i % 40) as f64 * 23.0;
    let y0 = 60.0 + ((i / 40) % 20) as f64 * 36.0 + (i / 800) as f64 * 760.0;
    let q = |v: f64| (v * 100.0).round() as i64;
    let a = |k: usize| k as f64 / (samples - 1) as f64;
    let xs: Vec<i64> = (0..samples)
        .map(|k| q(x0 + a(k) * 18.0 + (a(k) * 12.6).sin() * 3.0))
        .collect();
    let ys: Vec<i64> = (0..samples)
        .map(|k| q(y0 + (a(k) * 12.6).cos() * 4.0))
        .collect();
    let ts: Vec<usize> = (0..samples).map(|k| k * 1000 / 240).collect();
    let ps: Vec<i64> = (0..samples).map(|k| 350 + (k * 5) as i64).collect();
    let tilts: Vec<i64> = (0..samples).map(|k| 600 + k as i64).collect();
    let nodes: Vec<Value> = [0, 20, 41, 62, 83]
        .iter()
        .map(|&j| json!({"x":xs[j],"y":ys[j],"in":{"x":xs[j] - 150,"y":ys[j]},"out":{"x":xs[j] + 150,"y":ys[j]},"mode":"smooth","corner":false,"sample":j}))
        .collect();
    json!({"id":format!("s{i}"),"kind":"stroke","tool":"round","brush_version":1,"width":3,"color":"#000000FF","opacity":1,
        "samples":{"x":xs,"y":ys,"t":ts,"pressure":ps,"tilt":tilts},"nodes":nodes})
}

fn peak_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

#[test]
#[ignore = "measurement, run with --ignored"]
fn measure_a_30_page_note() {
    let pages = 30;
    let items: Vec<Value> = (0..pages * 800).map(stroke).collect();
    let raw = serde_json::to_vec(&json!({"page":{"width":1000,"background":{"kind":"blank"}},"items":items})).unwrap();
    drop(items);
    let before = peak_kb();
    let start = std::time::Instant::now();
    let doc: Value = serde_json::from_slice(&raw).unwrap();
    let parse = start.elapsed();
    let mut limits = NOTE_LIMITS;
    limits.max_items = 100_000;
    limits.max_document_bytes = usize::MAX;
    let start = std::time::Instant::now();
    let checked = note_documents::check_document(Family::Original, 1, &doc, &limits);
    let check = start.elapsed();
    println!(
        "30 pages: {} bytes JSON; parse {parse:?}, schema check {check:?} ({}); peak memory {} MB (before parse {} MB)",
        raw.len(),
        checked.is_ok(),
        peak_kb() / 1024,
        before / 1024
    );
}
