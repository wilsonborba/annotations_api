//! Anonymous public discovery: published-note search and shuffled browsing
//! (`GET /notes`), the public reader (`GET /notes/{note_id}`), public authors and
//! their public projects.
//!
//! - Only notes with a published representation take part. Every note answer is
//!   built by `publication::public_summary` or `publication::public_note`; every
//!   project answer by `projects::public_project`. Drafts, private titles, private
//!   tags, private projects and Instant rooms never reach this module.
//! - The search index (`note_search`) is derived from published content only: the
//!   publication-time title and tags and the published Typed text. Publish and
//!   republish rebuild the row of the note; unpublish removes it in the same
//!   transaction. It keeps a short summary and distinct search terms, never a
//!   document body. Handwriting is never transcribed for search.
//! - Words are split at spaces and punctuation and compared in lower case, with
//!   prefix matching. There is no language-specific word segmentation.
//! - Browsing order: a `browse_token` holds a random seed and the time the browse
//!   started. Each author's notes are shuffled by the seed; the first round takes
//!   one note of every author in a shuffled author order, the next round the
//!   second note, and so on. The same token always gives the same order, and the
//!   `cursor` (round and author) continues it without duplicates. Notes published
//!   or republished after the browse started join the next browse. There are no
//!   likes and no popularity ranking.
use crate::{
    core::{self, DISCOVERY_LIMITS},
    dal::Store,
    domain::{
        names,
        note_saves::Representation,
        notes::{self, NoteError},
        projects, publication,
    },
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::collections::{BTreeMap, HashSet};

type R<T> = std::result::Result<T, NoteError>;

// Search index ------------------------------------------------------------------

fn spans_text(value: Option<&Value>, out: &mut String) {
    for span in value.and_then(Value::as_array).into_iter().flatten() {
        if let Some(text) = span.get("text").and_then(Value::as_str) {
            out.push_str(text);
        }
    }
    out.push('\n');
}

/// The plain text of a published Typed document in block order: headings,
/// paragraphs, lists, tables and figure captions. Handwriting blocks and cards
/// (links, files, Reports embeds, tasks, quizzes) add no text.
pub fn typed_text(document: &Value) -> String {
    let mut out = String::new();
    for block in document
        .get("blocks")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        match block.get("kind").and_then(Value::as_str) {
            Some("heading" | "paragraph") => spans_text(block.get("spans"), &mut out),
            Some("list") => {
                for item in block
                    .get("items")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    spans_text(item.get("spans"), &mut out);
                }
            }
            Some("table") => {
                for row in block
                    .get("rows")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    for cell in row
                        .get("cells")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        spans_text(cell.get("spans"), &mut out);
                    }
                }
            }
            Some("figure") => spans_text(block.get("caption"), &mut out),
            _ => {}
        }
    }
    out
}

/// The short summary: the first characters of the published Typed text with
/// whitespace collapsed. Empty when no Typed text is published.
pub fn summary(text: &str) -> String {
    let mut out = String::new();
    for (count, word) in text.split_whitespace().enumerate() {
        if count > 0 {
            out.push(' ');
        }
        out.push_str(word);
        if out.chars().count() >= DISCOVERY_LIMITS.summary_chars {
            break;
        }
    }
    out.chars().take(DISCOVERY_LIMITS.summary_chars).collect()
}

fn separator(c: char) -> bool {
    c.is_whitespace()
        || c.is_control()
        || (c.is_ascii() && !c.is_ascii_alphanumeric())
        || ('\u{2000}'..='\u{206F}').contains(&c)
        || ('\u{3000}'..='\u{303F}').contains(&c)
        || ('\u{FF00}'..='\u{FF0F}').contains(&c)
}

/// Search terms of a text in order: lower-case words split at whitespace and
/// punctuation. Words longer than the term limit are left out.
pub fn terms(text: &str) -> Vec<String> {
    text.split(separator)
        .filter(|w| !w.is_empty() && w.chars().count() <= DISCOVERY_LIMITS.max_term_chars)
        .map(names::normalize)
        .collect()
}

/// A term as a quoted PostgreSQL text search lexeme.
fn lexeme(term: &str) -> String {
    format!("'{}'", term.replace('\\', "\\\\").replace('\'', "''"))
}

/// Rebuild the published tags and the search index row of a note inside the
/// publish transaction, after the publication title is saved. `typed` is the
/// text of the published Typed revision, if Typed is published.
pub(crate) async fn rebuild(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    postgres: bool,
    note_id: &str,
    typed: Option<&str>,
) -> R<()> {
    // The tags at publication time; later tag edits stay private.
    for sql in [
        "DELETE FROM note_publication_tags WHERE note_id=$1",
        "INSERT INTO note_publication_tags(note_id,tag_id) SELECT note_id,tag_id FROM note_tags WHERE note_id=$1",
        "DELETE FROM note_search WHERE note_id=$1",
    ] {
        sqlx::query(sql).bind(note_id).execute(&mut **tx).await?;
    }
    let mut text = String::new();
    if let Some(row) = sqlx::query("SELECT title FROM note_publication_titles WHERE note_id=$1")
        .bind(note_id)
        .fetch_optional(&mut **tx)
        .await?
    {
        text.push_str(&row.get::<String, _>("title"));
        text.push('\n');
    }
    for row in sqlx::query("SELECT g.name FROM note_publication_tags pt JOIN tags g ON g.tag_id=pt.tag_id WHERE pt.note_id=$1")
        .bind(note_id)
        .fetch_all(&mut **tx)
        .await?
    {
        text.push_str(&row.get::<String, _>("name"));
        text.push('\n');
    }
    let summary = typed.map(summary).unwrap_or_default();
    if let Some(typed) = typed {
        text.push_str(typed);
    }
    let mut distinct = HashSet::new();
    let terms: Vec<String> = terms(&text)
        .into_iter()
        .filter(|t| distinct.insert(t.clone()))
        .take(DISCOVERY_LIMITS.max_terms)
        .collect();
    let (sql, vector) = if postgres {
        (
            "INSERT INTO note_search(note_id,summary,search_vector) VALUES($1,$2,CAST($3 AS tsvector))",
            terms
                .iter()
                .map(|t| lexeme(t))
                .collect::<Vec<_>>()
                .join(" "),
        )
    } else {
        (
            "INSERT INTO note_search(note_id,summary,search_vector) VALUES($1,$2,$3)",
            format!(" {} ", terms.join(" ")),
        )
    };
    sqlx::query(sql)
        .bind(note_id)
        .bind(&summary)
        .bind(&vector)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

// Browsing order ----------------------------------------------------------------

/// The browse of one reader: a random seed and the time the browse started.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Browse {
    started: i64,
    seed: String,
}
impl Browse {
    fn new() -> Self {
        Self {
            started: core::now(),
            seed: core::id().replace('-', ""),
        }
    }
    /// `<started>.<seed>`: a number and 16 to 64 lower-case hex digits.
    fn parse(raw: &str) -> R<Self> {
        raw.split_once('.')
            .and_then(|(t, seed)| {
                let started = t.parse::<i64>().ok().filter(|t| *t >= 0)?;
                let hex = (16..=64).contains(&seed.len())
                    && seed
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
                hex.then(|| Self {
                    started,
                    seed: seed.to_string(),
                })
            })
            .ok_or_else(|| {
                NoteError::invalid("invalid_browse_token", "The browse_token is not valid")
            })
    }
    fn token(&self) -> String {
        format!("{}.{}", self.started, self.seed)
    }
    fn key(&self, kind: &str, id: &str) -> u64 {
        let digest = Sha256::digest(format!("{}:{kind}:{id}", self.seed).as_bytes());
        u64::from_be_bytes(digest[..8].try_into().unwrap_or([0; 8]))
    }
}

/// The place of a note in a browse: its round, then the shuffled author key and
/// the author id. One author has one note per round, so a place is unique.
type Place = (i64, u64, String);

fn place_cursor(place: &Place) -> String {
    format!("{}.{}", place.0, place.2)
}

/// Author-balanced shuffled order of `(note_id, author_id)` pairs.
fn order(browse: &Browse, notes: Vec<(String, String)>) -> Vec<(Place, String)> {
    let mut by_author: BTreeMap<String, Vec<(u64, String)>> = BTreeMap::new();
    for (note_id, author_id) in notes {
        let key = browse.key("note", &note_id);
        by_author.entry(author_id).or_default().push((key, note_id));
    }
    let mut out = vec![];
    for (author_id, mut list) in by_author {
        list.sort();
        let author_key = browse.key("author", &author_id);
        for (round, (_, note_id)) in list.into_iter().enumerate() {
            out.push(((round as i64, author_key, author_id.clone()), note_id));
        }
    }
    out.sort();
    out
}

// Routes ------------------------------------------------------------------------

/// Query of `GET /notes`. All values are raw query strings.
#[derive(Debug, Default)]
pub struct ListQuery {
    pub q: Option<String>,
    pub author_id: Option<String>,
    pub language: Option<String>,
    pub tag: Option<String>,
    pub project_id: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<String>,
    pub browse_token: Option<String>,
}

/// A filter value: absent or blank means no filter; otherwise an id-like token.
fn filter(value: Option<&str>, name: &str) -> R<String> {
    match value.map(str::trim) {
        None | Some("") => Ok(String::new()),
        Some(v) if notes::token(v) => Ok(v.to_string()),
        Some(_) => Err(NoteError::invalid(
            "invalid_filter",
            format!("The filter is not valid: {name}"),
        )),
    }
}

/// The published notes of a public project, from `projects::public_note_ids`.
async fn project_notes(store: &Store, project_id: &str) -> R<HashSet<String>> {
    const PAGE: i64 = 500;
    let mut out = HashSet::new();
    let mut after: Option<String> = None;
    loop {
        let page = projects::public_note_ids(store, project_id, after.as_deref(), PAGE).await?;
        let full = page.len() as i64 == PAGE;
        after = page.last().cloned();
        out.extend(page);
        if !full {
            return Ok(out);
        }
    }
}

/// `GET /notes`: published-note search and author-balanced shuffled browsing.
///
/// `q` searches the published title, summary, tags and Typed text. `author_id`,
/// `language`, `tag` and `project_id` filter. Without `browse_token` a new browse
/// starts; the answer always returns the token to send with `cursor`.
pub async fn list(store: &Store, query: &ListQuery) -> R<Value> {
    let limit = notes::page_limit(query.limit.as_deref())?;
    let q = query.q.as_deref().map(str::trim).unwrap_or("");
    if q.chars().count() > DISCOVERY_LIMITS.max_query_chars {
        return Err(NoteError::invalid(
            "query_too_long",
            format!(
                "q has more than {} characters",
                DISCOVERY_LIMITS.max_query_chars
            ),
        ));
    }
    let author_id = filter(query.author_id.as_deref(), "author_id")?;
    let language = filter(query.language.as_deref(), "language")?;
    let project_id = filter(query.project_id.as_deref(), "project_id")?;
    let tag = query
        .tag
        .as_deref()
        .map(names::normalize)
        .unwrap_or_default();
    let browse = match query.browse_token.as_deref() {
        Some(raw) => Browse::parse(raw)?,
        None => Browse::new(),
    };
    let after: Option<Place> = match query.cursor.as_deref() {
        None => None,
        Some(_) if query.browse_token.is_none() => {
            return Err(NoteError::invalid(
                "browse_token_required",
                "Send the browse_token of the first page with the cursor",
            ));
        }
        Some(c) => {
            let (round, author) = c
                .split_once('.')
                .and_then(|(r, a)| Some((r.parse::<i64>().ok().filter(|r| *r >= 0)?, a)))
                .filter(|(_, a)| notes::token(a))
                .ok_or_else(|| NoteError::invalid("invalid_cursor", "The cursor is not valid"))?;
            Some((round, browse.key("author", author), author.to_string()))
        }
    };
    let words = terms(q);
    let empty = json!({"items": [], "next_cursor": Value::Null, "browse_token": browse.token()});
    if !q.is_empty() && words.is_empty() {
        return Ok(empty);
    }

    let postgres = store.postgres();
    let mut sql = String::from(
        "SELECT p.note_id,a.author_id FROM note_publications p \
         JOIN notes n ON n.note_id=p.note_id JOIN authors a ON a.owner_id=n.owner_id \
         LEFT JOIN note_search s ON s.note_id=p.note_id \
         WHERE EXISTS(SELECT 1 FROM note_published_representations r WHERE r.note_id=p.note_id) \
         AND p.updated_at<=$1 AND ($2='' OR a.author_id=$2) AND ($3='' OR p.language_code=$3) \
         AND ($4='' OR EXISTS(SELECT 1 FROM note_publication_tags pt JOIN tags g ON g.tag_id=pt.tag_id WHERE pt.note_id=p.note_id AND g.normalized_name=$4))",
    );
    let mut text_binds = vec![];
    if !words.is_empty() {
        if postgres {
            sql.push_str(" AND s.search_vector @@ CAST($5 AS tsquery)");
            text_binds.push(
                words
                    .iter()
                    .map(|w| format!("{}:*", lexeme(w)))
                    .collect::<Vec<_>>()
                    .join(" & "),
            );
        } else {
            for (i, w) in words.iter().enumerate() {
                sql.push_str(&format!(" AND s.search_vector LIKE ${}", i + 5));
                text_binds.push(format!("% {w}%"));
            }
        }
    }
    let mut statement = sqlx::query(&sql)
        .bind(browse.started)
        .bind(&author_id)
        .bind(&language)
        .bind(&tag);
    for value in &text_binds {
        statement = statement.bind(value);
    }
    let mut found: Vec<(String, String)> = statement
        .fetch_all(&store.db)
        .await?
        .iter()
        .map(|r| (r.get("note_id"), r.get("author_id")))
        .collect();
    if !project_id.is_empty() {
        let members = project_notes(store, &project_id).await?;
        found.retain(|(note_id, _)| members.contains(note_id));
    }

    let rest: Vec<(Place, String)> = order(&browse, found)
        .into_iter()
        .filter(|(place, _)| after.as_ref().is_none_or(|a| place > a))
        .collect();
    let more = rest.len() as i64 > limit;
    let mut items = vec![];
    let mut next = Value::Null;
    for (place, note_id) in rest.iter().take(limit as usize) {
        next = json!(place_cursor(place));
        match publication::public_summary(store, note_id).await {
            Ok(item) => items.push(item),
            // Unpublished after the list query: leave it out.
            Err(e) if e.code == "not_found" => {}
            Err(e) => return Err(e),
        }
    }
    let mut out = empty;
    out["items"] = Value::Array(items);
    if more {
        out["next_cursor"] = next;
    }
    Ok(out)
}

/// `GET /notes/{note_id}?view=original|typed&language=`: the public reader.
/// Without `view` the reader opens the default view: the main representation when
/// it is published, else the published one. A `language` other than the published
/// language is not found.
pub async fn note(
    store: &Store,
    note_id: &str,
    view: Option<&str>,
    language: Option<&str>,
) -> R<Value> {
    let view =
        match view {
            None => None,
            Some(v) => Some(Representation::parse(v).ok_or_else(|| {
                NoteError::invalid("invalid_view", "view must be original or typed")
            })?),
        };
    let header = publication::public_summary(store, note_id).await?;
    if let Some(language) = language.filter(|l| !l.is_empty())
        && header["language"] != language
    {
        return Err(NoteError::not_found());
    }
    let view = match view {
        Some(v) => v,
        None => header["default_view"]
            .as_str()
            .and_then(Representation::parse)
            .ok_or_else(NoteError::not_found)?,
    };
    publication::public_note(store, note_id, Some(view)).await
}

/// `GET /authors/{author_id}`: the minimal public identity of an author with at
/// least one published note or public project. Never the owner account id.
pub async fn author(store: &Store, author_id: &str) -> R<Value> {
    let row = sqlx::query(
        "SELECT a.author_id,o.display_name FROM authors a JOIN owners o ON o.id=a.owner_id WHERE a.author_id=$1 \
         AND (EXISTS(SELECT 1 FROM notes n JOIN note_published_representations r ON r.note_id=n.note_id WHERE n.owner_id=a.owner_id) \
         OR EXISTS(SELECT 1 FROM projects p WHERE p.owner_id=a.owner_id AND p.visibility='public'))",
    )
    .bind(author_id)
    .fetch_optional(&store.db)
    .await?
    .ok_or_else(NoteError::not_found)?;
    Ok(json!({
        "author_id": row.get::<String, _>("author_id"),
        "name": publication::author_name(row.get("display_name")),
    }))
}

/// `GET /authors/{author_id}/projects`: the author's public projects in project
/// id order, each from `projects::public_project`, with a cursor.
pub async fn author_projects(
    store: &Store,
    author_id: &str,
    cursor: Option<&str>,
    limit: Option<&str>,
) -> R<Value> {
    let limit = notes::page_limit(limit)?;
    if cursor.is_some_and(|c| !notes::token(c)) {
        return Err(NoteError::invalid(
            "invalid_cursor",
            "The cursor is not valid",
        ));
    }
    author(store, author_id).await?;
    let ids = projects::public_project_ids(store, author_id, cursor, limit + 1).await?;
    let more = ids.len() as i64 > limit;
    let mut items = vec![];
    let mut next = Value::Null;
    for id in ids.iter().take(limit as usize) {
        next = json!(id);
        match projects::public_project(store, id).await {
            Ok(project) => items.push(project),
            Err(e) if e.code == "not_found" => {}
            Err(e) => return Err(e),
        }
    }
    Ok(json!({
        "items": items,
        "next_cursor": if more { next } else { Value::Null },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_text_skips_cards_and_handwriting() {
        let doc = json!({"blocks": [
            {"id": "h", "kind": "heading", "level": 1, "spans": [{"text": "Cell "}, {"text": "cycle"}]},
            {"id": "r", "kind": "reports_embed", "report_id": "rep1", "title": "Private chart"},
            {"id": "l", "kind": "list", "ordered": false, "items": [{"spans": [{"text": "Mitosis"}]}]},
            {"id": "t", "kind": "table", "rows": [{"cells": [{"spans": [{"text": "G1"}]}]}]},
            {"id": "f", "kind": "figure", "file_id": "f1", "caption": [{"text": "Diagram"}]},
            {"id": "w", "kind": "handwriting", "width": 10, "height": 10, "ink": []}
        ]});
        assert_eq!(typed_text(&doc), "Cell cycle\nMitosis\nG1\nDiagram\n");
    }

    #[test]
    fn summary_is_short_and_collapses_whitespace() {
        assert_eq!(summary("  One\n\ntwo   three "), "One two three");
        let long = "word ".repeat(100);
        assert_eq!(
            summary(&long).chars().count(),
            DISCOVERY_LIMITS.summary_chars
        );
    }

    #[test]
    fn terms_split_at_spaces_and_punctuation_and_keep_thai_runs() {
        assert_eq!(
            terms("Photo-synthesis, CELLS! ภาษาไทย…ดี"),
            vec!["photo", "synthesis", "cells", "ภาษาไทย", "ดี"]
        );
    }

    #[test]
    fn browse_token_round_trips_and_rejects_bad_values() {
        let b = Browse::new();
        assert_eq!(Browse::parse(&b.token()).unwrap(), b);
        for bad in ["", "x", "1.XYZ", "-1.0123456789abcdef", "1.0123"] {
            assert!(Browse::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn order_puts_one_note_of_each_author_per_round() {
        let browse = Browse::parse("1.0123456789abcdef").unwrap();
        let notes: Vec<(String, String)> = (0..9)
            .map(|i| (format!("n{i}"), format!("a{}", i % 3)))
            .collect();
        let ordered = order(&browse, notes);
        for round in ordered.chunks(3) {
            let authors: HashSet<&String> = round.iter().map(|(p, _)| &p.2).collect();
            assert_eq!(authors.len(), 3);
        }
    }
}
