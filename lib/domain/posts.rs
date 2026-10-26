use crate::{
    core::{self, Error, Result},
    dal::Store,
    domain::schemas,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Translation {
    pub language: String,
    pub title: String,
    #[serde(default)]
    pub summary: String,
    #[serde(default = "default_schema")]
    pub schema_name: String,
    pub schema_version: Option<i64>,
    pub body: Value,
}
fn default_schema() -> String {
    "article-body".into()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WritePost {
    pub owner_id: String,
    pub slug: String,
    #[serde(default = "public")]
    pub visibility: String,
    #[serde(default)]
    pub cover_url: String,
    pub channels: Vec<String>,
    #[serde(default)]
    pub topics: Vec<String>,
    pub translations: Vec<Translation>,
}
fn public() -> String {
    "public".into()
}
#[derive(Default, Deserialize, Serialize)]
pub struct Query {
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub channel: String,
    #[serde(default)]
    pub lang: String,
    #[serde(default)]
    pub offset: i64,
}

pub async fn catalog_write(store: &Store, body: Value) -> Result<Value> {
    let mut tx = store.db.begin().await?;
    for (table, idcol, namecol) in [
        ("owners", "id", "display_name"),
        ("channels", "id", "name"),
        ("topics", "id", "name"),
        ("languages", "code", "label"),
    ] {
        if let Some(rows) = body.get(table) {
            let rows = rows
                .as_array()
                .ok_or_else(|| Error::bad("Catalog collections must be lists"))?;
            for row in rows {
                let id = row[idcol]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| Error::bad("Catalog entry needs an ID"))?;
                let name = row[namecol]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| Error::bad("Catalog entry needs a name"))?;
                sqlx::query(&format!("INSERT INTO {table}({idcol},{namecol}) VALUES($1,$2) ON CONFLICT({idcol}) DO UPDATE SET {namecol}=excluded.{namecol}")).bind(id).bind(name).execute(&mut *tx).await?;
            }
        }
    }
    tx.commit().await?;
    store.cache.invalidate().await;
    Ok(json!({"saved":true}))
}
pub async fn catalog(store: &Store) -> Result<Value> {
    let mut out = json!({});
    for (table, idcol, namecol) in [
        ("channels", "id", "name"),
        ("topics", "id", "name"),
        ("languages", "code", "label"),
    ] {
        let rows = sqlx::query(&format!(
            "SELECT {idcol} AS id,{namecol} AS name FROM {table} ORDER BY {idcol}"
        ))
        .fetch_all(&store.db)
        .await?;
        out[table] = Value::Array(
            rows.into_iter()
                .map(|r| json!({"id":r.get::<String,_>("id"),"name":r.get::<String,_>("name")}))
                .collect(),
        );
    }
    Ok(out)
}
pub async fn write(store: &Store, id: &str, input: WritePost) -> Result<Value> {
    if !core::slug(id)
        || !core::slug(&input.slug)
        || input.owner_id.is_empty()
        || !matches!(input.visibility.as_str(), "public" | "private")
        || input.channels.is_empty()
        || input.translations.is_empty()
    {
        return Err(Error::bad(
            "Owner, slug, channels, translations and a valid visibility are required",
        ));
    }
    if !input.cover_url.is_empty() {
        schemas::validate_url(&input.cover_url)?;
    }
    let mut langs = std::collections::HashSet::new();
    let mut validated = vec![];
    // Validate every translation before any document write.
    for tr in &input.translations {
        if tr.title.trim().is_empty() || !langs.insert(&tr.language) {
            return Err(Error::bad("Each language needs one nonempty title"));
        }
        if sqlx::query("SELECT code FROM languages WHERE code=$1")
            .bind(&tr.language)
            .fetch_optional(&store.db)
            .await?
            .is_none()
        {
            return Err(Error::bad("Register the language before publishing"));
        }
        validated
            .push(schemas::validate(store, &tr.schema_name, tr.schema_version, &tr.body).await?);
    }
    let mut created: Vec<String> = vec![];
    for (tr, version) in input.translations.iter().zip(&validated) {
        let docid = core::id();
        let doc = json!({"type":"article_translation","post_id":id,"language":tr.language,"schema_name":tr.schema_name,"schema_version":version,"body":tr.body});
        if let Err(e) = store.docs.put(&docid, &doc).await {
            for did in &created {
                let _ = store.docs.delete(did).await;
            }
            return Err(e);
        }
        created.push(docid);
    }
    let saved:Result<Vec<String>>=async {
  let mut tx=store.db.begin().await?;
  let old=sqlx::query("SELECT document_id FROM post_translations WHERE post_id=$1").bind(id).fetch_all(&mut *tx).await?.iter().map(|r|r.get::<String,_>("document_id")).collect();
  sqlx::query("INSERT INTO posts(id,owner_id,slug,visibility,status,cover_url,created_at,updated_at) VALUES($1,$2,$3,$4,'draft',$5,$6,$6) ON CONFLICT(id) DO UPDATE SET owner_id=excluded.owner_id,slug=excluded.slug,visibility=excluded.visibility,cover_url=excluded.cover_url,updated_at=excluded.updated_at").bind(id).bind(&input.owner_id).bind(&input.slug).bind(&input.visibility).bind(&input.cover_url).bind(core::now()).execute(&mut *tx).await?;
  for table in ["post_translations","post_channels","post_topics"]{sqlx::query(&format!("DELETE FROM {table} WHERE post_id=$1")).bind(id).execute(&mut *tx).await?;}
  for ((tr,version),docid) in input.translations.iter().zip(&validated).zip(&created){sqlx::query("INSERT INTO post_translations(post_id,language_code,title,summary,document_id,schema_name,schema_version) VALUES($1,$2,$3,$4,$5,$6,$7)").bind(id).bind(&tr.language).bind(&tr.title).bind(&tr.summary).bind(docid).bind(&tr.schema_name).bind(*version).execute(&mut *tx).await?;}
  for channel in &input.channels{sqlx::query("INSERT INTO post_channels(post_id,channel_id) VALUES($1,$2)").bind(id).bind(channel).execute(&mut *tx).await?;}
  for topic in &input.topics{sqlx::query("INSERT INTO post_topics(post_id,topic_id) VALUES($1,$2)").bind(id).bind(topic).execute(&mut *tx).await?;}
  tx.commit().await?;Ok(old)
 }.await;
    match saved {
        Err(e) => {
            for did in created {
                let _ = store.docs.delete(&did).await;
            }
            Err(e)
        }
        Ok(old) => {
            for did in old {
                if store.docs.delete(&did).await.is_err() {
                    tracing::warn!(document=%did,"Superseded body cleanup failed")
                }
            }
            store.cache.invalidate().await;
            Ok(json!({"id":id,"saved":true}))
        }
    }
}
pub async fn publish(store: &Store, id: &str) -> Result<Value> {
    let r=sqlx::query("UPDATE posts SET status='published',updated_at=$1 WHERE id=$2 AND EXISTS(SELECT 1 FROM post_translations WHERE post_id=$2)").bind(core::now()).bind(id).execute(&store.db).await?;
    if r.rows_affected() != 1 {
        return Err(Error::missing());
    }
    store.cache.invalidate().await;
    Ok(json!({"id":id,"status":"published"}))
}
async fn summary(store: &Store, row: &sqlx::any::AnyRow, lang: &str) -> Result<Value> {
    let id = row.get::<String, _>("id");
    let trs=sqlx::query("SELECT language_code,title,summary FROM post_translations WHERE post_id=$1 ORDER BY language_code").bind(&id).fetch_all(&store.db).await?;
    let selected = trs
        .iter()
        .find(|r| r.get::<String, _>("language_code") == lang)
        .or_else(|| {
            trs.iter()
                .find(|r| r.get::<String, _>("language_code") == "en")
        })
        .or(trs.first())
        .ok_or_else(Error::missing)?;
    let mut out = json!({"id":id,"slug":row.get::<String,_>("slug"),"owner_id":row.get::<String,_>("owner_id"),"cover_url":row.get::<String,_>("cover_url"),"updated_at":row.get::<i64,_>("updated_at"),"language":selected.get::<String,_>("language_code"),"title":selected.get::<String,_>("title"),"summary":selected.get::<String,_>("summary"),"available_languages":trs.iter().map(|r|r.get::<String,_>("language_code")).collect::<Vec<_>>()});
    for (key, table, col) in [
        ("channels", "post_channels", "channel_id"),
        ("topics", "post_topics", "topic_id"),
    ] {
        let rows = sqlx::query(&format!(
            "SELECT {col} AS id FROM {table} WHERE post_id=$1 ORDER BY {col}"
        ))
        .bind(&id)
        .fetch_all(&store.db)
        .await?;
        out[key] = json!(
            rows.iter()
                .map(|r| r.get::<String, _>("id"))
                .collect::<Vec<_>>()
        );
    }
    Ok(out)
}
pub async fn list(store: &Store, q: &Query) -> Result<Value> {
    if q.offset < 0 {
        return Err(Error::bad("Offset must not be negative"));
    }
    let text = if q.q.is_empty() {
        String::new()
    } else {
        format!("%{}%", q.q.to_lowercase())
    };
    let rows=sqlx::query("SELECT p.* FROM posts p WHERE p.status='published' AND p.visibility='public' AND ($1='' OR EXISTS(SELECT 1 FROM post_translations t WHERE t.post_id=p.id AND (lower(t.title) LIKE $1 OR lower(t.summary) LIKE $1))) AND ($2='' OR EXISTS(SELECT 1 FROM post_topics t WHERE t.post_id=p.id AND t.topic_id=$2)) AND ($3='' OR EXISTS(SELECT 1 FROM post_channels c WHERE c.post_id=p.id AND c.channel_id=$3)) ORDER BY p.updated_at DESC,p.id LIMIT 24 OFFSET $4").bind(text).bind(&q.topic).bind(&q.channel).bind(q.offset).fetch_all(&store.db).await?;
    let mut items = vec![];
    for row in rows {
        items.push(summary(store, &row, &q.lang).await?)
    }
    Ok(json!({"items":items,"offset":q.offset}))
}
pub async fn detail(
    store: &Store,
    key: &str,
    lang: &str,
    private: bool,
    channel: &str,
) -> Result<Value> {
    let row=sqlx::query("SELECT * FROM posts WHERE (id=$1 OR slug=$1) AND ($2=1 OR (visibility='public' AND status='published'))").bind(key).bind(i64::from(private)).fetch_optional(&store.db).await?.ok_or_else(Error::missing)?;
    let mut out = summary(store, &row, lang).await?;
    if !channel.is_empty()
        && !out["channels"]
            .as_array()
            .is_some_and(|cs| cs.iter().any(|c| c == channel))
    {
        return Err(Error::missing());
    }
    if !lang.is_empty() && out["language"] != lang {
        return Err(Error::missing());
    }
    let tr=sqlx::query("SELECT document_id,schema_name,schema_version FROM post_translations WHERE post_id=$1 AND language_code=$2").bind(row.get::<String,_>("id")).bind(out["language"].as_str().unwrap_or("en")).fetch_one(&store.db).await?;
    let doc = store.docs.get(&tr.get::<String, _>("document_id")).await?;
    out["body"] = doc["body"].clone();
    out["schema_name"] = json!(tr.get::<String, _>("schema_name"));
    out["schema_version"] = json!(tr.get::<i64, _>("schema_version"));
    Ok(out)
}
