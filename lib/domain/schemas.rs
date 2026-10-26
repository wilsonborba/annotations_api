use crate::{
    core::{Error, Result, slug},
    dal::Store,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub path: String,
    pub field_type: String,
    #[serde(default)]
    pub required: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Draft {
    pub name: String,
    pub version: i64,
    pub fields: Vec<Field>,
}

pub async fn draft(store: &Store, input: Draft) -> Result<Value> {
    if !slug(&input.name) || input.version < 1 || input.fields.is_empty() {
        return Err(Error::bad(
            "A name, positive version and fields are required",
        ));
    }
    let mut paths = std::collections::HashSet::new();
    for field in &input.fields {
        if field.path.is_empty()
            || !paths.insert(&field.path)
            || !matches!(
                field.field_type.as_str(),
                "string" | "number" | "integer" | "boolean" | "object" | "array"
            )
        {
            return Err(Error::bad("Invalid or duplicate schema field"));
        }
    }
    let mut tx = store.db.begin().await?;
    let existing = sqlx::query("SELECT status FROM schema_versions WHERE name=$1 AND version=$2")
        .bind(&input.name)
        .bind(input.version)
        .fetch_optional(&mut *tx)
        .await?;
    if existing.is_some_and(|r| r.get::<String, _>("status") != "draft") {
        return Err(Error::conflict("Published schema versions are immutable"));
    }
    sqlx::query("INSERT INTO schema_versions(name,version,status,is_default) VALUES($1,$2,'draft',0) ON CONFLICT(name,version) DO NOTHING").bind(&input.name).bind(input.version).execute(&mut *tx).await?;
    sqlx::query("DELETE FROM schema_fields WHERE schema_name=$1 AND schema_version=$2")
        .bind(&input.name)
        .bind(input.version)
        .execute(&mut *tx)
        .await?;
    for field in input.fields {
        sqlx::query("INSERT INTO schema_fields(schema_name,schema_version,path,field_type,required) VALUES($1,$2,$3,$4,$5)").bind(&input.name).bind(input.version).bind(field.path).bind(field.field_type).bind(i64::from(field.required)).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    store.cache.invalidate().await;
    Ok(serde_json::json!({"name":input.name,"version":input.version,"status":"draft"}))
}
pub async fn publish(store: &Store, name: &str, version: i64) -> Result<Value> {
    let mut tx = store.db.begin().await?;
    let r=sqlx::query("UPDATE schema_versions SET status='published' WHERE name=$1 AND version=$2 AND status='draft'").bind(name).bind(version).execute(&mut *tx).await?;
    if r.rows_affected() != 1 {
        return Err(Error::conflict("Only an existing draft can be published"));
    }
    sqlx::query("UPDATE schema_versions SET is_default=0 WHERE name=$1")
        .bind(name)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE schema_versions SET is_default=1 WHERE name=$1 AND version=$2")
        .bind(name)
        .bind(version)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    store.cache.invalidate().await;
    Ok(serde_json::json!({"name":name,"version":version,"status":"published"}))
}
pub async fn list(store: &Store) -> Result<Value> {
    let rows = sqlx::query(
        "SELECT name,version,status,is_default FROM schema_versions ORDER BY name,version",
    )
    .fetch_all(&store.db)
    .await?;
    Ok(Value::Array(rows.into_iter().map(|r|serde_json::json!({"name":r.get::<String,_>("name"),"version":r.get::<i64,_>("version"),"status":r.get::<String,_>("status"),"is_default":r.get::<i64,_>("is_default")==1})).collect()))
}
fn values_at<'a>(value: &'a Value, path: &str) -> Vec<&'a Value> {
    let mut values = vec![value];
    for part in path.split('.') {
        let array = part.ends_with("[]");
        let key = part.trim_end_matches("[]");
        values = values
            .into_iter()
            .filter_map(|v| v.get(key))
            .flat_map(|v| {
                if array {
                    v.as_array().map(|v| v.iter().collect()).unwrap_or_default()
                } else {
                    vec![v]
                }
            })
            .collect();
    }
    values
}
pub async fn validate(
    store: &Store,
    name: &str,
    version: Option<i64>,
    body: &Value,
) -> Result<i64> {
    let row=if let Some(v)=version{sqlx::query("SELECT version FROM schema_versions WHERE name=$1 AND version=$2 AND status='published'").bind(name).bind(v).fetch_optional(&store.db).await?}
 else{sqlx::query("SELECT version FROM schema_versions WHERE name=$1 AND is_default=1 AND status='published'").bind(name).fetch_optional(&store.db).await?}.ok_or_else(||Error::bad("A registered published schema version is required"))?;
    let version = row.get::<i64, _>("version");
    let fields=sqlx::query("SELECT path,field_type,required FROM schema_fields WHERE schema_name=$1 AND schema_version=$2").bind(name).bind(version).fetch_all(&store.db).await?;
    let roots: std::collections::HashSet<String> = fields
        .iter()
        .map(|r| {
            r.get::<String, _>("path")
                .split('.')
                .next()
                .unwrap_or("")
                .trim_end_matches("[]")
                .to_string()
        })
        .collect();
    let object = body
        .as_object()
        .ok_or_else(|| Error::bad("Document body must be an object"))?;
    if object.keys().any(|k| !roots.contains(k)) {
        return Err(Error::bad("Document contains an unregistered root field"));
    }
    for field in fields {
        let path = field.get::<String, _>("path");
        let values = values_at(body, &path);
        let typ = field.get::<String, _>("field_type");
        if values.is_empty() && field.get::<i64, _>("required") == 1 {
            return Err(Error::bad(format!("Missing required field: {path}")));
        }
        for value in values {
            let valid = match typ.as_str() {
                "string" => value.is_string(),
                "number" => value.is_number(),
                "integer" => value.is_i64() || value.is_u64(),
                "boolean" => value.is_boolean(),
                "array" => value.is_array(),
                "object" => value.is_object(),
                _ => false,
            };
            if !valid {
                return Err(Error::bad(format!("Invalid type for {path}")));
            }
        }
    }
    validate_blocks(body)?;
    Ok(version)
}
pub fn validate_blocks(body: &Value) -> Result<()> {
    let blocks = body["blocks"]
        .as_array()
        .ok_or_else(|| Error::bad("blocks must be an array"))?;
    if blocks.is_empty() {
        return Err(Error::bad("At least one block is required"));
    }
    for block in blocks {
        let typ = block["type"]
            .as_str()
            .ok_or_else(|| Error::bad("Each block needs a type"))?;
        match typ {
            "heading" | "paragraph" | "quote" => {
                if !block["text"].is_string() {
                    return Err(Error::bad("Text blocks require text"));
                }
            }
            "list" => {
                if !block["items"]
                    .as_array()
                    .is_some_and(|a| a.iter().all(Value::is_string))
                {
                    return Err(Error::bad("Lists require text items"));
                }
            }
            "image" | "link" | "youtube" | "pdf" | "source" => {
                validate_url(block["url"].as_str().unwrap_or(""))?;
            }
            "reports" => {
                let url = validate_url(block["url"].as_str().unwrap_or(""))?;
                if url.host_str() != Some("reports.asodya.com") {
                    return Err(Error::bad("Reports embeds must use the Reports origin"));
                }
            }
            _ => return Err(Error::bad("Unsupported block type")),
        }
        if let Some(color) = block.get("color")
            && !matches!(
                color.as_str(),
                Some("default" | "muted" | "purple" | "green")
            )
        {
            return Err(Error::bad("Unsupported colour"));
        }
        for flag in ["bold", "italic", "underline"] {
            if block.get(flag).is_some_and(|v| !v.is_boolean()) {
                return Err(Error::bad("Formatting flags must be booleans"));
            }
        }
    }
    Ok(())
}
pub fn validate_url(raw: &str) -> Result<url::Url> {
    let url = url::Url::parse(raw).map_err(|_| Error::bad("Invalid URL"))?;
    if !matches!(url.scheme(), "https" | "http")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Error::bad(
            "Only HTTP(S) links without credentials are supported",
        ));
    }
    Ok(url)
}
