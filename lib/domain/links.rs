use crate::{
    core::{Error, Result},
    dal::Store,
    domain::schemas::validate_url,
};
use serde_json::{Value, json};
pub fn metadata(raw: &str) -> Result<Value> {
    let url = validate_url(raw)?;
    let domain = url.host_str().ok_or_else(|| Error::bad("Missing domain"))?;
    let provider = if domain == "drive.google.com" {
        "google-drive"
    } else if domain == "discord.com" || domain == "discord.gg" {
        "discord"
    } else if domain == "youtube.com" || domain.ends_with(".youtube.com") || domain == "youtu.be" {
        "youtube"
    } else {
        "link"
    };
    let mut out =
        json!({"url":raw,"title":domain,"domain":domain,"provider":provider,"thumbnail":""});
    if provider == "youtube" {
        let video = if domain == "youtu.be" {
            url.path().trim_start_matches('/').to_string()
        } else if url.path() == "/watch" {
            url.query_pairs()
                .find(|(k, _)| k == "v")
                .map(|(_, v)| v.into_owned())
                .unwrap_or_default()
        } else {
            url.path()
                .strip_prefix("/shorts/")
                .or_else(|| url.path().strip_prefix("/embed/"))
                .unwrap_or("")
                .to_string()
        };
        if video.len() == 11
            && video
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            out["embed_url"] = json!(format!("https://www.youtube-nocookie.com/embed/{video}"));
        }
    }
    Ok(out)
}
fn public_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => {
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_broadcast()
                && ip.octets()[0] != 0
                && !(ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1]))
        }
        std::net::IpAddr::V6(ip) => {
            !ip.is_loopback()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && (ip.segments()[0] & 0xfe00 != 0xfc00)
                && (ip.segments()[0] & 0xffc0 != 0xfe80)
                && ip.to_ipv4_mapped().is_none_or(|ip| public_ip(ip.into()))
        }
    }
}
pub async fn preview(store: &Store, raw: &str) -> Result<Value> {
    let mut out = metadata(raw)?;
    let url = validate_url(raw)?;
    let host = url.host_str().unwrap();
    let addresses = tokio::net::lookup_host((host, url.port_or_known_default().unwrap_or(443)))
        .await
        .map_err(|_| Error::bad("Destination is unavailable"))?
        .collect::<Vec<_>>();
    if addresses.is_empty() || addresses.iter().any(|a| !public_ip(a.ip())) {
        return Err(Error::bad("Link previews require a public destination"));
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(5))
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(|_| Error::unavailable())?;
    let key = format!("pub:link:{raw}");
    let epoch = store.cache.epoch().await;
    if let Some(epoch) = epoch
        && let Some(text) = store.cache.get_at(epoch, &key).await
        && let Ok(v) = serde_json::from_str(&text)
    {
        return Ok(v);
    }
    if let Ok(mut response) = client.get(raw).send().await
        && response.status().is_success()
        && response
            .headers()
            .get("content-type")
            .is_some_and(|v| v.to_str().unwrap_or("").contains("text/html"))
    {
        let mut data = vec![];
        while let Ok(Some(chunk)) = response.chunk().await {
            if data.len() + chunk.len() > 512_000 {
                break;
            }
            data.extend(chunk);
        }
        let text = String::from_utf8_lossy(&data);
        for (property, key) in [("og:title", "title"), ("og:image", "thumbnail")] {
            let patterns = [
                format!(
                    r#"(?is)<meta[^>]+(?:property|name)=["']{property}["'][^>]+content=["']([^"']+)["']"#
                ),
                format!(
                    r#"(?is)<meta[^>]+content=["']([^"']+)["'][^>]+(?:property|name)=["']{property}["']"#
                ),
            ];
            for pattern in patterns {
                if let Some(value) = regex::Regex::new(&pattern)
                    .unwrap()
                    .captures(&text)
                    .and_then(|c| c.get(1))
                {
                    if key != "thumbnail" || validate_url(value.as_str()).is_ok() {
                        out[key] = json!(value.as_str());
                    }
                    break;
                }
            }
        }
    }
    if let Some(epoch) = epoch {
        store.cache.put_at(epoch, &key, &out.to_string()).await;
    }
    Ok(out)
}
