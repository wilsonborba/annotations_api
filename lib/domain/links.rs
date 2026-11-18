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

/// Conservative globally routable unicast policy, including IPv4-mapped IPv6.
fn public_ip(ip: std::net::IpAddr) -> bool {
    use std::net::IpAddr;
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_broadcast()
                && a != 0
                && a < 224
                && !(a == 100 && (64..=127).contains(&b))
                && !(a == 192 && (b == 0 || (b == 2) || (b == 88 && c == 99)))
                && !(a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                && !(a == 203 && b == 0 && c == 113)
        }
        IpAddr::V6(ip) => {
            if let Some(v4) = ip.to_ipv4_mapped() {
                return public_ip(v4.into());
            }
            let s = ip.segments();
            // Only global unicast; exclude special-purpose, documentation and 6to4.
            s[0] & 0xe000 == 0x2000
                && !(s[0] == 0x2001 && s[1] < 0x200)
                && !(s[0] == 0x2001 && s[1] == 0xdb8)
                && s[0] != 0x2002
                && !(s[0] == 0x3fff && s[1] < 0x1000)
        }
    }
}

pub async fn preview(store: &Store, raw: &str) -> Result<Value> {
    let url = validate_url(raw)?;
    let key = format!("pub:link:v2:{url}");
    let epoch = store.cache.epoch().await;
    if let Some(epoch) = epoch
        && let Some(text) = store.cache.get_at(epoch, &key).await
        && let Ok(value) = serde_json::from_str(&text)
    {
        return Ok(value);
    }
    let value = preview_with(
        raw,
        &crate::dal::links::Network,
        &crate::dal::links::Network,
        crate::core::LINK_PREVIEW_LIMITS,
    )
    .await?;
    if let Some(epoch) = epoch {
        store.cache.put_at(epoch, &key, &value.to_string()).await;
    }
    Ok(value)
}

/// The single deadline includes DNS, every redirect, headers and the full body.
pub async fn preview_with(
    raw: &str,
    resolver: &impl crate::dal::links::Resolver,
    transport: &impl crate::dal::links::Transport,
    limits: crate::core::LinkPreviewLimits,
) -> Result<Value> {
    tokio::time::timeout(limits.timeout, fetch(raw, resolver, transport, limits))
        .await
        .map_err(|_| Error::bad("link_preview_timeout"))?
}
async fn fetch(
    raw: &str,
    resolver: &impl crate::dal::links::Resolver,
    transport: &impl crate::dal::links::Transport,
    limits: crate::core::LinkPreviewLimits,
) -> Result<Value> {
    use futures_util::StreamExt;
    let mut url = validate_url(raw)?;
    for hop in 0..=limits.redirects {
        let port = url
            .port_or_known_default()
            .ok_or_else(|| Error::bad("Invalid URL"))?;
        let addresses = match url.host().ok_or_else(|| Error::bad("Missing domain"))? {
            url::Host::Ipv4(ip) => vec![std::net::SocketAddr::new(ip.into(), port)],
            url::Host::Ipv6(ip) => vec![std::net::SocketAddr::new(ip.into(), port)],
            url::Host::Domain(host) => resolver.resolve(host, port).await?,
        };
        if addresses.is_empty()
            || addresses
                .iter()
                .any(|a| !public_ip(a.ip()) || a.port() != port)
        {
            return Err(Error::bad("link_preview_destination_rejected"));
        }
        let mut response = transport.get(&url, &addresses).await?;
        if matches!(response.status, 301 | 302 | 303 | 307 | 308) {
            if hop == limits.redirects {
                return Err(Error::bad("link_preview_redirect_limit"));
            }
            let target = url
                .join(
                    response
                        .location
                        .as_deref()
                        .ok_or_else(|| Error::bad("link_preview_invalid_redirect"))?,
                )
                .map_err(|_| Error::bad("link_preview_invalid_redirect"))?;
            url = validate_url(target.as_str())?;
            continue;
        }
        if !(200..300).contains(&response.status) {
            return Err(Error::bad("link_preview_unavailable"));
        }
        let mime = response
            .content_type
            .as_deref()
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim();
        if !mime.eq_ignore_ascii_case("text/html")
            && !mime.eq_ignore_ascii_case("application/xhtml+xml")
        {
            return Err(Error::bad("link_preview_content_type_rejected"));
        }
        if response
            .content_length
            .is_some_and(|n| n > limits.bytes as u64)
        {
            return Err(Error::bad("link_preview_too_large"));
        }
        let mut data = Vec::new();
        while let Some(chunk) = response.body.next().await {
            let chunk = chunk?;
            if chunk.len() > limits.bytes.saturating_sub(data.len()) {
                return Err(Error::bad("link_preview_too_large"));
            }
            data.extend(chunk);
        }
        return page_metadata(&url, &String::from_utf8_lossy(&data));
    }
    unreachable!()
}
fn page_metadata(url: &url::Url, text: &str) -> Result<Value> {
    let mut out = metadata(url.as_str())?;
    out["site_name"] = json!(url.host_str().unwrap_or(""));
    out["description"] = json!("");
    if let Some(title) = regex::Regex::new(r"(?is)<title[^>]*>(.*?)</title>")
        .unwrap()
        .captures(text)
        .and_then(|c| c.get(1))
    {
        out["title"] = json!(title.as_str().trim());
    }
    for (property, key) in [
        ("og:title", "title"),
        ("og:site_name", "site_name"),
        ("description", "description"),
        ("og:description", "description"),
        ("og:image", "thumbnail"),
    ] {
        for pattern in [
            format!(
                r#"(?is)<meta\b[^>]+(?:property|name)\s*=\s*["']{property}["'][^>]+content\s*=\s*["']([^"']*)["']"#
            ),
            format!(
                r#"(?is)<meta\b[^>]+content\s*=\s*["']([^"']*)["'][^>]+(?:property|name)\s*=\s*["']{property}["']"#
            ),
        ] {
            if let Some(value) = regex::Regex::new(&pattern)
                .unwrap()
                .captures(text)
                .and_then(|c| c.get(1))
            {
                if key == "thumbnail" {
                    if let Ok(image) = url.join(value.as_str())
                        && validate_url(image.as_str()).is_ok()
                    {
                        out[key] = json!(image.as_str());
                    }
                } else {
                    out[key] = json!(value.as_str());
                }
                break;
            }
        }
    }
    Ok(out)
}
