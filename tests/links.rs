use annotations_api::domain::links;
#[test]
fn provider_cards_fall_back_without_remote_metadata() {
    assert_eq!(
        links::metadata("https://drive.google.com/file/d/example/view").unwrap()["provider"],
        "google-drive"
    );
    assert_eq!(
        links::metadata("https://discord.gg/example").unwrap()["provider"],
        "discord"
    );
    assert_eq!(
        links::metadata("https://example.com").unwrap()["thumbnail"],
        ""
    );
    assert!(links::metadata("javascript:alert(1)").is_err());
}
#[tokio::test]
async fn previews_do_not_fetch_private_hosts() {
    let store = annotations_api::dal::Store::fixture().await;
    assert!(
        links::preview(&store, "http://127.0.0.1:8001")
            .await
            .is_err()
    );
}

#[test]
fn youtube_embed_is_derived_only_for_a_valid_video_identifier() {
    use annotations_api::domain::links::metadata;
    assert_eq!(
        metadata("https://youtu.be/dQw4w9WgXcQ").unwrap()["embed_url"],
        "https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ"
    );
    assert!(
        metadata("https://youtube.com/watch?v=bad")
            .unwrap()
            .get("embed_url")
            .is_none()
    );
}

use annotations_api::{
    core::{Error, LINK_PREVIEW_LIMITS, Result},
    dal::links::{Resolver, Response, Transport},
};
use futures_util::{future::BoxFuture, stream};
use std::{collections::VecDeque, net::SocketAddr, sync::Mutex, time::Duration};
use url::Url;

struct Double {
    addresses: Vec<SocketAddr>,
    responses: Mutex<VecDeque<Response>>,
    calls: Mutex<Vec<(String, Vec<SocketAddr>)>>,
    resolved: Mutex<Vec<String>>,
}
impl Double {
    fn new(responses: Vec<Response>) -> Self {
        Self {
            addresses: vec!["93.184.215.14:443".parse().unwrap()],
            responses: Mutex::new(responses.into()),
            calls: Mutex::default(),
            resolved: Mutex::default(),
        }
    }
    async fn preview(&self, url: &str) -> Result<serde_json::Value> {
        links::preview_with(url, self, self, LINK_PREVIEW_LIMITS).await
    }
}
impl Resolver for Double {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> BoxFuture<'a, Result<Vec<SocketAddr>>> {
        Box::pin(async move {
            self.resolved.lock().unwrap().push(host.into());
            if host == "private.example" {
                return Ok(vec![SocketAddr::from(([10, 1, 2, 3], port))]);
            }
            Ok(self
                .addresses
                .iter()
                .map(|a| SocketAddr::new(a.ip(), port))
                .collect())
        })
    }
}
impl Transport for Double {
    fn get<'a>(
        &'a self,
        url: &'a Url,
        addresses: &'a [SocketAddr],
    ) -> BoxFuture<'a, Result<Response>> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push((url.to_string(), addresses.to_vec()));
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| Error::bad("unexpected fetch"))
        })
    }
}
fn response(status: u16, location: Option<&str>, body: Vec<Vec<u8>>) -> Response {
    Response {
        status,
        location: location.map(str::to_owned),
        content_type: Some("text/html; charset=utf-8".into()),
        content_length: None,
        body: Box::pin(stream::iter(body.into_iter().map(Ok))),
    }
}
fn html(text: &str) -> Response {
    response(200, None, vec![text.as_bytes().to_vec()])
}

#[tokio::test]
async fn rejects_nonpublic_literals_without_dns_or_transport() {
    for host in [
        "127.0.0.1",
        "10.2.3.4",
        "192.168.1.1",
        "172.16.0.1",
        "169.254.1.1",
        "0.0.0.0",
        "100.64.0.1",
        "192.0.2.1",
        "198.51.100.1",
        "203.0.113.1",
        "198.18.0.1",
        "224.0.0.1",
        "240.0.0.1",
        "[::1]",
        "[::]",
        "[fc00::1]",
        "[fe80::1]",
        "[ff02::1]",
        "[::ffff:127.0.0.1]",
        "[2001:db8::1]",
        "[2002:a00:1::1]",
        "[64:ff9b::a00:1]",
        "[3fff::1]",
    ] {
        let d = Double::new(vec![]);
        let e = d.preview(&format!("http://{host}/")).await.unwrap_err();
        assert_eq!(e.1, "link_preview_destination_rejected", "{host}");
        assert!(d.calls.lock().unwrap().is_empty());
        assert!(d.resolved.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn rejects_private_empty_and_mixed_dns_answers() {
    for answers in [
        vec![],
        vec!["10.1.2.3:443"],
        vec!["93.184.215.14:443", "192.168.1.1:443"],
    ] {
        let mut d = Double::new(vec![]);
        d.addresses = answers.into_iter().map(|a| a.parse().unwrap()).collect();
        assert_eq!(
            d.preview("https://example.com").await.unwrap_err().1,
            "link_preview_destination_rejected"
        );
        assert!(d.calls.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn rejects_private_redirects_before_next_request() {
    for target in [
        "http://127.0.0.1/",
        "http://10.1.1.1/",
        "http://[::1]/",
        "https://private.example/",
    ] {
        let d = Double::new(vec![response(302, Some(target), vec![])]);
        assert_eq!(
            d.preview("https://example.com").await.unwrap_err().1,
            "link_preview_destination_rejected"
        );
        assert_eq!(d.calls.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn rejects_unsafe_initial_and_redirect_urls() {
    for target in [
        "file:///etc/passwd",
        "ftp://example.com",
        "https://user:pass@example.com",
    ] {
        let d = Double::new(vec![response(302, Some(target), vec![])]);
        assert!(d.preview(target).await.is_err());
        assert!(d.calls.lock().unwrap().is_empty());
        assert!(d.preview("https://example.com").await.is_err());
        assert_eq!(d.calls.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn public_html_returns_only_metadata_and_pins_resolved_addresses() {
    let d = Double::new(vec![html(
        r#"<title>Fallback</title><meta property="og:title" content="Page"><meta content="Site" property="og:site_name"><meta name="description" content="Summary"><meta property="og:image" content="/picture.png"><script>secret()</script>"#,
    )]);
    let value = d.preview("https://example.com/page").await.unwrap();
    assert_eq!(value["title"], "Page");
    assert_eq!(value["site_name"], "Site");
    assert_eq!(value["description"], "Summary");
    assert_eq!(value["thumbnail"], "https://example.com/picture.png");
    assert!(value.get("body").is_none());
    assert!(!value.to_string().contains("secret"));
    let calls = d.calls.lock().unwrap();
    assert_eq!(calls[0].0, "https://example.com/page");
    assert_eq!(calls[0].1, d.addresses);
}
#[tokio::test]
async fn redirects_resolve_each_hop_and_support_relative_locations() {
    let d = Double::new(vec![
        response(301, Some("/next"), vec![]),
        response(307, Some("https://other.example/final"), vec![]),
        html("<title>Final</title>"),
    ]);
    assert_eq!(
        d.preview("https://example.com").await.unwrap()["title"],
        "Final"
    );
    assert_eq!(
        *d.resolved.lock().unwrap(),
        ["example.com", "example.com", "other.example"]
    );
    assert_eq!(d.calls.lock().unwrap().len(), 3);
}
#[tokio::test]
async fn redirects_stop_at_limit_and_reject_missing_location() {
    let d = Double::new(
        (0..=LINK_PREVIEW_LIMITS.redirects)
            .map(|_| response(302, Some("/again"), vec![]))
            .collect(),
    );
    assert_eq!(
        d.preview("https://example.com").await.unwrap_err().1,
        "link_preview_redirect_limit"
    );
    assert_eq!(
        d.calls.lock().unwrap().len(),
        LINK_PREVIEW_LIMITS.redirects + 1
    );
    let d = Double::new(vec![response(302, None, vec![])]);
    assert_eq!(
        d.preview("https://example.com").await.unwrap_err().1,
        "link_preview_invalid_redirect"
    );
}
#[tokio::test]
async fn rejects_mime_errors_and_incomplete_body() {
    for mime in [
        None,
        Some("text/html-fake"),
        Some("application/json"),
        Some("image/png"),
    ] {
        let mut r = html("ignored");
        r.content_type = mime.map(str::to_owned);
        let d = Double::new(vec![r]);
        assert_eq!(
            d.preview("https://example.com").await.unwrap_err().1,
            "link_preview_content_type_rejected"
        );
    }
    let d = Double::new(vec![response(500, None, vec![])]);
    assert_eq!(
        d.preview("https://example.com").await.unwrap_err().1,
        "link_preview_unavailable"
    );
    let mut r = html("");
    r.body = Box::pin(stream::iter(vec![
        Ok(b"<title>Partial</title>".to_vec()),
        Err(Error::bad("link_preview_unavailable")),
    ]));
    let d = Double::new(vec![r]);
    assert!(d.preview("https://example.com").await.is_err());
}
#[tokio::test]
async fn enforces_declared_and_streaming_size_limits() {
    let limits = annotations_api::core::LinkPreviewLimits {
        bytes: 32,
        ..LINK_PREVIEW_LIMITS
    };
    let mut declared = html("");
    declared.content_length = Some(33);
    for r in [
        declared,
        response(200, None, vec![vec![b'x'; 16], vec![b'x'; 17]]),
    ] {
        let d = Double::new(vec![r]);
        assert_eq!(
            links::preview_with("https://example.com", &d, &d, limits)
                .await
                .unwrap_err()
                .1,
            "link_preview_too_large"
        );
    }
    let d = Double::new(vec![response(200, None, vec![vec![b'x'; 32]])]);
    assert!(
        links::preview_with("https://example.com", &d, &d, limits)
            .await
            .is_ok()
    );
}
#[tokio::test]
async fn deadline_stops_slow_streams_and_dns() {
    let limits = annotations_api::core::LinkPreviewLimits {
        timeout: Duration::from_millis(20),
        ..LINK_PREVIEW_LIMITS
    };
    let mut r = html("");
    r.body = Box::pin(stream::pending());
    let d = Double::new(vec![r]);
    assert_eq!(
        links::preview_with("https://example.com", &d, &d, limits)
            .await
            .unwrap_err()
            .1,
        "link_preview_timeout"
    );
    struct SlowDns;
    impl Resolver for SlowDns {
        fn resolve<'a>(&'a self, _: &'a str, _: u16) -> BoxFuture<'a, Result<Vec<SocketAddr>>> {
            Box::pin(std::future::pending())
        }
    }
    assert_eq!(
        links::preview_with("https://example.com", &SlowDns, &d, limits)
            .await
            .unwrap_err()
            .1,
        "link_preview_timeout"
    );
}

#[tokio::test]
async fn deadline_stops_waiting_for_response_headers() {
    struct SlowTransport;
    impl Transport for SlowTransport {
        fn get<'a>(&'a self, _: &'a Url, _: &'a [SocketAddr]) -> BoxFuture<'a, Result<Response>> {
            Box::pin(std::future::pending())
        }
    }
    let limits = annotations_api::core::LinkPreviewLimits {
        timeout: Duration::from_millis(20),
        ..LINK_PREVIEW_LIMITS
    };
    let d = Double::new(vec![]);
    assert_eq!(
        links::preview_with("https://example.com", &d, &SlowTransport, limits)
            .await
            .unwrap_err()
            .1,
        "link_preview_timeout"
    );
}

#[tokio::test]
async fn accepts_public_ipv6_literals_and_case_insensitive_html_mime() {
    let mut r = html("<title>IPv6</title>");
    r.content_type = Some("Text/HTML; charset=UTF-8".into());
    let d = Double::new(vec![r]);
    assert_eq!(
        d.preview("https://[2606:4700:4700::1111]/").await.unwrap()["title"],
        "IPv6"
    );
    assert!(d.resolved.lock().unwrap().is_empty());
    assert_eq!(
        d.calls.lock().unwrap()[0].1,
        ["[2606:4700:4700::1111]:443".parse::<SocketAddr>().unwrap()]
    );
}
