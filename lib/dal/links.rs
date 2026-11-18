//! Network primitives for previews. Destination and response policy live in the domain.
use crate::core::{Error, Result};
use futures_util::{Stream, StreamExt, future::BoxFuture};
use std::{net::SocketAddr, pin::Pin};
use url::Url;

pub type Body = Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>;
pub struct Response {
    pub status: u16,
    pub location: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<u64>,
    pub body: Body,
}
pub trait Resolver: Sync {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> BoxFuture<'a, Result<Vec<SocketAddr>>>;
}
pub trait Transport: Sync {
    fn get<'a>(
        &'a self,
        url: &'a Url,
        addresses: &'a [SocketAddr],
    ) -> BoxFuture<'a, Result<Response>>;
}
pub struct Network;
impl Resolver for Network {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> BoxFuture<'a, Result<Vec<SocketAddr>>> {
        Box::pin(async move {
            tokio::net::lookup_host((host, port))
                .await
                .map(|a| a.collect())
                .map_err(|_| Error::bad("link_preview_unavailable"))
        })
    }
}
impl Transport for Network {
    fn get<'a>(
        &'a self,
        url: &'a Url,
        addresses: &'a [SocketAddr],
    ) -> BoxFuture<'a, Result<Response>> {
        Box::pin(async move {
            // A fresh client has no caller headers, cookie store, pooled connection or proxy.
            // Retain the URL hostname for Host and TLS, but connect only to checked addresses.
            let client = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(crate::core::LINK_PREVIEW_LIMITS.timeout)
                .resolve_to_addrs(url.host_str().unwrap(), addresses)
                .build()
                .map_err(|_| Error::bad("link_preview_unavailable"))?;
            let response = client
                .get(url.clone())
                .send()
                .await
                .map_err(|_| Error::bad("link_preview_unavailable"))?;
            let header = |name| {
                response
                    .headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned)
            };
            Ok(Response {
                status: response.status().as_u16(),
                location: header("location"),
                content_type: header("content-type"),
                content_length: response.content_length(),
                body: Box::pin(response.bytes_stream().map(|chunk| {
                    chunk
                        .map(|v| v.to_vec())
                        .map_err(|_| Error::bad("link_preview_unavailable"))
                })),
            })
        })
    }
}
