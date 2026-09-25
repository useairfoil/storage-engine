use std::{collections::HashMap, sync::LazyLock};

use axum::http::{HeaderMap, HeaderName, HeaderValue, Method};
use reqwest::{Request, Response, StatusCode};
use se_meta_store::catalog::RestCatalogConfig;
use serde::Deserialize;
use tracing::debug;

/// HTTP client shared by all upstream catalog clients.
static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

/// Hop-by-hop headers, which must not be forwarded by proxies (RFC 9110
/// section 7.6.1).
static HOP_BY_HOP_HEADERS: [HeaderName; 8] = [
    HeaderName::from_static("connection"),
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-authenticate"),
    HeaderName::from_static("proxy-authorization"),
    HeaderName::from_static("te"),
    HeaderName::from_static("trailer"),
    HeaderName::from_static("transfer-encoding"),
    HeaderName::from_static("upgrade"),
];

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invalid upstream header name {name}: {source}")]
    InvalidHeaderName {
        name: String,
        source: axum::http::header::InvalidHeaderName,
    },
    #[error("invalid upstream header value for {name}: {source}")]
    InvalidHeaderValue {
        name: String,
        source: axum::http::header::InvalidHeaderValue,
    },
    #[error("upstream authentication failed with status {0}")]
    Authentication(StatusCode),
    #[error("upstream HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
}

pub(super) struct Client {
    token: Option<String>,
    credential: Option<(Option<String>, String)>,
    token_endpoint: String,
    extra_headers: HeaderMap,
    extra_oauth_params: HashMap<String, String>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

impl Client {
    pub(super) fn new(config: &RestCatalogConfig) -> Result<Self, Error> {
        let properties = &config.properties;
        let uri = config.uri.trim_end_matches('/').to_owned();
        let token_endpoint = properties
            .get("oauth2-server-uri")
            .cloned()
            .unwrap_or_else(|| format!("{uri}/v1/oauth/tokens"));

        let credential = properties.get("credential").map(|credential| {
            credential
                .split_once(':')
                .map(|(id, secret)| (Some(id.to_owned()), secret.to_owned()))
                .unwrap_or_else(|| (None, credential.to_owned()))
        });

        let mut extra_headers = HeaderMap::new();
        for (property, value) in properties {
            let Some(name) = property.strip_prefix("header.") else {
                continue;
            };
            let header_name =
                HeaderName::try_from(name).map_err(|source| Error::InvalidHeaderName {
                    name: name.to_owned(),
                    source,
                })?;
            let header_value =
                HeaderValue::try_from(value).map_err(|source| Error::InvalidHeaderValue {
                    name: name.to_owned(),
                    source,
                })?;
            extra_headers.insert(header_name, header_value);
        }

        let mut extra_oauth_params = HashMap::from([(
            "scope".to_owned(),
            properties
                .get("scope")
                .cloned()
                .unwrap_or_else(|| "catalog".to_owned()),
        )]);
        for property in ["audience", "resource"] {
            if let Some(value) = properties.get(property) {
                extra_oauth_params.insert(property.to_owned(), value.clone());
            }
        }

        Ok(Self {
            token: properties.get("token").cloned(),
            credential,
            token_endpoint,
            extra_headers,
            extra_oauth_params,
        })
    }

    pub(super) async fn send(
        &self,
        method: Method,
        uri: String,
        headers: HeaderMap,
        body: impl Into<reqwest::Body>,
    ) -> Result<Response, Error> {
        let mut request = self.build_request(method, uri, headers, body)?;
        self.authenticate(&mut request).await?;

        debug!(
            method = %request.method(),
            url = %request.url(),
            "forwarding request to upstream Iceberg catalog"
        );

        Ok(HTTP_CLIENT.execute(request).await?)
    }

    fn build_request(
        &self,
        method: Method,
        uri: String,
        mut headers: HeaderMap,
        body: impl Into<reqwest::Body>,
    ) -> Result<Request, Error> {
        remove_proxy_headers(&mut headers);
        for (name, value) in &self.extra_headers {
            headers.insert(name, value.clone());
        }

        Ok(HTTP_CLIENT
            .request(method, uri)
            .headers(headers)
            .body(body)
            .build()?)
    }

    async fn authenticate(&self, request: &mut Request) -> Result<(), Error> {
        let token = match (&self.token, &self.credential) {
            (Some(token), _) => Some(token.clone()),
            (None, Some(credential)) => Some(self.exchange_credential_for_token(credential).await?),
            (None, None) => None,
        };

        if let Some(token) = token {
            let value = HeaderValue::try_from(format!("Bearer {token}")).map_err(|source| {
                Error::InvalidHeaderValue {
                    name: "authorization".to_owned(),
                    source,
                }
            })?;
            request
                .headers_mut()
                .insert(HeaderName::from_static("authorization"), value);
        }

        Ok(())
    }

    async fn exchange_credential_for_token(
        &self,
        (client_id, client_secret): &(Option<String>, String),
    ) -> Result<String, Error> {
        let mut params = self.extra_oauth_params.clone();
        params.insert("grant_type".to_owned(), "client_credentials".to_owned());
        params.insert("client_secret".to_owned(), client_secret.clone());
        if let Some(client_id) = client_id {
            params.insert("client_id".to_owned(), client_id.clone());
        }

        let mut request = HTTP_CLIENT
            .post(&self.token_endpoint)
            .headers(self.extra_headers.clone())
            .form(&params)
            .build()?;
        request.headers_mut().insert(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let response = HTTP_CLIENT.execute(request).await?;
        if response.status() != StatusCode::OK {
            return Err(Error::Authentication(response.status()));
        }

        Ok(response.json::<TokenResponse>().await?.access_token)
    }
}

fn remove_proxy_headers(headers: &mut HeaderMap) {
    headers.remove(HeaderName::from_static("host"));
    headers.remove(HeaderName::from_static("content-length"));
    for header in &HOP_BY_HOP_HEADERS {
        headers.remove(header);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn config(properties: HashMap<String, String>) -> RestCatalogConfig {
        RestCatalogConfig {
            uri: "https://catalog.example.com/".to_owned(),
            warehouse: None,
            properties,
        }
    }

    #[test]
    fn properties_do_not_change_uri() {
        let client = Client::new(&config(HashMap::from([(
            "prefix".to_owned(),
            "warehouses/main".to_owned(),
        )])))
        .unwrap();
        let uri = "https://catalog.example.com/v1/namespaces?pageToken=next";

        let request = client
            .build_request(Method::GET, uri.to_owned(), HeaderMap::new(), Vec::new())
            .unwrap();

        assert_eq!(request.url().as_str(), uri);
    }

    #[test]
    fn reads_custom_headers() {
        let client = Client::new(&config(HashMap::from([(
            "header.x-catalog-token".to_owned(),
            "secret".to_owned(),
        )])))
        .unwrap();

        assert_eq!(client.extra_headers["x-catalog-token"], "secret");
    }
}
