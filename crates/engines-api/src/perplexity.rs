//! Perplexity Sonar / OpenAI-compatible realtime web-search engine
//! (keyed: `SEARCH_MCP_PERPLEXITY_API_KEY`).
//!
//! Defaults to `https://api.perplexity.ai` with model `sonar-pro`, and can be
//! pointed at any OpenAI-compatible `/chat/completions` endpoint or realtime
//! web model via `SEARCH_MCP_PERPLEXITY_BASE_URL` and
//! `SEARCH_MCP_PERPLEXITY_MODEL`.

use crate::common::{
    CallBudget, effective_domains, filter_and_truncate_hits, iso_date, make_hit,
    parse_search_params, resolve_required_key, search_op, send_json_keyed, strip_html_and_unescape,
};
use async_trait::async_trait;
use fsm_core::fixture::canonical_json;
use fsm_core::{
    AuthMode, Availability, Capability, Config, FsmError, Hit, HitKind, HttpRequest, PoolRole,
    Source, SourceDecl, SourceDeps, SourceLimits, SrcCx, SrcOutput, TransportProfile,
};
use serde_json::{Map, Value, json};

pub const DEFAULT_BASE_URL: &str = "https://api.perplexity.ai";
pub const DEFAULT_MODEL: &str = "sonar-pro";
pub const PERPLEXITY_API_KEY_ENV: &str = "SEARCH_MCP_PERPLEXITY_API_KEY";
pub const PERPLEXITY_BASE_URL_ENV: &str = "SEARCH_MCP_PERPLEXITY_BASE_URL";
pub const PERPLEXITY_MODEL_ENV: &str = "SEARCH_MCP_PERPLEXITY_MODEL";

/// Perplexity Sonar / OpenAI-compatible realtime web-search engine.
pub struct PerplexityEngine {
    decl: SourceDecl,
    deps: SourceDeps,
}

impl PerplexityEngine {
    pub fn new(deps: SourceDeps) -> Self {
        let mut decl = SourceDecl::new(
            "perplexity",
            "Perplexity Sonar",
            "Perplexity Sonar Pro / OpenAI-compatible realtime web-search model (keyed via SEARCH_MCP_PERPLEXITY_API_KEY).",
        )
        .with_scopes(&["web"])
        .with_caps(&[Capability::Search])
        .with_op(search_op(
            "Run a realtime web-grounded search via Perplexity Sonar Pro or an OpenAI-compatible web model.",
            10,
            r#"source.call("perplexity.search", { query: "rust 1.85 release notes", limit: 10 })"#,
        ));
        decl.aliases = vec![
            "sonar".to_owned(),
            "sonar-pro".to_owned(),
            "perplexity.ai".to_owned(),
        ];
        decl.home_url = Some("https://www.perplexity.ai".to_owned());
        decl.auth = AuthMode::RequiredKey {
            env: PERPLEXITY_API_KEY_ENV.to_owned(),
        };
        decl.transport = TransportProfile::Plain;
        decl.pool_role = PoolRole::Explicit;
        let mut limits = SourceLimits::new(2.0, 4, 2, 15_000, 3_600);
        limits.host_bucket = Some("api.perplexity.ai".to_owned());
        decl.limits = limits;
        Self { decl, deps }
    }

    fn endpoint(cfg: &Config) -> String {
        let base = cfg
            .api_key(PERPLEXITY_BASE_URL_ENV)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_BASE_URL)
            .trim_end_matches('/');
        if base.ends_with("/chat/completions") {
            base.to_owned()
        } else {
            format!("{base}/chat/completions")
        }
    }

    fn model(cfg: &Config) -> String {
        cfg.api_key(PERPLEXITY_MODEL_ENV)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_MODEL)
            .to_owned()
    }

    /// Map a Perplexity / OpenAI-compatible `/chat/completions` JSON payload into hits.
    pub fn map_results(payload: &Value) -> Vec<Hit> {
        let answer = payload
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|c| c.first())
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("content"))
            .and_then(Value::as_str)
            .map(strip_html_and_unescape)
            .unwrap_or_default();

        let mut results = Vec::new();

        if let Some(items) = payload.get("search_results").and_then(Value::as_array) {
            for item in items {
                let Some(obj) = item.as_object() else {
                    continue;
                };
                let url = obj
                    .get("url")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if url.is_empty() {
                    continue;
                }
                let raw_title = obj.get("title").and_then(Value::as_str).unwrap_or("");
                let title = strip_html_and_unescape(raw_title);
                let title = if title.is_empty() {
                    url.to_owned()
                } else {
                    title
                };
                let snippet = strip_html_and_unescape(
                    obj.get("snippet")
                        .or_else(|| obj.get("content"))
                        .and_then(Value::as_str)
                        .unwrap_or(&answer),
                );
                let date = iso_date(obj.get("date").or_else(|| obj.get("published_date")));
                results.push(make_hit(
                    "perplexity",
                    title,
                    url.to_owned(),
                    snippet,
                    (!date.is_empty()).then_some(date),
                    Some("api"),
                    HitKind::Web,
                ));
            }
        }

        if results.is_empty()
            && let Some(citations) = payload.get("citations").and_then(Value::as_array)
        {
            for (idx, item) in citations.iter().enumerate() {
                let Some(url) = item.as_str().map(str::trim).filter(|u| !u.is_empty()) else {
                    continue;
                };
                let snippet = if idx == 0 {
                    answer.clone()
                } else {
                    String::new()
                };
                results.push(make_hit(
                    "perplexity",
                    url.to_owned(),
                    url.to_owned(),
                    snippet,
                    None,
                    Some("api"),
                    HitKind::Web,
                ));
            }
        }

        if results.is_empty() && !answer.is_empty() {
            results.push(make_hit(
                "perplexity",
                "Perplexity Sonar Answer".to_owned(),
                "https://www.perplexity.ai".to_owned(),
                answer,
                None,
                Some("api"),
                HitKind::Web,
            ));
        }

        results
    }
}

#[async_trait]
impl Source for PerplexityEngine {
    fn decl(&self) -> &SourceDecl {
        &self.decl
    }

    fn availability(&self, cfg: &Config) -> Availability {
        if cfg
            .api_key(PERPLEXITY_API_KEY_ENV)
            .is_some_and(|k| !k.trim().is_empty())
        {
            Availability::Available
        } else {
            Availability::NeedsKey {
                env: PERPLEXITY_API_KEY_ENV.to_owned(),
            }
        }
    }

    async fn call(&self, op: &str, params: Value, cx: &SrcCx) -> Result<SrcOutput, FsmError> {
        let (query, limit) = parse_search_params("perplexity", op, &params, 10)?;
        let key = resolve_required_key("perplexity", PERPLEXITY_API_KEY_ENV, cx, &self.deps.cfg)?;
        if query.trim().is_empty() {
            return Ok(SrcOutput::Hits(Vec::new()));
        }
        let (include_domains, exclude_domains) = effective_domains(&params, cx);

        let mut body = Map::new();
        body.insert("model".to_owned(), json!(Self::model(&self.deps.cfg)));
        body.insert(
            "messages".to_owned(),
            json!([
                {
                    "role": "system",
                    "content": "Be precise and concise. Return factual, up-to-date information with citations."
                },
                {
                    "role": "user",
                    "content": query
                }
            ]),
        );
        let wire = canonical_json(&Value::Object(body));

        let endpoint = Self::endpoint(&self.deps.cfg);
        let req = HttpRequest::post(&endpoint, wire)
            .header("Authorization", format!("Bearer {key}"))
            .redact_header("Authorization")
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .for_source("perplexity");

        let budget = CallBudget::from_cx(cx, &self.decl);
        let payload = send_json_keyed(
            self.deps.net.as_ref(),
            req,
            self.decl.transport,
            &self.decl,
            PERPLEXITY_API_KEY_ENV,
            cx,
            &budget,
        )
        .await?;

        let hits = Self::map_results(&payload);
        Ok(SrcOutput::Hits(filter_and_truncate_hits(
            hits,
            &include_domains,
            &exclude_domains,
            limit,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_search_results_and_citations() {
        let payload = json!({
            "choices": [{
                "message": {
                    "content": "Rust 1.85 was released with 2024 edition support."
                }
            }],
            "search_results": [{
                "title": "Announcing Rust 1.85.0",
                "url": "https://blog.rust-lang.org/2025/02/20/Rust-1.85.0.html",
                "snippet": "Rust 1.85.0 stabilizes the 2024 edition.",
                "date": "2025-02-20"
            }]
        });
        let hits = PerplexityEngine::map_results(&payload);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].title, "Announcing Rust 1.85.0");
        assert_eq!(
            hits[0].url,
            "https://blog.rust-lang.org/2025/02/20/Rust-1.85.0.html"
        );
        assert_eq!(hits[0].published_at.as_deref(), Some("2025-02-20"));
    }

    #[test]
    fn falls_back_to_citations_when_search_results_absent() {
        let payload = json!({
            "choices": [{
                "message": {
                    "content": "Answer text."
                }
            }],
            "citations": [
                "https://example.com/a",
                "https://example.com/b"
            ]
        });
        let hits = PerplexityEngine::map_results(&payload);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].url, "https://example.com/a");
        assert_eq!(hits[0].snippet, "Answer text.");
        assert_eq!(hits[1].url, "https://example.com/b");
    }
}
