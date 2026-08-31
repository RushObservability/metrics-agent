use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};

use crate::{controller::Controller, metrics};

pub fn router(controller: Arc<Controller>) -> Router {
    let [ui_path, ui_index_path, ui_styles_path, ui_app_path] =
        ui_route_paths(&controller.config().ui_path);
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/metrics", get(prometheus_metrics))
        .route("/api/v1/status", get(status))
        .route("/api/v1/metrics-summary", get(metrics_summary))
        .route(&ui_path, get(ui_redirect))
        .route(&ui_index_path, get(ui_index))
        .route(&ui_styles_path, get(ui_styles))
        .route(&ui_app_path, get(ui_app))
        .with_state(controller)
}

const UI_INDEX: &str = include_str!("../ui/index.html");
const UI_STYLES: &str = include_str!("../ui/styles.css");
const UI_APP: &str = include_str!("../ui/app.js");

fn normalize_ui_path(path: &str) -> String {
    let path = path.trim();
    let path = if path.is_empty() { "/ui/" } else { path };
    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    let path = path.trim_end_matches('/');
    if path.is_empty() {
        "/ui".to_string()
    } else {
        path.to_string()
    }
}

fn ui_route_paths(path: &str) -> [String; 4] {
    let path = normalize_ui_path(path);
    [
        path.clone(),
        format!("{path}/"),
        format!("{path}/styles.css"),
        format!("{path}/app.js"),
    ]
}

fn ui_disabled_response() -> Response {
    (StatusCode::NOT_FOUND, "metrics-agent UI is disabled\n").into_response()
}

fn diagnostics_disabled_response(enabled: bool) -> Option<Response> {
    if enabled {
        None
    } else {
        Some(
            (
                StatusCode::NOT_FOUND,
                "metrics-agent diagnostics are disabled\n",
            )
                .into_response(),
        )
    }
}

fn no_store_json<T: serde::Serialize>(value: T) -> Response {
    let mut response = Json(value).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn ui_redirect_response(enabled: bool, path: &str) -> Response {
    if !enabled {
        return ui_disabled_response();
    }
    Redirect::temporary(&format!("{}/", normalize_ui_path(path))).into_response()
}

fn ui_asset_response(enabled: bool, content_type: &'static str, body: &'static str) -> Response {
    if !enabled {
        return ui_disabled_response();
    }
    ([(header::CONTENT_TYPE, content_type)], body).into_response()
}

fn readiness_response(ready: bool) -> Response {
    if ready {
        (StatusCode::OK, "ok\n").into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "informer caches are not ready\n",
        )
            .into_response()
    }
}

async fn ui_redirect(State(controller): State<Arc<Controller>>) -> impl IntoResponse {
    ui_redirect_response(controller.config().ui_enabled, &controller.config().ui_path)
}

async fn ui_index(State(controller): State<Arc<Controller>>) -> impl IntoResponse {
    ui_asset_response(
        controller.config().ui_enabled,
        "text/html; charset=utf-8",
        UI_INDEX,
    )
}

async fn ui_styles(State(controller): State<Arc<Controller>>) -> impl IntoResponse {
    ui_asset_response(
        controller.config().ui_enabled,
        "text/css; charset=utf-8",
        UI_STYLES,
    )
}

async fn ui_app(State(controller): State<Arc<Controller>>) -> impl IntoResponse {
    ui_asset_response(
        controller.config().ui_enabled,
        "application/javascript; charset=utf-8",
        UI_APP,
    )
}

async fn livez() -> &'static str {
    "ok\n"
}

async fn readyz(State(controller): State<Arc<Controller>>) -> impl IntoResponse {
    readiness_response(controller.status.snapshot().await.ready)
}

async fn prometheus_metrics(State(controller): State<Arc<Controller>>) -> impl IntoResponse {
    let body = metrics::render(&controller.status.snapshot().await);
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

async fn status(State(controller): State<Arc<Controller>>) -> Response {
    if let Some(response) = diagnostics_disabled_response(controller.config().ui_enabled) {
        return response;
    }
    no_store_json(controller.status.snapshot().await)
}

async fn metrics_summary(State(controller): State<Arc<Controller>>) -> Response {
    if let Some(response) = diagnostics_disabled_response(controller.config().ui_enabled) {
        return response;
    }
    no_store_json(controller.status.metrics_summary().await)
}

#[cfg(test)]
mod tests {
    use super::{
        UI_APP, UI_INDEX, UI_STYLES, diagnostics_disabled_response, livez, no_store_json,
        normalize_ui_path, readiness_response, ui_asset_response, ui_redirect_response,
        ui_route_paths,
    };
    use axum::{
        body::to_bytes,
        http::{StatusCode, header},
    };

    #[test]
    fn normalizes_ui_paths_for_default_custom_and_root_paths() {
        assert_eq!(normalize_ui_path(""), "/ui");
        assert_eq!(normalize_ui_path("  "), "/ui");
        assert_eq!(normalize_ui_path("ui/"), "/ui");
        assert_eq!(normalize_ui_path("/control-room///"), "/control-room");
        assert_eq!(normalize_ui_path("/"), "/ui");
        assert_eq!(
            ui_route_paths("console/"),
            [
                "/console".to_string(),
                "/console/".to_string(),
                "/console/styles.css".to_string(),
                "/console/app.js".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn live_and_ready_handlers_report_health_transitions() {
        assert_eq!(livez().await, "ok\n");
        let not_ready = readiness_response(false);
        assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(not_ready.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"informer caches are not ready\n");

        let ready = readiness_response(true);
        assert_eq!(ready.status(), StatusCode::OK);
        let body = to_bytes(ready.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"ok\n");
    }

    #[tokio::test]
    async fn enabled_ui_handlers_redirect_and_return_embedded_assets() {
        let redirect = ui_redirect_response(true, "console/");
        assert_eq!(redirect.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(redirect.headers()[header::LOCATION], "/console/");

        let index = ui_asset_response(true, "text/html; charset=utf-8", UI_INDEX);
        assert_eq!(index.status(), StatusCode::OK);
        assert_eq!(
            index.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        let index_body = to_bytes(index.into_body(), usize::MAX).await.unwrap();
        assert!(index_body.starts_with(b"<!doctype html>"));

        let styles = ui_asset_response(true, "text/css; charset=utf-8", UI_STYLES);
        assert_eq!(styles.status(), StatusCode::OK);
        assert_eq!(
            styles.headers()[header::CONTENT_TYPE],
            "text/css; charset=utf-8"
        );

        let app = ui_asset_response(true, "application/javascript; charset=utf-8", UI_APP);
        assert_eq!(app.status(), StatusCode::OK);
        assert_eq!(
            app.headers()[header::CONTENT_TYPE],
            "application/javascript; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn disabled_ui_returns_not_found_for_every_ui_endpoint() {
        for response in [
            ui_redirect_response(false, "/console/"),
            ui_asset_response(false, "text/html; charset=utf-8", UI_INDEX),
            ui_asset_response(false, "text/css; charset=utf-8", UI_STYLES),
            ui_asset_response(false, "application/javascript; charset=utf-8", UI_APP),
        ] {
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(body.as_ref(), b"metrics-agent UI is disabled\n");
        }
    }

    #[tokio::test]
    async fn detailed_diagnostics_are_opt_in_and_not_cacheable() {
        let disabled = diagnostics_disabled_response(false).unwrap();
        assert_eq!(disabled.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(disabled.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"metrics-agent diagnostics are disabled\n");
        assert!(diagnostics_disabled_response(true).is_none());

        let response = no_store_json(serde_json::json!({ "status": "ok" }));
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    }

    #[test]
    fn route_paths_are_stable_for_default_and_custom_ui_prefixes() {
        assert_eq!(
            ui_route_paths("/ui/"),
            [
                "/ui".to_string(),
                "/ui/".to_string(),
                "/ui/styles.css".to_string(),
                "/ui/app.js".to_string(),
            ]
        );
        assert_eq!(
            ui_route_paths("control-room"),
            [
                "/control-room".to_string(),
                "/control-room/".to_string(),
                "/control-room/styles.css".to_string(),
                "/control-room/app.js".to_string(),
            ]
        );
    }
}
