mod m3u8;
mod segment;
mod segmentation;
mod video_metadata;
mod ffprobe;

use std::collections::HashMap;
use anyhow::Result;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use tower_http::cors::{CorsLayer, Any};
use tower::ServiceBuilder;

// ─── State ────────────────────────────────────────────────────────────────────

struct AppState {
    redis: Mutex<redis::aio::MultiplexedConnection>,
    /// Limite le nombre de process FFmpeg simultanés.
    /// Défaut : MAX_CONCURRENT_FFMPEG (env) ou 4.
    ffmpeg_sem: Arc<Semaphore>,
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("watermark_service=debug".parse()?))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let redis_client = redis::Client::open(redis_url)?;
    let redis_conn   = redis_client.get_multiplexed_async_connection().await?;

    let max_ffmpeg = std::env::var("MAX_CONCURRENT_FFMPEG")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4usize);

    tracing::info!("MAX_CONCURRENT_FFMPEG = {max_ffmpeg}");

    let state = Arc::new(AppState {
        redis:      Mutex::new(redis_conn),
        ffmpeg_sem: Arc::new(Semaphore::new(max_ffmpeg)),
    });

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_origin(Any);

    let app = Router::new()
        .route("/metadata",                   post(extract_metadata))
        .route("/session",                    post(create_session))
        .route("/session/{session_id}/m3u8",   get(get_m3u8))
        .route("/segment/{session_id}/{index_with_ext}", get(get_segment))
        .route("/health",                     get(health))
        .with_state(state)
        .layer(ServiceBuilder::new().layer(cors));

    let addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into());
    tracing::info!("watermark-service écoute sur {addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// ─── POST /metadata ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ExtractMetadataRequest {
    video_key:   String,
    presign_url: String,
}

#[derive(Debug, Serialize)]
struct ExtractMetadataResponse {
    video_key:      String,
    duration_secs:  f64,
    width:          u32,
    height:         u32,
    codec:          String,
    fps:            f64,
    keyframe_count: usize,
}

async fn extract_metadata(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<ExtractMetadataRequest>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("extraction metadata pour {}", req.video_key);

    let mut redis = state.redis.lock().await;
    let (meta, keyframes) = video_metadata::extract_and_store(
        &req.video_key,
        &req.presign_url,
        &mut *redis,
    ).await?;

    Ok(Json(ExtractMetadataResponse {
        video_key:      req.video_key,
        duration_secs:  meta.duration_secs(),
        width:          meta.width,
        height:         meta.height,
        fps:            meta.fps(),
        codec:          meta.codec,
        keyframe_count: keyframes.len(),
    }))
}

// ─── POST /session ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct CreateSessionRequest {
    video_key:   String,
    viewer_id:   String,
    firstname:   String,
    lastname:    String,
    presign_url: String,
}

#[derive(Debug, Serialize)]
struct CreateSessionResponse {
    session_id:     String,
    total_segments: usize,
    duration_secs:  f64,
}

async fn create_session(
    State(state): State<Arc<AppState>>,
    Json(req):    Json<CreateSessionRequest>,
) -> Result<impl IntoResponse, AppError> {

    let mut redis = state.redis.lock().await;

    let meta = video_metadata::get_meta(&req.video_key, &mut *redis)
        .await?
        .ok_or_else(|| AppError::not_found("vidéo non trouvée — lance d'abord POST /metadata"))?;

    let keyframes = video_metadata::get_keyframes(&req.video_key, &mut *redis)
        .await?
        .ok_or_else(|| AppError::not_found("keyframes manquants"))?;

    let session_id = make_session_id(&req.viewer_id, &req.video_key);

    let segments = segmentation::compute_segments(
        &keyframes.timestamps_secs,
        meta.duration_secs(),
        4.0,
    );

    let session_ttl  = meta.duration_ms as u64 / 1000 + 3600;
    let session_key  = format!("session:{session_id}");
    let segments_json = serde_json::to_string(&segments)?;

    // Utilisation explicite du trait pour éviter les conflits (E0034)
    let _: () = redis::AsyncCommands::hset_multiple(
        &mut *redis,
        &session_key,
        &[
            ("video_key",   req.video_key.as_str()),
            ("viewer_id",   req.viewer_id.as_str()),
            ("firstname",   req.firstname.as_str()),
            ("lastname",    req.lastname.as_str()),
            ("presign_url", req.presign_url.as_str()),
            ("segments",    segments_json.as_str()),
        ]
    ).await?;

    let _: () = redis::AsyncCommands::expire(&mut *redis, &session_key, session_ttl as i64).await?;

    tracing::info!(
        "session {session_id} : {} segments, {:.1}s (viewer {})",
        segments.len(), meta.duration_secs(), req.viewer_id
    );

    Ok((StatusCode::CREATED, Json(CreateSessionResponse {
        session_id,
        total_segments: segments.len(),
        duration_secs:  meta.duration_secs(),
    })))
}

// ─── GET /session/:session_id/m3u8 ───────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct M3u8Query {
    base_url: Option<String>,
}

async fn get_m3u8(
    State(state):     State<Arc<AppState>>,
    Path(session_id): Path<String>,
    Query(query):     Query<M3u8Query>,
) -> Result<impl IntoResponse, AppError> {

    let mut redis = state.redis.lock().await;

    // Utilisation explicite du trait
    let segments_json: Option<String> = redis::AsyncCommands::hget(
        &mut *redis,
        format!("session:{session_id}"),
        "segments"
    ).await?;

    let segments_json = segments_json
        .ok_or_else(|| AppError::not_found("session inconnue ou expirée"))?;

    let segments: Vec<segmentation::Segment> = serde_json::from_str(&segments_json)?;

    let segment_base = match &query.base_url {
        Some(base) => format!("{}/segment", base.trim_end_matches('/')),
        None       => "/segment".to_string(),
    };

    let playlist = m3u8::M3u8Builder::new(&session_id, &segments)
        .segment_base(&segment_base)
        .build();

    let mut headers = HeaderMap::new();

    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apple.mpegurl"),
    );

    headers.insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );

    headers.insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
    headers.insert("Access-Control-Allow-Methods", HeaderValue::from_static("GET, OPTIONS"));

    Ok((StatusCode::OK, headers, playlist))
}

// ─── GET /segment/:session_id/:index ─────────────────────────────────────────

async fn get_segment(
    State(state):     State<Arc<AppState>>,
    Path((session_id, index_with_ext)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {

    let index: usize = index_with_ext
        .replace(".ts", "")
        .parse()
        .map_err(|_| AppError::not_found("Index invalide"))?;

    // ── 1. Récupérer la session depuis Valkey ─────────────────────────────────
    let session_key = format!("session:{session_id}");

    // On récupère TOUT le hash sous forme de HashMap<String, String>
    let mut session_data: HashMap<String, String> = {
        let mut redis = state.redis.lock().await;

        // On utilise le trait explicitement pour éviter les conflits de noms (E0034)
        redis::AsyncCommands::hgetall(&mut *redis, &session_key)
            .await
            .map_err(|e| {
                tracing::error!("Erreur Redis hgetall: {e}");
                AppError::not_found("session inconnue ou expirée")
            })?
    };

    // Si la map est vide, c'est que la clé n'existe pas dans Redis
    if session_data.is_empty() {
        return Err(AppError::not_found("session introuvable"));
    }

    // Extraction des données (on utilise .remove() pour obtenir des String possédées)
    let firstname   = session_data.remove("firstname").ok_or_else(|| AppError::not_found("firstname manquant"))?;
    let lastname    = session_data.remove("lastname").ok_or_else(|| AppError::not_found("lastname manquant"))?;
    let presign_url = session_data.remove("presign_url").ok_or_else(|| AppError::not_found("presign_url manquante"))?;
    let segs_json   = session_data.remove("segments").ok_or_else(|| AppError::not_found("segments manquants"))?;

    let segments: Vec<segmentation::Segment> = serde_json::from_str(&segs_json)?;

    let seg = segments.get(index)
        .ok_or_else(|| AppError::not_found(&format!("segment {index} inexistant")))?;

    tracing::debug!(
        "segment {session_id}/{index} : {:.3}s → {:.3}s ({:.3}s)",
        seg.start_secs, seg.end_secs, seg.duration_secs
    );

    // ── 2. Acquérir un slot FFmpeg ───────────────────────────────────────────
    let _permit = state.ffmpeg_sem
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| anyhow::anyhow!("semaphore fermé"))?;

    // ── 3. Spawn FFmpeg ───────────────────────────────────────────────────────
    // Note : on passe des &String, ce qui est correct ici
    let (ffmpeg, stream) = segment::FfmpegHandle::spawn(
        &presign_url,
        seg.start_secs,
        seg.duration_secs,
        &firstname,
        &lastname,
    ).await?;

    let body = Body::from_stream(PermitStream::new(stream, _permit, ffmpeg));

    // ── 4. Headers HTTP ───────────────────────────────────────────────────────
    let mut headers = HeaderMap::new();
    headers.insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("video/mp2t"));
    headers.insert(axum::http::header::CACHE_CONTROL, HeaderValue::from_static("public, max-age=86400, immutable"));

    headers.insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
    headers.insert("Access-Control-Allow-Methods", HeaderValue::from_static("GET, OPTIONS"));

    Ok((StatusCode::OK, headers, body))
}

// ─── PermitStream ─────────────────────────────────────────────────────────────

use std::pin::Pin;
use std::task::{Context, Poll};
use axum::http::Method;
use futures_core::Stream;
use tokio::sync::OwnedSemaphorePermit;

struct PermitStream<S> {
    inner:   S,
    _permit: OwnedSemaphorePermit,
    _ffmpeg: segment::FfmpegHandle,
}

impl<S> PermitStream<S> {
    fn new(inner: S, permit: OwnedSemaphorePermit, ffmpeg: segment::FfmpegHandle) -> Self {
        Self { inner, _permit: permit, _ffmpeg: ffmpeg }
    }
}

impl<S, E> Stream for PermitStream<S>
where
    S: Stream<Item = Result<bytes::Bytes, E>> + Unpin,
{
    type Item = Result<bytes::Bytes, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

// ─── GET /health ──────────────────────────────────────────────────────────────

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut redis = state.redis.lock().await;
    let ok: bool  = redis::cmd("PING")
        .query_async::<String>(&mut *redis)
        .await
        .map(|r| r == "PONG")
        .unwrap_or(false);

    let slots_available = state.ffmpeg_sem.available_permits();

    if ok {
        (StatusCode::OK, format!("ok — ffmpeg slots disponibles: {slots_available}"))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "redis unreachable".into())
    }
}

// ─── Session ID ───────────────────────────────────────────────────────────────

fn make_session_id(viewer_id: &str, video_key: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    viewer_id.hash(&mut h);
    video_key.hash(&mut h);
    format!("{:016x}", h.finish())
}

// ─── Error handling ───────────────────────────────────────────────────────────

#[derive(Debug)]
struct AppError {
    status:  StatusCode,
    message: String,
}

impl AppError {
    fn not_found(msg: &str) -> Self {
        Self { status: StatusCode::NOT_FOUND, message: msg.into() }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        tracing::warn!("{} — {}", self.status, self.message);
        (self.status, self.message).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(e: E) -> Self {
        let err = e.into();
        tracing::error!("{err:?}");
        Self { status: StatusCode::INTERNAL_SERVER_ERROR, message: err.to_string() }
    }
}