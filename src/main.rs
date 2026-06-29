use std::{
    env,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client, primitives::ByteStream};
use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{Path, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

type ApiResult<T> = Result<T, ApiError>;

const WEEK: i64 = 7 * 24 * 60 * 60;
const X_FILENAME: HeaderName = HeaderName::from_static("x-filename");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = Env::from_env()?;
    let db = Db::open(&env.db_path)?;
    let s3 = r2_client(&env).await;
    let addr: SocketAddr = env.listen_addr.parse()?;
    let state = Arc::new(AppState {
        s3,
        db,
        bucket: env.bucket,
        base_url: env.base_url,
        limits: env.limits,
    });

    cleanup_expired(state.clone()).await;
    tokio::spawn(cleanup_loop(state.clone()));

    let app = Router::new()
        .route("/", get(index))
        .route("/upload", post(upload).put(upload))
        .route("/f/{id}", get(download))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("lake listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

struct Env {
    access_key_id: String,
    secret_access_key: String,
    bucket: String,
    endpoint: String,
    region: String,
    listen_addr: String,
    db_path: String,
    base_url: Option<String>,
    limits: Limits,
}

impl Env {
    fn from_env() -> Result<Self, String> {
        let account_id = required("R2_ACCOUNT_ID")?;
        let endpoint = env::var("R2_ENDPOINT")
            .unwrap_or_else(|_| format!("https://{account_id}.r2.cloudflarestorage.com"));

        Ok(Self {
            endpoint,
            access_key_id: required("R2_ACCESS_KEY_ID")?,
            secret_access_key: required("R2_SECRET_ACCESS_KEY")?,
            bucket: required("R2_BUCKET")?,
            region: env::var("R2_REGION").unwrap_or_else(|_| "auto".into()),
            listen_addr: env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into()),
            db_path: env::var("DB_PATH").unwrap_or_else(|_| "lake.sqlite3".into()),
            base_url: env::var("BASE_URL")
                .ok()
                .map(|s| s.trim_end_matches('/').into()),
            limits: Limits::from_env()?,
        })
    }
}

#[derive(Clone)]
struct Limits {
    storage_bytes: i64,
    class_a_ops: i64,
    class_b_ops: i64,
    guard_ratio: f64,
    max_upload_bytes: usize,
    ttl_seconds: i64,
    max_downloads: i64,
}

impl Limits {
    fn from_env() -> Result<Self, String> {
        Ok(Self {
            storage_bytes: var_i64("STORAGE_LIMIT_BYTES", 10_000_000_000)?,
            class_a_ops: var_i64("CLASS_A_LIMIT", 1_000_000)?,
            class_b_ops: var_i64("CLASS_B_LIMIT", 10_000_000)?,
            guard_ratio: var_f64("LIMIT_GUARD_RATIO", 0.80)?,
            max_upload_bytes: var_usize("MAX_UPLOAD_BYTES", 100_000_000)?,
            ttl_seconds: var_i64("TTL_SECONDS", WEEK)?,
            max_downloads: var_i64("MAX_DOWNLOADS", 20)?,
        })
    }

    fn storage_guard(&self) -> i64 {
        guarded(self.storage_bytes, self.guard_ratio)
    }

    fn class_a_guard(&self) -> i64 {
        guarded(self.class_a_ops, self.guard_ratio)
    }

    fn class_b_guard(&self) -> i64 {
        guarded(self.class_b_ops, self.guard_ratio)
    }
}

struct AppState {
    s3: Client,
    db: Db,
    bucket: String,
    base_url: Option<String>,
    limits: Limits,
}

async fn r2_client(env: &Env) -> Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(env.region.clone()))
        .endpoint_url(env.endpoint.clone())
        .credentials_provider(Credentials::new(
            env.access_key_id.clone(),
            env.secret_access_key.clone(),
            None,
            None,
            "r2",
        ))
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();
    Client::from_conf(s3_config)
}

async fn index(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Html<String> {
    let base = escape_html(&base_url(&state, &headers));
    Html(format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>lake</title>
<style>
:root{{font:16px/1.45 system-ui,sans-serif;color:#111;background:#fafafa}}
body{{margin:0;display:grid;min-height:100vh;place-items:center}}
main{{width:min(680px,calc(100vw - 40px))}}
h1{{font-size:clamp(38px,8vw,76px);line-height:1;margin:0 0 18px}}
p{{color:#555;margin:12px 0}}
code{{display:block;white-space:pre-wrap;overflow-wrap:anywhere;background:#111;color:#fff;padding:16px;border-radius:8px}}
</style>
</head>
<body>
<main>
<h1>lake</h1>
<code>curl -fsS -H "X-Filename: file" --data-binary @file {base}/upload</code>
<p>No sign-up. Links expire after 7 days or 20 downloads.</p>
</main>
</body>
</html>"#
    ))
}

async fn upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<Response> {
    cleanup_expired(state.clone()).await;

    let size = content_length(&headers)?;
    if size == 0 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "empty uploads are not useful",
        ));
    }
    if size > state.limits.max_upload_bytes as i64 {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "file is too large",
        ));
    }

    let id = Uuid::new_v4().simple().to_string();
    let key = format!("shares/{id}");
    let now = now();
    let file = NewFile {
        id: id.clone(),
        key: key.clone(),
        name: filename(&headers, &id),
        content_type: content_type(&headers),
        size_bytes: size,
        created_at: now,
        expires_at: now + state.limits.ttl_seconds,
        max_downloads: state.limits.max_downloads,
    };

    state.db.begin_upload(&file, &state.limits)?;
    let bytes = match to_bytes(body, state.limits.max_upload_bytes).await {
        Ok(bytes) if bytes.len() as i64 == size => bytes,
        Ok(_) => {
            state.db.mark_removed(&id)?;
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "content-length mismatch",
            ));
        }
        Err(_) => {
            state.db.mark_removed(&id)?;
            return Err(ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "file is too large",
            ));
        }
    };

    if let Err(err) = put_object(&state, &file, bytes).await {
        state.db.mark_removed(&id)?;
        return Err(err);
    }

    state.db.mark_ready(&id)?;
    Ok((
        StatusCode::CREATED,
        format!("{}/f/{id}\n", base_url(&state, &headers)),
    )
        .into_response())
}

async fn download(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let ticket = state.db.begin_download(&id, &state.limits)?;
    let object = state
        .s3
        .get_object()
        .bucket(&state.bucket)
        .key(&ticket.key)
        .send()
        .await
        .map_err(|err| {
            ApiError::new(
                StatusCode::BAD_GATEWAY,
                format!("r2 download failed: {err}"),
            )
        })?;

    let bytes = object
        .body
        .collect()
        .await
        .map_err(|err| ApiError::new(StatusCode::BAD_GATEWAY, format!("r2 body failed: {err}")))?
        .into_bytes();

    if ticket.delete_after {
        delete_remote(&state, &ticket.id, &ticket.key).await;
    }

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, header_value(&ticket.content_type)?);
    headers.insert(
        header::CONTENT_DISPOSITION,
        header_value(&format!("attachment; filename=\"{}\"", ticket.name))?,
    );
    Ok((headers, bytes).into_response())
}

async fn put_object(state: &AppState, file: &NewFile, bytes: Bytes) -> ApiResult<()> {
    state
        .s3
        .put_object()
        .bucket(&state.bucket)
        .key(&file.key)
        .content_type(file.content_type.clone())
        .body(ByteStream::from(bytes))
        .send()
        .await
        .map_err(|err| {
            ApiError::new(StatusCode::BAD_GATEWAY, format!("r2 upload failed: {err}"))
        })?;
    Ok(())
}

async fn cleanup_loop(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(Duration::from_secs(60 * 60)).await;
        cleanup_expired(state.clone()).await;
    }
}

async fn cleanup_expired(state: Arc<AppState>) {
    let Ok(files) = state.db.expired_files(100) else {
        return;
    };
    for (id, key) in files {
        delete_remote(&state, &id, &key).await;
    }
}

async fn delete_remote(state: &AppState, id: &str, key: &str) {
    if state.db.reserve_class_a(&state.limits).unwrap_or(false) {
        match state
            .s3
            .delete_object()
            .bucket(&state.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => {
                let _ = state.db.mark_removed(id);
            }
            Err(err) => eprintln!("r2 delete failed for {key}: {err}"),
        }
    }
}

// ponytail: single-process SQLite counters; move this to Durable Objects or a DB row lock if running replicas.
struct Db {
    conn: Mutex<Connection>,
}

impl Db {
    fn open(path: &str) -> Result<Self, String> {
        let conn = Connection::open(path).map_err(|err| err.to_string())?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: Connection) -> Result<Self, String> {
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY,
                key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                content_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                downloads INTEGER NOT NULL DEFAULT 0,
                max_downloads INTEGER NOT NULL,
                ready INTEGER NOT NULL DEFAULT 0,
                removed_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS files_cleanup ON files(removed_at, expires_at, downloads);
            CREATE TABLE IF NOT EXISTS usage (
                month TEXT PRIMARY KEY,
                class_a INTEGER NOT NULL DEFAULT 0,
                class_b INTEGER NOT NULL DEFAULT 0
            );
            "#,
        )
        .map_err(|err| err.to_string())?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn begin_upload(&self, file: &NewFile, limits: &Limits) -> ApiResult<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(ApiError::db)?;
        let month = current_month(&tx)?;
        ensure_usage(&tx, &month)?;
        let (class_a, class_b) = usage(&tx, &month)?;
        let storage = active_storage(&tx)?;

        if storage + file.size_bytes > limits.storage_guard() {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "upload disabled: storage guard reached",
            ));
        }
        if class_a + 1 > limits.class_a_guard() {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "upload disabled: Class A guard reached",
            ));
        }
        if class_b >= limits.class_b_guard() {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "upload disabled: Class B guard reached",
            ));
        }

        tx.execute(
            "UPDATE usage SET class_a = class_a + 1 WHERE month = ?",
            [&month],
        )
        .map_err(ApiError::db)?;
        tx.execute(
            r#"INSERT INTO files
            (id, key, name, content_type, size_bytes, created_at, expires_at, max_downloads)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
            params![
                file.id,
                file.key,
                file.name,
                file.content_type,
                file.size_bytes,
                file.created_at,
                file.expires_at,
                file.max_downloads
            ],
        )
        .map_err(ApiError::db)?;
        tx.commit().map_err(ApiError::db)?;
        Ok(())
    }

    fn begin_download(&self, id: &str, limits: &Limits) -> ApiResult<DownloadTicket> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(ApiError::db)?;
        let Some(row) = tx
            .query_row(
                r#"SELECT key, name, content_type, expires_at, downloads, max_downloads
                FROM files WHERE id = ? AND ready = 1 AND removed_at IS NULL"#,
                [id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(ApiError::db)?
        else {
            return Err(ApiError::new(StatusCode::NOT_FOUND, "link not found"));
        };

        let (key, name, content_type, expires_at, downloads, max_downloads) = row;
        if expires_at <= now() || downloads >= max_downloads {
            return Err(ApiError::new(StatusCode::GONE, "link expired"));
        }

        let month = current_month(&tx)?;
        ensure_usage(&tx, &month)?;
        let (_, class_b) = usage(&tx, &month)?;
        if class_b + 1 > limits.class_b_guard() {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "download disabled: Class B guard reached",
            ));
        }

        let next_downloads = downloads + 1;
        tx.execute(
            "UPDATE usage SET class_b = class_b + 1 WHERE month = ?",
            [&month],
        )
        .map_err(ApiError::db)?;
        tx.execute(
            "UPDATE files SET downloads = ? WHERE id = ?",
            params![next_downloads, id],
        )
        .map_err(ApiError::db)?;
        tx.commit().map_err(ApiError::db)?;

        Ok(DownloadTicket {
            id: id.into(),
            key,
            name,
            content_type,
            delete_after: next_downloads >= max_downloads,
        })
    }

    fn mark_ready(&self, id: &str) -> ApiResult<()> {
        self.lock()?
            .execute("UPDATE files SET ready = 1 WHERE id = ?", [id])
            .map_err(ApiError::db)?;
        Ok(())
    }

    fn mark_removed(&self, id: &str) -> ApiResult<()> {
        self.lock()?
            .execute(
                "UPDATE files SET removed_at = ? WHERE id = ?",
                params![now(), id],
            )
            .map_err(ApiError::db)?;
        Ok(())
    }

    fn expired_files(&self, limit: i64) -> ApiResult<Vec<(String, String)>> {
        self.lock()?
            .prepare(
                r#"SELECT id, key FROM files
                WHERE removed_at IS NULL
                  AND (expires_at <= ? OR downloads >= max_downloads OR (ready = 0 AND created_at <= ?))
                LIMIT ?"#,
            )
            .map_err(ApiError::db)?
            .query_map(params![now(), now() - 60 * 60, limit], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(ApiError::db)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(ApiError::db)
    }

    fn reserve_class_a(&self, limits: &Limits) -> ApiResult<bool> {
        let mut conn = self.lock()?;
        let tx = conn.transaction().map_err(ApiError::db)?;
        let month = current_month(&tx)?;
        ensure_usage(&tx, &month)?;
        let (class_a, _) = usage(&tx, &month)?;
        if class_a + 1 > limits.class_a_guard() {
            return Ok(false);
        }
        tx.execute(
            "UPDATE usage SET class_a = class_a + 1 WHERE month = ?",
            [&month],
        )
        .map_err(ApiError::db)?;
        tx.commit().map_err(ApiError::db)?;
        Ok(true)
    }

    fn lock(&self) -> ApiResult<std::sync::MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "database lock failed"))
    }
}

struct NewFile {
    id: String,
    key: String,
    name: String,
    content_type: String,
    size_bytes: i64,
    created_at: i64,
    expires_at: i64,
    max_downloads: i64,
}

struct DownloadTicket {
    id: String,
    key: String,
    name: String,
    content_type: String,
    delete_after: bool,
}

fn current_month(conn: &Connection) -> ApiResult<String> {
    conn.query_row("SELECT strftime('%Y-%m','now')", [], |row| row.get(0))
        .map_err(ApiError::db)
}

fn ensure_usage(conn: &Connection, month: &str) -> ApiResult<()> {
    conn.execute("INSERT OR IGNORE INTO usage (month) VALUES (?)", [month])
        .map_err(ApiError::db)?;
    Ok(())
}

fn usage(conn: &Connection, month: &str) -> ApiResult<(i64, i64)> {
    conn.query_row(
        "SELECT class_a, class_b FROM usage WHERE month = ?",
        [month],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .map_err(ApiError::db)
}

fn active_storage(conn: &Connection) -> ApiResult<i64> {
    conn.query_row(
        "SELECT COALESCE(SUM(size_bytes), 0) FROM files WHERE removed_at IS NULL",
        [],
        |row| row.get(0),
    )
    .map_err(ApiError::db)
}

fn content_length(headers: &HeaderMap) -> ApiResult<i64> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .ok_or_else(|| ApiError::new(StatusCode::LENGTH_REQUIRED, "content-length required"))
}

fn content_type(headers: &HeaderMap) -> String {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() < 128 && !value.bytes().any(|b| b < 32 || b == 127))
        .unwrap_or("application/octet-stream")
        .to_string()
}

fn filename(headers: &HeaderMap, fallback: &str) -> String {
    let raw = headers
        .get(&X_FILENAME)
        .and_then(|value| value.to_str().ok())
        .unwrap_or(fallback);
    let cleaned: String = raw
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        .take(120)
        .collect();
    if cleaned.is_empty() {
        "download".into()
    } else {
        cleaned
    }
}

fn base_url(state: &AppState, headers: &HeaderMap) -> String {
    if let Some(base) = &state.base_url {
        return base.clone();
    }
    let proto = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .filter(|value| matches!(*value, "http" | "https"))
        .unwrap_or("http");
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost:3000");
    format!("{proto}://{host}")
}

fn header_value(value: &str) -> ApiResult<HeaderValue> {
    HeaderValue::from_str(value)
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "invalid response header"))
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn guarded(limit: i64, ratio: f64) -> i64 {
    ((limit as f64) * ratio).floor() as i64
}

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("{name} is required"))
}

fn var_i64(name: &str, default: i64) -> Result<i64, String> {
    env::var(name)
        .map(|value| {
            value
                .parse::<i64>()
                .map_err(|_| format!("{name} must be an integer"))
        })
        .unwrap_or(Ok(default))
}

fn var_usize(name: &str, default: usize) -> Result<usize, String> {
    env::var(name)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| format!("{name} must be an integer"))
        })
        .unwrap_or(Ok(default))
}

fn var_f64(name: &str, default: f64) -> Result<f64, String> {
    env::var(name)
        .map(|value| {
            value
                .parse::<f64>()
                .map_err(|_| format!("{name} must be a number"))
        })
        .unwrap_or(Ok(default))
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn db(err: rusqlite::Error) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("database failed: {err}"),
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, format!("{}\n", self.message)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_guard_stops_before_storage_limit() {
        let db = Db::from_connection(Connection::open_in_memory().unwrap()).unwrap();
        let limits = Limits {
            storage_bytes: 100,
            class_a_ops: 10,
            class_b_ops: 10,
            guard_ratio: 0.8,
            max_upload_bytes: 100,
            ttl_seconds: WEEK,
            max_downloads: 20,
        };

        let first = test_file("a", 70);
        db.begin_upload(&first, &limits).unwrap();

        let second = test_file("b", 11);
        let err = db.begin_upload(&second, &limits).unwrap_err();
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(err.message.contains("storage guard"));
    }

    fn test_file(id: &str, size_bytes: i64) -> NewFile {
        NewFile {
            id: id.into(),
            key: format!("shares/{id}"),
            name: id.into(),
            content_type: "application/octet-stream".into(),
            size_bytes,
            created_at: now(),
            expires_at: now() + WEEK,
            max_downloads: 20,
        }
    }
}
