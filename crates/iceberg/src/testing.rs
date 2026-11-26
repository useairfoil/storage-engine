//! Controllable in-process Iceberg REST catalog for tests.

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use iceberg::{
    NamespaceIdent, TableCommit, TableIdent, TableRequirement, TableUpdate,
    io::FileIO,
    spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    },
    table::Table,
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

const NAMESPACE_SEPARATOR: char = '\x1f';

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFault {
    FailBeforeApply,
    ApplyThenFail,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedRequest {
    Config {
        prefix: Option<String>,
    },
    OAuthToken {
        prefix: Option<String>,
    },
    ListNamespaces {
        prefix: Option<String>,
        parent: Option<NamespaceIdent>,
    },
    ListTables {
        prefix: Option<String>,
        namespace: NamespaceIdent,
    },
    LoadTable {
        prefix: Option<String>,
        ident: TableIdent,
    },
    CommitTable {
        prefix: Option<String>,
        ident: TableIdent,
        applied: bool,
    },
}

pub struct MockRestCatalog {
    addr: SocketAddr,
    state: Arc<MockCatalogState>,
    handle: tokio::task::JoinHandle<()>,
}

#[derive(Default)]
struct MockCatalogState {
    inner: Mutex<InnerState>,
}

#[derive(Default)]
struct InnerState {
    namespaces: BTreeSet<Vec<String>>,
    tables: HashMap<TableIdent, Table>,
    required_header: Option<(String, String)>,
    config_defaults: HashMap<String, String>,
    config_overrides: HashMap<String, String>,
    commit_faults: VecDeque<CommitFault>,
    requests: Vec<RecordedRequest>,
    commits: usize,
}

#[derive(Deserialize)]
struct ListNamespacesQuery {
    parent: Option<String>,
}

#[derive(Deserialize)]
struct CommitRequest {
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

/// Builds a minimal in-memory Iceberg table for catalog tests.
pub fn test_table(
    ident: TableIdent,
    uuid: Uuid,
    format_version: FormatVersion,
    properties: HashMap<String, String>,
) -> iceberg::Result<Table> {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "value",
            Type::Primitive(PrimitiveType::String),
        ))])
        .build()?;

    let location = format!("memory://{}", ident.name);
    let metadata_location = format!("{location}/metadata/00000-{uuid}.metadata.json");
    let metadata = TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        location,
        format_version,
        properties,
    )?
    .assign_uuid(uuid)
    .build()?
    .metadata;

    Table::builder()
        .runtime(iceberg::Runtime::try_current()?)
        .identifier(ident)
        .metadata(metadata)
        .metadata_location(metadata_location)
        .file_io(FileIO::new_with_memory())
        .build()
}

impl MockRestCatalog {
    /// Starts a mock Iceberg REST catalog bound to an ephemeral loopback port.
    pub async fn start() -> std::io::Result<Self> {
        let state = Arc::new(MockCatalogState::default());
        let router = Router::new()
            .route("/v1/oauth/tokens", post(oauth_token))
            .route("/{prefix}/v1/oauth/tokens", post(prefixed_oauth_token))
            .route("/v1/config", get(config))
            .route("/{prefix}/v1/config", get(prefixed_config))
            .route("/v1/namespaces", get(list_namespaces))
            .route("/{prefix}/v1/namespaces", get(prefixed_list_namespaces))
            .route("/v1/namespaces/{namespace}/tables", get(list_tables))
            .route(
                "/{prefix}/v1/namespaces/{namespace}/tables",
                get(prefixed_list_tables),
            )
            .route(
                "/v1/namespaces/{namespace}/tables/{table}",
                get(load_table).post(commit_table),
            )
            .route(
                "/{prefix}/v1/namespaces/{namespace}/tables/{table}",
                get(prefixed_load_table).post(prefixed_commit_table),
            )
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        Ok(Self {
            addr,
            state,
            handle,
        })
    }

    /// Returns the base HTTP URI of the mock catalog.
    pub fn uri(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Returns the HTTP URI of the mock catalog scoped to a prefix.
    pub fn uri_with_prefix(&self, prefix: &str) -> String {
        format!("http://{}/{prefix}", self.addr)
    }

    /// Registers a namespace and all of its ancestor namespaces.
    pub fn add_namespace(&self, namespace: &NamespaceIdent) {
        self.state.lock().register_namespace(namespace);
    }

    /// Inserts or replaces a table and registers its namespace hierarchy.
    pub fn put_table(&self, table: Table) {
        let mut inner = self.state.lock();
        inner.register_namespace(table.identifier().namespace());
        inner.tables.insert(table.identifier().clone(), table);
    }

    /// Removes and returns a table by identifier if present.
    pub fn remove_table(&self, ident: &TableIdent) -> Option<Table> {
        self.state.lock().tables.remove(ident)
    }

    /// Returns a clone of the current table state if present.
    pub fn table(&self, ident: &TableIdent) -> Option<Table> {
        self.state.lock().tables.get(ident).cloned()
    }

    /// Requires a specific HTTP header and value on catalog requests.
    pub fn require_header(&self, name: impl Into<String>, value: impl Into<String>) {
        self.state.lock().required_header = Some((name.into(), value.into()));
    }

    /// Clears any required HTTP header check.
    pub fn clear_required_header(&self) {
        self.state.lock().required_header = None;
    }

    /// Sets the default properties returned by `/v1/config`.
    pub fn set_config_defaults(&self, defaults: HashMap<String, String>) {
        self.state.lock().config_defaults = defaults;
    }

    /// Sets the override properties returned by `/v1/config`.
    pub fn set_config_overrides(&self, overrides: HashMap<String, String>) {
        self.state.lock().config_overrides = overrides;
    }

    /// Enqueues a fault to trigger on the next table commit request.
    pub fn fail_next_commit(&self, fault: CommitFault) {
        self.state.lock().commit_faults.push_back(fault);
    }

    /// Clears any queued commit faults.
    pub fn clear_commit_faults(&self) {
        self.state.lock().commit_faults.clear();
    }

    /// Returns all requests recorded by the mock catalog in arrival order.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.state.lock().requests.clone()
    }

    /// Clears the recorded request log.
    pub fn clear_requests(&self) {
        self.state.lock().requests.clear();
    }

    /// Returns the number of `/v1/config` requests received.
    pub fn config_probes(&self) -> usize {
        self.state
            .lock()
            .requests
            .iter()
            .filter(|request| matches!(request, RecordedRequest::Config { .. }))
            .count()
    }

    /// Returns the number of table commits successfully applied on the server.
    pub fn commit_count(&self) -> usize {
        self.state.lock().commits
    }
}

impl Drop for MockRestCatalog {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl MockCatalogState {
    fn lock(&self) -> MutexGuard<'_, InnerState> {
        // PANIC: MockCatalogState mutex is only poisoned if a test handler panics.
        self.inner.lock().unwrap()
    }
}

impl InnerState {
    fn register_namespace(&mut self, namespace: &NamespaceIdent) {
        let levels = namespace.as_ref();

        for len in 1..=levels.len() {
            self.namespaces.insert(levels[..len].to_vec());
        }
    }

    fn check_headers(&self, headers: &HeaderMap) -> Option<Response> {
        let (name, expected) = self.required_header.as_ref()?;
        let actual = headers
            .get(name.as_str())
            .and_then(|value| value.to_str().ok());

        if actual != Some(expected.as_str()) {
            return Some(error_response(
                StatusCode::UNAUTHORIZED,
                "NotAuthorizedException",
                "invalid credentials",
            ));
        }

        None
    }
}

async fn oauth_token(State(state): State<Arc<MockCatalogState>>) -> Json<Value> {
    handle_oauth_token(&state, None)
}

async fn prefixed_oauth_token(
    State(state): State<Arc<MockCatalogState>>,
    Path(prefix): Path<String>,
) -> Json<Value> {
    handle_oauth_token(&state, Some(prefix))
}

fn handle_oauth_token(state: &MockCatalogState, prefix: Option<String>) -> Json<Value> {
    state
        .lock()
        .requests
        .push(RecordedRequest::OAuthToken { prefix });

    Json(json!({
        "access_token": "mock-token",
        "token_type": "bearer",
    }))
}

async fn config(State(state): State<Arc<MockCatalogState>>, headers: HeaderMap) -> Response {
    handle_config(&state, None, &headers)
}

async fn prefixed_config(
    State(state): State<Arc<MockCatalogState>>,
    Path(prefix): Path<String>,
    headers: HeaderMap,
) -> Response {
    handle_config(&state, Some(prefix), &headers)
}

fn handle_config(
    state: &MockCatalogState,
    prefix: Option<String>,
    headers: &HeaderMap,
) -> Response {
    let mut inner = state.lock();
    inner.requests.push(RecordedRequest::Config { prefix });

    if let Some(response) = inner.check_headers(headers) {
        return response;
    }

    Json(json!({
        "defaults": inner.config_defaults,
        "overrides": inner.config_overrides,
    }))
    .into_response()
}

async fn list_namespaces(
    State(state): State<Arc<MockCatalogState>>,
    Query(query): Query<ListNamespacesQuery>,
    headers: HeaderMap,
) -> Response {
    handle_list_namespaces(&state, None, query.parent, &headers)
}

async fn prefixed_list_namespaces(
    State(state): State<Arc<MockCatalogState>>,
    Path(prefix): Path<String>,
    Query(query): Query<ListNamespacesQuery>,
    headers: HeaderMap,
) -> Response {
    handle_list_namespaces(&state, Some(prefix), query.parent, &headers)
}

fn handle_list_namespaces(
    state: &MockCatalogState,
    prefix: Option<String>,
    parent: Option<String>,
    headers: &HeaderMap,
) -> Response {
    let parent = match parent.filter(|value| !value.is_empty()) {
        Some(raw) => match parse_namespace(&raw) {
            Ok(ident) => Some(ident),
            Err(error) => return bad_request(error),
        },
        None => None,
    };

    let mut inner = state.lock();
    inner.requests.push(RecordedRequest::ListNamespaces {
        prefix,
        parent: parent.clone(),
    });

    if let Some(response) = inner.check_headers(headers) {
        return response;
    }

    let namespaces: Vec<&Vec<String>> = match &parent {
        None => inner
            .namespaces
            .iter()
            .filter(|levels| levels.len() == 1)
            .collect(),
        Some(parent) => {
            let parent_levels = parent.as_ref();

            if !inner.namespaces.contains(parent_levels) {
                return error_response(
                    StatusCode::NOT_FOUND,
                    "NoSuchNamespaceException",
                    format!("namespace not found: {parent}"),
                );
            }

            inner
                .namespaces
                .iter()
                .filter(|levels| {
                    levels.len() == parent_levels.len() + 1 && levels.starts_with(parent_levels)
                })
                .collect()
        }
    };

    Json(json!({ "namespaces": namespaces })).into_response()
}

async fn list_tables(
    State(state): State<Arc<MockCatalogState>>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Response {
    handle_list_tables(&state, None, &namespace, &headers)
}

async fn prefixed_list_tables(
    State(state): State<Arc<MockCatalogState>>,
    Path((prefix, namespace)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    handle_list_tables(&state, Some(prefix), &namespace, &headers)
}

fn handle_list_tables(
    state: &MockCatalogState,
    prefix: Option<String>,
    namespace: &str,
    headers: &HeaderMap,
) -> Response {
    let namespace = match parse_namespace(namespace) {
        Ok(ident) => ident,
        Err(error) => return bad_request(error),
    };

    let mut inner = state.lock();
    inner.requests.push(RecordedRequest::ListTables {
        prefix,
        namespace: namespace.clone(),
    });

    if let Some(response) = inner.check_headers(headers) {
        return response;
    }

    if !inner.namespaces.contains(namespace.as_ref()) {
        return error_response(
            StatusCode::NOT_FOUND,
            "NoSuchNamespaceException",
            format!("namespace not found: {namespace}"),
        );
    }

    let mut identifiers: Vec<TableIdent> = inner
        .tables
        .keys()
        .filter(|ident| ident.namespace() == &namespace)
        .cloned()
        .collect();
    identifiers.sort_by(|left, right| left.name.cmp(&right.name));

    Json(json!({ "identifiers": identifiers })).into_response()
}

async fn load_table(
    State(state): State<Arc<MockCatalogState>>,
    Path((namespace, table)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    handle_load_table(&state, None, &namespace, table, &headers)
}

async fn prefixed_load_table(
    State(state): State<Arc<MockCatalogState>>,
    Path((prefix, namespace, table)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    handle_load_table(&state, Some(prefix), &namespace, table, &headers)
}

fn handle_load_table(
    state: &MockCatalogState,
    prefix: Option<String>,
    namespace: &str,
    table: String,
    headers: &HeaderMap,
) -> Response {
    let namespace = match parse_namespace(namespace) {
        Ok(ident) => ident,
        Err(error) => return bad_request(error),
    };
    let ident = TableIdent::new(namespace, table);

    let mut inner = state.lock();
    inner.requests.push(RecordedRequest::LoadTable {
        prefix,
        ident: ident.clone(),
    });

    if let Some(response) = inner.check_headers(headers) {
        return response;
    }

    let Some(table) = inner.tables.get(&ident) else {
        return error_response(
            StatusCode::NOT_FOUND,
            "NoSuchTableException",
            format!("table not found: {ident}"),
        );
    };

    table_response(table)
}

async fn commit_table(
    State(state): State<Arc<MockCatalogState>>,
    Path((namespace, table)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<CommitRequest>,
) -> Response {
    handle_commit_table(&state, None, &namespace, table, &headers, request)
}

async fn prefixed_commit_table(
    State(state): State<Arc<MockCatalogState>>,
    Path((prefix, namespace, table)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(request): Json<CommitRequest>,
) -> Response {
    handle_commit_table(&state, Some(prefix), &namespace, table, &headers, request)
}

fn handle_commit_table(
    state: &MockCatalogState,
    prefix: Option<String>,
    namespace: &str,
    table: String,
    headers: &HeaderMap,
    request: CommitRequest,
) -> Response {
    let namespace = match parse_namespace(namespace) {
        Ok(ident) => ident,
        Err(error) => return bad_request(error),
    };
    let ident = TableIdent::new(namespace, table);

    let mut inner = state.lock();

    if let Some(response) = inner.check_headers(headers) {
        inner.requests.push(RecordedRequest::CommitTable {
            prefix,
            ident,
            applied: false,
        });
        return response;
    }

    let fault = inner.commit_faults.pop_front();

    if fault == Some(CommitFault::FailBeforeApply) {
        inner.requests.push(RecordedRequest::CommitTable {
            prefix,
            ident,
            applied: false,
        });
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
            "injected commit failure before apply",
        );
    }

    let Some(current) = inner.tables.get(&ident).cloned() else {
        inner.requests.push(RecordedRequest::CommitTable {
            prefix,
            ident: ident.clone(),
            applied: false,
        });
        return error_response(
            StatusCode::NOT_FOUND,
            "NoSuchTableException",
            format!("table not found: {ident}"),
        );
    };

    let commit = TableCommit::builder()
        .ident(current.identifier().clone())
        .updates(request.updates)
        .requirements(request.requirements)
        .build();

    match commit.apply(current) {
        Ok(updated) => {
            inner.commits += 1;
            inner.requests.push(RecordedRequest::CommitTable {
                prefix,
                ident: ident.clone(),
                applied: true,
            });
            let response = if fault == Some(CommitFault::ApplyThenFail) {
                error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalServerError",
                    "injected uncertain commit",
                )
            } else {
                table_response(&updated)
            };
            inner.tables.insert(ident, updated);
            response
        }
        Err(error) => {
            inner.requests.push(RecordedRequest::CommitTable {
                prefix,
                ident,
                applied: false,
            });
            error_response(
                StatusCode::CONFLICT,
                "CommitFailedException",
                error.to_string(),
            )
        }
    }
}

fn parse_namespace(raw: &str) -> iceberg::Result<NamespaceIdent> {
    let levels: Vec<String> = raw.split(NAMESPACE_SEPARATOR).map(str::to_owned).collect();

    NamespaceIdent::from_vec(levels)
}

fn bad_request(error: iceberg::Error) -> Response {
    error_response(
        StatusCode::BAD_REQUEST,
        "BadRequestException",
        error.to_string(),
    )
}

fn table_response(table: &Table) -> Response {
    Json(json!({
        "metadata": table.metadata(),
        "metadata-location": table.metadata_location(),
        "config": {},
    }))
    .into_response()
}

fn error_response(status: StatusCode, error_type: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({
            "error": {
                "message": message.into(),
                "type": error_type,
                "code": status.as_u16(),
            }
        })),
    )
        .into_response()
}
