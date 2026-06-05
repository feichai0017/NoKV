use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io::Read;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flate2::read::GzDecoder;
use nokvfs_client::{ArtifactRepository, ArtifactRepositoryOptions};
use nokvfs_meta::{HoltMetadataStore, NoKvFs};
use nokvfs_object::{S3ObjectStore, S3ObjectStoreOptions};
use nokvfs_types::{BodyDescriptor, FileType, InodeAttr, MountId};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tar::Archive;
use walkdir::WalkDir;

const DEFAULT_MODE_DIR: u32 = 0o755;
const DEFAULT_UID: u32 = 1000;
const DEFAULT_GID: u32 = 1000;
const DEFAULT_BUCKET: &str = "nokv-yanex-demo";
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:9000";
const DEFAULT_ACCESS_KEY: &str = "rustfsadmin";
const DEFAULT_SECRET_KEY: &str = "rustfsadmin";
const RUNS_PREFIX: &str = "/runs";
const NOKV_PREFIX: &str = "yanex";
const DEFAULT_REPEATS: usize = 10;
const TOOL_CALL_TIMEOUT_MS: u64 = 30_000;
const SQLITE_PROGRESS_OPS: i32 = 1_000;

fn main() {
    if let Err(err) = run(env::args().skip(1).collect()) {
        eprintln!("error: {err}");
        eprintln!(
            "\nUsage:\n  harness prepare --archive PATH --data-root PATH [--reset] [s3 options]\n  harness verify --data-root PATH [--run-id ID] [--nokv-posix-root PATH] [s3 options]\n  harness tools --arm ARM\n  harness list-tasks\n  harness show-task --task-id ID\n  harness gold --data-root PATH --task-id ID\n  harness judge --data-root PATH --arm ARM --task-id ID --answer-json PATH\n  harness run-task --data-root PATH --arm ARM --task-id ID --output-jsonl PATH [--model MODEL] [--max-completion-tokens N]\n  harness run-batch --data-root PATH --output-jsonl PATH [--repeats N|--repeat N] [--arm ARM] [--task-id ID]\n  harness sqlite-show-schema --db PATH\n  harness sqlite-query --db PATH --sql SQL\n  harness sqlite-read-blob --db PATH --blob-ref REF --offset N --limit N\n  harness agentfs-ls|agentfs-stat|agentfs-read|agentfs-grep|agentfs-find --data-root PATH --path PATH [...]\n  harness nokv-list|nokv-stat|nokv-read --data-root PATH --path PATH [...]\n"
        );
        std::process::exit(2);
    }
}

fn run(args: Vec<String>) -> Result<(), HarnessError> {
    let Some(command) = args.first().map(String::as_str) else {
        return Err(HarnessError::MissingCommand);
    };
    let options = Options::parse(&args[1..])?;
    match command {
        "prepare" => prepare(options),
        "verify" => verify(options).map(|report| {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("verification report serializes")
            );
        }),
        "sqlite-show-schema" => show_schema(options),
        "sqlite-query" => query_sql(options),
        "sqlite-read-blob" => read_blob(options),
        "agentfs-ls" => agentfs_ls(options),
        "agentfs-stat" => agentfs_stat(options),
        "agentfs-read" => agentfs_read(options),
        "agentfs-grep" => agentfs_grep(options),
        "agentfs-find" => agentfs_find(options),
        "posix-ls" => posix_ls(options),
        "posix-stat" => posix_stat(options),
        "posix-read" => posix_read(options),
        "posix-grep" => posix_grep(options),
        "posix-find" => posix_find(options),
        "nokv-list" => nokv_list(options),
        "nokv-stat" => nokv_stat(options),
        "nokv-read" => nokv_read(options),
        "tools" => print_tool_registry(options),
        "list-tasks" => list_tasks(),
        "show-task" => show_task(options),
        "gold" => print_gold(options),
        "judge" => judge(options),
        "run-task" => run_benchmark_task(options),
        "run-batch" => run_benchmark_batch(options),
        other => Err(HarnessError::UnknownCommand(other.to_owned())),
    }
}

#[derive(Clone, Debug, Default)]
struct Options {
    archive: Option<PathBuf>,
    data_root: Option<PathBuf>,
    db: Option<PathBuf>,
    sql: Option<String>,
    blob_ref: Option<String>,
    offset: Option<u64>,
    limit: Option<usize>,
    path: Option<String>,
    pattern: Option<String>,
    recursive: bool,
    arm: Option<String>,
    task_id: Option<String>,
    output_jsonl: Option<PathBuf>,
    answer_json: Option<PathBuf>,
    model: Option<String>,
    expected_generation: Option<u64>,
    max_turns: Option<usize>,
    max_tool_calls: Option<usize>,
    max_completion_tokens: Option<usize>,
    repeats: Option<usize>,
    run_id: Option<String>,
    nokv_posix_root: Option<PathBuf>,
    reset: bool,
    s3: S3Options,
}

#[derive(Clone, Debug)]
struct S3Options {
    bucket: String,
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
}

#[derive(Clone, Debug)]
struct CorpusRun {
    id: String,
    metadata_bytes: Vec<u8>,
    metadata: Value,
    params_bytes: Option<Vec<u8>>,
    params: Option<serde_yaml::Value>,
    metrics_bytes: Option<Vec<u8>>,
    metrics: Option<Value>,
    dependencies_bytes: Option<Vec<u8>>,
    dependencies: Option<Value>,
    artifacts: Vec<CorpusFile>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BenchmarkTaskSet {
    task_set_id: String,
    version: u32,
    scope: String,
    tasks: Vec<BenchmarkTask>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BenchmarkTask {
    task_id: String,
    category: String,
    prompt: String,
    gold_sql: String,
    expected: ExpectedSpec,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ExpectedSpec {
    kind: String,
    answer_key: Option<String>,
    #[serde(default)]
    run_id_column: Option<String>,
    #[serde(default)]
    columns: Vec<String>,
}

#[derive(Clone, Debug)]
struct CorpusFile {
    relative_path: String,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
struct DerivedIndexes {
    files: BTreeMap<String, Vec<u8>>,
    status_counts: BTreeMap<String, usize>,
    tag_counts: BTreeMap<String, usize>,
    script_counts: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Serialize)]
struct RunSummary {
    experiment_id: String,
    name: Option<String>,
    status: Option<String>,
    project: Option<String>,
    script: Option<String>,
    created_at: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    tags: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct PrepareManifest {
    archive_sha256: String,
    run_count: usize,
    metadata_files: usize,
    metrics_files: usize,
    params_files: usize,
    dependencies_files: usize,
    artifact_files: usize,
    missing_artifact_refs: usize,
    sqlite_db: String,
    nokv_meta: String,
    status_counts: BTreeMap<String, usize>,
    tag_counts: BTreeMap<String, usize>,
    script_counts: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Serialize)]
struct VerificationReport {
    run_id_sample: String,
    sqlite: SqliteMaterializationReport,
    nokv_native: NamespaceMaterializationReport,
    nokv_posix: Option<NamespaceMaterializationReport>,
    all_available_reads_match: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
struct SqliteMaterializationReport {
    run_count: usize,
    raw_metadata_checked: usize,
    raw_params_checked: usize,
    raw_metrics_checked: usize,
    raw_dependencies_checked: usize,
    raw_existing_artifacts_checked: usize,
    raw_missing_artifacts_checked: usize,
    agentfs_files_checked: usize,
    index_files_checked: usize,
    mismatches: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct NamespaceMaterializationReport {
    files_checked: usize,
    run_files_checked: usize,
    index_files_checked: usize,
    missing_artifacts_checked: usize,
    metadata_generations_observed: usize,
    body_descriptors_checked: usize,
    mismatches: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SqliteQueryToolResult {
    tool: &'static str,
    row_limit: usize,
    truncated: bool,
    rows: Vec<SqliteQueryToolRow>,
}

#[derive(Clone, Debug, Serialize)]
struct SqliteQueryToolRow {
    row: Value,
    evidence: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SqliteReadBlobToolResult {
    tool: &'static str,
    blob_ref: String,
    evidence: String,
    total_size_bytes: usize,
    digest: String,
    offset: u64,
    requested_limit: usize,
    bytes_read: usize,
    content_utf8: Option<String>,
    content_hex: String,
}

#[derive(Clone, Debug, Serialize)]
struct ToolDefinition {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Clone, Debug, Serialize)]
struct FileEntry {
    name: String,
    path: String,
    file_type: String,
    size_bytes: Option<u64>,
    digest: Option<String>,
    evidence: String,
    inode: Option<u64>,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    modified_ms: Option<i64>,
    generation: Option<u64>,
    body: Option<BodyDescriptorSummary>,
}

#[derive(Clone, Debug, Serialize)]
struct BodyDescriptorSummary {
    producer: String,
    digest_uri: String,
    size: u64,
    content_type: String,
    manifest_id: String,
    generation: u64,
    chunk_size: u64,
    block_size: u64,
}

#[derive(Clone, Debug, Serialize)]
struct ListToolResult {
    tool: &'static str,
    path: String,
    evidence: String,
    entries: Vec<FileEntry>,
    row_limit: usize,
    truncated: bool,
}

#[derive(Clone, Debug, Serialize)]
struct StatToolResult {
    tool: &'static str,
    path: String,
    evidence: String,
    file_type: String,
    size_bytes: Option<u64>,
    digest: Option<String>,
    inode: Option<u64>,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    modified_ms: Option<i64>,
    generation: Option<u64>,
    body: Option<BodyDescriptorSummary>,
}

#[derive(Clone, Debug, Serialize)]
struct ReadToolResult {
    tool: &'static str,
    path: String,
    evidence: String,
    total_size_bytes: u64,
    digest: Option<String>,
    offset: u64,
    requested_limit: usize,
    bytes_read: usize,
    content_utf8: Option<String>,
    content_hex: String,
    generation: Option<u64>,
    body: Option<BodyDescriptorSummary>,
    generation_mismatch: Option<GenerationMismatch>,
}

#[derive(Clone, Debug, Serialize)]
struct GenerationMismatch {
    expected: u64,
    current: u64,
}

#[derive(Clone, Debug, Serialize)]
struct GrepToolResult {
    tool: &'static str,
    path: String,
    pattern: String,
    recursive: bool,
    matches: Vec<GrepMatch>,
    files_scanned: usize,
    bytes_read: usize,
    row_limit: usize,
    truncated: bool,
}

#[derive(Clone, Debug, Serialize)]
struct GrepMatch {
    path: String,
    line_number: usize,
    snippet: String,
    evidence: String,
}

#[derive(Clone, Debug, Serialize)]
struct FindToolResult {
    tool: &'static str,
    path: String,
    pattern: String,
    matches: Vec<FileEntry>,
    row_limit: usize,
    truncated: bool,
}

#[derive(Clone, Debug, Serialize)]
struct JudgeResult {
    task_success: bool,
    evidence_precision: Option<f64>,
    evidence_checked: usize,
    evidence_supported: usize,
    expected_run_ids: Vec<String>,
    actual_run_ids: Vec<String>,
    mismatches: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkRunTelemetry {
    record_type: String,
    run_id: String,
    arm_id: String,
    task_id: String,
    repeat_index: usize,
    model: String,
    started_at_unix_ms: u128,
    completed_at_unix_ms: u128,
    api_calls: Vec<ApiCallTelemetry>,
    tool_calls: Vec<ToolCallTelemetry>,
    derived_metrics: DerivedMetrics,
    judge: Option<JudgeResult>,
    final_answer: Option<Value>,
    run_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ToolCallStartTelemetry {
    record_type: String,
    run_id: String,
    arm_id: String,
    task_id: String,
    repeat_index: usize,
    call_id: String,
    tool_name: String,
    arguments: Value,
    started_at_unix_ms: u128,
}

#[derive(Clone, Debug, Serialize)]
struct ApiCallTelemetry {
    request_id: Option<String>,
    model: String,
    started_at_unix_ms: u128,
    completed_at_unix_ms: u128,
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    total_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    cached_prompt_tokens: Option<u64>,
    accepted_prediction_tokens: Option<u64>,
    rejected_prediction_tokens: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct ToolCallTelemetry {
    call_id: String,
    tool_name: String,
    arguments: Value,
    started_at_unix_ms: u128,
    completed_at_unix_ms: u128,
    status: String,
    bytes_read: usize,
    result_token_estimate: usize,
    error: Option<String>,
    timed_out: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
struct DerivedMetrics {
    interface_card_tokens: usize,
    task_prompt_tokens: usize,
    execution_cost_usd: Option<f64>,
    all_in_cost_usd: Option<f64>,
    wall_time_ms: u128,
    task_success: Option<bool>,
    evidence_precision: Option<f64>,
    tool_call_count: usize,
    tool_result_tokens: usize,
    tool_bytes_read: usize,
    invalid_sql_count: usize,
    wrong_tool_count: usize,
    tool_timeout_count: usize,
    missing_evidence_count: usize,
    overread_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    tools: Vec<OpenAiTool>,
    temperature: f64,
    max_completion_tokens: usize,
    response_format: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChatMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OpenAiToolCall>>,
}

#[derive(Clone, Debug, Serialize)]
struct OpenAiTool {
    #[serde(rename = "type")]
    tool_type: String,
    function: OpenAiFunctionDefinition,
}

#[derive(Clone, Debug, Serialize)]
struct OpenAiFunctionDefinition {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OpenAiToolCall {
    id: String,
    #[serde(rename = "type")]
    tool_type: String,
    function: OpenAiToolCallFunction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OpenAiToolCallFunction {
    name: String,
    arguments: String,
}

#[derive(Clone, Debug, Deserialize)]
struct ChatCompletionResponse {
    id: Option<String>,
    model: Option<String>,
    choices: Vec<ChatChoice>,
    usage: Option<ChatUsage>,
}

#[derive(Clone, Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
    finish_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ChatUsage {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    total_tokens: Option<u64>,
    prompt_tokens_details: Option<PromptTokenDetails>,
    completion_tokens_details: Option<CompletionTokenDetails>,
}

#[derive(Clone, Debug, Deserialize)]
struct PromptTokenDetails {
    cached_tokens: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
struct CompletionTokenDetails {
    reasoning_tokens: Option<u64>,
    accepted_prediction_tokens: Option<u64>,
    rejected_prediction_tokens: Option<u64>,
}

struct ToolExecutionOutcome {
    content: Value,
    bytes_read: usize,
    result_token_estimate: usize,
    status: String,
    error: Option<String>,
    invalid_sql: bool,
    wrong_tool: bool,
    timed_out: bool,
    fatal_run_error: bool,
}

enum ArmRuntime {
    SqliteRaw {
        conn: Connection,
    },
    SqliteAgentFs {
        conn: Connection,
    },
    NokvPosix {
        root: PathBuf,
    },
    NokvNative {
        service: Box<NoKvFs<HoltMetadataStore, S3ObjectStore>>,
    },
}

struct ToolWorker {
    request_tx: mpsc::Sender<ToolWorkerRequest>,
    timeout: Duration,
}

struct ToolWorkerRequest {
    call: OpenAiToolCall,
    response_tx: mpsc::Sender<ToolExecutionOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BatchPlanItem {
    arm_id: String,
    task_id: String,
    repeat_index: usize,
}

#[derive(Debug)]
enum HarnessError {
    MissingCommand,
    UnknownCommand(String),
    MissingOption(&'static str),
    UnknownOption(String),
    InvalidNumber { option: &'static str, value: String },
    Io(String),
    Json(String),
    Yaml(String),
    Sql(String),
    NoKv(String),
    Corpus(String),
    Judge(String),
    Http(String),
    ToolTimeout { tool: String, limit_ms: u128 },
}

impl Default for S3Options {
    fn default() -> Self {
        Self {
            bucket: DEFAULT_BUCKET.to_owned(),
            endpoint: DEFAULT_ENDPOINT.to_owned(),
            access_key_id: DEFAULT_ACCESS_KEY.to_owned(),
            secret_access_key: DEFAULT_SECRET_KEY.to_owned(),
        }
    }
}

impl Options {
    fn parse(args: &[String]) -> Result<Self, HarnessError> {
        let mut options = Self::default();
        let mut index = 0;
        while index < args.len() {
            match args[index].as_str() {
                "--archive" => {
                    index += 1;
                    options.archive = Some(PathBuf::from(value(args, index, "--archive")?));
                }
                "--data-root" => {
                    index += 1;
                    options.data_root = Some(PathBuf::from(value(args, index, "--data-root")?));
                }
                "--db" => {
                    index += 1;
                    options.db = Some(PathBuf::from(value(args, index, "--db")?));
                }
                "--sql" => {
                    index += 1;
                    options.sql = Some(value(args, index, "--sql")?.to_owned());
                }
                "--blob-ref" => {
                    index += 1;
                    options.blob_ref = Some(value(args, index, "--blob-ref")?.to_owned());
                }
                "--offset" => {
                    index += 1;
                    options.offset = Some(parse_u64(value(args, index, "--offset")?, "--offset")?);
                }
                "--limit" => {
                    index += 1;
                    options.limit = Some(parse_usize(value(args, index, "--limit")?, "--limit")?);
                }
                "--path" => {
                    index += 1;
                    options.path = Some(value(args, index, "--path")?.to_owned());
                }
                "--pattern" => {
                    index += 1;
                    options.pattern = Some(value(args, index, "--pattern")?.to_owned());
                }
                "--recursive" => options.recursive = true,
                "--arm" => {
                    index += 1;
                    options.arm = Some(value(args, index, "--arm")?.to_owned());
                }
                "--task-id" => {
                    index += 1;
                    options.task_id = Some(value(args, index, "--task-id")?.to_owned());
                }
                "--output-jsonl" => {
                    index += 1;
                    options.output_jsonl =
                        Some(PathBuf::from(value(args, index, "--output-jsonl")?));
                }
                "--answer-json" => {
                    index += 1;
                    options.answer_json = Some(PathBuf::from(value(args, index, "--answer-json")?));
                }
                "--model" => {
                    index += 1;
                    options.model = Some(value(args, index, "--model")?.to_owned());
                }
                "--expected-generation" => {
                    index += 1;
                    options.expected_generation = Some(parse_u64(
                        value(args, index, "--expected-generation")?,
                        "--expected-generation",
                    )?);
                }
                "--max-turns" => {
                    index += 1;
                    options.max_turns = Some(parse_usize(
                        value(args, index, "--max-turns")?,
                        "--max-turns",
                    )?);
                }
                "--max-tool-calls" => {
                    index += 1;
                    options.max_tool_calls = Some(parse_usize(
                        value(args, index, "--max-tool-calls")?,
                        "--max-tool-calls",
                    )?);
                }
                "--max-completion-tokens" => {
                    index += 1;
                    options.max_completion_tokens = Some(parse_usize(
                        value(args, index, "--max-completion-tokens")?,
                        "--max-completion-tokens",
                    )?);
                }
                "--repeat" | "--repeats" => {
                    index += 1;
                    options.repeats =
                        Some(parse_usize(value(args, index, "--repeats")?, "--repeats")?);
                }
                "--run-id" => {
                    index += 1;
                    options.run_id = Some(value(args, index, "--run-id")?.to_owned());
                }
                "--nokv-posix-root" => {
                    index += 1;
                    options.nokv_posix_root =
                        Some(PathBuf::from(value(args, index, "--nokv-posix-root")?));
                }
                "--reset" => options.reset = true,
                "--s3-bucket" => {
                    index += 1;
                    options.s3.bucket = value(args, index, "--s3-bucket")?.to_owned();
                }
                "--s3-endpoint" => {
                    index += 1;
                    options.s3.endpoint = value(args, index, "--s3-endpoint")?.to_owned();
                }
                "--s3-access-key-id" => {
                    index += 1;
                    options.s3.access_key_id = value(args, index, "--s3-access-key-id")?.to_owned();
                }
                "--s3-secret-access-key" => {
                    index += 1;
                    options.s3.secret_access_key =
                        value(args, index, "--s3-secret-access-key")?.to_owned();
                }
                other => return Err(HarnessError::UnknownOption(other.to_owned())),
            }
            index += 1;
        }
        Ok(options)
    }
}

fn prepare(options: Options) -> Result<(), HarnessError> {
    let archive = options
        .archive
        .as_ref()
        .ok_or(HarnessError::MissingOption("--archive"))?;
    let data_root = options
        .data_root
        .as_ref()
        .ok_or(HarnessError::MissingOption("--data-root"))?;
    let corpus_root = data_root.join("corpus");
    let sqlite_path = sqlite_path(data_root);
    let nokv_meta_path = nokv_meta_path(data_root);

    if options.reset {
        remove_if_exists(&corpus_root)?;
        remove_if_exists(&sqlite_path)?;
        remove_if_exists(&nokv_meta_path)?;
    }
    fs::create_dir_all(data_root).map_err(from_io)?;
    if !corpus_root.exists() {
        extract_archive(archive, &corpus_root)?;
    }

    let runs = load_runs(&corpus_root)?;
    let indexes = derive_indexes(&runs)?;
    prepare_sqlite(&sqlite_path, &runs, &indexes)?;
    prepare_nokv(&nokv_meta_path, &options.s3, &runs, &indexes)?;

    let manifest = PrepareManifest {
        archive_sha256: file_sha256(archive)?,
        run_count: runs.len(),
        metadata_files: runs.len(),
        metrics_files: runs
            .iter()
            .filter(|run| run.metrics_bytes.is_some())
            .count(),
        params_files: runs.iter().filter(|run| run.params_bytes.is_some()).count(),
        dependencies_files: runs
            .iter()
            .filter(|run| run.dependencies_bytes.is_some())
            .count(),
        artifact_files: runs.iter().map(|run| run.artifacts.len()).sum(),
        missing_artifact_refs: runs.iter().map(missing_artifact_ref_count).sum(),
        sqlite_db: sqlite_path.display().to_string(),
        nokv_meta: nokv_meta_path.display().to_string(),
        status_counts: indexes.status_counts,
        tag_counts: indexes.tag_counts,
        script_counts: indexes.script_counts,
    };
    fs::write(
        data_root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("manifest serializes"),
    )
    .map_err(from_io)?;
    println!(
        "prepared run_count={} sqlite={} nokv_meta={}",
        manifest.run_count, manifest.sqlite_db, manifest.nokv_meta
    );
    Ok(())
}

fn verify(options: Options) -> Result<VerificationReport, HarnessError> {
    let data_root = options
        .data_root
        .as_ref()
        .ok_or(HarnessError::MissingOption("--data-root"))?;
    let db = Connection::open_with_flags(
        sqlite_path(data_root),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(from_sql)?;
    let run_id_sample = match options.run_id {
        Some(run_id) => run_id,
        None => first_experiment_id(&db)?,
    };
    let sqlite = verify_sqlite_materialization(&db)?;
    let service = open_existing_nokv(&nokv_meta_path(data_root), &options.s3)?;
    let nokv_native = verify_nokv_namespace(&db, &service)?;
    let nokv_posix = if let Some(root) = options.nokv_posix_root.as_ref() {
        Some(verify_posix_namespace(&db, root)?)
    } else {
        None
    };
    let all_available_reads_match = sqlite.mismatches.is_empty()
        && nokv_native.mismatches.is_empty()
        && nokv_posix
            .as_ref()
            .map(|report| report.mismatches.is_empty())
            .unwrap_or(true);
    Ok(VerificationReport {
        run_id_sample,
        sqlite,
        nokv_native,
        nokv_posix,
        all_available_reads_match,
    })
}

fn show_schema(options: Options) -> Result<(), HarnessError> {
    let db = options.db.ok_or(HarnessError::MissingOption("--db"))?;
    let conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(from_sql)?;
    let mut stmt = conn
        .prepare(
            "SELECT sql FROM sqlite_schema WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY name",
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(from_sql)?;
    for row in rows {
        println!("{}", row.map_err(from_sql)?);
    }
    Ok(())
}

fn query_sql(options: Options) -> Result<(), HarnessError> {
    let db = options.db.ok_or(HarnessError::MissingOption("--db"))?;
    let sql = options.sql.ok_or(HarnessError::MissingOption("--sql"))?;
    let conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(from_sql)?;
    let result = execute_sqlite_query_tool(&conn, &sql)?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
    Ok(())
}

fn execute_sqlite_query_tool(
    conn: &Connection,
    sql: &str,
) -> Result<SqliteQueryToolResult, HarnessError> {
    execute_sqlite_query_tool_with_timeout(conn, sql, Duration::from_millis(TOOL_CALL_TIMEOUT_MS))
}

fn execute_sqlite_query_tool_with_timeout(
    conn: &Connection,
    sql: &str,
    timeout: Duration,
) -> Result<SqliteQueryToolResult, HarnessError> {
    let trimmed = sql.trim_start().to_ascii_lowercase();
    if !(trimmed.starts_with("select")
        || trimmed.starts_with("with")
        || trimmed.starts_with("explain"))
    {
        return Err(HarnessError::Sql(
            "sqlite-query only allows SELECT, WITH, and EXPLAIN statements".to_owned(),
        ));
    }
    let timed_out = Arc::new(AtomicBool::new(false));
    let timed_out_for_handler = Arc::clone(&timed_out);
    let started = Instant::now();
    conn.progress_handler(
        SQLITE_PROGRESS_OPS,
        Some(move || {
            let should_interrupt = started.elapsed() >= timeout;
            if should_interrupt {
                timed_out_for_handler.store(true, Ordering::Relaxed);
            }
            should_interrupt
        }),
    );
    let _progress_guard = SqliteProgressGuard { conn };

    let mut stmt = conn
        .prepare(sql)
        .map_err(|err| sqlite_timeout_or_error(err, &timed_out, timeout))?;
    let names = stmt
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let rows = stmt
        .query_map([], |row| {
            let mut out = serde_json::Map::new();
            for (index, name) in names.iter().enumerate() {
                let value = sqlite_value(row, index)?;
                out.insert(name.clone(), value);
            }
            Ok(Value::Object(out))
        })
        .map_err(|err| sqlite_timeout_or_error(err, &timed_out, timeout))?;
    let mut tool_rows = Vec::new();
    let row_limit = 1000;
    let mut truncated = false;
    for row in rows.take(row_limit + 1) {
        let row = row.map_err(|err| sqlite_timeout_or_error(err, &timed_out, timeout))?;
        if tool_rows.len() == row_limit {
            truncated = true;
            break;
        }
        tool_rows.push(SqliteQueryToolRow {
            evidence: evidence_refs_for_row(&row),
            row,
        });
    }
    Ok(SqliteQueryToolResult {
        tool: "query_sql",
        row_limit,
        truncated,
        rows: tool_rows,
    })
}

struct SqliteProgressGuard<'conn> {
    conn: &'conn Connection,
}

impl Drop for SqliteProgressGuard<'_> {
    fn drop(&mut self) {
        self.conn.progress_handler(0, None::<fn() -> bool>);
    }
}

fn sqlite_timeout_or_error(
    err: impl Error,
    timed_out: &AtomicBool,
    timeout: Duration,
) -> HarnessError {
    if timed_out.load(Ordering::Relaxed) {
        HarnessError::ToolTimeout {
            tool: "query_sql".to_owned(),
            limit_ms: duration_millis(timeout),
        }
    } else {
        from_sql(err)
    }
}

fn read_blob(options: Options) -> Result<(), HarnessError> {
    let db = options.db.ok_or(HarnessError::MissingOption("--db"))?;
    let blob_ref = options
        .blob_ref
        .ok_or(HarnessError::MissingOption("--blob-ref"))?;
    let offset = options.offset.unwrap_or(0);
    let limit = options
        .limit
        .ok_or(HarnessError::MissingOption("--limit"))?;
    let conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(from_sql)?;
    let result = read_blob_tool(&conn, &blob_ref, offset, limit)?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
    Ok(())
}

fn read_blob_tool(
    conn: &Connection,
    blob_ref: &str,
    offset: u64,
    limit: usize,
) -> Result<SqliteReadBlobToolResult, HarnessError> {
    let (size_bytes, digest, content): (i64, String, Vec<u8>) = conn
        .query_row(
            "SELECT size_bytes, digest, content FROM blobs WHERE blob_ref = ?1",
            params![blob_ref],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(from_sql)?;
    let total_size_bytes = usize::try_from(size_bytes).map_err(|_| {
        HarnessError::Sql(format!(
            "blob {blob_ref} has negative or oversized size {size_bytes}"
        ))
    })?;
    let start = usize::try_from(offset).map_err(|_| HarnessError::InvalidNumber {
        option: "--offset",
        value: offset.to_string(),
    })?;
    let end = start.saturating_add(limit).min(content.len());
    let range = if start >= content.len() {
        Vec::new()
    } else {
        content[start..end].to_vec()
    };
    Ok(SqliteReadBlobToolResult {
        tool: "read_blob",
        blob_ref: blob_ref.to_owned(),
        evidence: format!("sqlite://blobs/{blob_ref}"),
        total_size_bytes,
        digest,
        offset,
        requested_limit: limit,
        bytes_read: range.len(),
        content_utf8: std::str::from_utf8(&range).ok().map(str::to_owned),
        content_hex: bytes_hex(&range),
    })
}

fn agentfs_ls(options: Options) -> Result<(), HarnessError> {
    let conn = open_readonly_db_option(&options)?;
    let path = required_path(&options)?;
    print_json(&agentfs_list_tool(&conn, &path)?)
}

fn agentfs_stat(options: Options) -> Result<(), HarnessError> {
    let conn = open_readonly_db_option(&options)?;
    let path = required_path(&options)?;
    print_json(&agentfs_stat_tool(&conn, &path)?)
}

fn agentfs_read(options: Options) -> Result<(), HarnessError> {
    let conn = open_readonly_db_option(&options)?;
    let path = required_path(&options)?;
    let offset = options.offset.unwrap_or(0);
    let limit = options
        .limit
        .ok_or(HarnessError::MissingOption("--limit"))?;
    print_json(&agentfs_read_tool(&conn, &path, offset, limit)?)
}

fn agentfs_grep(options: Options) -> Result<(), HarnessError> {
    let conn = open_readonly_db_option(&options)?;
    let path = required_path(&options)?;
    let pattern = required_pattern(&options)?;
    print_json(&agentfs_grep_tool(
        &conn,
        &path,
        &pattern,
        options.recursive,
    )?)
}

fn agentfs_find(options: Options) -> Result<(), HarnessError> {
    let conn = open_readonly_db_option(&options)?;
    let path = required_path(&options)?;
    let pattern = required_pattern(&options)?;
    print_json(&agentfs_find_tool(&conn, &path, &pattern)?)
}

fn posix_ls(options: Options) -> Result<(), HarnessError> {
    let root = required_posix_root(&options)?;
    let path = required_path(&options)?;
    print_json(&posix_list_tool(&root, &path)?)
}

fn posix_stat(options: Options) -> Result<(), HarnessError> {
    let root = required_posix_root(&options)?;
    let path = required_path(&options)?;
    print_json(&posix_stat_tool(&root, &path)?)
}

fn posix_read(options: Options) -> Result<(), HarnessError> {
    let root = required_posix_root(&options)?;
    let path = required_path(&options)?;
    let offset = options.offset.unwrap_or(0);
    let limit = options
        .limit
        .ok_or(HarnessError::MissingOption("--limit"))?;
    print_json(&posix_read_tool(&root, &path, offset, limit)?)
}

fn posix_grep(options: Options) -> Result<(), HarnessError> {
    let root = required_posix_root(&options)?;
    let path = required_path(&options)?;
    let pattern = required_pattern(&options)?;
    print_json(&posix_grep_tool(&root, &path, &pattern, options.recursive)?)
}

fn posix_find(options: Options) -> Result<(), HarnessError> {
    let root = required_posix_root(&options)?;
    let path = required_path(&options)?;
    let pattern = required_pattern(&options)?;
    print_json(&posix_find_tool(&root, &path, &pattern)?)
}

fn nokv_list(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let service = open_existing_nokv(&nokv_meta_path(&data_root), &options.s3)?;
    let path = required_path(&options)?;
    print_json(&nokv_list_tool(&service, &path)?)
}

fn nokv_stat(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let service = open_existing_nokv(&nokv_meta_path(&data_root), &options.s3)?;
    let path = required_path(&options)?;
    print_json(&nokv_stat_tool(&service, &path)?)
}

fn nokv_read(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let service = open_existing_nokv(&nokv_meta_path(&data_root), &options.s3)?;
    let path = required_path(&options)?;
    let offset = options.offset.unwrap_or(0);
    let limit = options
        .limit
        .ok_or(HarnessError::MissingOption("--limit"))?;
    print_json(&nokv_read_tool(
        &service,
        &path,
        offset,
        limit,
        options.expected_generation,
    )?)
}

fn agentfs_list_tool(conn: &Connection, path: &str) -> Result<ListToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let rows = agentfs_file_rows(conn)?;
    if !rows.contains_key(&path) {
        return Err(HarnessError::Corpus(format!(
            "{path}: AgentFS path not found"
        )));
    }
    let mut entries = Vec::new();
    let row_limit = 1000;
    let mut truncated = false;
    for row in rows.values() {
        if let Some(name) = immediate_child_name(&path, &row.path) {
            if entries.len() == row_limit {
                truncated = true;
                break;
            }
            entries.push(agentfs_entry(row, name));
        }
    }
    Ok(ListToolResult {
        tool: "ls",
        evidence: format!("agentfs://{path}"),
        path,
        entries,
        row_limit,
        truncated,
    })
}

fn agentfs_stat_tool(conn: &Connection, path: &str) -> Result<StatToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let row = agentfs_file_row(conn, &path)?;
    Ok(StatToolResult {
        tool: "stat",
        evidence: format!("agentfs://{path}"),
        path,
        file_type: row.file_type,
        size_bytes: row.size_bytes,
        digest: row.digest,
        inode: None,
        mode: None,
        uid: None,
        gid: None,
        modified_ms: None,
        generation: None,
        body: None,
    })
}

fn agentfs_read_tool(
    conn: &Connection,
    path: &str,
    offset: u64,
    limit: usize,
) -> Result<ReadToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let row = agentfs_file_row(conn, &path)?;
    if row.file_type != "file" {
        return Err(HarnessError::Corpus(format!(
            "{path}: AgentFS path is not a file"
        )));
    }
    let content = row
        .content
        .ok_or_else(|| HarnessError::Corpus(format!("{path}: AgentFS file has no content")))?;
    let range = bytes_range(&content, offset, limit)?;
    Ok(ReadToolResult {
        tool: "read",
        evidence: format!("agentfs://{path}"),
        path,
        total_size_bytes: content.len() as u64,
        digest: row.digest,
        offset,
        requested_limit: limit,
        bytes_read: range.len(),
        content_utf8: std::str::from_utf8(&range).ok().map(str::to_owned),
        content_hex: bytes_hex(&range),
        generation: None,
        body: None,
        generation_mismatch: None,
    })
}

fn agentfs_grep_tool(
    conn: &Connection,
    path: &str,
    pattern: &str,
    recursive: bool,
) -> Result<GrepToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let rows = agentfs_file_rows(conn)?;
    if !rows.contains_key(&path) {
        return Err(HarnessError::Corpus(format!(
            "{path}: AgentFS path not found"
        )));
    }
    let files = agentfs_candidate_files(&rows, &path, recursive);
    let (matches, files_scanned, bytes_read, truncated) = grep_byte_files(
        files
            .into_iter()
            .map(|row| (row.path.as_str(), row.content.as_deref())),
        pattern,
        "agentfs://",
    );
    Ok(GrepToolResult {
        tool: "grep",
        path,
        pattern: pattern.to_owned(),
        recursive,
        matches,
        files_scanned,
        bytes_read,
        row_limit: 100,
        truncated,
    })
}

fn agentfs_find_tool(
    conn: &Connection,
    path: &str,
    pattern: &str,
) -> Result<FindToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let rows = agentfs_file_rows(conn)?;
    if !rows.contains_key(&path) {
        return Err(HarnessError::Corpus(format!(
            "{path}: AgentFS path not found"
        )));
    }
    let row_limit = 100;
    let mut matches = Vec::new();
    let mut truncated = false;
    for row in rows.values() {
        if row.path == path || !is_under_path(&path, &row.path) {
            continue;
        }
        let name = path_name(&row.path);
        if wildcard_match(pattern, name) || wildcard_match(pattern, &row.path) {
            if matches.len() == row_limit {
                truncated = true;
                break;
            }
            matches.push(agentfs_entry(row, name));
        }
    }
    Ok(FindToolResult {
        tool: "find",
        path,
        pattern: pattern.to_owned(),
        matches,
        row_limit,
        truncated,
    })
}

fn posix_list_tool(root: &Path, path: &str) -> Result<ListToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let local = posix_local_path(root, &path);
    let mut entries = Vec::new();
    let row_limit = 1000;
    let mut truncated = false;
    for entry in fs::read_dir(&local).map_err(from_io)? {
        if entries.len() == row_limit {
            truncated = true;
            break;
        }
        let entry = entry.map_err(from_io)?;
        let name = entry.file_name().to_string_lossy().to_string();
        let child_path = join_absolute_path(&path, &name);
        entries.push(posix_entry(
            &child_path,
            &entry.metadata().map_err(from_io)?,
        ));
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(ListToolResult {
        tool: "ls",
        evidence: format!("nokv-fuse://{path}"),
        path,
        entries,
        row_limit,
        truncated,
    })
}

fn posix_stat_tool(root: &Path, path: &str) -> Result<StatToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let metadata = fs::metadata(posix_local_path(root, &path)).map_err(from_io)?;
    let entry = posix_entry(&path, &metadata);
    Ok(StatToolResult {
        tool: "stat",
        path: entry.path,
        evidence: entry.evidence,
        file_type: entry.file_type,
        size_bytes: entry.size_bytes,
        digest: None,
        inode: entry.inode,
        mode: entry.mode,
        uid: entry.uid,
        gid: entry.gid,
        modified_ms: entry.modified_ms,
        generation: None,
        body: None,
    })
}

fn posix_read_tool(
    root: &Path,
    path: &str,
    offset: u64,
    limit: usize,
) -> Result<ReadToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let local = posix_local_path(root, &path);
    let content = fs::read(&local).map_err(from_io)?;
    let range = bytes_range(&content, offset, limit)?;
    Ok(ReadToolResult {
        tool: "read",
        evidence: format!("nokv-fuse://{path}"),
        path,
        total_size_bytes: content.len() as u64,
        digest: None,
        offset,
        requested_limit: limit,
        bytes_read: range.len(),
        content_utf8: std::str::from_utf8(&range).ok().map(str::to_owned),
        content_hex: bytes_hex(&range),
        generation: None,
        body: None,
        generation_mismatch: None,
    })
}

fn posix_grep_tool(
    root: &Path,
    path: &str,
    pattern: &str,
    recursive: bool,
) -> Result<GrepToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let local = posix_local_path(root, &path);
    let mut files = Vec::new();
    if local.is_file() {
        files.push(path.clone());
    } else if recursive {
        for entry in WalkDir::new(&local).into_iter() {
            let entry = entry.map_err(|err| HarnessError::Io(err.to_string()))?;
            if entry.file_type().is_file() {
                files.push(posix_namespace_path(root, entry.path())?);
            }
        }
    } else {
        for entry in fs::read_dir(&local).map_err(from_io)? {
            let entry = entry.map_err(from_io)?;
            if entry.metadata().map_err(from_io)?.is_file() {
                files.push(join_absolute_path(
                    &path,
                    &entry.file_name().to_string_lossy(),
                ));
            }
        }
    }
    files.sort();
    let file_bytes = files
        .iter()
        .map(|namespace_path| {
            let bytes = fs::read(posix_local_path(root, namespace_path)).ok();
            (namespace_path.as_str(), bytes)
        })
        .collect::<Vec<_>>();
    let (matches, files_scanned, bytes_read, truncated) = grep_byte_files(
        file_bytes
            .iter()
            .map(|(path, bytes)| (*path, bytes.as_deref())),
        pattern,
        "nokv-fuse://",
    );
    Ok(GrepToolResult {
        tool: "grep",
        path,
        pattern: pattern.to_owned(),
        recursive,
        matches,
        files_scanned,
        bytes_read,
        row_limit: 100,
        truncated,
    })
}

fn posix_find_tool(root: &Path, path: &str, pattern: &str) -> Result<FindToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let local = posix_local_path(root, &path);
    let row_limit = 100;
    let mut matches = Vec::new();
    let mut truncated = false;
    for entry in WalkDir::new(&local).into_iter() {
        let entry = entry.map_err(|err| HarnessError::Io(err.to_string()))?;
        let namespace_path = posix_namespace_path(root, entry.path())?;
        if namespace_path == path {
            continue;
        }
        let name = path_name(&namespace_path);
        if wildcard_match(pattern, name) || wildcard_match(pattern, &namespace_path) {
            if matches.len() == row_limit {
                truncated = true;
                break;
            }
            matches.push(posix_entry(
                &namespace_path,
                &entry.metadata().map_err(from_io)?,
            ));
        }
    }
    matches.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(FindToolResult {
        tool: "find",
        path,
        pattern: pattern.to_owned(),
        matches,
        row_limit,
        truncated,
    })
}

fn nokv_list_tool(
    service: &NoKvFs<HoltMetadataStore, S3ObjectStore>,
    path: &str,
) -> Result<ListToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let mut entries = service.read_dir_plus_path(&path).map_err(from_nokv)?;
    entries.sort_by(|left, right| {
        left.dentry
            .name
            .as_bytes()
            .cmp(right.dentry.name.as_bytes())
    });
    let row_limit = 1000;
    let truncated = entries.len() > row_limit;
    let entries = entries
        .into_iter()
        .take(row_limit)
        .map(|entry| {
            let name = String::from_utf8_lossy(entry.dentry.name.as_bytes()).to_string();
            let child_path = join_absolute_path(&path, &name);
            nokv_entry(&child_path, &entry.attr, entry.body.as_ref())
        })
        .collect();
    Ok(ListToolResult {
        tool: "list",
        evidence: format!("nokv-native://{path}"),
        path,
        entries,
        row_limit,
        truncated,
    })
}

fn nokv_stat_tool(
    service: &NoKvFs<HoltMetadataStore, S3ObjectStore>,
    path: &str,
) -> Result<StatToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let metadata = service
        .stat_path(&path)
        .map_err(from_nokv)?
        .ok_or_else(|| HarnessError::NoKv(format!("{path}: path not found")))?;
    Ok(nokv_stat_result(
        &path,
        &metadata.attr,
        metadata.body.as_ref(),
    ))
}

fn nokv_read_tool(
    service: &NoKvFs<HoltMetadataStore, S3ObjectStore>,
    path: &str,
    offset: u64,
    limit: usize,
    expected_generation: Option<u64>,
) -> Result<ReadToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let metadata = service
        .stat_path(&path)
        .map_err(from_nokv)?
        .ok_or_else(|| HarnessError::NoKv(format!("{path}: path not found")))?;
    if metadata.attr.file_type != FileType::File {
        return Err(HarnessError::NoKv(format!("{path}: path is not a file")));
    }
    if let Some(expected) = expected_generation {
        if expected != metadata.attr.generation {
            return Ok(ReadToolResult {
                tool: "read",
                evidence: format!(
                    "nokv-native://{path}@generation:{}",
                    metadata.attr.generation
                ),
                path,
                total_size_bytes: metadata.attr.size,
                digest: metadata.body.as_ref().map(|body| body.digest_uri.clone()),
                offset,
                requested_limit: limit,
                bytes_read: 0,
                content_utf8: None,
                content_hex: String::new(),
                generation: Some(metadata.attr.generation),
                body: metadata.body.as_ref().map(body_summary),
                generation_mismatch: Some(GenerationMismatch {
                    expected,
                    current: metadata.attr.generation,
                }),
            });
        }
    }
    let range = service
        .read_file(metadata.attr.inode, offset, limit)
        .map_err(from_nokv)?;
    Ok(ReadToolResult {
        tool: "read",
        evidence: format!(
            "nokv-native://{path}@generation:{}",
            metadata.attr.generation
        ),
        path,
        total_size_bytes: metadata.attr.size,
        digest: metadata.body.as_ref().map(|body| body.digest_uri.clone()),
        offset,
        requested_limit: limit,
        bytes_read: range.len(),
        content_utf8: std::str::from_utf8(&range).ok().map(str::to_owned),
        content_hex: bytes_hex(&range),
        generation: Some(metadata.attr.generation),
        body: metadata.body.as_ref().map(body_summary),
        generation_mismatch: None,
    })
}

#[derive(Clone, Debug)]
struct AgentFsRow {
    path: String,
    file_type: String,
    size_bytes: Option<u64>,
    digest: Option<String>,
    content: Option<Vec<u8>>,
}

fn open_readonly_db_option(options: &Options) -> Result<Connection, HarnessError> {
    let db = if let Some(db) = options.db.as_ref() {
        db.clone()
    } else {
        sqlite_path(&required_data_root(options)?)
    };
    Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(from_sql)
}

fn required_data_root(options: &Options) -> Result<PathBuf, HarnessError> {
    options
        .data_root
        .clone()
        .ok_or(HarnessError::MissingOption("--data-root"))
}

fn required_path(options: &Options) -> Result<String, HarnessError> {
    options
        .path
        .clone()
        .ok_or(HarnessError::MissingOption("--path"))
}

fn required_pattern(options: &Options) -> Result<String, HarnessError> {
    options
        .pattern
        .clone()
        .ok_or(HarnessError::MissingOption("--pattern"))
}

fn required_posix_root(options: &Options) -> Result<PathBuf, HarnessError> {
    options
        .nokv_posix_root
        .clone()
        .ok_or(HarnessError::MissingOption("--nokv-posix-root"))
}

fn print_json(value: &impl Serialize) -> Result<(), HarnessError> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).expect("tool result serializes")
    );
    Ok(())
}

fn agentfs_file_rows(conn: &Connection) -> Result<BTreeMap<String, AgentFsRow>, HarnessError> {
    let mut stmt = conn
        .prepare("SELECT path, file_type, size_bytes, digest, content FROM files ORDER BY path")
        .map_err(from_sql)?;
    let rows = stmt
        .query_map([], |row| {
            let path: String = row.get(0)?;
            let size: Option<i64> = row.get(2)?;
            let size_bytes = size.and_then(|value| u64::try_from(value).ok());
            Ok(AgentFsRow {
                path,
                file_type: row.get(1)?,
                size_bytes,
                digest: row.get(3)?,
                content: row.get(4)?,
            })
        })
        .map_err(from_sql)?;
    let mut out = BTreeMap::new();
    for row in rows {
        let row = row.map_err(from_sql)?;
        out.insert(row.path.clone(), row);
    }
    Ok(out)
}

fn agentfs_file_row(conn: &Connection, path: &str) -> Result<AgentFsRow, HarnessError> {
    let size_to_u64 = |value: Option<i64>| value.and_then(|value| u64::try_from(value).ok());
    conn.query_row(
        "SELECT path, file_type, size_bytes, digest, content FROM files WHERE path = ?1",
        params![path],
        |row| {
            Ok(AgentFsRow {
                path: row.get(0)?,
                file_type: row.get(1)?,
                size_bytes: size_to_u64(row.get(2)?),
                digest: row.get(3)?,
                content: row.get(4)?,
            })
        },
    )
    .optional()
    .map_err(from_sql)?
    .ok_or_else(|| HarnessError::Corpus(format!("{path}: AgentFS path not found")))
}

fn agentfs_entry(row: &AgentFsRow, name: &str) -> FileEntry {
    FileEntry {
        name: name.to_owned(),
        path: row.path.clone(),
        file_type: row.file_type.clone(),
        size_bytes: row.size_bytes,
        digest: row.digest.clone(),
        evidence: format!("agentfs://{}", row.path),
        inode: None,
        mode: None,
        uid: None,
        gid: None,
        modified_ms: None,
        generation: None,
        body: None,
    }
}

fn agentfs_candidate_files<'a>(
    rows: &'a BTreeMap<String, AgentFsRow>,
    path: &str,
    recursive: bool,
) -> Vec<&'a AgentFsRow> {
    let mut files = Vec::new();
    if let Some(row) = rows.get(path) {
        if row.file_type == "file" {
            files.push(row);
            return files;
        }
    }
    for row in rows.values() {
        if row.file_type != "file" {
            continue;
        }
        if recursive {
            if is_under_path(path, &row.path) {
                files.push(row);
            }
        } else if immediate_child_name(path, &row.path).is_some() {
            files.push(row);
        }
    }
    files
}

fn grep_byte_files<'a>(
    files: impl Iterator<Item = (&'a str, Option<&'a [u8]>)>,
    pattern: &str,
    evidence_prefix: &str,
) -> (Vec<GrepMatch>, usize, usize, bool) {
    let row_limit = 100;
    let mut matches = Vec::new();
    let mut files_scanned = 0;
    let mut bytes_read = 0;
    let mut truncated = false;
    for (path, content) in files {
        let Some(content) = content else {
            continue;
        };
        if content.contains(&0) {
            continue;
        }
        files_scanned += 1;
        bytes_read += content.len();
        let text = String::from_utf8_lossy(content);
        for (index, line) in text.lines().enumerate() {
            if line.contains(pattern) {
                if matches.len() == row_limit {
                    truncated = true;
                    return (matches, files_scanned, bytes_read, truncated);
                }
                matches.push(GrepMatch {
                    path: path.to_owned(),
                    line_number: index + 1,
                    snippet: line.chars().take(240).collect(),
                    evidence: format!("{evidence_prefix}{path}#L{}", index + 1),
                });
            }
        }
    }
    (matches, files_scanned, bytes_read, truncated)
}

fn normalize_absolute_path(path: &str) -> String {
    let mut out = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    while out.len() > 1 && out.ends_with('/') {
        out.pop();
    }
    out
}

fn immediate_child_name<'a>(parent: &str, child: &'a str) -> Option<&'a str> {
    if child == parent {
        return None;
    }
    let rest = if parent == "/" {
        child.strip_prefix('/')?
    } else {
        child.strip_prefix(&format!("{parent}/"))?
    };
    if rest.is_empty() || rest.contains('/') {
        None
    } else {
        Some(rest)
    }
}

fn is_under_path(parent: &str, child: &str) -> bool {
    if parent == "/" {
        child != "/"
    } else {
        child.starts_with(&format!("{parent}/"))
    }
}

fn path_name(path: &str) -> &str {
    path.rsplit('/')
        .find(|part| !part.is_empty())
        .unwrap_or("/")
}

fn join_absolute_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return value == pattern || value.contains(pattern);
    }
    let mut rest = value;
    let starts_with_wildcard = pattern.starts_with('*');
    let ends_with_wildcard = pattern.ends_with('*');
    let parts = pattern
        .split('*')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return true;
    }
    if !starts_with_wildcard {
        let Some(first) = parts.first() else {
            return true;
        };
        if !rest.starts_with(first) {
            return false;
        }
        rest = &rest[first.len()..];
    }
    for part in parts.iter().skip(if starts_with_wildcard { 0 } else { 1 }) {
        let Some(index) = rest.find(part) else {
            return false;
        };
        rest = &rest[index + part.len()..];
    }
    ends_with_wildcard
        || parts
            .last()
            .map(|last| value.ends_with(last))
            .unwrap_or(true)
}

fn bytes_range(content: &[u8], offset: u64, limit: usize) -> Result<Vec<u8>, HarnessError> {
    let start = usize::try_from(offset).map_err(|_| HarnessError::InvalidNumber {
        option: "--offset",
        value: offset.to_string(),
    })?;
    if start >= content.len() {
        return Ok(Vec::new());
    }
    let end = start.saturating_add(limit).min(content.len());
    Ok(content[start..end].to_vec())
}

fn posix_local_path(root: &Path, path: &str) -> PathBuf {
    root.join(path.trim_start_matches('/'))
}

fn posix_namespace_path(root: &Path, path: &Path) -> Result<String, HarnessError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|err| HarnessError::Io(err.to_string()))?;
    Ok(format!(
        "/{}",
        relative.to_string_lossy().replace('\\', "/")
    ))
}

fn posix_entry(path: &str, metadata: &fs::Metadata) -> FileEntry {
    FileEntry {
        name: path_name(path).to_owned(),
        path: path.to_owned(),
        file_type: if metadata.is_dir() {
            "directory".to_owned()
        } else if metadata.is_file() {
            "file".to_owned()
        } else {
            "other".to_owned()
        },
        size_bytes: metadata.is_file().then_some(metadata.len()),
        digest: None,
        evidence: format!("nokv-fuse://{path}"),
        inode: posix_inode(metadata),
        mode: posix_mode(metadata),
        uid: posix_uid(metadata),
        gid: posix_gid(metadata),
        modified_ms: posix_modified_ms(metadata),
        generation: None,
        body: None,
    }
}

#[cfg(unix)]
fn posix_inode(metadata: &fs::Metadata) -> Option<u64> {
    Some(metadata.ino())
}

#[cfg(not(unix))]
fn posix_inode(_metadata: &fs::Metadata) -> Option<u64> {
    None
}

#[cfg(unix)]
fn posix_mode(metadata: &fs::Metadata) -> Option<u32> {
    Some(metadata.mode())
}

#[cfg(not(unix))]
fn posix_mode(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

#[cfg(unix)]
fn posix_uid(metadata: &fs::Metadata) -> Option<u32> {
    Some(metadata.uid())
}

#[cfg(not(unix))]
fn posix_uid(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

#[cfg(unix)]
fn posix_gid(metadata: &fs::Metadata) -> Option<u32> {
    Some(metadata.gid())
}

#[cfg(not(unix))]
fn posix_gid(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

fn posix_modified_ms(metadata: &fs::Metadata) -> Option<i64> {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
}

fn nokv_entry(path: &str, attr: &InodeAttr, body: Option<&BodyDescriptor>) -> FileEntry {
    FileEntry {
        name: path_name(path).to_owned(),
        path: path.to_owned(),
        file_type: file_type_name(attr.file_type).to_owned(),
        size_bytes: (attr.file_type == FileType::File).then_some(attr.size),
        digest: body.map(|body| body.digest_uri.clone()),
        evidence: format!("nokv-native://{path}@generation:{}", attr.generation),
        inode: Some(attr.inode.get()),
        mode: Some(attr.mode),
        uid: Some(attr.uid),
        gid: Some(attr.gid),
        modified_ms: i64::try_from(attr.mtime_ms).ok(),
        generation: Some(attr.generation),
        body: body.map(body_summary),
    }
}

fn nokv_stat_result(path: &str, attr: &InodeAttr, body: Option<&BodyDescriptor>) -> StatToolResult {
    StatToolResult {
        tool: "stat",
        path: path.to_owned(),
        evidence: format!("nokv-native://{path}@generation:{}", attr.generation),
        file_type: file_type_name(attr.file_type).to_owned(),
        size_bytes: (attr.file_type == FileType::File).then_some(attr.size),
        digest: body.map(|body| body.digest_uri.clone()),
        inode: Some(attr.inode.get()),
        mode: Some(attr.mode),
        uid: Some(attr.uid),
        gid: Some(attr.gid),
        modified_ms: i64::try_from(attr.mtime_ms).ok(),
        generation: Some(attr.generation),
        body: body.map(body_summary),
    }
}

fn body_summary(body: &BodyDescriptor) -> BodyDescriptorSummary {
    BodyDescriptorSummary {
        producer: body.producer.clone(),
        digest_uri: body.digest_uri.clone(),
        size: body.size,
        content_type: body.content_type.clone(),
        manifest_id: body.manifest_id.clone(),
        generation: body.generation,
        chunk_size: body.chunk_size,
        block_size: body.block_size,
    }
}

fn file_type_name(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "directory",
        FileType::Symlink => "symlink",
    }
}

fn print_tool_registry(options: Options) -> Result<(), HarnessError> {
    let arm = options.arm.ok_or(HarnessError::MissingOption("--arm"))?;
    print_json(&tool_registry_for_arm(&arm)?)
}

fn tool_registry_for_arm(arm: &str) -> Result<Vec<ToolDefinition>, HarnessError> {
    let names = match arm {
        "sqlite_raw_v1" => vec![
            (
                "show_schema",
                "Return the SQLite schema for the Yanex corpus.",
                json!({"type":"object","properties":{},"additionalProperties":false}),
            ),
            (
                "query_sql",
                "Execute a read-only SQL query over Yanex relational tables.",
                json!({"type":"object","required":["sql"],"properties":{"sql":{"type":"string"}},"additionalProperties":false}),
            ),
            (
                "read_blob",
                "Read a byte range from a blob_ref returned by SQL rows.",
                json!({"type":"object","required":["blob_ref","offset","limit"],"properties":{"blob_ref":{"type":"string"},"offset":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":65536}},"additionalProperties":false}),
            ),
        ],
        "sqlite_agentfs_v1" | "nokv_posix_v1" => vec![
            (
                "ls",
                "List directory entries.",
                json!({"type":"object","required":["path"],"properties":{"path":{"type":"string"}},"additionalProperties":false}),
            ),
            (
                "stat",
                "Return file or directory metadata without reading file content.",
                json!({"type":"object","required":["path"],"properties":{"path":{"type":"string"}},"additionalProperties":false}),
            ),
            (
                "read",
                "Read a file byte range.",
                json!({"type":"object","required":["path","offset","limit"],"properties":{"path":{"type":"string"},"offset":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":65536}},"additionalProperties":false}),
            ),
            (
                "grep",
                "Search text files by substring pattern.",
                json!({"type":"object","required":["path","pattern","recursive"],"properties":{"path":{"type":"string"},"pattern":{"type":"string"},"recursive":{"type":"boolean"}},"additionalProperties":false}),
            ),
            (
                "find",
                "Find paths by name or wildcard pattern.",
                json!({"type":"object","required":["path","pattern"],"properties":{"path":{"type":"string"},"pattern":{"type":"string"}},"additionalProperties":false}),
            ),
        ],
        "nokv_native_v1" => vec![
            (
                "list",
                "List NoKV namespace children with metadata.",
                json!({"type":"object","required":["path"],"properties":{"path":{"type":"string"}},"additionalProperties":false}),
            ),
            (
                "stat",
                "Return NoKV path metadata, generation, and body descriptor.",
                json!({"type":"object","required":["path"],"properties":{"path":{"type":"string"}},"additionalProperties":false}),
            ),
            (
                "read",
                "Read a NoKV file byte range with optional expected_generation check.",
                json!({"type":"object","required":["path","offset","limit"],"properties":{"path":{"type":"string"},"offset":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":65536},"expected_generation":{"type":["integer","null"],"minimum":1}},"additionalProperties":false}),
            ),
        ],
        other => {
            return Err(HarnessError::Corpus(format!(
                "unknown benchmark arm {other}"
            )))
        }
    };
    Ok(names
        .into_iter()
        .map(|(name, description, parameters)| ToolDefinition {
            name: name.to_owned(),
            description: description.to_owned(),
            parameters,
        })
        .collect())
}

fn phase1_task_set() -> Result<BenchmarkTaskSet, HarnessError> {
    serde_yaml::from_str(include_str!("../../tasks/phase1_readonly.yaml"))
        .map_err(|err| HarnessError::Yaml(err.to_string()))
}

fn default_repeats() -> usize {
    DEFAULT_REPEATS
}

fn benchmark_arm_ids() -> &'static [&'static str] {
    &[
        "sqlite_raw_v1",
        "nokv_native_v1",
        "sqlite_agentfs_v1",
        "nokv_posix_v1",
    ]
}

fn batch_plan(
    arm: Option<&str>,
    task_id: Option<&str>,
    repeats: Option<usize>,
    task_set: &BenchmarkTaskSet,
) -> Vec<BatchPlanItem> {
    let repeat_count = repeats.unwrap_or_else(default_repeats);
    let arms = arm.map(|arm| vec![arm.to_owned()]).unwrap_or_else(|| {
        benchmark_arm_ids()
            .iter()
            .map(|arm| (*arm).to_owned())
            .collect()
    });
    let tasks = task_set
        .tasks
        .iter()
        .filter(|task| task_id.map(|id| id == task.task_id).unwrap_or(true))
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    let mut plan = Vec::new();
    for arm_id in arms {
        for task_id in &tasks {
            for repeat_index in 0..repeat_count {
                plan.push(BatchPlanItem {
                    arm_id: arm_id.clone(),
                    task_id: task_id.clone(),
                    repeat_index,
                });
            }
        }
    }
    plan
}

fn list_tasks() -> Result<(), HarnessError> {
    print_json(&phase1_task_set()?)
}

fn show_task(options: Options) -> Result<(), HarnessError> {
    let task_id = options
        .task_id
        .ok_or(HarnessError::MissingOption("--task-id"))?;
    let task = find_task(&task_id)?;
    print_json(&task)
}

fn find_task(task_id: &str) -> Result<BenchmarkTask, HarnessError> {
    phase1_task_set()?
        .tasks
        .into_iter()
        .find(|task| task.task_id == task_id)
        .ok_or_else(|| HarnessError::Corpus(format!("unknown task_id {task_id}")))
}

fn print_gold(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let task_id = options
        .task_id
        .ok_or(HarnessError::MissingOption("--task-id"))?;
    let task = find_task(&task_id)?;
    let conn = Connection::open_with_flags(
        sqlite_path(&data_root),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(from_sql)?;
    let rows = query_gold_rows(&conn, &task)?;
    print_json(&json!({
        "task_id": task.task_id,
        "gold_rows": rows,
    }))
}

fn judge(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let task_id = options
        .task_id
        .ok_or(HarnessError::MissingOption("--task-id"))?;
    let arm = options.arm.ok_or(HarnessError::MissingOption("--arm"))?;
    let answer_json = options
        .answer_json
        .ok_or(HarnessError::MissingOption("--answer-json"))?;
    let task = find_task(&task_id)?;
    let conn = Connection::open_with_flags(
        sqlite_path(&data_root),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(from_sql)?;
    let answer = serde_json::from_slice(&fs::read(answer_json).map_err(from_io)?)
        .map_err(|err| HarnessError::Json(err.to_string()))?;
    print_json(&judge_answer(&conn, &task, &arm, &answer)?)
}

fn run_benchmark_task(options: Options) -> Result<(), HarnessError> {
    let arm = options
        .arm
        .as_deref()
        .ok_or(HarnessError::MissingOption("--arm"))?;
    let task_id = options
        .task_id
        .as_deref()
        .ok_or(HarnessError::MissingOption("--task-id"))?;
    let final_answer = run_benchmark_task_once(&options, arm, task_id, 0)?;
    print_json(&final_answer)
}

fn run_benchmark_batch(options: Options) -> Result<(), HarnessError> {
    let _ = required_data_root(&options)?;
    let _ = options
        .output_jsonl
        .as_ref()
        .ok_or(HarnessError::MissingOption("--output-jsonl"))?;
    let task_set = phase1_task_set()?;
    let plan = batch_plan(
        options.arm.as_deref(),
        options.task_id.as_deref(),
        options.repeats,
        &task_set,
    );
    if plan.is_empty() {
        return Err(HarnessError::Corpus(
            "batch plan is empty; check --arm, --task-id, and --repeats".to_owned(),
        ));
    }
    if plan.iter().any(|item| item.arm_id == "nokv_posix_v1") && options.nokv_posix_root.is_none() {
        return Err(HarnessError::MissingOption(
            "--nokv-posix-root for nokv_posix_v1",
        ));
    }
    for item in &plan {
        run_benchmark_task_once(&options, &item.arm_id, &item.task_id, item.repeat_index)?;
    }
    print_json(&json!({
        "status": "completed",
        "runs": plan.len(),
        "repeats": options.repeats.unwrap_or_else(default_repeats),
        "output_jsonl": options.output_jsonl.as_ref().map(|path| path.display().to_string()),
    }))
}

fn run_benchmark_task_once(
    options: &Options,
    arm: &str,
    task_id: &str,
    repeat_index: usize,
) -> Result<Value, HarnessError> {
    let data_root = required_data_root(options)?;
    let output_jsonl = options
        .output_jsonl
        .clone()
        .ok_or(HarnessError::MissingOption("--output-jsonl"))?;
    let task = find_task(task_id)?;
    let tools = tool_registry_for_arm(arm)?;
    let started = now_unix_ms();
    let run_id = format!("{}-{}-r{}-{started}", arm, task.task_id, repeat_index);
    let model = options
        .model
        .clone()
        .or_else(|| env::var("OPENAI_MODEL").ok())
        .ok_or(HarnessError::MissingOption("--model or OPENAI_MODEL"))?;
    let api_key =
        env::var("OPENAI_API_KEY").map_err(|_| HarnessError::MissingOption("OPENAI_API_KEY"))?;
    let endpoint = env::var("OPENAI_CHAT_COMPLETIONS_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1/chat/completions".to_owned());
    let max_turns = options.max_turns.unwrap_or(20);
    let max_tool_calls = options.max_tool_calls.unwrap_or(80);
    let max_completion_tokens = options.max_completion_tokens.unwrap_or(4096);
    let arm_card = arm_card_yaml(arm)?;
    let mut messages = benchmark_messages(arm_card, &task);
    let openai_tools = openai_tools(&tools);
    let client = reqwest::blocking::Client::new();
    let tool_worker = ToolWorker::start(
        arm,
        &data_root,
        options.nokv_posix_root.as_ref(),
        &options.s3,
        &tools,
        Duration::from_millis(TOOL_CALL_TIMEOUT_MS),
    );
    let mut api_calls = Vec::new();
    let mut tool_calls = Vec::new();
    let mut invalid_sql_count = 0;
    let mut wrong_tool_count = 0;
    let mut tool_timeout_count = 0;
    let mut tool_bytes_read = 0;
    let mut tool_result_tokens = 0;
    let mut final_answer = None;

    for _turn in 0..max_turns {
        let request = ChatCompletionRequest {
            model: model.clone(),
            messages: messages.clone(),
            tools: openai_tools.clone(),
            temperature: 0.0,
            max_completion_tokens,
            response_format: json!({"type": "json_object"}),
        };
        let api_started = now_unix_ms();
        let response = match send_chat_completion(&client, &endpoint, &api_key, &request) {
            Ok(response) => response,
            Err(err) if is_recoverable_run_api_error(&err) => {
                let error = err.to_string();
                let final_answer =
                    run_error_answer(&task, &error, tool_calls.len(), tool_bytes_read);
                let completed = now_unix_ms();
                let mut derived = DerivedMetrics {
                    interface_card_tokens: estimate_tokens(arm_card),
                    task_prompt_tokens: estimate_tokens(&task.prompt),
                    wall_time_ms: completed.saturating_sub(started),
                    task_success: Some(false),
                    evidence_precision: None,
                    tool_call_count: tool_calls.len(),
                    tool_result_tokens,
                    tool_bytes_read,
                    invalid_sql_count,
                    wrong_tool_count,
                    tool_timeout_count,
                    missing_evidence_count: final_answer
                        .get("missing_evidence")
                        .and_then(Value::as_array)
                        .map(Vec::len)
                        .unwrap_or_default(),
                    overread_bytes: 0,
                    ..DerivedMetrics::default()
                };
                apply_usage_costs(&mut derived, &api_calls, &model);
                let telemetry = BenchmarkRunTelemetry {
                    record_type: "benchmark_run".to_owned(),
                    run_id,
                    arm_id: arm.to_owned(),
                    task_id: task.task_id,
                    repeat_index,
                    model,
                    started_at_unix_ms: started,
                    completed_at_unix_ms: completed,
                    api_calls,
                    tool_calls,
                    derived_metrics: derived,
                    judge: None,
                    final_answer: Some(final_answer.clone()),
                    run_error: Some(error),
                };
                append_jsonl(&output_jsonl, &telemetry)?;
                return Ok(final_answer);
            }
            Err(err) => return Err(err),
        };
        let api_completed = now_unix_ms();
        api_calls.push(api_call_telemetry(
            &response,
            &model,
            api_started,
            api_completed,
        ));
        let Some(choice) = response.choices.into_iter().next() else {
            return Err(HarnessError::Http(
                "OpenAI response did not include a choice".to_owned(),
            ));
        };
        let message = choice.message;
        let assistant_message = message.clone();
        if let Some(calls) = message.tool_calls.clone() {
            if tool_calls.len() + calls.len() > max_tool_calls {
                return Err(HarnessError::Http(format!(
                    "tool call budget exceeded: max_tool_calls={max_tool_calls}"
                )));
            }
            messages.push(assistant_message);
            for call in calls {
                let tool_started = now_unix_ms();
                let arguments = tool_call_arguments(&call);
                append_tool_call_start_jsonl(
                    &output_jsonl,
                    &run_id,
                    arm,
                    &task.task_id,
                    repeat_index,
                    &call,
                    tool_started,
                )?;
                let outcome = tool_worker.execute_tool_call(&call);
                let tool_completed = now_unix_ms();
                invalid_sql_count += usize::from(outcome.invalid_sql);
                wrong_tool_count += usize::from(outcome.wrong_tool);
                tool_timeout_count += usize::from(outcome.timed_out);
                tool_bytes_read += outcome.bytes_read;
                tool_result_tokens += outcome.result_token_estimate;
                tool_calls.push(ToolCallTelemetry {
                    call_id: call.id.clone(),
                    tool_name: call.function.name.clone(),
                    arguments,
                    started_at_unix_ms: tool_started,
                    completed_at_unix_ms: tool_completed,
                    status: outcome.status.clone(),
                    bytes_read: outcome.bytes_read,
                    result_token_estimate: outcome.result_token_estimate,
                    error: outcome.error.clone(),
                    timed_out: outcome.timed_out,
                });
                if outcome.fatal_run_error {
                    let error = outcome.error.clone().unwrap_or_else(|| {
                        format!("tool {} failed with a fatal run error", call.function.name)
                    });
                    let final_answer =
                        run_error_answer(&task, &error, tool_calls.len(), tool_bytes_read);
                    let completed = now_unix_ms();
                    let mut derived = DerivedMetrics {
                        interface_card_tokens: estimate_tokens(arm_card),
                        task_prompt_tokens: estimate_tokens(&task.prompt),
                        wall_time_ms: completed.saturating_sub(started),
                        task_success: Some(false),
                        evidence_precision: None,
                        tool_call_count: tool_calls.len(),
                        tool_result_tokens,
                        tool_bytes_read,
                        invalid_sql_count,
                        wrong_tool_count,
                        tool_timeout_count,
                        missing_evidence_count: final_answer
                            .get("missing_evidence")
                            .and_then(Value::as_array)
                            .map(Vec::len)
                            .unwrap_or_default(),
                        overread_bytes: 0,
                        ..DerivedMetrics::default()
                    };
                    apply_usage_costs(&mut derived, &api_calls, &model);
                    let telemetry = BenchmarkRunTelemetry {
                        record_type: "benchmark_run".to_owned(),
                        run_id,
                        arm_id: arm.to_owned(),
                        task_id: task.task_id,
                        repeat_index,
                        model,
                        started_at_unix_ms: started,
                        completed_at_unix_ms: completed,
                        api_calls,
                        tool_calls,
                        derived_metrics: derived,
                        judge: None,
                        final_answer: Some(final_answer.clone()),
                        run_error: Some(error),
                    };
                    append_jsonl(&output_jsonl, &telemetry)?;
                    return Ok(final_answer);
                }
                messages.push(ChatMessage {
                    role: "tool".to_owned(),
                    content: Some(outcome.content.to_string()),
                    tool_call_id: Some(call.id),
                    tool_calls: None,
                });
            }
            continue;
        }
        if let Some(content) = message.content {
            let parsed = serde_json::from_str::<Value>(&content)
                .map_err(|err| HarnessError::Json(format!("final answer is not JSON: {err}")))?;
            final_answer = Some(parsed);
            break;
        }
        if choice.finish_reason.as_deref() == Some("stop") {
            break;
        }
    }

    let final_answer = final_answer.ok_or_else(|| {
        HarnessError::Http("model did not produce a final JSON answer before max_turns".to_owned())
    })?;
    let conn = Connection::open_with_flags(
        sqlite_path(&data_root),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(from_sql)?;
    let completed = now_unix_ms();
    let missing_evidence_count = final_answer
        .get("missing_evidence")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let judge_result = match judge_answer(&conn, &task, arm, &final_answer) {
        Ok(judge_result) => judge_result,
        Err(err) if is_recoverable_run_judge_error(&err) => {
            let error = err.to_string();
            let mut derived = DerivedMetrics {
                interface_card_tokens: estimate_tokens(arm_card),
                task_prompt_tokens: estimate_tokens(&task.prompt),
                wall_time_ms: completed.saturating_sub(started),
                task_success: Some(false),
                evidence_precision: None,
                tool_call_count: tool_calls.len(),
                tool_result_tokens,
                tool_bytes_read,
                invalid_sql_count,
                wrong_tool_count,
                tool_timeout_count,
                missing_evidence_count,
                overread_bytes: 0,
                ..DerivedMetrics::default()
            };
            apply_usage_costs(&mut derived, &api_calls, &model);
            let telemetry = BenchmarkRunTelemetry {
                record_type: "benchmark_run".to_owned(),
                run_id,
                arm_id: arm.to_owned(),
                task_id: task.task_id,
                repeat_index,
                model,
                started_at_unix_ms: started,
                completed_at_unix_ms: completed,
                api_calls,
                tool_calls,
                derived_metrics: derived,
                judge: None,
                final_answer: Some(final_answer.clone()),
                run_error: Some(error),
            };
            append_jsonl(&output_jsonl, &telemetry)?;
            return Ok(final_answer);
        }
        Err(err) => return Err(err),
    };
    let mut derived = DerivedMetrics {
        interface_card_tokens: estimate_tokens(arm_card),
        task_prompt_tokens: estimate_tokens(&task.prompt),
        wall_time_ms: completed.saturating_sub(started),
        task_success: Some(judge_result.task_success),
        evidence_precision: judge_result.evidence_precision,
        tool_call_count: tool_calls.len(),
        tool_result_tokens,
        tool_bytes_read,
        invalid_sql_count,
        wrong_tool_count,
        tool_timeout_count,
        missing_evidence_count,
        overread_bytes: 0,
        ..DerivedMetrics::default()
    };
    apply_usage_costs(&mut derived, &api_calls, &model);
    let telemetry = BenchmarkRunTelemetry {
        record_type: "benchmark_run".to_owned(),
        run_id,
        arm_id: arm.to_owned(),
        task_id: task.task_id,
        repeat_index,
        model,
        started_at_unix_ms: started,
        completed_at_unix_ms: completed,
        api_calls,
        tool_calls,
        derived_metrics: derived,
        judge: Some(judge_result),
        final_answer: Some(final_answer.clone()),
        run_error: None,
    };
    append_jsonl(&output_jsonl, &telemetry)?;
    Ok(final_answer)
}

fn append_tool_call_start_jsonl(
    path: &Path,
    run_id: &str,
    arm: &str,
    task_id: &str,
    repeat_index: usize,
    call: &OpenAiToolCall,
    started_at_unix_ms: u128,
) -> Result<(), HarnessError> {
    let telemetry = ToolCallStartTelemetry {
        record_type: "tool_call_start".to_owned(),
        run_id: run_id.to_owned(),
        arm_id: arm.to_owned(),
        task_id: task_id.to_owned(),
        repeat_index,
        call_id: call.id.clone(),
        tool_name: call.function.name.clone(),
        arguments: tool_call_arguments(call),
        started_at_unix_ms,
    };
    append_jsonl(path, &telemetry)
}

fn append_jsonl(path: &Path, record: &impl Serialize) -> Result<(), HarnessError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(from_io)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(from_io)?;
    let line = serde_json::to_string(record).map_err(|err| HarnessError::Json(err.to_string()))?;
    file.write_all(line.as_bytes()).map_err(from_io)?;
    file.write_all(b"\n").map_err(from_io)
}

fn tool_call_arguments(call: &OpenAiToolCall) -> Value {
    serde_json::from_str(&call.function.arguments)
        .unwrap_or_else(|_| json!({"raw": call.function.arguments.clone()}))
}

fn run_error_answer(
    task: &BenchmarkTask,
    error: &str,
    tool_call_count: usize,
    bytes_read_estimate: usize,
) -> Value {
    json!({
        "task_id": task.task_id.clone(),
        "status": "failed",
        "answer": Value::Null,
        "run_ids": [],
        "evidence": [],
        "missing_evidence": [error],
        "operations_summary": {
            "tool_call_count": tool_call_count,
            "bytes_read_estimate": bytes_read_estimate,
            "notes": "run terminated after a fatal tool execution error"
        },
        "confidence": "low"
    })
}

fn estimate_tokens(text: &str) -> usize {
    text.split_whitespace().count().max(text.len() / 4)
}

fn benchmark_messages(arm_card: &str, task: &BenchmarkTask) -> Vec<ChatMessage> {
    vec![
        ChatMessage {
            role: "system".to_owned(),
            content: Some(format!(
                "{}\n\nBase profile YAML:\n{}\n\nRubric YAML:\n{}",
                base_system_message(),
                include_str!("../../base_profile.yaml"),
                include_str!("../../rubric/phase1_readonly.yaml")
            )),
            tool_call_id: None,
            tool_calls: None,
        },
        ChatMessage {
            role: "system".to_owned(),
            content: Some(format!(
                "{}\n\nCurrent arm card YAML:\n{}",
                base_developer_message(),
                arm_card
            )),
            tool_call_id: None,
            tool_calls: None,
        },
        ChatMessage {
            role: "user".to_owned(),
            content: Some(format!(
                "Task ID: {}\nCategory: {}\n\n{}",
                task.task_id, task.category, task.prompt
            )),
            tool_call_id: None,
            tool_calls: None,
        },
    ]
}

fn base_system_message() -> &'static str {
    "You are an experiment tracking analysis agent working over a Yanex experiment corpus.\n\
You must solve the requested task using only the interface tools exposed for the current benchmark arm.\n\
Treat every run as stateless. Do not assume memory from previous tasks, previous arms, or previous attempts.\n\
Prefer small, evidence-directed reads. Inspect schema, listings, metadata, or summaries before reading large bodies.\n\
Do not guess missing values. If evidence is absent, inconsistent, or ambiguous, report that explicitly.\n\
Cite concrete evidence handles or paths for every factual claim that affects the final answer.\n\
Keep tool use bounded and stop as soon as the answer is sufficiently supported."
}

fn base_developer_message() -> &'static str {
    "Output exactly one JSON object matching the benchmark schema.\n\
Do not include markdown outside the JSON object.\n\
If the interface cannot support a requested operation, set status to \"blocked\" or \"partial\" and explain the missing capability.\n\
Evidence handles must be stable identifiers returned by the current arm's tools."
}

fn arm_card_yaml(arm: &str) -> Result<&'static str, HarnessError> {
    match arm {
        "sqlite_raw_v1" => Ok(include_str!("../../arms/sqlite_raw.yaml")),
        "sqlite_agentfs_v1" => Ok(include_str!("../../arms/sqlite_agentfs.yaml")),
        "nokv_posix_v1" => Ok(include_str!("../../arms/nokv_posix.yaml")),
        "nokv_native_v1" => Ok(include_str!("../../arms/nokv_native.yaml")),
        other => Err(HarnessError::Corpus(format!(
            "unknown benchmark arm {other}"
        ))),
    }
}

fn openai_tools(tools: &[ToolDefinition]) -> Vec<OpenAiTool> {
    tools
        .iter()
        .map(|tool| OpenAiTool {
            tool_type: "function".to_owned(),
            function: OpenAiFunctionDefinition {
                name: tool.name.clone(),
                description: tool.description.clone(),
                parameters: tool.parameters.clone(),
            },
        })
        .collect()
}

fn send_chat_completion(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    api_key: &str,
    request: &ChatCompletionRequest,
) -> Result<ChatCompletionResponse, HarnessError> {
    let response = client
        .post(endpoint)
        .bearer_auth(api_key)
        .json(request)
        .send()
        .map_err(|err| HarnessError::Http(err.to_string()))?;
    let status = response.status();
    let text = response
        .text()
        .map_err(|err| HarnessError::Http(err.to_string()))?;
    if !status.is_success() {
        return Err(HarnessError::Http(format!(
            "OpenAI chat completion HTTP {status}: {text}"
        )));
    }
    serde_json::from_str(&text).map_err(|err| {
        HarnessError::Json(format!(
            "OpenAI chat completion response parse error: {err}; {text}"
        ))
    })
}

fn is_recoverable_run_api_error(err: &HarnessError) -> bool {
    let HarnessError::Http(message) = err else {
        return false;
    };
    message.contains("context_length_exceeded")
        || message.contains("Input tokens exceed the configured limit")
        || message.contains("model did not produce a final JSON answer before max_turns")
}

fn is_recoverable_run_judge_error(err: &HarnessError) -> bool {
    matches!(err, HarnessError::Judge(_))
}

fn api_call_telemetry(
    response: &ChatCompletionResponse,
    requested_model: &str,
    started: u128,
    completed: u128,
) -> ApiCallTelemetry {
    let usage = response.usage.as_ref();
    ApiCallTelemetry {
        request_id: response.id.clone(),
        model: response
            .model
            .clone()
            .unwrap_or_else(|| requested_model.to_owned()),
        started_at_unix_ms: started,
        completed_at_unix_ms: completed,
        prompt_tokens: usage.and_then(|usage| usage.prompt_tokens),
        completion_tokens: usage.and_then(|usage| usage.completion_tokens),
        total_tokens: usage.and_then(|usage| usage.total_tokens),
        reasoning_tokens: usage
            .and_then(|usage| usage.completion_tokens_details.as_ref())
            .and_then(|details| details.reasoning_tokens),
        cached_prompt_tokens: usage
            .and_then(|usage| usage.prompt_tokens_details.as_ref())
            .and_then(|details| details.cached_tokens),
        accepted_prediction_tokens: usage
            .and_then(|usage| usage.completion_tokens_details.as_ref())
            .and_then(|details| details.accepted_prediction_tokens),
        rejected_prediction_tokens: usage
            .and_then(|usage| usage.completion_tokens_details.as_ref())
            .and_then(|details| details.rejected_prediction_tokens),
    }
}

fn apply_usage_costs(metrics: &mut DerivedMetrics, api_calls: &[ApiCallTelemetry], _model: &str) {
    let input_rate = env::var("OPENAI_INPUT_USD_PER_1M_TOKENS")
        .ok()
        .and_then(|value| value.parse::<f64>().ok());
    let output_rate = env::var("OPENAI_OUTPUT_USD_PER_1M_TOKENS")
        .ok()
        .and_then(|value| value.parse::<f64>().ok());
    let (Some(input_rate), Some(output_rate)) = (input_rate, output_rate) else {
        return;
    };
    let prompt_tokens = api_calls
        .iter()
        .filter_map(|call| call.prompt_tokens)
        .sum::<u64>() as f64;
    let completion_tokens = api_calls
        .iter()
        .filter_map(|call| call.completion_tokens)
        .sum::<u64>() as f64;
    let cost =
        prompt_tokens * input_rate / 1_000_000.0 + completion_tokens * output_rate / 1_000_000.0;
    metrics.execution_cost_usd = Some(cost);
    metrics.all_in_cost_usd = Some(cost);
}

impl ToolWorker {
    fn start(
        arm: &str,
        data_root: &Path,
        nokv_posix_root: Option<&PathBuf>,
        s3: &S3Options,
        registry: &[ToolDefinition],
        timeout: Duration,
    ) -> Self {
        let (request_tx, request_rx) = mpsc::channel::<ToolWorkerRequest>();
        let arm = arm.to_owned();
        let data_root = data_root.to_owned();
        let nokv_posix_root = nokv_posix_root.cloned();
        let s3 = s3.clone();
        let registry = registry.to_vec();
        thread::spawn(move || {
            let runtime = ArmRuntime::open(&arm, &data_root, nokv_posix_root.as_ref(), &s3);
            match runtime {
                Ok(mut runtime) => {
                    for request in request_rx {
                        let outcome = runtime.execute_tool_call(&arm, &registry, &request.call);
                        let _ = request.response_tx.send(outcome);
                    }
                }
                Err(err) => {
                    let error = err.to_string();
                    for request in request_rx {
                        let _ = request.response_tx.send(tool_error_outcome(
                            error.clone(),
                            false,
                            false,
                        ));
                    }
                }
            }
        });
        Self {
            request_tx,
            timeout,
        }
    }

    fn execute_tool_call(&self, call: &OpenAiToolCall) -> ToolExecutionOutcome {
        let (response_tx, response_rx) = mpsc::channel();
        if self
            .request_tx
            .send(ToolWorkerRequest {
                call: call.clone(),
                response_tx,
            })
            .is_err()
        {
            return tool_error_outcome("tool worker is not available".to_owned(), false, false);
        }
        match response_rx.recv_timeout(self.timeout) {
            Ok(outcome) => outcome,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                tool_timeout_outcome(call.function.name.clone(), self.timeout)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => tool_error_outcome(
                "tool worker stopped before returning a result".to_owned(),
                false,
                false,
            ),
        }
    }
}

impl ArmRuntime {
    fn open(
        arm: &str,
        data_root: &Path,
        nokv_posix_root: Option<&PathBuf>,
        s3: &S3Options,
    ) -> Result<Self, HarnessError> {
        match arm {
            "sqlite_raw_v1" => Ok(Self::SqliteRaw {
                conn: Connection::open_with_flags(
                    sqlite_path(data_root),
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .map_err(from_sql)?,
            }),
            "sqlite_agentfs_v1" => Ok(Self::SqliteAgentFs {
                conn: Connection::open_with_flags(
                    sqlite_path(data_root),
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .map_err(from_sql)?,
            }),
            "nokv_posix_v1" => Ok(Self::NokvPosix {
                root: nokv_posix_root
                    .cloned()
                    .ok_or(HarnessError::MissingOption("--nokv-posix-root"))?,
            }),
            "nokv_native_v1" => Ok(Self::NokvNative {
                service: Box::new(open_existing_nokv(&nokv_meta_path(data_root), s3)?),
            }),
            other => Err(HarnessError::Corpus(format!(
                "unknown benchmark arm {other}"
            ))),
        }
    }

    fn execute_tool_call(
        &mut self,
        arm: &str,
        registry: &[ToolDefinition],
        call: &OpenAiToolCall,
    ) -> ToolExecutionOutcome {
        if !registry.iter().any(|tool| tool.name == call.function.name) {
            return tool_error_outcome(
                format!(
                    "tool {} is not registered for arm {arm}",
                    call.function.name
                ),
                false,
                true,
            );
        }
        let args = match serde_json::from_str::<Value>(&call.function.arguments) {
            Ok(args) => args,
            Err(err) => {
                return tool_error_outcome(
                    format!("tool arguments are not valid JSON: {err}"),
                    false,
                    false,
                )
            }
        };
        let result = match self {
            Self::SqliteRaw { conn } => execute_sqlite_raw_tool(conn, &call.function.name, &args),
            Self::SqliteAgentFs { conn } => execute_agentfs_tool(conn, &call.function.name, &args),
            Self::NokvPosix { root } => execute_posix_tool(root, &call.function.name, &args),
            Self::NokvNative { service } => {
                execute_nokv_tool(service.as_ref(), &call.function.name, &args)
            }
        };
        match result {
            Ok(content) => tool_success_outcome(content),
            Err(err) => {
                if matches!(err, HarnessError::ToolTimeout { .. }) {
                    return tool_timeout_error_outcome(err.to_string());
                }
                let invalid_sql =
                    matches!(self, Self::SqliteRaw { .. }) && call.function.name == "query_sql";
                tool_error_outcome(err.to_string(), invalid_sql, false)
            }
        }
    }
}

fn execute_sqlite_raw_tool(
    conn: &Connection,
    name: &str,
    args: &Value,
) -> Result<Value, HarnessError> {
    match name {
        "show_schema" => Ok(json!({
            "tool": "show_schema",
            "schema": sqlite_schema_text(conn)?,
            "evidence": ["sqlite://schema"]
        })),
        "query_sql" => {
            let sql = required_string_arg(args, "sql")?;
            to_json_value(execute_sqlite_query_tool(conn, &sql)?)
        }
        "read_blob" => {
            let blob_ref = required_string_arg(args, "blob_ref")?;
            let offset = optional_u64_arg(args, "offset").unwrap_or(0);
            let limit = required_usize_arg(args, "limit")?;
            to_json_value(read_blob_tool(conn, &blob_ref, offset, limit)?)
        }
        other => Err(HarnessError::Corpus(format!(
            "unknown sqlite raw tool {other}"
        ))),
    }
}

fn execute_agentfs_tool(
    conn: &Connection,
    name: &str,
    args: &Value,
) -> Result<Value, HarnessError> {
    let path = required_string_arg(args, "path")?;
    match name {
        "ls" => to_json_value(agentfs_list_tool(conn, &path)?),
        "stat" => to_json_value(agentfs_stat_tool(conn, &path)?),
        "read" => to_json_value(agentfs_read_tool(
            conn,
            &path,
            optional_u64_arg(args, "offset").unwrap_or(0),
            required_usize_arg(args, "limit")?,
        )?),
        "grep" => to_json_value(agentfs_grep_tool(
            conn,
            &path,
            &required_string_arg(args, "pattern")?,
            required_bool_arg(args, "recursive")?,
        )?),
        "find" => to_json_value(agentfs_find_tool(
            conn,
            &path,
            &required_string_arg(args, "pattern")?,
        )?),
        other => Err(HarnessError::Corpus(format!(
            "unknown AgentFS tool {other}"
        ))),
    }
}

fn execute_posix_tool(root: &Path, name: &str, args: &Value) -> Result<Value, HarnessError> {
    let path = required_string_arg(args, "path")?;
    match name {
        "ls" => to_json_value(posix_list_tool(root, &path)?),
        "stat" => to_json_value(posix_stat_tool(root, &path)?),
        "read" => to_json_value(posix_read_tool(
            root,
            &path,
            optional_u64_arg(args, "offset").unwrap_or(0),
            required_usize_arg(args, "limit")?,
        )?),
        "grep" => to_json_value(posix_grep_tool(
            root,
            &path,
            &required_string_arg(args, "pattern")?,
            required_bool_arg(args, "recursive")?,
        )?),
        "find" => to_json_value(posix_find_tool(
            root,
            &path,
            &required_string_arg(args, "pattern")?,
        )?),
        other => Err(HarnessError::Corpus(format!("unknown POSIX tool {other}"))),
    }
}

fn execute_nokv_tool(
    service: &NoKvFs<HoltMetadataStore, S3ObjectStore>,
    name: &str,
    args: &Value,
) -> Result<Value, HarnessError> {
    let path = required_string_arg(args, "path")?;
    match name {
        "list" => to_json_value(nokv_list_tool(service, &path)?),
        "stat" => to_json_value(nokv_stat_tool(service, &path)?),
        "read" => to_json_value(nokv_read_tool(
            service,
            &path,
            optional_u64_arg(args, "offset").unwrap_or(0),
            required_usize_arg(args, "limit")?,
            optional_u64_arg(args, "expected_generation"),
        )?),
        other => Err(HarnessError::Corpus(format!(
            "unknown NoKV native tool {other}"
        ))),
    }
}

fn to_json_value(value: impl Serialize) -> Result<Value, HarnessError> {
    serde_json::to_value(value).map_err(|err| HarnessError::Json(err.to_string()))
}

fn sqlite_schema_text(conn: &Connection) -> Result<String, HarnessError> {
    let mut stmt = conn
        .prepare(
            "SELECT sql FROM sqlite_schema WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY name",
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(from_sql)?;
    let mut out = String::new();
    for row in rows {
        out.push_str(&row.map_err(from_sql)?);
        out.push('\n');
    }
    Ok(out)
}

fn required_string_arg(args: &Value, name: &'static str) -> Result<String, HarnessError> {
    args.get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(HarnessError::MissingOption(name))
}

fn required_usize_arg(args: &Value, name: &'static str) -> Result<usize, HarnessError> {
    args.get(name)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(HarnessError::MissingOption(name))
}

fn optional_u64_arg(args: &Value, name: &'static str) -> Option<u64> {
    args.get(name).and_then(Value::as_u64)
}

fn required_bool_arg(args: &Value, name: &'static str) -> Result<bool, HarnessError> {
    args.get(name)
        .and_then(Value::as_bool)
        .ok_or(HarnessError::MissingOption(name))
}

fn tool_success_outcome(content: Value) -> ToolExecutionOutcome {
    let result_text = content.to_string();
    ToolExecutionOutcome {
        bytes_read: bytes_read_from_tool_result(&content),
        result_token_estimate: estimate_tokens(&result_text),
        content,
        status: "success".to_owned(),
        error: None,
        invalid_sql: false,
        wrong_tool: false,
        timed_out: false,
        fatal_run_error: false,
    }
}

fn tool_error_outcome(error: String, invalid_sql: bool, wrong_tool: bool) -> ToolExecutionOutcome {
    let content = json!({
        "status": "error",
        "error": error.clone(),
        "invalid_sql": invalid_sql,
        "wrong_tool": wrong_tool
    });
    let result_text = content.to_string();
    ToolExecutionOutcome {
        content,
        bytes_read: 0,
        result_token_estimate: estimate_tokens(&result_text),
        status: "error".to_owned(),
        error: Some(error),
        invalid_sql,
        wrong_tool,
        timed_out: false,
        fatal_run_error: false,
    }
}

fn tool_timeout_outcome(tool_name: String, timeout: Duration) -> ToolExecutionOutcome {
    tool_timeout_error_outcome(format!(
        "tool {tool_name} timed out after {}ms",
        duration_millis(timeout)
    ))
}

fn tool_timeout_error_outcome(error: String) -> ToolExecutionOutcome {
    let content = json!({
        "status": "error",
        "error": error.clone(),
        "timed_out": true
    });
    let result_text = content.to_string();
    ToolExecutionOutcome {
        content,
        bytes_read: 0,
        result_token_estimate: estimate_tokens(&result_text),
        status: "timeout".to_owned(),
        error: Some(error),
        invalid_sql: false,
        wrong_tool: false,
        timed_out: true,
        fatal_run_error: true,
    }
}

fn bytes_read_from_tool_result(value: &Value) -> usize {
    if let Some(bytes) = value.get("bytes_read").and_then(Value::as_u64) {
        return usize::try_from(bytes).unwrap_or(usize::MAX);
    }
    value
        .get("bytes_read")
        .and_then(Value::as_i64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_else(|| {
            value
                .get("bytes_read_estimate")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or(0)
        })
}

fn judge_answer(
    conn: &Connection,
    task: &BenchmarkTask,
    arm: &str,
    answer: &Value,
) -> Result<JudgeResult, HarnessError> {
    let gold_rows = query_gold_rows(conn, task)?;
    let mut mismatches = Vec::new();
    let expected_run_ids = expected_run_ids(&gold_rows, task.expected.run_id_column.as_deref());
    let actual_run_ids = answer
        .get("run_ids")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !expected_run_ids.is_empty() && actual_run_ids != expected_run_ids {
        mismatches.push(format!(
            "run_ids differ; expected {:?}, got {:?}",
            expected_run_ids, actual_run_ids
        ));
    }

    let answer_root = answer.get("answer").unwrap_or(&Value::Null);
    match task.expected.kind.as_str() {
        "status_counts" => {
            let key = task.expected.answer_key.as_deref().ok_or_else(|| {
                HarnessError::Judge("status_counts requires answer_key".to_owned())
            })?;
            let actual_rows = answer_root
                .get(key)
                .and_then(Value::as_array)
                .ok_or_else(|| HarnessError::Judge(format!("answer.{key} must be an array")))?;
            compare_row_arrays(
                &gold_rows,
                actual_rows,
                &task.expected.columns,
                &mut mismatches,
            );
            let expected_total = gold_rows
                .iter()
                .filter_map(|row| row.get("count").and_then(sqlite_value_as_i64))
                .sum::<i64>();
            let actual_total = answer_root
                .get("total_runs")
                .and_then(sqlite_value_as_i64)
                .unwrap_or(-1);
            if expected_total != actual_total {
                mismatches.push(format!(
                    "answer.total_runs differs; expected {expected_total}, got {actual_total}"
                ));
            }
        }
        "rows_exact" => {
            let key =
                task.expected.answer_key.as_deref().ok_or_else(|| {
                    HarnessError::Judge("rows_exact requires answer_key".to_owned())
                })?;
            let actual_rows = answer_root
                .get(key)
                .and_then(Value::as_array)
                .ok_or_else(|| HarnessError::Judge(format!("answer.{key} must be an array")))?;
            compare_row_arrays(
                &gold_rows,
                actual_rows,
                &task.expected.columns,
                &mut mismatches,
            );
        }
        "first_row_object" => {
            let key = task.expected.answer_key.as_deref().ok_or_else(|| {
                HarnessError::Judge("first_row_object requires answer_key".to_owned())
            })?;
            let actual = answer_root
                .get(key)
                .ok_or_else(|| HarnessError::Judge(format!("answer.{key} is missing")))?;
            let expected = gold_rows.first().cloned().unwrap_or(Value::Null);
            compare_objects(
                &expected,
                actual,
                &task.expected.columns,
                "answer",
                &mut mismatches,
            );
        }
        "index_count_consistency" => {
            judge_index_count_consistency(&gold_rows, answer_root, &mut mismatches)?;
        }
        other => mismatches.push(format!("unsupported expected kind {other}")),
    }

    let (evidence_checked, evidence_supported) = evidence_support(conn, arm, answer)?;
    let evidence_precision = if evidence_checked == 0 {
        None
    } else {
        Some(evidence_supported as f64 / evidence_checked as f64)
    };
    if evidence_checked == 0 {
        mismatches.push("answer did not include evidence handles".to_owned());
    }
    Ok(JudgeResult {
        task_success: mismatches.is_empty(),
        evidence_precision,
        evidence_checked,
        evidence_supported,
        expected_run_ids,
        actual_run_ids,
        mismatches,
    })
}

fn query_gold_rows(conn: &Connection, task: &BenchmarkTask) -> Result<Vec<Value>, HarnessError> {
    execute_sqlite_query_tool(conn, &task.gold_sql).map(|result| {
        result
            .rows
            .into_iter()
            .map(|row| row.row)
            .collect::<Vec<_>>()
    })
}

fn expected_run_ids(rows: &[Value], column: Option<&str>) -> Vec<String> {
    let Some(column) = column else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| row.get(column).and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

fn compare_row_arrays(
    expected_rows: &[Value],
    actual_rows: &[Value],
    columns: &[String],
    mismatches: &mut Vec<String>,
) {
    if expected_rows.len() != actual_rows.len() {
        mismatches.push(format!(
            "row count differs; expected {}, got {}",
            expected_rows.len(),
            actual_rows.len()
        ));
        return;
    }
    for (index, (expected, actual)) in expected_rows.iter().zip(actual_rows).enumerate() {
        compare_objects(
            expected,
            actual,
            columns,
            &format!("row {index}"),
            mismatches,
        );
    }
}

fn compare_objects(
    expected: &Value,
    actual: &Value,
    columns: &[String],
    label: &str,
    mismatches: &mut Vec<String>,
) {
    for column in columns {
        let expected_value = expected.get(column).unwrap_or(&Value::Null);
        let actual_value = actual.get(column).unwrap_or(&Value::Null);
        if !values_match(expected_value, actual_value) {
            mismatches.push(format!(
                "{label}.{column} differs; expected {expected_value}, got {actual_value}"
            ));
        }
    }
}

fn values_match(expected: &Value, actual: &Value) -> bool {
    if expected == actual {
        return true;
    }
    match (expected.as_f64(), actual.as_f64()) {
        (Some(left), Some(right)) => {
            let abs = (left - right).abs();
            let rel = abs / left.abs().max(1.0);
            abs <= 1.0e-9 || rel <= 1.0e-6
        }
        _ => matches!(
            (expected.as_i64(), actual.as_bool()),
            (Some(0), Some(false)) | (Some(1), Some(true))
        ),
    }
}

fn judge_index_count_consistency(
    gold_rows: &[Value],
    answer_root: &Value,
    mismatches: &mut Vec<String>,
) -> Result<(), HarnessError> {
    let Some(row) = gold_rows.first() else {
        mismatches.push("gold query returned no index consistency row".to_owned());
        return Ok(());
    };
    let expected_metadata = row
        .get("metadata_completed_count")
        .and_then(sqlite_value_as_i64)
        .ok_or_else(|| HarnessError::Judge("missing metadata_completed_count".to_owned()))?;
    let index_json = row
        .get("completed_index_json")
        .and_then(Value::as_str)
        .ok_or_else(|| HarnessError::Judge("missing completed_index_json".to_owned()))?;
    let index_count = serde_json::from_str::<Value>(index_json)
        .map_err(|err| HarnessError::Json(err.to_string()))?
        .as_array()
        .map(Vec::len)
        .ok_or_else(|| HarnessError::Json("completed index is not a JSON array".to_owned()))?;
    let expected = json!({
        "metadata_completed_count": expected_metadata,
        "index_completed_count": index_count,
        "consistent": expected_metadata == index_count as i64,
    });
    compare_objects(
        &expected,
        answer_root,
        &[
            "metadata_completed_count".to_owned(),
            "index_completed_count".to_owned(),
            "consistent".to_owned(),
        ],
        "answer",
        mismatches,
    );
    Ok(())
}

fn evidence_support(
    conn: &Connection,
    arm: &str,
    answer: &Value,
) -> Result<(usize, usize), HarnessError> {
    let evidence = answer
        .get("evidence")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.get("handle").and_then(Value::as_str))
        .collect::<Vec<_>>();
    let mut supported = 0;
    for handle in &evidence {
        if evidence_handle_supported(conn, arm, handle)? {
            supported += 1;
        }
    }
    Ok((evidence.len(), supported))
}

fn evidence_handle_supported(
    conn: &Connection,
    arm: &str,
    handle: &str,
) -> Result<bool, HarnessError> {
    match arm {
        "sqlite_raw_v1" => sqlite_evidence_supported(conn, handle),
        "sqlite_agentfs_v1" => {
            let Some(path) = handle.strip_prefix("agentfs://") else {
                return Ok(false);
            };
            let path = path.split('#').next().unwrap_or(path);
            Ok(sqlite_count(
                conn,
                &format!(
                    "SELECT COUNT(*) FROM files WHERE path = '{}'",
                    escape_sql_literal(path)
                ),
            )? > 0)
        }
        "nokv_posix_v1" => Ok(handle.starts_with("nokv-fuse:///yanex/")),
        "nokv_native_v1" => Ok(handle.starts_with("nokv-native:///yanex/")),
        _ => Ok(false),
    }
}

fn sqlite_evidence_supported(conn: &Connection, handle: &str) -> Result<bool, HarnessError> {
    if let Some(id) = handle.strip_prefix("sqlite://experiments/") {
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM experiments WHERE experiment_id = '{}'",
                escape_sql_literal(id)
            ),
        )? > 0);
    }
    if let Some(blob_ref) = handle.strip_prefix("sqlite://blobs/") {
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM blobs WHERE blob_ref = '{}'",
                escape_sql_literal(blob_ref)
            ),
        )? > 0);
    }
    if let Some(rest) = handle.strip_prefix("sqlite://artifacts/") {
        let Some((run_id, artifact_path)) = rest.split_once('/') else {
            return Ok(false);
        };
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM artifacts WHERE experiment_id = '{}' AND artifact_path = '{}'",
                escape_sql_literal(run_id),
                escape_sql_literal(artifact_path)
            ),
        )? > 0);
    }
    if let Some(rest) = handle.strip_prefix("sqlite://params/") {
        let Some((run_id, param_path)) = rest.split_once('/') else {
            return Ok(false);
        };
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM params WHERE experiment_id = '{}' AND param_path = '{}'",
                escape_sql_literal(run_id),
                escape_sql_literal(param_path)
            ),
        )? > 0);
    }
    if let Some(rest) = handle.strip_prefix("sqlite://metrics/") {
        let parts = rest.split('/').collect::<Vec<_>>();
        if parts.len() < 3 {
            return Ok(false);
        }
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM metrics WHERE experiment_id = '{}' AND metric_name = '{}' AND step = {}",
                escape_sql_literal(parts[0]),
                escape_sql_literal(parts[1]),
                escape_sql_literal(parts[2])
            ),
        )? > 0);
    }
    if let Some(run_id) = handle.strip_prefix("sqlite://git_state/") {
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM git_state WHERE experiment_id = '{}'",
                escape_sql_literal(run_id)
            ),
        )? > 0);
    }
    Ok(false)
}

impl BenchmarkRunTelemetry {
    #[cfg(test)]
    fn to_jsonl_line(&self) -> String {
        serde_json::to_string(self).expect("benchmark telemetry serializes")
    }

    #[cfg(test)]
    fn sample_for_test() -> Self {
        Self {
            record_type: "benchmark_run".to_owned(),
            run_id: "unit-run".to_owned(),
            arm_id: "sqlite_raw_v1".to_owned(),
            task_id: "status_counts".to_owned(),
            repeat_index: 0,
            model: "test-model".to_owned(),
            started_at_unix_ms: 1,
            completed_at_unix_ms: 2,
            api_calls: vec![ApiCallTelemetry {
                request_id: Some("req-test".to_owned()),
                model: "test-model".to_owned(),
                started_at_unix_ms: 1,
                completed_at_unix_ms: 2,
                prompt_tokens: Some(10),
                completion_tokens: Some(5),
                total_tokens: Some(15),
                reasoning_tokens: Some(0),
                cached_prompt_tokens: Some(0),
                accepted_prediction_tokens: Some(0),
                rejected_prediction_tokens: Some(0),
            }],
            tool_calls: vec![ToolCallTelemetry {
                call_id: "call-test".to_owned(),
                tool_name: "query_sql".to_owned(),
                arguments: json!({"sql":"SELECT 1"}),
                started_at_unix_ms: 1,
                completed_at_unix_ms: 2,
                status: "success".to_owned(),
                bytes_read: 0,
                result_token_estimate: 1,
                error: None,
                timed_out: false,
            }],
            derived_metrics: DerivedMetrics {
                tool_call_count: 1,
                task_success: Some(true),
                evidence_precision: Some(1.0),
                ..DerivedMetrics::default()
            },
            judge: Some(JudgeResult {
                task_success: true,
                evidence_precision: Some(1.0),
                evidence_checked: 1,
                evidence_supported: 1,
                expected_run_ids: vec!["run-1".to_owned()],
                actual_run_ids: vec!["run-1".to_owned()],
                mismatches: Vec::new(),
            }),
            final_answer: Some(json!({"status":"success"})),
            run_error: None,
        }
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn duration_millis(duration: Duration) -> u128 {
    duration.as_millis()
}

fn evidence_refs_for_row(row: &Value) -> Vec<String> {
    let Some(object) = row.as_object() else {
        return Vec::new();
    };
    let experiment_id = object.get("experiment_id").and_then(Value::as_str);
    let mut refs = Vec::new();
    if let Some(experiment_id) = experiment_id {
        push_evidence_ref(&mut refs, format!("sqlite://experiments/{experiment_id}"));
        if let Some(param_path) = object.get("param_path").and_then(Value::as_str) {
            push_evidence_ref(
                &mut refs,
                format!("sqlite://params/{experiment_id}/{param_path}"),
            );
        }
        if let Some(metric_name) = object.get("metric_name").and_then(Value::as_str) {
            if let Some(step) = object.get("step").and_then(sqlite_value_as_i64) {
                push_evidence_ref(
                    &mut refs,
                    format!("sqlite://metrics/{experiment_id}/{metric_name}/{step}"),
                );
            }
        }
        if let (Some(dependency_id), Some(relation)) = (
            object
                .get("dependency_experiment_id")
                .and_then(Value::as_str),
            object.get("relation").and_then(Value::as_str),
        ) {
            push_evidence_ref(
                &mut refs,
                format!("sqlite://dependencies/{experiment_id}/{dependency_id}/{relation}"),
            );
        }
        if let Some(artifact_path) = object.get("artifact_path").and_then(Value::as_str) {
            push_evidence_ref(
                &mut refs,
                format!("sqlite://artifacts/{experiment_id}/{artifact_path}"),
            );
        }
        if object.contains_key("commit_hash")
            || object.contains_key("dirty")
            || object.contains_key("diff_blob_ref")
        {
            push_evidence_ref(&mut refs, format!("sqlite://git_state/{experiment_id}"));
        }
    }
    if let Some(blob_ref) = object.get("blob_ref").and_then(Value::as_str) {
        push_evidence_ref(&mut refs, format!("sqlite://blobs/{blob_ref}"));
    }
    if let Some(diff_blob_ref) = object.get("diff_blob_ref").and_then(Value::as_str) {
        push_evidence_ref(&mut refs, format!("sqlite://blobs/{diff_blob_ref}"));
    }
    refs
}

fn push_evidence_ref(refs: &mut Vec<String>, value: String) {
    if !refs.contains(&value) {
        refs.push(value);
    }
}

fn sqlite_value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn verify_sqlite_materialization(
    conn: &Connection,
) -> Result<SqliteMaterializationReport, HarnessError> {
    let mut report = SqliteMaterializationReport {
        run_count: sqlite_count(conn, "SELECT COUNT(*) FROM experiments")?,
        ..SqliteMaterializationReport::default()
    };

    let files = sqlite_agentfs_files(conn)?;
    for (path, bytes) in &files {
        if path.starts_with("/index/") {
            report.index_files_checked += 1;
        } else {
            report.agentfs_files_checked += 1;
        }
        if let Some((run_id, kind, artifact_path)) = parse_agentfs_run_file(path) {
            match kind {
                "metadata.json" => {
                    report.raw_metadata_checked += 1;
                    let raw = query_single_blob(
                        conn,
                        "SELECT CAST(metadata_json AS BLOB) FROM experiments WHERE experiment_id = ?1",
                        run_id,
                    )?;
                    if raw != *bytes {
                        report
                            .mismatches
                            .push(format!("{path}: metadata bytes differ from experiments"));
                    }
                }
                "params.yaml" => {
                    let expected = expected_params(bytes)?;
                    report.raw_params_checked += expected.len();
                    let actual = query_params(conn, run_id)?;
                    compare_maps(&mut report.mismatches, path, "params", &expected, &actual);
                }
                "metrics.json" => {
                    let expected = expected_metrics(Path::new(path), bytes)?;
                    report.raw_metrics_checked += expected.len();
                    let actual = query_metrics(conn, run_id)?;
                    compare_maps(&mut report.mismatches, path, "metrics", &expected, &actual);
                }
                "dependencies.json" => {
                    let expected = expected_dependencies(bytes)?;
                    report.raw_dependencies_checked += expected.len();
                    let actual = query_dependencies(conn, run_id)?;
                    compare_maps(
                        &mut report.mismatches,
                        path,
                        "dependencies",
                        &expected,
                        &actual,
                    );
                }
                "artifacts" => {
                    let Some(artifact_path) = artifact_path else {
                        continue;
                    };
                    report.raw_existing_artifacts_checked += 1;
                    verify_existing_artifact(conn, &mut report, run_id, artifact_path, bytes)?;
                }
                _ => {}
            }
        }
    }

    verify_missing_artifacts(conn, &files, &mut report)?;
    Ok(report)
}

fn verify_nokv_namespace(
    conn: &Connection,
    service: &NoKvFs<HoltMetadataStore, S3ObjectStore>,
) -> Result<NamespaceMaterializationReport, HarnessError> {
    let files = sqlite_agentfs_files(conn)?;
    let missing = missing_artifact_paths(conn)?;
    let mut report = NamespaceMaterializationReport::default();
    let mut generations = BTreeSet::new();
    for (agentfs_path, bytes) in &files {
        let nokv_path = nokv_path_for_agentfs(agentfs_path)?;
        let Some(metadata) = service.stat_path(&nokv_path).map_err(from_nokv)? else {
            report
                .mismatches
                .push(format!("{nokv_path}: missing NoKV file"));
            continue;
        };
        let body = match metadata.body.as_ref() {
            Some(body) => body,
            None => {
                report
                    .mismatches
                    .push(format!("{nokv_path}: missing body descriptor"));
                continue;
            }
        };
        generations.insert(metadata.attr.generation);
        report.body_descriptors_checked += 1;
        if body.size != bytes.len() as u64 {
            report.mismatches.push(format!(
                "{nokv_path}: body descriptor size {} != expected {}",
                body.size,
                bytes.len()
            ));
        }
        if body.digest_uri != sha256_uri(bytes) {
            report
                .mismatches
                .push(format!("{nokv_path}: body digest differs"));
        }
        let len = usize::try_from(metadata.attr.size)
            .map_err(|_| HarnessError::NoKv(format!("{nokv_path}: file size exceeds usize")))?;
        let read = service
            .read_file(metadata.attr.inode, 0, len)
            .map_err(from_nokv)?;
        if read != *bytes {
            report
                .mismatches
                .push(format!("{nokv_path}: NoKV bytes differ"));
        }
        report.files_checked += 1;
        if agentfs_path.starts_with("/index/") {
            report.index_files_checked += 1;
        } else {
            report.run_files_checked += 1;
        }
    }
    for (run_id, artifact_path) in missing {
        let nokv_path = format!("/yanex/runs/{run_id}/artifacts/{artifact_path}");
        if service.stat_path(&nokv_path).map_err(from_nokv)?.is_some() {
            report
                .mismatches
                .push(format!("{nokv_path}: missing artifact unexpectedly exists"));
        }
        report.missing_artifacts_checked += 1;
    }
    report.metadata_generations_observed = generations.len();
    Ok(report)
}

fn verify_posix_namespace(
    conn: &Connection,
    root: &Path,
) -> Result<NamespaceMaterializationReport, HarnessError> {
    let files = sqlite_agentfs_files(conn)?;
    let missing = missing_artifact_paths(conn)?;
    let mut report = NamespaceMaterializationReport::default();
    for (agentfs_path, bytes) in &files {
        let path = root.join(posix_relative_path_for_agentfs(agentfs_path)?);
        let read = fs::read(&path).map_err(from_io)?;
        if read != *bytes {
            report
                .mismatches
                .push(format!("{}: POSIX bytes differ", path.display()));
        }
        report.files_checked += 1;
        if agentfs_path.starts_with("/index/") {
            report.index_files_checked += 1;
        } else {
            report.run_files_checked += 1;
        }
    }
    for (run_id, artifact_path) in missing {
        let path = root.join(format!("yanex/runs/{run_id}/artifacts/{artifact_path}"));
        if path.exists() {
            report.mismatches.push(format!(
                "{}: missing artifact unexpectedly exists",
                path.display()
            ));
        }
        report.missing_artifacts_checked += 1;
    }
    Ok(report)
}

fn sqlite_agentfs_files(conn: &Connection) -> Result<BTreeMap<String, Vec<u8>>, HarnessError> {
    let mut stmt = conn
        .prepare("SELECT path, content FROM files WHERE file_type = 'file' ORDER BY path")
        .map_err(from_sql)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(from_sql)?;
    let mut files = BTreeMap::new();
    for row in rows {
        let (path, bytes) = row.map_err(from_sql)?;
        files.insert(path, bytes);
    }
    Ok(files)
}

fn parse_agentfs_run_file(path: &str) -> Option<(&str, &str, Option<&str>)> {
    let rest = path.strip_prefix("/runs/")?;
    let (run_id, suffix) = rest.split_once('/')?;
    if let Some(artifact_path) = suffix.strip_prefix("artifacts/") {
        return Some((run_id, "artifacts", Some(artifact_path)));
    }
    Some((run_id, suffix, None))
}

fn nokv_path_for_agentfs(path: &str) -> Result<String, HarnessError> {
    if let Some(rest) = path.strip_prefix("/runs/") {
        Ok(format!("/yanex/runs/{rest}"))
    } else if let Some(rest) = path.strip_prefix("/index/") {
        Ok(format!("/yanex/index/{rest}"))
    } else {
        Err(HarnessError::Corpus(format!(
            "unsupported AgentFS file path {path}"
        )))
    }
}

fn posix_relative_path_for_agentfs(path: &str) -> Result<String, HarnessError> {
    Ok(nokv_path_for_agentfs(path)?
        .trim_start_matches('/')
        .to_owned())
}

fn expected_params(bytes: &[u8]) -> Result<BTreeMap<String, String>, HarnessError> {
    let yaml = parse_yaml_bytes(Path::new("params.yaml"), bytes)?;
    let mut flattened = Vec::new();
    flatten_yaml("", &yaml, &mut flattened);
    Ok(flattened
        .into_iter()
        .map(|(path, value)| {
            (
                path,
                format!(
                    "{}\t{}",
                    serde_json::to_string(&yaml_to_json(&value)).expect("yaml json serializes"),
                    yaml_type(&value)
                ),
            )
        })
        .collect())
}

fn query_params(conn: &Connection, run_id: &str) -> Result<BTreeMap<String, String>, HarnessError> {
    let mut stmt = conn
        .prepare(
            "SELECT param_path, value_json, value_type FROM params WHERE experiment_id = ?1 ORDER BY param_path",
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map(params![run_id], |row| {
            let path: String = row.get(0)?;
            let value_json: String = row.get(1)?;
            let value_type: String = row.get(2)?;
            Ok((path, format!("{value_json}\t{value_type}")))
        })
        .map_err(from_sql)?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(from_sql)
}

fn expected_metrics(path: &Path, bytes: &[u8]) -> Result<BTreeMap<String, String>, HarnessError> {
    let value = parse_metrics_bytes(path, bytes)?;
    let mut expected = BTreeMap::new();
    let Some(records) = value.as_array() else {
        return Ok(expected);
    };
    for (ordinal, record) in records.iter().enumerate() {
        let Some(object) = record.as_object() else {
            continue;
        };
        let step = object
            .get("step")
            .and_then(Value::as_i64)
            .unwrap_or(ordinal as i64);
        let timestamp = object
            .get("timestamp")
            .and_then(Value::as_str)
            .unwrap_or("");
        for (name, value) in object {
            if name == "step" || name == "timestamp" {
                continue;
            }
            expected.insert(
                format!("{name}\t{step}"),
                format!(
                    "{}\t{}\t{}",
                    metric_real(value)
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                    timestamp,
                    serde_json::to_string(value).expect("metric value serializes")
                ),
            );
        }
    }
    Ok(expected)
}

fn query_metrics(
    conn: &Connection,
    run_id: &str,
) -> Result<BTreeMap<String, String>, HarnessError> {
    let mut stmt = conn
        .prepare(
            "SELECT metric_name, step, value, timestamp, value_json FROM metrics WHERE experiment_id = ?1 ORDER BY metric_name, step",
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map(params![run_id], |row| {
            let name: String = row.get(0)?;
            let step: i64 = row.get(1)?;
            let value: Option<f64> = row.get(2)?;
            let timestamp: Option<String> = row.get(3)?;
            let value_json: String = row.get(4)?;
            Ok((
                format!("{name}\t{step}"),
                format!(
                    "{}\t{}\t{}",
                    value.map(|value| value.to_string()).unwrap_or_default(),
                    timestamp.unwrap_or_default(),
                    value_json
                ),
            ))
        })
        .map_err(from_sql)?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(from_sql)
}

fn expected_dependencies(bytes: &[u8]) -> Result<BTreeMap<String, String>, HarnessError> {
    let value = parse_json_bytes(Path::new("dependencies.json"), bytes)?;
    let Some(document) = value.as_object() else {
        return Ok(BTreeMap::new());
    };
    let dependencies = document
        .get("dependencies")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|map| map.iter());
    let metadata = document.get("metadata").and_then(Value::as_object);
    let mut expected = BTreeMap::new();
    for (relation, dependency) in dependencies {
        let Some(dependency_id) = dependency.as_str() else {
            continue;
        };
        let metadata_json = metadata
            .and_then(|items| items.get(dependency_id))
            .map(|value| serde_json::to_string(value).expect("dependency metadata serializes"))
            .unwrap_or_default();
        let status = metadata
            .and_then(|items| items.get(dependency_id))
            .and_then(|value| value.get("status_at_resolution"))
            .and_then(Value::as_str)
            .unwrap_or("");
        expected.insert(
            format!("{dependency_id}\t{relation}"),
            format!("{status}\t{metadata_json}"),
        );
    }
    Ok(expected)
}

fn query_dependencies(
    conn: &Connection,
    run_id: &str,
) -> Result<BTreeMap<String, String>, HarnessError> {
    let mut stmt = conn
        .prepare(
            "SELECT dependency_experiment_id, relation, status_at_resolution, metadata_json FROM dependencies WHERE experiment_id = ?1 ORDER BY dependency_experiment_id, relation",
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map(params![run_id], |row| {
            let dependency_id: String = row.get(0)?;
            let relation: String = row.get(1)?;
            let status: Option<String> = row.get(2)?;
            let metadata_json: Option<String> = row.get(3)?;
            Ok((
                format!("{dependency_id}\t{relation}"),
                format!(
                    "{}\t{}",
                    status.unwrap_or_default(),
                    metadata_json.unwrap_or_default()
                ),
            ))
        })
        .map_err(from_sql)?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(from_sql)
}

fn compare_maps(
    mismatches: &mut Vec<String>,
    path: &str,
    label: &str,
    expected: &BTreeMap<String, String>,
    actual: &BTreeMap<String, String>,
) {
    if expected != actual {
        mismatches.push(format!(
            "{path}: {label} differ; expected {} rows, got {} rows",
            expected.len(),
            actual.len()
        ));
    }
}

fn verify_existing_artifact(
    conn: &Connection,
    report: &mut SqliteMaterializationReport,
    run_id: &str,
    artifact_path: &str,
    bytes: &[u8],
) -> Result<(), HarnessError> {
    let row: Option<(i64, i64, String, String)> = conn
        .query_row(
            "SELECT \"exists\", size_bytes, digest, blob_ref FROM artifacts WHERE experiment_id = ?1 AND artifact_path = ?2",
            params![run_id, artifact_path],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(from_sql)?;
    let Some((exists, size_bytes, digest, blob_ref)) = row else {
        report.mismatches.push(format!(
            "/runs/{run_id}/artifacts/{artifact_path}: missing artifacts row"
        ));
        return Ok(());
    };
    if exists != 1 {
        report.mismatches.push(format!(
            "/runs/{run_id}/artifacts/{artifact_path}: artifacts row is not marked existing"
        ));
    }
    if size_bytes != bytes.len() as i64 {
        report.mismatches.push(format!(
            "/runs/{run_id}/artifacts/{artifact_path}: artifact size differs"
        ));
    }
    if digest != sha256_uri(bytes) {
        report.mismatches.push(format!(
            "/runs/{run_id}/artifacts/{artifact_path}: artifact digest differs"
        ));
    }
    let blob = query_single_blob(
        conn,
        "SELECT content FROM blobs WHERE blob_ref = ?1",
        &blob_ref,
    )?;
    if blob != bytes {
        report.mismatches.push(format!(
            "/runs/{run_id}/artifacts/{artifact_path}: blob bytes differ"
        ));
    }
    Ok(())
}

fn verify_missing_artifacts(
    conn: &Connection,
    files: &BTreeMap<String, Vec<u8>>,
    report: &mut SqliteMaterializationReport,
) -> Result<(), HarnessError> {
    for (run_id, artifact_path) in missing_artifact_paths(conn)? {
        report.raw_missing_artifacts_checked += 1;
        let agentfs_path = format!("/runs/{run_id}/artifacts/{artifact_path}");
        if files.contains_key(&agentfs_path) {
            report.mismatches.push(format!(
                "{agentfs_path}: missing artifact exists in AgentFS"
            ));
        }
        let blob_ref = format!("artifact:{run_id}:{artifact_path}");
        let blob_count = sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM blobs WHERE blob_ref = '{}'",
                escape_sql_literal(&blob_ref)
            ),
        )?;
        if blob_count != 0 {
            report
                .mismatches
                .push(format!("{blob_ref}: missing artifact has a blob row"));
        }
    }
    Ok(())
}

fn missing_artifact_paths(conn: &Connection) -> Result<Vec<(String, String)>, HarnessError> {
    let mut stmt = conn
        .prepare(
            "SELECT experiment_id, artifact_path FROM artifacts WHERE \"exists\" = 0 ORDER BY experiment_id, artifact_path",
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(from_sql)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(from_sql)
}

fn sqlite_count(conn: &Connection, sql: &str) -> Result<usize, HarnessError> {
    let value: i64 = conn
        .query_row(sql, [], |row| row.get(0))
        .map_err(from_sql)?;
    usize::try_from(value).map_err(|_| HarnessError::Sql(format!("negative count from {sql}")))
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn extract_archive(archive: &Path, destination: &Path) -> Result<(), HarnessError> {
    fs::create_dir_all(destination).map_err(from_io)?;
    let file = fs::File::open(archive).map_err(from_io)?;
    let decoder = GzDecoder::new(file);
    Archive::new(decoder).unpack(destination).map_err(from_io)
}

fn load_runs(corpus_root: &Path) -> Result<Vec<CorpusRun>, HarnessError> {
    let experiments_root = corpus_root.join("experiments_leon");
    if !experiments_root.is_dir() {
        return Err(HarnessError::Corpus(format!(
            "expected corpus root {}",
            experiments_root.display()
        )));
    }
    let mut runs = Vec::new();
    for entry in fs::read_dir(&experiments_root).map_err(from_io)? {
        let entry = entry.map_err(from_io)?;
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }
        let id = dir
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| HarnessError::Corpus("run directory is not UTF-8".to_owned()))?
            .to_owned();
        let metadata_path = dir.join("metadata.json");
        if !metadata_path.exists() {
            continue;
        }
        let metadata_bytes = fs::read(&metadata_path).map_err(from_io)?;
        let metadata = parse_json_bytes(&metadata_path, &metadata_bytes)?;
        let params_path = dir.join("params.yaml");
        let params_bytes = read_optional(&params_path)?;
        let params = params_bytes
            .as_ref()
            .map(|bytes| parse_yaml_bytes(&params_path, bytes))
            .transpose()?;
        let metrics_path = dir.join("metrics.json");
        let metrics_bytes = read_optional(&metrics_path)?;
        let metrics = metrics_bytes
            .as_ref()
            .map(|bytes| parse_metrics_bytes(&metrics_path, bytes))
            .transpose()?;
        let dependencies_path = dir.join("dependencies.json");
        let dependencies_bytes = read_optional(&dependencies_path)?;
        let dependencies = dependencies_bytes
            .as_ref()
            .map(|bytes| parse_json_bytes(&dependencies_path, bytes))
            .transpose()?;
        let artifacts = load_artifacts(&dir.join("artifacts"))?;
        runs.push(CorpusRun {
            id,
            metadata_bytes,
            metadata,
            params_bytes,
            params,
            metrics_bytes,
            metrics,
            dependencies_bytes,
            dependencies,
            artifacts,
        });
    }
    runs.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(runs)
}

fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, HarnessError> {
    if path.exists() {
        fs::read(path).map(Some).map_err(from_io)
    } else {
        Ok(None)
    }
}

fn parse_json_bytes(path: &Path, bytes: &[u8]) -> Result<Value, HarnessError> {
    serde_json::from_slice(bytes)
        .map_err(|err| HarnessError::Json(format!("{}: {err}", path.display())))
}

fn parse_yaml_bytes(path: &Path, bytes: &[u8]) -> Result<serde_yaml::Value, HarnessError> {
    serde_yaml::from_slice(bytes)
        .map_err(|err| HarnessError::Yaml(format!("{}: {err}", path.display())))
}

fn parse_metrics_bytes(path: &Path, bytes: &[u8]) -> Result<Value, HarnessError> {
    match serde_json::from_slice(bytes) {
        Ok(value) => Ok(value),
        Err(strict_err) => {
            let text = std::str::from_utf8(bytes).map_err(|err| {
                HarnessError::Json(format!(
                    "{}: {strict_err}; utf8 error: {err}",
                    path.display()
                ))
            })?;
            let Some(sanitized) = sanitize_nonfinite_json_literals(text) else {
                return Err(HarnessError::Json(format!(
                    "{}: {strict_err}",
                    path.display()
                )));
            };
            serde_json::from_str(&sanitized).map_err(|err| {
                HarnessError::Json(format!(
                    "{}: {err}; original strict error: {strict_err}",
                    path.display()
                ))
            })
        }
    }
}

fn sanitize_nonfinite_json_literals(input: &str) -> Option<String> {
    let mut output = String::with_capacity(input.len());
    let mut changed = false;
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;

    while index < input.len() {
        let ch = input[index..]
            .chars()
            .next()
            .expect("index is on a UTF-8 boundary");
        if in_string {
            output.push(ch);
            index += ch.len_utf8();
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        if ch == '"' {
            in_string = true;
            output.push(ch);
            index += ch.len_utf8();
            continue;
        }
        if let Some((literal, len)) = nonfinite_literal_at(input, index) {
            output.push('"');
            output.push_str(literal);
            output.push('"');
            index += len;
            changed = true;
            continue;
        }
        output.push(ch);
        index += ch.len_utf8();
    }

    changed.then_some(output)
}

fn nonfinite_literal_at(input: &str, index: usize) -> Option<(&'static str, usize)> {
    for literal in ["-Infinity", "Infinity", "NaN"] {
        if input[index..].starts_with(literal)
            && is_json_delimiter(input[index + literal.len()..].chars().next())
        {
            return Some((literal, literal.len()));
        }
    }
    None
}

fn is_json_delimiter(ch: Option<char>) -> bool {
    matches!(
        ch,
        None | Some(',' | '}' | ']' | ':' | ' ' | '\n' | '\r' | '\t')
    )
}

fn load_artifacts(root: &Path) -> Result<Vec<CorpusFile>, HarnessError> {
    if !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in WalkDir::new(root).into_iter() {
        let entry = entry.map_err(|err| HarnessError::Io(err.to_string()))?;
        if !entry.file_type().is_file() {
            continue;
        }
        if is_appledouble(entry.file_name()) {
            continue;
        }
        let relative = entry
            .path()
            .strip_prefix(root)
            .map_err(|err| HarnessError::Io(err.to_string()))?
            .to_string_lossy()
            .replace('\\', "/");
        files.push(CorpusFile {
            relative_path: relative,
            bytes: fs::read(entry.path()).map_err(from_io)?,
        });
    }
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(files)
}

fn is_appledouble(name: &OsStr) -> bool {
    name.to_str()
        .map(|value| value.starts_with("._"))
        .unwrap_or(false)
}

fn derive_indexes(runs: &[CorpusRun]) -> Result<DerivedIndexes, HarnessError> {
    let mut summaries = Vec::new();
    for run in runs {
        summaries.push(run_summary(run));
    }
    let mut files = BTreeMap::new();
    let experiments = serde_json::to_vec_pretty(&summaries).expect("index serializes");
    files.insert("/index/experiments.json".to_owned(), experiments);

    let mut by_status = BTreeMap::<String, Vec<RunSummary>>::new();
    let mut by_tag = BTreeMap::<String, Vec<RunSummary>>::new();
    let mut by_script = BTreeMap::<String, Vec<RunSummary>>::new();
    for summary in &summaries {
        if let Some(status) = &summary.status {
            by_status
                .entry(status.clone())
                .or_default()
                .push(summary.clone());
        }
        if let Some(script) = &summary.script {
            by_script
                .entry(script.clone())
                .or_default()
                .push(summary.clone());
        }
        for tag in &summary.tags {
            by_tag.entry(tag.clone()).or_default().push(summary.clone());
        }
    }

    for (status, values) in &by_status {
        files.insert(
            format!("/index/status/{}.json", safe_index_component(status)),
            serde_json::to_vec_pretty(values).expect("status index serializes"),
        );
    }
    for (tag, values) in &by_tag {
        files.insert(
            format!("/index/tags/{}.json", safe_index_component(tag)),
            serde_json::to_vec_pretty(values).expect("tag index serializes"),
        );
    }
    for (script, values) in &by_script {
        files.insert(
            format!("/index/scripts/{}.json", safe_index_component(script)),
            serde_json::to_vec_pretty(values).expect("script index serializes"),
        );
    }

    Ok(DerivedIndexes {
        files,
        status_counts: by_status
            .into_iter()
            .map(|(key, values)| (key, values.len()))
            .collect(),
        tag_counts: by_tag
            .into_iter()
            .map(|(key, values)| (key, values.len()))
            .collect(),
        script_counts: by_script
            .into_iter()
            .map(|(key, values)| (key, values.len()))
            .collect(),
    })
}

fn run_summary(run: &CorpusRun) -> RunSummary {
    let tags = run
        .metadata
        .pointer("/cli_args/tag")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    RunSummary {
        experiment_id: run.id.clone(),
        name: json_str(&run.metadata, "/name"),
        status: json_str(&run.metadata, "/status"),
        project: json_str(&run.metadata, "/project"),
        script: json_str(&run.metadata, "/cli_args/script").map(script_name),
        created_at: json_str(&run.metadata, "/created_at"),
        started_at: json_str(&run.metadata, "/started_at"),
        completed_at: json_str(&run.metadata, "/completed_at"),
        tags,
    }
}

fn prepare_sqlite(
    path: &Path,
    runs: &[CorpusRun],
    indexes: &DerivedIndexes,
) -> Result<(), HarnessError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(from_io)?;
    }
    remove_if_exists(path)?;
    let mut conn = Connection::open(path).map_err(from_sql)?;
    create_schema(&conn)?;
    let tx = conn.transaction().map_err(from_sql)?;
    for run in runs {
        insert_run(&tx, run)?;
    }
    for run in runs {
        insert_agentfs_run_files(&tx, run)?;
    }
    for (path, bytes) in &indexes.files {
        insert_file(&tx, path, "file", bytes)?;
    }
    insert_index_dirs(&tx, indexes)?;
    tx.commit().map_err(from_sql)?;
    Ok(())
}

fn create_schema(conn: &Connection) -> Result<(), HarnessError> {
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;
        CREATE TABLE experiments (
            experiment_id TEXT PRIMARY KEY,
            name TEXT,
            status TEXT,
            project TEXT,
            script_path TEXT,
            command TEXT,
            created_at TEXT,
            started_at TEXT,
            completed_at TEXT,
            duration REAL,
            tags_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL
        );
        CREATE TABLE params (
            experiment_id TEXT NOT NULL,
            param_path TEXT NOT NULL,
            value_json TEXT NOT NULL,
            value_type TEXT NOT NULL,
            PRIMARY KEY (experiment_id, param_path)
        );
        CREATE TABLE metrics (
            experiment_id TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            step INTEGER,
            value REAL,
            timestamp TEXT,
            value_json TEXT NOT NULL,
            PRIMARY KEY (experiment_id, metric_name, step)
        );
        CREATE TABLE dependencies (
            experiment_id TEXT NOT NULL,
            dependency_experiment_id TEXT NOT NULL,
            relation TEXT NOT NULL,
            status_at_resolution TEXT,
            metadata_json TEXT,
            PRIMARY KEY (experiment_id, dependency_experiment_id, relation)
        );
        CREATE TABLE artifacts (
            experiment_id TEXT NOT NULL,
            artifact_path TEXT NOT NULL,
            artifact_type TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            digest TEXT NOT NULL,
            blob_ref TEXT NOT NULL,
            "exists" INTEGER NOT NULL,
            PRIMARY KEY (experiment_id, artifact_path)
        );
        CREATE TABLE git_state (
            experiment_id TEXT PRIMARY KEY,
            commit_hash TEXT,
            branch TEXT,
            dirty INTEGER,
            patch_file TEXT,
            diff_blob_ref TEXT
        );
        CREATE TABLE blobs (
            blob_ref TEXT PRIMARY KEY,
            size_bytes INTEGER NOT NULL,
            digest TEXT NOT NULL,
            content BLOB NOT NULL
        );
        CREATE TABLE files (
            path TEXT PRIMARY KEY,
            file_type TEXT NOT NULL,
            size_bytes INTEGER,
            digest TEXT,
            content BLOB,
            source TEXT
        );
        CREATE INDEX idx_experiments_status ON experiments(status);
        CREATE INDEX idx_experiments_script ON experiments(script_path);
        CREATE INDEX idx_metrics_name_value ON metrics(metric_name, value);
        CREATE INDEX idx_artifacts_blob_ref ON artifacts(blob_ref);
        "#,
    )
    .map_err(from_sql)
}

fn insert_run(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let summary = run_summary(run);
    let tags_json = serde_json::to_string(&summary.tags).expect("tags serialize");
    let command = run
        .metadata
        .pointer("/cli_args")
        .map(|value| serde_json::to_string(value).expect("cli args serialize"));
    conn.execute(
        "INSERT INTO experiments VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            run.id,
            summary.name,
            summary.status,
            summary.project,
            summary.script,
            command,
            summary.created_at,
            summary.started_at,
            summary.completed_at,
            run.metadata.pointer("/duration").and_then(Value::as_f64),
            tags_json,
            String::from_utf8_lossy(&run.metadata_bytes).to_string(),
        ],
    )
    .map_err(from_sql)?;
    insert_params(conn, run)?;
    insert_metrics(conn, run)?;
    insert_dependencies(conn, run)?;
    insert_artifacts(conn, run)?;
    insert_git_state(conn, run)?;
    Ok(())
}

fn insert_params(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let Some(params) = &run.params else {
        return Ok(());
    };
    let mut flattened = Vec::new();
    flatten_yaml("", params, &mut flattened);
    for (path, value) in flattened {
        let value_json =
            serde_json::to_string(&yaml_to_json(&value)).expect("yaml json serializes");
        conn.execute(
            "INSERT INTO params VALUES (?1, ?2, ?3, ?4)",
            params![run.id, path, value_json, yaml_type(&value)],
        )
        .map_err(from_sql)?;
    }
    Ok(())
}

fn insert_metrics(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let Some(Value::Array(records)) = &run.metrics else {
        return Ok(());
    };
    for (ordinal, record) in records.iter().enumerate() {
        let Some(object) = record.as_object() else {
            continue;
        };
        let step = object
            .get("step")
            .and_then(Value::as_i64)
            .unwrap_or(ordinal as i64);
        let timestamp = object.get("timestamp").and_then(Value::as_str);
        for (name, value) in object {
            if name == "step" || name == "timestamp" {
                continue;
            }
            conn.execute(
                "INSERT OR REPLACE INTO metrics VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    run.id,
                    name,
                    step,
                    metric_real(value),
                    timestamp,
                    serde_json::to_string(value).expect("metric value serializes")
                ],
            )
            .map_err(from_sql)?;
        }
    }
    Ok(())
}

fn metric_real(value: &Value) -> Option<f64> {
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    match value.as_str() {
        Some("Infinity") => Some(f64::INFINITY),
        Some("-Infinity") => Some(f64::NEG_INFINITY),
        Some("NaN") => None,
        _ => None,
    }
}

fn insert_dependencies(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let Some(Value::Object(document)) = &run.dependencies else {
        return Ok(());
    };
    let dependencies = document
        .get("dependencies")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|map| map.iter());
    let metadata = document.get("metadata").and_then(Value::as_object);
    for (relation, dependency) in dependencies {
        let Some(dependency_id) = dependency.as_str() else {
            continue;
        };
        let metadata_json = metadata
            .and_then(|items| items.get(dependency_id))
            .map(|value| serde_json::to_string(value).expect("dependency metadata serializes"));
        let status = metadata
            .and_then(|items| items.get(dependency_id))
            .and_then(|value| value.get("status_at_resolution"))
            .and_then(Value::as_str);
        conn.execute(
            "INSERT INTO dependencies VALUES (?1, ?2, ?3, ?4, ?5)",
            params![run.id, dependency_id, relation, status, metadata_json],
        )
        .map_err(from_sql)?;
    }
    Ok(())
}

fn insert_artifacts(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let mut existing_paths = BTreeSet::new();
    for artifact in &run.artifacts {
        existing_paths.insert(artifact.relative_path.clone());
        let blob_ref = format!("artifact:{}:{}", run.id, artifact.relative_path);
        let digest = sha256_uri(&artifact.bytes);
        conn.execute(
            "INSERT INTO artifacts VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
            params![
                run.id,
                artifact.relative_path,
                artifact_type(&artifact.relative_path),
                artifact.bytes.len() as i64,
                digest,
                blob_ref,
            ],
        )
        .map_err(from_sql)?;
        conn.execute(
            "INSERT INTO blobs VALUES (?1, ?2, ?3, ?4)",
            params![
                blob_ref,
                artifact.bytes.len() as i64,
                digest,
                artifact.bytes
            ],
        )
        .map_err(from_sql)?;
    }
    for artifact_path in declared_artifact_paths(run) {
        if existing_paths.contains(&artifact_path) {
            continue;
        }
        let blob_ref = format!("artifact:{}:{artifact_path}", run.id);
        conn.execute(
            "INSERT OR IGNORE INTO artifacts VALUES (?1, ?2, ?3, 0, '', ?4, 0)",
            params![
                run.id,
                artifact_path,
                artifact_type(&artifact_path),
                blob_ref,
            ],
        )
        .map_err(from_sql)?;
    }
    Ok(())
}

fn insert_git_state(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let git = run.metadata.get("git").unwrap_or(&Value::Null);
    let patch_file = git.get("patch_file").and_then(Value::as_str);
    let diff_blob_ref = patch_file.map(|path| format!("artifact:{}:{path}", run.id));
    conn.execute(
        "INSERT INTO git_state VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            run.id,
            git.get("commit_hash").and_then(Value::as_str),
            git.get("branch").and_then(Value::as_str),
            git.get("has_uncommitted_changes")
                .and_then(Value::as_bool)
                .map(|value| if value { 1 } else { 0 }),
            patch_file,
            diff_blob_ref,
        ],
    )
    .map_err(from_sql)?;
    Ok(())
}

fn declared_artifact_paths(run: &CorpusRun) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    if let Some(patch_file) = git_patch_file(run) {
        paths.insert(patch_file);
    }
    paths
}

fn missing_artifact_ref_count(run: &CorpusRun) -> usize {
    let existing = run
        .artifacts
        .iter()
        .map(|artifact| artifact.relative_path.as_str())
        .collect::<BTreeSet<_>>();
    declared_artifact_paths(run)
        .into_iter()
        .filter(|path| !existing.contains(path.as_str()))
        .count()
}

fn git_patch_file(run: &CorpusRun) -> Option<String> {
    run.metadata
        .get("git")
        .and_then(|git| git.get("patch_file"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn insert_agentfs_run_files(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    insert_dir(conn, "/")?;
    insert_dir(conn, RUNS_PREFIX)?;
    let run_root = format!("{RUNS_PREFIX}/{}", run.id);
    insert_dir(conn, &run_root)?;
    insert_file(
        conn,
        &format!("{run_root}/metadata.json"),
        "file",
        &run.metadata_bytes,
    )?;
    if let Some(bytes) = &run.params_bytes {
        insert_file(conn, &format!("{run_root}/params.yaml"), "file", bytes)?;
    }
    if let Some(bytes) = &run.metrics_bytes {
        insert_file(conn, &format!("{run_root}/metrics.json"), "file", bytes)?;
    }
    if let Some(bytes) = &run.dependencies_bytes {
        insert_file(
            conn,
            &format!("{run_root}/dependencies.json"),
            "file",
            bytes,
        )?;
    }
    if !run.artifacts.is_empty() {
        insert_dir(conn, &format!("{run_root}/artifacts"))?;
    }
    for artifact in &run.artifacts {
        let path = format!("{run_root}/artifacts/{}", artifact.relative_path);
        insert_parent_dirs(conn, &path)?;
        insert_file(conn, &path, "file", &artifact.bytes)?;
    }
    Ok(())
}

fn insert_index_dirs(conn: &Connection, indexes: &DerivedIndexes) -> Result<(), HarnessError> {
    insert_dir(conn, "/")?;
    insert_dir(conn, "/index")?;
    insert_dir(conn, "/index/status")?;
    insert_dir(conn, "/index/tags")?;
    insert_dir(conn, "/index/scripts")?;
    for path in indexes.files.keys() {
        insert_parent_dirs(conn, path)?;
    }
    Ok(())
}

fn insert_parent_dirs(conn: &Connection, path: &str) -> Result<(), HarnessError> {
    insert_dir(conn, "/")?;
    let mut current = String::new();
    let components = path.trim_matches('/').split('/').collect::<Vec<_>>();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        current.push('/');
        current.push_str(component);
        insert_dir(conn, &current)?;
    }
    Ok(())
}

fn insert_dir(conn: &Connection, path: &str) -> Result<(), HarnessError> {
    conn.execute(
        "INSERT OR IGNORE INTO files(path, file_type, size_bytes, digest, content, source) VALUES (?1, 'directory', NULL, NULL, NULL, 'etl')",
        params![path],
    )
    .map_err(from_sql)?;
    Ok(())
}

fn insert_file(
    conn: &Connection,
    path: &str,
    file_type: &str,
    bytes: &[u8],
) -> Result<(), HarnessError> {
    insert_parent_dirs(conn, path)?;
    conn.execute(
        "INSERT OR REPLACE INTO files VALUES (?1, ?2, ?3, ?4, ?5, 'etl')",
        params![
            path,
            file_type,
            bytes.len() as i64,
            sha256_uri(bytes),
            bytes
        ],
    )
    .map_err(from_sql)?;
    Ok(())
}

fn prepare_nokv(
    meta_path: &Path,
    s3: &S3Options,
    runs: &[CorpusRun],
    indexes: &DerivedIndexes,
) -> Result<(), HarnessError> {
    if let Some(parent) = meta_path.parent() {
        fs::create_dir_all(parent).map_err(from_io)?;
    }
    fs::create_dir_all(meta_path).map_err(from_io)?;
    let metadata = HoltMetadataStore::open_file(meta_path).map_err(from_nokv)?;
    let objects = S3ObjectStore::new(s3_options(s3)).map_err(from_nokv)?;
    let service = NoKvFs::new(
        MountId::new(1).expect("mount id is non-zero"),
        metadata,
        objects,
    );
    service
        .bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(from_nokv)?;
    let repo = ArtifactRepository::with_options(
        service,
        ArtifactRepositoryOptions {
            producer: "yanex-agent-benchmark-etl".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            ..ArtifactRepositoryOptions::default()
        },
    );
    for run in runs {
        repo.put_bytes(
            &format!("{NOKV_PREFIX}/runs/{}/metadata.json", run.id),
            run.metadata_bytes.clone(),
        )
        .map_err(from_nokv)?;
        if let Some(bytes) = &run.params_bytes {
            repo.put_bytes(
                &format!("{NOKV_PREFIX}/runs/{}/params.yaml", run.id),
                bytes.clone(),
            )
            .map_err(from_nokv)?;
        }
        if let Some(bytes) = &run.metrics_bytes {
            repo.put_bytes(
                &format!("{NOKV_PREFIX}/runs/{}/metrics.json", run.id),
                bytes.clone(),
            )
            .map_err(from_nokv)?;
        }
        if let Some(bytes) = &run.dependencies_bytes {
            repo.put_bytes(
                &format!("{NOKV_PREFIX}/runs/{}/dependencies.json", run.id),
                bytes.clone(),
            )
            .map_err(from_nokv)?;
        }
        for artifact in &run.artifacts {
            repo.put_bytes(
                &format!(
                    "{NOKV_PREFIX}/runs/{}/artifacts/{}",
                    run.id, artifact.relative_path
                ),
                artifact.bytes.clone(),
            )
            .map_err(from_nokv)?;
        }
    }
    for (path, bytes) in &indexes.files {
        repo.put_bytes(&format!("{NOKV_PREFIX}{}", path), bytes.clone())
            .map_err(from_nokv)?;
    }
    Ok(())
}

fn open_existing_nokv(
    meta_path: &Path,
    s3: &S3Options,
) -> Result<NoKvFs<HoltMetadataStore, S3ObjectStore>, HarnessError> {
    let metadata = HoltMetadataStore::open_file(meta_path).map_err(from_nokv)?;
    let objects = S3ObjectStore::new(s3_options(s3)).map_err(from_nokv)?;
    NoKvFs::open_existing(
        MountId::new(1).expect("mount id is non-zero"),
        metadata,
        objects,
    )
    .map_err(from_nokv)
}

fn s3_options(s3: &S3Options) -> S3ObjectStoreOptions {
    S3ObjectStoreOptions::rustfs(
        s3.bucket.clone(),
        s3.endpoint.clone(),
        s3.access_key_id.clone(),
        s3.secret_access_key.clone(),
    )
}

fn sqlite_path(data_root: &Path) -> PathBuf {
    data_root.join("sqlite").join("yanex.db")
}

fn nokv_meta_path(data_root: &Path) -> PathBuf {
    data_root.join("nokv").join("meta")
}

fn first_experiment_id(conn: &Connection) -> Result<String, HarnessError> {
    conn.query_row(
        "SELECT experiment_id FROM experiments ORDER BY experiment_id LIMIT 1",
        [],
        |row| row.get(0),
    )
    .map_err(from_sql)
}

fn query_single_blob(conn: &Connection, sql: &str, value: &str) -> Result<Vec<u8>, HarnessError> {
    conn.query_row(sql, params![value], |row| row.get::<_, Vec<u8>>(0))
        .map_err(from_sql)
}

fn sqlite_value(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Value> {
    let value = row.get_ref(index)?;
    Ok(match value {
        rusqlite::types::ValueRef::Null => Value::Null,
        rusqlite::types::ValueRef::Integer(value) => json!(value),
        rusqlite::types::ValueRef::Real(value) => json!(value),
        rusqlite::types::ValueRef::Text(value) => {
            Value::String(String::from_utf8_lossy(value).to_string())
        }
        rusqlite::types::ValueRef::Blob(value) => json!({
            "blob_bytes": value.len(),
            "sha256": sha256_uri(value),
        }),
    })
}

fn flatten_yaml(
    prefix: &str,
    value: &serde_yaml::Value,
    out: &mut Vec<(String, serde_yaml::Value)>,
) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            for (key, child) in map {
                let Some(key) = key.as_str() else {
                    continue;
                };
                let path = if prefix.is_empty() {
                    key.to_owned()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_yaml(&path, child, out);
            }
        }
        _ => out.push((prefix.to_owned(), value.clone())),
    }
}

fn yaml_to_json(value: &serde_yaml::Value) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn yaml_type(value: &serde_yaml::Value) -> &'static str {
    match value {
        serde_yaml::Value::Null => "null",
        serde_yaml::Value::Bool(_) => "boolean",
        serde_yaml::Value::Number(_) => "number",
        serde_yaml::Value::String(_) => "string",
        serde_yaml::Value::Sequence(_) => "array",
        serde_yaml::Value::Mapping(_) => "object",
        serde_yaml::Value::Tagged(_) => "tagged",
    }
}

fn json_str(value: &Value, pointer: &str) -> Option<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn script_name(value: String) -> String {
    Path::new(&value)
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or(&value)
        .to_owned()
}

fn safe_index_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn artifact_type(path: &str) -> &'static str {
    match Path::new(path).extension().and_then(OsStr::to_str) {
        Some("json") => "json",
        Some("txt") => "text",
        Some("patch") => "patch",
        Some("yaml") | Some("yml") => "yaml",
        _ => "binary",
    }
}

fn sha256_uri(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn bytes_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("hex writes to string");
    }
    output
}

fn file_sha256(path: &Path) -> Result<String, HarnessError> {
    let mut file = fs::File::open(path).map_err(from_io)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(from_io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn remove_if_exists(path: &Path) -> Result<(), HarnessError> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_dir() {
        fs::remove_dir_all(path).map_err(from_io)
    } else {
        fs::remove_file(path).map_err(from_io)
    }
}

fn value<'a>(
    args: &'a [String],
    index: usize,
    option: &'static str,
) -> Result<&'a str, HarnessError> {
    args.get(index)
        .map(String::as_str)
        .ok_or(HarnessError::MissingOption(option))
}

fn parse_u64(raw: &str, option: &'static str) -> Result<u64, HarnessError> {
    raw.parse::<u64>().map_err(|_| HarnessError::InvalidNumber {
        option,
        value: raw.to_owned(),
    })
}

fn parse_usize(raw: &str, option: &'static str) -> Result<usize, HarnessError> {
    raw.parse::<usize>()
        .map_err(|_| HarnessError::InvalidNumber {
            option,
            value: raw.to_owned(),
        })
}

fn from_io(err: impl Error) -> HarnessError {
    HarnessError::Io(err.to_string())
}

fn from_sql(err: impl Error) -> HarnessError {
    HarnessError::Sql(err.to_string())
}

fn from_nokv(err: impl Error) -> HarnessError {
    HarnessError::NoKv(err.to_string())
}

impl fmt::Display for HarnessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingCommand => write!(f, "missing command"),
            Self::UnknownCommand(command) => write!(f, "unknown command {command}"),
            Self::MissingOption(option) => write!(f, "{option} is required"),
            Self::UnknownOption(option) => write!(f, "unknown option {option}"),
            Self::InvalidNumber { option, value } => {
                write!(f, "{option} expects a valid number, got {value}")
            }
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Json(err) => write!(f, "json error: {err}"),
            Self::Yaml(err) => write!(f, "yaml error: {err}"),
            Self::Sql(err) => write!(f, "sqlite error: {err}"),
            Self::NoKv(err) => write!(f, "nokv error: {err}"),
            Self::Corpus(err) => write!(f, "corpus error: {err}"),
            Self::Judge(err) => write!(f, "judge error: {err}"),
            Self::Http(err) => write!(f, "http error: {err}"),
            Self::ToolTimeout { tool, limit_ms } => {
                write!(f, "tool {tool} timed out after {limit_ms}ms")
            }
        }
    }
}

impl Error for HarnessError {}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::OptionalExtension;

    fn sample_run() -> CorpusRun {
        let metadata_bytes = br#"{
          "id":"run-1",
          "name":"demo",
          "status":"completed",
          "project":"origami",
          "created_at":"2026-01-01T00:00:00",
          "started_at":"2026-01-01T00:00:01",
          "completed_at":"2026-01-01T00:00:02",
          "duration":1.0,
          "cli_args":{"script":"scripts/train.py","tag":["sweep","final"]}
        }"#
        .to_vec();
        CorpusRun {
            id: "run-1".to_owned(),
            metadata: serde_json::from_slice(&metadata_bytes).unwrap(),
            metadata_bytes,
            params_bytes: Some(b"alpha: 1\nnested:\n  beta: true\n".to_vec()),
            params: Some(serde_yaml::from_slice(b"alpha: 1\nnested:\n  beta: true\n").unwrap()),
            metrics_bytes: Some(br#"[{"step":0,"accuracy":0.9,"timestamp":"now"}]"#.to_vec()),
            metrics: Some(serde_json::from_slice(br#"[{"step":0,"accuracy":0.9,"timestamp":"now"}]"#).unwrap()),
            dependencies_bytes: Some(br#"{"dependencies":{"model":"base"},"metadata":{"base":{"status_at_resolution":"completed"}}}"#.to_vec()),
            dependencies: Some(serde_json::from_slice(br#"{"dependencies":{"model":"base"},"metadata":{"base":{"status_at_resolution":"completed"}}}"#).unwrap()),
            artifacts: vec![CorpusFile {
                relative_path: "stdout.txt".to_owned(),
                bytes: b"log".to_vec(),
            }],
        }
    }

    #[test]
    fn run_summary_extracts_warm_interface_fields() {
        let summary = run_summary(&sample_run());
        assert_eq!(summary.experiment_id, "run-1");
        assert_eq!(summary.status.as_deref(), Some("completed"));
        assert_eq!(summary.script.as_deref(), Some("train.py"));
        assert_eq!(summary.tags, vec!["sweep", "final"]);
    }

    #[test]
    fn derived_indexes_use_profile_paths() {
        let indexes = derive_indexes(&[sample_run()]).unwrap();
        assert!(indexes.files.contains_key("/index/experiments.json"));
        assert!(indexes.files.contains_key("/index/status/completed.json"));
        assert!(indexes.files.contains_key("/index/tags/sweep.json"));
        assert!(indexes.files.contains_key("/index/scripts/train.py.json"));
    }

    #[test]
    fn sqlite_schema_materializes_raw_and_agentfs_views() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        let indexes = derive_indexes(std::slice::from_ref(&run)).unwrap();
        insert_run(&conn, &run).unwrap();
        insert_agentfs_run_files(&conn, &run).unwrap();
        for (path, bytes) in indexes.files {
            insert_file(&conn, &path, "file", &bytes).unwrap();
        }
        let status: String = conn
            .query_row(
                "SELECT status FROM experiments WHERE experiment_id='run-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
        let file: Option<Vec<u8>> = conn
            .query_row(
                "SELECT content FROM files WHERE path='/runs/run-1/metadata.json'",
                [],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(file.unwrap(), run.metadata_bytes);
        let metric: f64 = conn
            .query_row(
                "SELECT value FROM metrics WHERE experiment_id='run-1' AND metric_name='accuracy'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(metric, 0.9);
    }

    #[test]
    fn sqlite_verifier_checks_all_materialized_surfaces() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        let indexes = derive_indexes(std::slice::from_ref(&run)).unwrap();
        insert_run(&conn, &run).unwrap();
        insert_agentfs_run_files(&conn, &run).unwrap();
        for (path, bytes) in indexes.files {
            insert_file(&conn, &path, "file", &bytes).unwrap();
        }
        insert_index_dirs(&conn, &derive_indexes(std::slice::from_ref(&run)).unwrap()).unwrap();

        let report = verify_sqlite_materialization(&conn).unwrap();

        assert_eq!(report.run_count, 1);
        assert_eq!(report.raw_metadata_checked, 1);
        assert_eq!(report.raw_params_checked, 2);
        assert_eq!(report.raw_metrics_checked, 1);
        assert_eq!(report.raw_dependencies_checked, 1);
        assert_eq!(report.raw_existing_artifacts_checked, 1);
        assert_eq!(report.agentfs_files_checked, 5);
        assert_eq!(report.index_files_checked, 5);
        assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
    }

    #[test]
    fn sqlite_query_tool_wraps_rows_with_stable_evidence_refs() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        insert_run(&conn, &run).unwrap();

        let result = execute_sqlite_query_tool(
            &conn,
            "SELECT experiment_id, artifact_path, blob_ref FROM artifacts ORDER BY artifact_path",
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].evidence,
            vec![
                "sqlite://experiments/run-1".to_owned(),
                "sqlite://artifacts/run-1/stdout.txt".to_owned(),
                "sqlite://blobs/artifact:run-1:stdout.txt".to_owned(),
            ]
        );
    }

    #[test]
    fn sqlite_query_tool_timeout_interrupts_runaway_query() {
        let conn = Connection::open_in_memory().unwrap();
        let started = Instant::now();

        let err = execute_sqlite_query_tool_with_timeout(
            &conn,
            "WITH RECURSIVE cnt(x) AS (VALUES(0) UNION ALL SELECT x + 1 FROM cnt) SELECT count(*) FROM cnt",
            Duration::ZERO,
        )
        .unwrap_err();

        assert!(
            matches!(
                err,
                HarnessError::ToolTimeout {
                    ref tool,
                    limit_ms: 0
                } if tool == "query_sql"
            ),
            "{err:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn sqlite_read_blob_tool_returns_ranges_and_evidence() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        insert_run(&conn, &run).unwrap();

        let result = read_blob_tool(&conn, "artifact:run-1:stdout.txt", 1, 2).unwrap();

        assert_eq!(result.blob_ref, "artifact:run-1:stdout.txt");
        assert_eq!(result.offset, 1);
        assert_eq!(result.bytes_read, 2);
        assert_eq!(result.content_utf8.as_deref(), Some("og"));
        assert_eq!(
            result.evidence,
            "sqlite://blobs/artifact:run-1:stdout.txt".to_owned()
        );
    }

    #[test]
    fn missing_git_patch_is_materialized_as_absent_artifact() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let mut run = sample_run();
        run.metadata["git"] = json!({
            "has_uncommitted_changes": true,
            "patch_file": "missing.patch"
        });
        insert_run(&conn, &run).unwrap();

        let exists: Option<i64> = conn
            .query_row(
                "SELECT \"exists\" FROM artifacts WHERE experiment_id='run-1' AND artifact_path='missing.patch'",
                [],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        let blob_ref: Option<String> = conn
            .query_row(
                "SELECT diff_blob_ref FROM git_state WHERE experiment_id='run-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(exists, Some(0));
        assert_eq!(blob_ref, Some("artifact:run-1:missing.patch".to_owned()));
    }

    #[test]
    fn load_artifacts_skips_appledouble_files() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("stdout.txt"), b"log").unwrap();
        fs::write(dir.path().join("._stdout.txt"), b"appledouble").unwrap();

        let artifacts = load_artifacts(dir.path()).unwrap();

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].relative_path, "stdout.txt");
        assert_eq!(artifacts[0].bytes, b"log");
    }

    #[test]
    fn json_parse_error_includes_path() {
        let path = PathBuf::from("/tmp/metrics.json");
        let err = parse_json_bytes(&path, br#"{"value": NaN}"#).unwrap_err();

        assert!(err.to_string().contains("/tmp/metrics.json"));
        assert!(err.to_string().contains("expected value"));
    }

    #[test]
    fn metrics_parser_accepts_nonfinite_values() {
        let path = PathBuf::from("/tmp/metrics.json");
        let metrics = parse_metrics_bytes(
            &path,
            br#"[{"val_loss": Infinity, "other": -Infinity, "note": "Infinity"}]"#,
        )
        .unwrap();
        let record = metrics.as_array().unwrap()[0].as_object().unwrap();

        assert_eq!(metric_real(&record["val_loss"]), Some(f64::INFINITY));
        assert_eq!(metric_real(&record["other"]), Some(f64::NEG_INFINITY));
        assert_eq!(record["note"].as_str(), Some("Infinity"));
    }

    #[test]
    fn index_component_sanitizes_path_separators() {
        assert_eq!(safe_index_component("scripts/train.py"), "scripts_train.py");
    }

    #[test]
    fn agentfs_tools_inspect_sqlite_projection_without_raw_sql() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        let indexes = derive_indexes(std::slice::from_ref(&run)).unwrap();
        insert_run(&conn, &run).unwrap();
        insert_agentfs_run_files(&conn, &run).unwrap();
        for (path, bytes) in indexes.files {
            insert_file(&conn, &path, "file", &bytes).unwrap();
        }
        insert_index_dirs(&conn, &derive_indexes(std::slice::from_ref(&run)).unwrap()).unwrap();

        let list = agentfs_list_tool(&conn, "/runs/run-1").unwrap();
        assert!(list
            .entries
            .iter()
            .any(|entry| entry.name == "metadata.json"));
        assert!(list.entries.iter().any(|entry| entry.name == "artifacts"));

        let stat = agentfs_stat_tool(&conn, "/runs/run-1/metrics.json").unwrap();
        assert_eq!(stat.file_type, "file");
        assert_eq!(stat.evidence, "agentfs:///runs/run-1/metrics.json");

        let read = agentfs_read_tool(&conn, "/runs/run-1/artifacts/stdout.txt", 1, 2).unwrap();
        assert_eq!(read.content_utf8.as_deref(), Some("og"));
        assert_eq!(read.bytes_read, 2);

        let grep = agentfs_grep_tool(&conn, "/runs/run-1", "accuracy", true).unwrap();
        assert_eq!(grep.matches.len(), 1);
        assert_eq!(grep.matches[0].path, "/runs/run-1/metrics.json");

        let find = agentfs_find_tool(&conn, "/", "metrics.json").unwrap();
        assert!(find
            .matches
            .iter()
            .any(|entry| entry.path == "/runs/run-1/metrics.json"));
    }

    #[test]
    fn tool_registry_exposes_only_the_selected_arm_surface() {
        assert_eq!(
            tool_names(&tool_registry_for_arm("sqlite_raw_v1").unwrap()),
            vec!["show_schema", "query_sql", "read_blob"]
        );
        assert_eq!(
            tool_names(&tool_registry_for_arm("sqlite_agentfs_v1").unwrap()),
            vec!["ls", "stat", "read", "grep", "find"]
        );
        assert_eq!(
            tool_names(&tool_registry_for_arm("nokv_posix_v1").unwrap()),
            vec!["ls", "stat", "read", "grep", "find"]
        );
        assert_eq!(
            tool_names(&tool_registry_for_arm("nokv_native_v1").unwrap()),
            vec!["list", "stat", "read"]
        );
    }

    #[test]
    fn phase1_tasks_are_fixed_to_ten_read_only_prompts() {
        let task_set = phase1_task_set().unwrap();

        assert_eq!(task_set.tasks.len(), 10);
        for task in &task_set.tasks {
            assert!(!task.prompt.trim().is_empty());
            assert!(!task.gold_sql.trim().is_empty());
            let lower = task.prompt.to_ascii_lowercase();
            assert!(!lower.contains("watch"));
            assert!(!lower.contains("snapshot"));
            assert!(!lower.contains("overwrite"));
        }
    }

    #[test]
    fn readme_documents_only_two_core_valid_ab_comparisons() {
        let readme = include_str!("../../README.md");

        assert!(readme.contains("## Valid Comparisons"));
        assert!(readme.contains("Raw SQLite tools vs NoKV Native Namespace"));
        assert!(readme.contains("SQLite AgentFS vs NoKV POSIX FUSE"));
        assert!(readme.contains("sensitivity/context"));
    }

    #[test]
    fn batch_plan_defaults_to_four_arms_ten_tasks_ten_repeats() {
        let tasks = phase1_task_set().unwrap();
        let plan = batch_plan(None, None, None, &tasks);

        assert_eq!(default_repeats(), 10);
        assert_eq!(plan.len(), 4 * 10 * 10);
        assert_eq!(plan[0].arm_id, "sqlite_raw_v1");
        assert_eq!(plan[0].task_id, "status_counts");
        assert_eq!(plan[0].repeat_index, 0);
        assert_eq!(plan[9].repeat_index, 9);
        assert_eq!(plan[10].task_id, "completed_scripts_top5");
    }

    #[test]
    fn batch_plan_repeats_every_task_for_selected_arm() {
        let tasks = phase1_task_set().unwrap();
        let plan = batch_plan(Some("nokv_native_v1"), None, Some(5), &tasks);

        assert_eq!(plan.len(), 10 * 5);
        assert_eq!(plan[0].task_id, "status_counts");
        assert_eq!(plan[0].repeat_index, 0);
        assert_eq!(plan[4].task_id, "status_counts");
        assert_eq!(plan[4].repeat_index, 4);
        assert_eq!(plan[5].task_id, "completed_scripts_top5");
        assert_eq!(plan[5].repeat_index, 0);
        assert_eq!(plan[49].task_id, "stdout_availability_by_script");
        assert_eq!(plan[49].repeat_index, 4);
    }

    #[test]
    fn repeat_option_alias_sets_repeat_count_not_task_count() {
        let options = Options::parse(&["--repeat".to_owned(), "5".to_owned()]).unwrap();

        assert_eq!(options.repeats, Some(5));
    }

    #[test]
    fn local_judge_checks_gold_rows_run_ids_and_evidence_prefixes() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        insert_run(&conn, &run).unwrap();
        let task = BenchmarkTask {
            task_id: "unit".to_owned(),
            category: "structured_analytics".to_owned(),
            prompt: "return the run".to_owned(),
            gold_sql: "SELECT experiment_id, status FROM experiments ORDER BY experiment_id"
                .to_owned(),
            expected: ExpectedSpec {
                kind: "rows_exact".to_owned(),
                answer_key: Some("runs".to_owned()),
                run_id_column: Some("experiment_id".to_owned()),
                columns: vec!["experiment_id".to_owned(), "status".to_owned()],
            },
        };
        let answer = json!({
            "task_id": "unit",
            "status": "success",
            "answer": {
                "runs": [{"experiment_id": "run-1", "status": "completed"}]
            },
            "run_ids": ["run-1"],
            "evidence": [{"handle": "sqlite://experiments/run-1", "claim": "status row"}],
            "missing_evidence": [],
            "operations_summary": {"tool_call_count": 1, "bytes_read_estimate": 0},
            "confidence": "high"
        });

        let result = judge_answer(&conn, &task, "sqlite_raw_v1", &answer).unwrap();

        assert!(result.task_success, "{result:?}");
        assert_eq!(result.evidence_precision, Some(1.0));
        assert_eq!(result.expected_run_ids, vec!["run-1"]);
    }

    #[test]
    fn telemetry_records_cost_correctness_and_tool_metrics_as_jsonl() {
        let telemetry = BenchmarkRunTelemetry::sample_for_test();
        let line = telemetry.to_jsonl_line();
        let parsed: Value = serde_json::from_str(&line).unwrap();

        assert_eq!(parsed["record_type"], "benchmark_run");
        assert_eq!(parsed["run_error"], Value::Null);
        assert_eq!(parsed["derived_metrics"]["tool_call_count"], 1);
        assert_eq!(parsed["derived_metrics"]["invalid_sql_count"], 0);
        assert_eq!(parsed["derived_metrics"]["wrong_tool_count"], 0);
        assert_eq!(parsed["derived_metrics"]["tool_timeout_count"], 0);
        assert_eq!(parsed["derived_metrics"]["missing_evidence_count"], 0);
        assert_eq!(parsed["derived_metrics"]["overread_bytes"], 0);
        assert_eq!(parsed["judge"]["task_success"], true);
    }

    #[test]
    fn tool_call_start_record_serializes_before_completion() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl = dir.path().join("telemetry.jsonl");
        let call = OpenAiToolCall {
            id: "call-test".to_owned(),
            tool_type: "function".to_owned(),
            function: OpenAiToolCallFunction {
                name: "query_sql".to_owned(),
                arguments: r#"{"sql":"SELECT 1"}"#.to_owned(),
            },
        };

        append_tool_call_start_jsonl(
            &jsonl,
            "run-test",
            "sqlite_raw_v1",
            "status_counts",
            2,
            &call,
            123,
        )
        .unwrap();

        let text = fs::read_to_string(jsonl).unwrap();
        let parsed: Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(parsed["record_type"], "tool_call_start");
        assert_eq!(parsed["run_id"], "run-test");
        assert_eq!(parsed["tool_name"], "query_sql");
        assert_eq!(parsed["arguments"]["sql"], "SELECT 1");
    }

    #[test]
    fn only_context_length_http_errors_are_recoverable_run_errors() {
        assert!(is_recoverable_run_api_error(&HarnessError::Http(
            "OpenAI chat completion HTTP 400 Bad Request: context_length_exceeded".to_owned()
        )));
        assert!(is_recoverable_run_api_error(&HarnessError::Http(
            "Input tokens exceed the configured limit of 272000 tokens".to_owned()
        )));
        assert!(is_recoverable_run_api_error(&HarnessError::Http(
            "model did not produce a final JSON answer before max_turns".to_owned()
        )));
        assert!(!is_recoverable_run_api_error(&HarnessError::Http(
            "OpenAI chat completion HTTP 500 Internal Server Error".to_owned()
        )));
    }

    #[test]
    fn judge_shape_errors_are_recoverable_run_errors() {
        assert!(is_recoverable_run_judge_error(&HarnessError::Judge(
            "answer.groups must be an array".to_owned()
        )));
        assert!(!is_recoverable_run_judge_error(&HarnessError::NoKv(
            "metadata store failed".to_owned()
        )));
    }

    fn tool_names(tools: &[ToolDefinition]) -> Vec<&str> {
        tools.iter().map(|tool| tool.name.as_str()).collect()
    }
}
