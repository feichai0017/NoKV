use nokvfs_meta::{
    MetadataStore, NamespaceBodyDescriptor, NamespaceCard, NamespaceCardKind,
    NamespaceFilterCapability, NamespaceFindField, NamespaceFindRequest, NamespaceFindResult,
    NamespaceInclude, NamespaceIndexValue, NamespaceListOptions, NamespaceListPage,
    NamespacePredicate, NamespacePredicateOp, NamespacePredicateValue, NamespaceQueryCatalog,
    NamespaceReadFormat, NamespaceReadItem, NamespaceReadOptions, NamespaceReadPage,
    NamespaceRecordCount, NamespaceRecordType, NamespaceSchema, NamespaceSort,
    NamespaceSortDirection, NamespaceSortField, NoKvFs, RecordCountProvenance,
};
use nokvfs_object::ObjectStore;
use serde_json::{json, Map, Value};

use crate::{ClientError, MetadataClient, NoKvFsClient};

const DEFAULT_AGENT_LIMIT: usize = 100;
const MAX_AGENT_PAGE_LIMIT: usize = 100;

#[derive(Clone, Debug, PartialEq)]
pub struct AgentToolDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub parameters: Value,
}

pub trait AgentNamespace {
    fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, ClientError>;

    fn agent_list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, ClientError>;

    fn agent_find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, ClientError>;

    fn agent_read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, ClientError>;
}

impl AgentNamespace for MetadataClient {
    fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, ClientError> {
        self.stat_card(path)
    }

    fn agent_list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, ClientError> {
        self.list_page(path, options)
    }

    fn agent_find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, ClientError> {
        self.find_paths(request)
    }

    fn agent_read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, ClientError> {
        self.read_page(path, options)
    }
}

impl<O> AgentNamespace for NoKvFsClient<O>
where
    O: ObjectStore,
{
    fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, ClientError> {
        self.stat_card(path)
    }

    fn agent_list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, ClientError> {
        self.list_page(path, options)
    }

    fn agent_find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, ClientError> {
        self.find_paths(request)
    }

    fn agent_read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, ClientError> {
        self.read_page(path, options)
    }
}

impl<M, O> AgentNamespace for NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, ClientError> {
        self.stat_card(path).map_err(ClientError::Metadata)
    }

    fn agent_list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, ClientError> {
        self.list_page(path, options).map_err(ClientError::Metadata)
    }

    fn agent_find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, ClientError> {
        self.find_paths(request).map_err(ClientError::Metadata)
    }

    fn agent_read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, ClientError> {
        self.read_page(path, options).map_err(ClientError::Metadata)
    }
}

pub fn agent_tool_definitions() -> Vec<AgentToolDefinition> {
    vec![
        AgentToolDefinition {
            name: "ls",
            description: "List one namespace page under a path.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_AGENT_PAGE_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "stat",
            description: "Return a namespace card for one path.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "read",
            description: "Read a structured page by default, or raw bytes when format is bytes.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "format": {"type": "string", "enum": ["structured", "bytes"]},
                    "cursor": {"type": ["string", "null"]},
                    "offset": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_AGENT_PAGE_LIMIT},
                    "expected_generation": {"type": ["integer", "null"], "minimum": 1}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "find",
            description:
                "Find paths using catalog field ids; returns match_count, so use limit 1 for counts.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "predicates": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["field", "op", "value"],
                            "properties": {
                                "field": {"type": "string"},
                                "op": {
                                    "type": "string",
                                    "enum": ["eq", "prefix", "suffix", "contains", "gt", "gte", "lt", "lte"]
                                },
                                "value": {"type": ["string", "integer", "boolean"], "minimum": 0}
                            },
                            "additionalProperties": false
                        }
                    },
                    "sort": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["field"],
                            "properties": {
                                "field": {"type": "string"},
                                "direction": {"type": "string", "enum": ["asc", "desc"]}
                            },
                            "additionalProperties": false
                        }
                    },
                    "include": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["body", "schema", "sample", "catalog"]}
                    },
                    "fields": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_AGENT_PAGE_LIMIT}
                },
                "additionalProperties": false
            }),
        },
    ]
}

pub fn execute_agent_tool<T>(namespace: &T, name: &str, args: &Value) -> Result<Value, ClientError>
where
    T: AgentNamespace + ?Sized,
{
    match name {
        "ls" => execute_ls(namespace, args),
        "stat" => execute_stat(namespace, args),
        "read" => execute_read(namespace, args),
        "find" => execute_find(namespace, args),
        other => Err(ClientError::Protocol(format!("unknown agent tool {other}"))),
    }
}

fn execute_ls<T>(namespace: &T, args: &Value) -> Result<Value, ClientError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let page = namespace.agent_list_page(
        path,
        NamespaceListOptions {
            cursor: optional_string_arg(args, "cursor")?,
            limit: optional_usize_arg(args, "limit")?.unwrap_or(DEFAULT_AGENT_LIMIT),
        },
    )?;
    Ok(list_page_json(&page))
}

fn execute_stat<T>(namespace: &T, args: &Value) -> Result<Value, ClientError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let card = namespace
        .agent_stat_card(path)?
        .ok_or_else(|| ClientError::NotFound(path.to_owned()))?;
    Ok(json!({
        "tool": "stat",
        "bytes_read": 0,
        "card": card_json(&card),
    }))
}

fn execute_read<T>(namespace: &T, args: &Value) -> Result<Value, ClientError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let options = NamespaceReadOptions {
        format: read_format_arg(args)?,
        cursor: optional_string_arg(args, "cursor")?,
        offset: optional_u64_arg(args, "offset")?.unwrap_or(0),
        limit: optional_usize_arg(args, "limit")?.unwrap_or(DEFAULT_AGENT_LIMIT),
        expected_generation: optional_u64_arg(args, "expected_generation")?,
    };
    guard_large_structured_pagination(namespace, path, &options)?;
    let page = namespace.agent_read_page(path, options)?;
    Ok(read_page_json(&page))
}

fn guard_large_structured_pagination<T>(
    namespace: &T,
    path: &str,
    options: &NamespaceReadOptions,
) -> Result<(), ClientError>
where
    T: AgentNamespace + ?Sized,
{
    if !matches!(options.format, NamespaceReadFormat::Structured) {
        return Ok(());
    }
    let Some(card) = namespace.agent_stat_card(path)? else {
        return Ok(());
    };
    let Some(record_count) = card.record_count else {
        return Ok(());
    };
    if record_count.count <= MAX_AGENT_PAGE_LIMIT {
        return Ok(());
    }
    Err(ClientError::Protocol(format!(
        "structured pagination for {path} has {} records; use stat record_count or find with catalog predicates and limit=1, then read match_count",
        record_count.count
    )))
}

fn execute_find<T>(namespace: &T, args: &Value) -> Result<Value, ClientError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let include = include_arg(args)?;
    let fields = fields_arg(args)?;
    let result = namespace.agent_find_paths(NamespaceFindRequest {
        path: path.to_owned(),
        predicates: predicates_arg(args)?,
        sort: sort_arg(args)?,
        include: include.clone(),
        cursor: optional_string_arg(args, "cursor")?,
        limit: optional_usize_arg(args, "limit")?.unwrap_or(DEFAULT_AGENT_LIMIT),
    })?;
    Ok(find_result_json(&result, &include, fields.as_deref()))
}

fn list_page_json(page: &NamespaceListPage) -> Value {
    json!({
        "tool": "ls",
        "bytes_read": 0,
        "path": page.path,
        "evidence": page.evidence,
        "snapshot_id": page.snapshot_id,
        "entry_count": page.entry_count,
        "entries": page.entries.iter().map(list_entry_json).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor,
        "truncated": page.truncated,
    })
}

fn find_result_json(
    result: &NamespaceFindResult,
    includes: &[NamespaceInclude],
    fields: Option<&[String]>,
) -> Value {
    json!({
        "tool": "find",
        "bytes_read": 0,
        "path": result.path,
        "evidence": result.evidence,
        "snapshot_id": result.snapshot_id,
        "match_count": result.match_count,
        "matches": result.matches.iter().map(|card| find_match_json(card, fields, includes)).collect::<Vec<_>>(),
        "next_cursor": result.next_cursor,
        "truncated": result.truncated,
        "scanned_entries": result.scanned_entries,
    })
}

fn read_page_json(page: &NamespaceReadPage) -> Value {
    let bytes_read = page
        .bytes
        .as_ref()
        .map(|bytes| bytes.len())
        .unwrap_or_else(|| page.items.iter().map(|item| item.value_json.len()).sum());
    json!({
        "tool": "read",
        "bytes_read": bytes_read,
        "path": page.path,
        "evidence": page.evidence,
        "snapshot_id": page.snapshot_id,
        "generation": page.generation,
        "total_size_bytes": page.total_size_bytes,
        "format": read_format_name(&page.format),
        "record_type": page.record_type.as_ref().map(record_type_name),
        "record_count": page.record_count,
        "cursor": page.cursor,
        "next_cursor": page.next_cursor,
        "truncated": page.truncated,
        "items": page.items.iter().map(read_item_json).collect::<Vec<_>>(),
        "bytes": page.bytes,
    })
}

fn card_json(card: &NamespaceCard) -> Value {
    json!({
        "path": card.path,
        "name": card.name,
        "kind": card_kind_name(&card.kind),
        "evidence": card.evidence,
        "snapshot_id": card.snapshot_id,
        "inode": card.inode.get(),
        "generation": card.generation,
        "size_bytes": card.size_bytes,
        "entry_count": card.entry_count,
        "record_count": card.record_count.as_ref().map(record_count_json),
        "schema": card.schema.as_ref().map(schema_json),
        "sample": card.sample,
        "body": card.body.as_ref().map(body_json),
        "catalog": catalog_json(&card.catalog),
        "indexed_values": card.indexed_values.iter().map(index_value_json).collect::<Vec<_>>(),
    })
}

fn list_entry_json(card: &NamespaceCard) -> Value {
    json!({
        "path": card.path,
        "name": card.name,
        "kind": card_kind_name(&card.kind),
        "evidence": card.evidence,
        "snapshot_id": card.snapshot_id,
        "inode": card.inode.get(),
        "generation": card.generation,
        "size_bytes": card.size_bytes,
        "entry_count": card.entry_count,
    })
}

fn find_match_json(
    card: &NamespaceCard,
    fields: Option<&[String]>,
    includes: &[NamespaceInclude],
) -> Value {
    let indexed_values = card
        .indexed_values
        .iter()
        .filter(|value| {
            fields
                .map(|fields| fields.iter().any(|field| field == &value.field.id))
                .unwrap_or(true)
        })
        .map(index_value_json)
        .collect::<Vec<_>>();
    if fields.is_some() {
        let mut object = Map::new();
        object.insert("path".to_owned(), json!(card.path));
        object.insert(
            "values".to_owned(),
            projected_values_json(card, fields.unwrap_or(&[])),
        );
        append_find_match_includes(&mut object, card, includes);
        return Value::Object(object);
    }
    let mut object = Map::new();
    object.insert("path".to_owned(), json!(card.path));
    object.insert("name".to_owned(), json!(card.name));
    object.insert("kind".to_owned(), json!(card_kind_name(&card.kind)));
    object.insert("evidence".to_owned(), json!(card.evidence));
    object.insert("snapshot_id".to_owned(), json!(card.snapshot_id));
    object.insert("inode".to_owned(), json!(card.inode.get()));
    object.insert("generation".to_owned(), json!(card.generation));
    object.insert("size_bytes".to_owned(), json!(card.size_bytes));
    object.insert("entry_count".to_owned(), json!(card.entry_count));
    object.insert("indexed_values".to_owned(), Value::Array(indexed_values));
    append_find_match_includes(&mut object, card, includes);
    Value::Object(object)
}

fn append_find_match_includes(
    object: &mut Map<String, Value>,
    card: &NamespaceCard,
    includes: &[NamespaceInclude],
) {
    for include in includes {
        match include {
            NamespaceInclude::Body => {
                object.insert(
                    "body".to_owned(),
                    card.body.as_ref().map(body_json).unwrap_or(Value::Null),
                );
            }
            NamespaceInclude::Schema => {
                object.insert(
                    "schema".to_owned(),
                    card.schema.as_ref().map(schema_json).unwrap_or(Value::Null),
                );
            }
            NamespaceInclude::Sample => {
                object.insert("sample".to_owned(), json!(card.sample));
            }
            NamespaceInclude::Catalog => {
                object.insert("catalog".to_owned(), catalog_json(&card.catalog));
            }
        }
    }
}

fn projected_values_json(card: &NamespaceCard, fields: &[String]) -> Value {
    let mut output = Map::new();
    for field in fields {
        let values = card
            .indexed_values
            .iter()
            .filter(|value| &value.field.id == field)
            .map(|value| predicate_value_json(&value.value))
            .collect::<Vec<_>>();
        match values.as_slice() {
            [] => {}
            [value] => {
                output.insert(field.clone(), value.clone());
            }
            _ => {
                output.insert(field.clone(), Value::Array(values));
            }
        }
    }
    Value::Object(output)
}

fn record_count_json(count: &NamespaceRecordCount) -> Value {
    json!({
        "count": count.count,
        "provenance": match count.provenance {
            RecordCountProvenance::LiveNamespace => "live_namespace",
            RecordCountProvenance::StructuredBody => "structured_body",
            RecordCountProvenance::MaterializedIndex => "materialized_index",
            RecordCountProvenance::Approximate => "approximate",
        }
    })
}

fn schema_json(schema: &NamespaceSchema) -> Value {
    json!({
        "record_type": record_type_name(&schema.record_type),
        "fields": schema.fields,
    })
}

fn body_json(body: &NamespaceBodyDescriptor) -> Value {
    json!({
        "producer": body.producer,
        "digest_uri": body.digest_uri,
        "size": body.size,
        "content_type": body.content_type,
        "manifest_id": body.manifest_id,
        "generation": body.generation,
        "chunk_size": body.chunk_size,
        "block_size": body.block_size,
    })
}

fn catalog_json(catalog: &NamespaceQueryCatalog) -> Value {
    json!({
        "filterable": catalog.filterable.iter().map(filter_capability_json).collect::<Vec<_>>(),
        "sortable": catalog.sortable.iter().map(|field| field.id.clone()).collect::<Vec<_>>(),
        "facetable": catalog.facetable.iter().map(|field| field.id.clone()).collect::<Vec<_>>(),
        "projections": catalog.projections.iter().map(include_name).collect::<Vec<_>>(),
    })
}

fn filter_capability_json(capability: &NamespaceFilterCapability) -> Value {
    json!({
        "field": capability.field.id,
        "operators": capability.operators.iter().map(predicate_op_name).collect::<Vec<_>>(),
    })
}

fn index_value_json(value: &NamespaceIndexValue) -> Value {
    json!({
        "field": value.field.id,
        "value": predicate_value_json(&value.value),
    })
}

fn predicate_value_json(value: &NamespacePredicateValue) -> Value {
    match value {
        NamespacePredicateValue::String(value) => json!(value),
        NamespacePredicateValue::U64(value) => json!(value),
    }
}

fn read_item_json(item: &NamespaceReadItem) -> Value {
    let value = serde_json::from_str::<Value>(&item.value_json)
        .unwrap_or_else(|_| Value::String(item.value_json.clone()));
    json!({
        "index": item.index,
        "value": value,
        "evidence": item.evidence,
    })
}

fn object_args(args: &Value) -> Result<&Map<String, Value>, ClientError> {
    args.as_object().ok_or_else(|| {
        ClientError::Protocol("agent tool arguments must be a JSON object".to_owned())
    })
}

fn required_string_arg<'a>(args: &'a Value, name: &'static str) -> Result<&'a str, ClientError> {
    object_args(args)?
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| ClientError::Protocol(format!("missing string argument {name}")))
}

fn optional_string_arg(args: &Value, name: &'static str) -> Result<Option<String>, ClientError> {
    let Some(value) = object_args(args)?.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|value| Some(value.to_owned()))
        .ok_or_else(|| ClientError::Protocol(format!("{name} must be a string or null")))
}

fn optional_u64_arg(args: &Value, name: &'static str) -> Result<Option<u64>, ClientError> {
    let Some(value) = object_args(args)?.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| ClientError::Protocol(format!("{name} must be a non-negative integer")))
}

fn optional_usize_arg(args: &Value, name: &'static str) -> Result<Option<usize>, ClientError> {
    optional_u64_arg(args, name)?
        .map(|value| {
            let value = usize::try_from(value)
                .map_err(|_| ClientError::Protocol(format!("{name} exceeds platform limit")))?;
            if value == 0 || value > MAX_AGENT_PAGE_LIMIT {
                return Err(ClientError::Protocol(format!(
                    "{name} must be between 1 and {MAX_AGENT_PAGE_LIMIT}"
                )));
            }
            Ok(value)
        })
        .transpose()
}

fn read_format_arg(args: &Value) -> Result<NamespaceReadFormat, ClientError> {
    match object_args(args)?.get("format").and_then(Value::as_str) {
        None | Some("structured") => Ok(NamespaceReadFormat::Structured),
        Some("bytes") => Ok(NamespaceReadFormat::Bytes),
        Some(other) => Err(ClientError::Protocol(format!(
            "unsupported read format {other}; expected structured or bytes"
        ))),
    }
}

fn predicates_arg(args: &Value) -> Result<Vec<NamespacePredicate>, ClientError> {
    let Some(value) = object_args(args)?.get("predicates") else {
        return Ok(Vec::new());
    };
    let predicates = value
        .as_array()
        .ok_or_else(|| ClientError::Protocol("predicates must be an array".to_owned()))?;
    predicates.iter().map(predicate_arg).collect()
}

fn predicate_arg(value: &Value) -> Result<NamespacePredicate, ClientError> {
    let object = value
        .as_object()
        .ok_or_else(|| ClientError::Protocol("predicate must be an object".to_owned()))?;
    let field = string_property(object, "field")?;
    let op = string_property(object, "op")?;
    let value = object
        .get("value")
        .ok_or_else(|| ClientError::Protocol("predicate is missing value".to_owned()))?;
    Ok(NamespacePredicate {
        field: NamespaceFindField::new(field),
        op: predicate_op_arg(op)?,
        value: predicate_value_arg(value)?,
    })
}

fn sort_arg(args: &Value) -> Result<Vec<NamespaceSort>, ClientError> {
    let Some(value) = object_args(args)?.get("sort") else {
        return Ok(Vec::new());
    };
    let sort = value
        .as_array()
        .ok_or_else(|| ClientError::Protocol("sort must be an array".to_owned()))?;
    sort.iter().map(sort_item_arg).collect()
}

fn sort_item_arg(value: &Value) -> Result<NamespaceSort, ClientError> {
    let object = value
        .as_object()
        .ok_or_else(|| ClientError::Protocol("sort item must be an object".to_owned()))?;
    let field = string_property(object, "field")?;
    let direction = object
        .get("direction")
        .and_then(Value::as_str)
        .unwrap_or("asc");
    Ok(NamespaceSort {
        field: NamespaceSortField::new(field),
        direction: match direction {
            "asc" => NamespaceSortDirection::Asc,
            "desc" => NamespaceSortDirection::Desc,
            other => {
                return Err(ClientError::Protocol(format!(
                    "unsupported sort direction {other}"
                )))
            }
        },
    })
}

fn include_arg(args: &Value) -> Result<Vec<NamespaceInclude>, ClientError> {
    let Some(value) = object_args(args)?.get("include") else {
        return Ok(Vec::new());
    };
    let includes = value
        .as_array()
        .ok_or_else(|| ClientError::Protocol("include must be an array".to_owned()))?;
    includes
        .iter()
        .map(|value| {
            let Some(include) = value.as_str() else {
                return Err(ClientError::Protocol(
                    "include entries must be strings".to_owned(),
                ));
            };
            match include {
                "body" => Ok(NamespaceInclude::Body),
                "schema" => Ok(NamespaceInclude::Schema),
                "sample" => Ok(NamespaceInclude::Sample),
                "catalog" => Ok(NamespaceInclude::Catalog),
                other => Err(ClientError::Protocol(format!(
                    "unsupported include projection {other}"
                ))),
            }
        })
        .collect()
}

fn fields_arg(args: &Value) -> Result<Option<Vec<String>>, ClientError> {
    let Some(value) = object_args(args)?.get("fields") else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let fields = value
        .as_array()
        .ok_or_else(|| ClientError::Protocol("fields must be an array".to_owned()))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| ClientError::Protocol("fields entries must be strings".to_owned()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(fields))
}

fn string_property<'a>(
    object: &'a Map<String, Value>,
    name: &'static str,
) -> Result<&'a str, ClientError> {
    object
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| ClientError::Protocol(format!("missing string property {name}")))
}

fn predicate_op_arg(op: &str) -> Result<NamespacePredicateOp, ClientError> {
    match op {
        "eq" => Ok(NamespacePredicateOp::Eq),
        "prefix" => Ok(NamespacePredicateOp::Prefix),
        "suffix" => Ok(NamespacePredicateOp::Suffix),
        "contains" => Ok(NamespacePredicateOp::Contains),
        "gt" | "greater_than" => Ok(NamespacePredicateOp::GreaterThan),
        "gte" | "greater_than_or_equal" => Ok(NamespacePredicateOp::GreaterThanOrEqual),
        "lt" | "less_than" => Ok(NamespacePredicateOp::LessThan),
        "lte" | "less_than_or_equal" => Ok(NamespacePredicateOp::LessThanOrEqual),
        other => Err(ClientError::Protocol(format!(
            "unsupported predicate operator {other}"
        ))),
    }
}

fn predicate_value_arg(value: &Value) -> Result<NamespacePredicateValue, ClientError> {
    if let Some(value) = value.as_str() {
        return Ok(NamespacePredicateValue::String(value.to_owned()));
    }
    if let Some(value) = value.as_bool() {
        return Ok(NamespacePredicateValue::U64(u64::from(value)));
    }
    if let Some(value) = value.as_u64() {
        return Ok(NamespacePredicateValue::U64(value));
    }
    Err(ClientError::Protocol(
        "predicate value must be a string, boolean, or non-negative integer".to_owned(),
    ))
}

fn card_kind_name(kind: &NamespaceCardKind) -> &'static str {
    match kind {
        NamespaceCardKind::File => "file",
        NamespaceCardKind::Directory => "directory",
        NamespaceCardKind::Symlink => "symlink",
    }
}

fn record_type_name(record_type: &NamespaceRecordType) -> &'static str {
    match record_type {
        NamespaceRecordType::DirectoryEntries => "directory_entries",
        NamespaceRecordType::JsonArray => "json_array",
        NamespaceRecordType::JsonObject => "json_object",
        NamespaceRecordType::YamlMapping => "yaml_mapping",
        NamespaceRecordType::TextLines => "text_lines",
    }
}

fn read_format_name(format: &NamespaceReadFormat) -> &'static str {
    match format {
        NamespaceReadFormat::Structured => "structured",
        NamespaceReadFormat::Bytes => "bytes",
    }
}

fn include_name(include: &NamespaceInclude) -> &'static str {
    match include {
        NamespaceInclude::Body => "body",
        NamespaceInclude::Schema => "schema",
        NamespaceInclude::Sample => "sample",
        NamespaceInclude::Catalog => "catalog",
    }
}

fn predicate_op_name(op: &NamespacePredicateOp) -> &'static str {
    match op {
        NamespacePredicateOp::Eq => "eq",
        NamespacePredicateOp::Prefix => "prefix",
        NamespacePredicateOp::Suffix => "suffix",
        NamespacePredicateOp::Contains => "contains",
        NamespacePredicateOp::GreaterThan => "gt",
        NamespacePredicateOp::GreaterThanOrEqual => "gte",
        NamespacePredicateOp::LessThan => "lt",
        NamespacePredicateOp::LessThanOrEqual => "lte",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_meta::NamespaceRecordCount;
    use nokvfs_types::InodeId;
    use std::cell::RefCell;

    struct FakeNamespace {
        last_find: RefCell<Option<NamespaceFindRequest>>,
        read_calls: RefCell<usize>,
        record_count: usize,
    }

    impl FakeNamespace {
        fn new() -> Self {
            Self {
                last_find: RefCell::new(None),
                read_calls: RefCell::new(0),
                record_count: 1,
            }
        }

        fn with_record_count(record_count: usize) -> Self {
            Self {
                last_find: RefCell::new(None),
                read_calls: RefCell::new(0),
                record_count,
            }
        }
    }

    impl AgentNamespace for FakeNamespace {
        fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, ClientError> {
            Ok(Some(sample_card(path, self.record_count)))
        }

        fn agent_list_page(
            &self,
            path: &str,
            _options: NamespaceListOptions,
        ) -> Result<NamespaceListPage, ClientError> {
            Ok(NamespaceListPage {
                path: path.to_owned(),
                evidence: "nokv-native:///runs".to_owned(),
                snapshot_id: Some(9),
                entry_count: 1,
                entries: vec![sample_card("/runs/run-1", self.record_count)],
                next_cursor: None,
                truncated: false,
            })
        }

        fn agent_find_paths(
            &self,
            request: NamespaceFindRequest,
        ) -> Result<NamespaceFindResult, ClientError> {
            self.last_find.replace(Some(request));
            Ok(NamespaceFindResult {
                path: "/runs".to_owned(),
                evidence: "nokv-native:///runs".to_owned(),
                snapshot_id: Some(9),
                match_count: 1,
                matches: vec![sample_card("/runs/run-1", self.record_count)],
                next_cursor: None,
                truncated: false,
                scanned_entries: 1,
            })
        }

        fn agent_read_page(
            &self,
            path: &str,
            _options: NamespaceReadOptions,
        ) -> Result<NamespaceReadPage, ClientError> {
            *self.read_calls.borrow_mut() += 1;
            Ok(NamespaceReadPage {
                path: path.to_owned(),
                evidence: "nokv-native:///runs/run-1/metadata.json@generation:7".to_owned(),
                snapshot_id: Some(9),
                generation: 7,
                total_size_bytes: 13,
                format: NamespaceReadFormat::Structured,
                record_type: Some(NamespaceRecordType::JsonObject),
                record_count: Some(1),
                cursor: None,
                next_cursor: None,
                truncated: false,
                items: vec![NamespaceReadItem {
                    index: 0,
                    value_json: r#"{"status":"completed"}"#.to_owned(),
                    evidence: "nokv-native:///runs/run-1/metadata.json@generation:7#item:0"
                        .to_owned(),
                }],
                bytes: None,
            })
        }
    }

    fn sample_card(path: &str, record_count: usize) -> NamespaceCard {
        NamespaceCard {
            path: path.to_owned(),
            name: path.rsplit('/').next().unwrap_or("").to_owned(),
            kind: NamespaceCardKind::File,
            evidence: format!("nokv-native://{path}@generation:7"),
            snapshot_id: Some(9),
            inode: InodeId::new(42).unwrap(),
            generation: 7,
            size_bytes: Some(13),
            entry_count: None,
            record_count: Some(NamespaceRecordCount {
                count: record_count,
                provenance: RecordCountProvenance::MaterializedIndex,
            }),
            schema: Some(NamespaceSchema {
                record_type: NamespaceRecordType::JsonObject,
                fields: vec!["status".to_owned()],
            }),
            sample: vec![r#"{"status":"completed"}"#.to_owned()],
            body: Some(NamespaceBodyDescriptor {
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                size: 13,
                content_type: "application/json".to_owned(),
                manifest_id: "runs/run-1/metadata.json".to_owned(),
                generation: 7,
                chunk_size: 4096,
                block_size: 4096,
            }),
            catalog: NamespaceQueryCatalog {
                filterable: vec![NamespaceFilterCapability {
                    field: NamespaceFindField::new("run.status"),
                    operators: vec![NamespacePredicateOp::Eq],
                }],
                sortable: vec![NamespaceSortField::new("run.status")],
                facetable: vec![NamespaceFindField::new("run.status")],
                projections: vec![
                    NamespaceInclude::Body,
                    NamespaceInclude::Schema,
                    NamespaceInclude::Sample,
                    NamespaceInclude::Catalog,
                ],
            },
            indexed_values: vec![NamespaceIndexValue {
                field: NamespaceFindField::new("run.status"),
                value: NamespacePredicateValue::String("completed".to_owned()),
            }],
        }
    }

    #[test]
    fn agent_tool_registry_uses_posix_like_names() {
        let names = agent_tool_definitions()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["ls", "stat", "read", "find"]);
    }

    #[test]
    fn find_tool_translates_catalog_field_ids() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "run.status", "op": "eq", "value": "completed"}],
                "sort": [{"field": "run.status", "direction": "desc"}],
                "include": ["catalog"],
                "limit": 5
            }),
        )
        .unwrap();

        let request = namespace.last_find.borrow().clone().unwrap();
        assert_eq!(request.predicates[0].field.id, "run.status");
        assert_eq!(
            request.predicates[0].value,
            NamespacePredicateValue::String("completed".to_owned())
        );
        assert_eq!(request.sort[0].field.id, "run.status");
        assert_eq!(request.include, vec![NamespaceInclude::Catalog]);
        assert_eq!(output["match_count"], 1);
        assert_eq!(
            output["matches"][0]["indexed_values"][0]["field"],
            "run.status"
        );
        assert!(output.get("catalog").is_none() || output["catalog"].is_null());
        assert_eq!(
            output["matches"][0]["catalog"]["filterable"][0]["field"],
            "run.status"
        );
    }

    #[test]
    fn find_tool_surfaces_requested_card_includes_on_each_match() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "include": ["body", "schema", "sample", "catalog"],
                "limit": 1
            }),
        )
        .unwrap();

        let request = namespace.last_find.borrow().clone().unwrap();
        assert_eq!(
            request.include,
            vec![
                NamespaceInclude::Body,
                NamespaceInclude::Schema,
                NamespaceInclude::Sample,
                NamespaceInclude::Catalog
            ]
        );
        let match_ = &output["matches"][0];
        assert_eq!(match_["body"]["content_type"], "application/json");
        assert_eq!(match_["schema"]["record_type"], "json_object");
        assert_eq!(match_["sample"][0], r#"{"status":"completed"}"#);
        assert_eq!(match_["catalog"]["filterable"][0]["field"], "run.status");
        assert!(output.get("catalog").is_none() || output["catalog"].is_null());
    }

    #[test]
    fn find_tool_accepts_boolean_predicates_as_u64_facets() {
        let namespace = FakeNamespace::new();

        execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "git.has_uncommitted_changes", "op": "eq", "value": true}],
                "limit": 5
            }),
        )
        .unwrap();

        let request = namespace.last_find.borrow().clone().unwrap();
        assert_eq!(
            request.predicates[0].field.id,
            "git.has_uncommitted_changes"
        );
        assert_eq!(request.predicates[0].value, NamespacePredicateValue::U64(1));
    }

    #[test]
    fn find_tool_projects_indexed_values_when_fields_are_requested() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "fields": ["run.id"],
                "limit": 5
            }),
        )
        .unwrap();

        assert_eq!(output["matches"][0]["path"], "/runs/run-1");
        assert_eq!(output["matches"][0]["values"], json!({}));
        assert!(output["matches"][0].get("indexed_values").is_none());
        assert!(output["matches"][0].get("name").is_none());
        assert!(output["matches"][0].get("generation").is_none());
        assert!(output.get("catalog").is_none() || output["catalog"].is_null());
        assert!(namespace.last_find.borrow().is_some());
    }

    #[test]
    fn find_tool_combines_field_projection_with_requested_includes() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "fields": ["run.status"],
                "include": ["schema"],
                "limit": 5
            }),
        )
        .unwrap();

        let match_ = &output["matches"][0];
        assert_eq!(match_["path"], "/runs/run-1");
        assert_eq!(match_["values"], json!({"run.status": "completed"}));
        assert_eq!(match_["schema"]["record_type"], "json_object");
        assert!(match_.get("indexed_values").is_none());
        assert!(match_.get("name").is_none());
        assert!(match_.get("generation").is_none());
    }

    #[test]
    fn ls_tool_keeps_entries_lightweight() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "ls",
            &json!({
                "path": "/runs",
                "limit": 1
            }),
        )
        .unwrap();

        assert_eq!(output["entries"][0]["path"], "/runs/run-1");
        assert!(output["entries"][0].get("record_count").is_none());
        assert!(output["entries"][0].get("sample").is_none());
        assert!(output["entries"][0].get("catalog").is_none());
    }

    #[test]
    fn read_tool_rejects_large_structured_pagination() {
        let namespace = FakeNamespace::with_record_count(MAX_AGENT_PAGE_LIMIT + 1);

        let err = execute_agent_tool(
            &namespace,
            "read",
            &json!({
                "path": "/index/large.json",
                "format": "structured",
                "cursor": "100",
                "limit": 100
            }),
        )
        .unwrap_err();

        assert!(
            matches!(err, ClientError::Protocol(ref message) if message.contains("find with catalog predicates")),
            "unexpected error: {err:?}"
        );
        assert_eq!(*namespace.read_calls.borrow(), 0);
    }

    #[test]
    fn read_tool_rejects_large_structured_initial_page() {
        let namespace = FakeNamespace::with_record_count(MAX_AGENT_PAGE_LIMIT + 1);

        let err = execute_agent_tool(
            &namespace,
            "read",
            &json!({
                "path": "/index/large.json",
                "format": "structured",
                "limit": 100
            }),
        )
        .unwrap_err();

        assert!(
            matches!(err, ClientError::Protocol(ref message) if message.contains("find with catalog predicates")),
            "unexpected error: {err:?}"
        );
        assert_eq!(*namespace.read_calls.borrow(), 0);
    }
}
