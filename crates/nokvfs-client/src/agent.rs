use nokvfs_meta::{
    MetadataStore, NamespaceAggregateGroup, NamespaceAggregateMeasure, NamespaceAggregateOp,
    NamespaceAggregateOutputMeasure, NamespaceAggregateRequest, NamespaceAggregateResult,
    NamespaceAggregateSort, NamespaceAggregateValue, NamespaceBodyDescriptor, NamespaceCard,
    NamespaceCardKind, NamespaceFacetSummary, NamespaceFacetValue, NamespaceFieldSource,
    NamespaceFieldSourceKind, NamespaceFieldValue, NamespaceFilterCapability, NamespaceFindField,
    NamespaceFindRequest, NamespaceFindResult, NamespaceInclude, NamespaceIndexValue,
    NamespaceListOptions, NamespaceListPage, NamespacePredicate, NamespacePredicateOp,
    NamespacePredicateValue, NamespaceQueryCatalog, NamespaceReadFormat, NamespaceReadItem,
    NamespaceReadOptions, NamespaceReadPage, NamespaceRecordCount, NamespaceRecordType,
    NamespaceSchema, NamespaceSort, NamespaceSortDirection, NamespaceSortField, NoKvFs,
    RecordCountProvenance,
};
use nokvfs_object::ObjectStore;
use serde_json::{json, Map, Value};

use crate::{ClientError, MetadataClient, NoKvFsClient};

const DEFAULT_AGENT_PAGE_LIMIT: usize = 100;
const MAX_AGENT_PAGE_LIMIT: usize = 100;
const DEFAULT_AGENT_FIND_LIMIT: usize = 10;
const MAX_AGENT_FIND_LIMIT: usize = 10;
const DEFAULT_AGENT_AGGREGATE_LIMIT: usize = 20;
const MAX_AGENT_AGGREGATE_LIMIT: usize = 100;

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

    fn agent_aggregate_paths(
        &self,
        request: NamespaceAggregateRequest,
    ) -> Result<NamespaceAggregateResult, ClientError>;

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

    fn agent_aggregate_paths(
        &self,
        request: NamespaceAggregateRequest,
    ) -> Result<NamespaceAggregateResult, ClientError> {
        self.aggregate_paths(request)
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

    fn agent_aggregate_paths(
        &self,
        request: NamespaceAggregateRequest,
    ) -> Result<NamespaceAggregateResult, ClientError> {
        self.aggregate_paths(request)
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

    fn agent_aggregate_paths(
        &self,
        request: NamespaceAggregateRequest,
    ) -> Result<NamespaceAggregateResult, ClientError> {
        self.aggregate_paths(request).map_err(ClientError::Metadata)
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
            name: "catalog",
            description: "Return compact catalog field capabilities for choosing indexed fields before find or aggregate.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "field_prefix": {"type": ["string", "null"]},
                    "include_facets": {"type": "boolean"}
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
            name: "aggregate",
            description: "Use for counts, grouped summaries, averages, minima, maxima, and ranked summaries over catalog-indexed namespace fields.",
            parameters: json!({
                "type": "object",
                "required": ["path", "measures"],
                "properties": {
                    "path": {"type": "string"},
                    "predicates": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["field", "op"],
                            "properties": {
                                "field": {"type": "string"},
                                "op": {
                                    "type": "string",
                                    "enum": ["eq", "ne", "in", "prefix", "suffix", "contains", "gt", "gte", "lt", "lte", "exists", "not_exists"]
                                },
                                "value": {
                                    "anyOf": [
                                        {"type": "string"},
                                        {"type": "integer", "minimum": 0},
                                        {"type": "number"},
                                        {"type": "boolean"},
                                        {
                                            "type": "array",
                                            "items": {"type": ["string", "integer", "number", "boolean"]}
                                        },
                                        {"type": "null"}
                                    ]
                                }
                            },
                            "additionalProperties": false
                        }
                    },
                    "group_by": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "measures": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["name", "op"],
                            "properties": {
                                "name": {"type": "string"},
                                "op": {"type": "string", "enum": ["count", "sum", "avg", "min", "max"]},
                                "field": {"type": ["string", "null"]}
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
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_AGENT_AGGREGATE_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "find",
            description:
                "Find paths using catalog field ids; returns match_count and optional predicate-filtered facets.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "predicates": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["field", "op"],
                            "properties": {
                                "field": {"type": "string"},
                                "op": {
                                    "type": "string",
                                    "enum": ["eq", "ne", "in", "prefix", "suffix", "contains", "gt", "gte", "lt", "lte", "exists", "not_exists"]
                                },
                                "value": {
                                    "anyOf": [
                                        {"type": "string"},
                                        {"type": "integer", "minimum": 0},
                                        {"type": "number"},
                                        {"type": "boolean"},
                                        {
                                            "type": "array",
                                            "items": {"type": ["string", "integer", "number", "boolean"]}
                                        },
                                        {"type": "null"}
                                    ]
                                }
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
                        "items": {"type": "string", "enum": ["body", "schema", "sample"]}
                    },
                    "fields": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "facets": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_AGENT_FIND_LIMIT}
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
        "catalog" => execute_catalog(namespace, args),
        "read" => execute_read(namespace, args),
        "find" => execute_find(namespace, args),
        "aggregate" => execute_aggregate(namespace, args),
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
            limit: optional_usize_arg(args, "limit")?.unwrap_or(DEFAULT_AGENT_PAGE_LIMIT),
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

fn execute_catalog<T>(namespace: &T, args: &Value) -> Result<Value, ClientError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let field_prefix = optional_string_arg(args, "field_prefix")?;
    let include_facets = optional_bool_arg(args, "include_facets")?.unwrap_or(true);
    let card = namespace
        .agent_stat_card(path)?
        .ok_or_else(|| ClientError::NotFound(path.to_owned()))?;
    let catalog = filtered_catalog(&card.catalog, field_prefix.as_deref(), include_facets);
    let catalog_empty = catalog_is_empty(&catalog);
    let child_catalogs = if catalog_empty {
        child_catalogs(namespace, &card.path, include_facets)?
    } else {
        Vec::new()
    };
    Ok(json!({
        "tool": "catalog",
        "bytes_read": 0,
        "path": card.path,
        "evidence": card.evidence,
        "snapshot_id": card.snapshot_id,
        "catalog_empty": catalog_empty,
        "scope_note": "catalogs are scoped to the path; call catalog on the directory you will search",
        "catalog": catalog_json(&catalog),
        "child_catalogs": child_catalogs,
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
        limit: optional_usize_arg(args, "limit")?.unwrap_or(DEFAULT_AGENT_PAGE_LIMIT),
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
        facets: facets_arg(args)?,
        cursor: optional_string_arg(args, "cursor")?,
        limit: optional_bounded_usize_arg(args, "limit", MAX_AGENT_FIND_LIMIT)?
            .unwrap_or(DEFAULT_AGENT_FIND_LIMIT),
    })?;
    Ok(find_result_json(&result, &include, fields.as_deref()))
}

fn execute_aggregate<T>(namespace: &T, args: &Value) -> Result<Value, ClientError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let predicates = predicates_arg(args)?;
    let group_by = group_by_arg(args)?;
    let measures = aggregate_measures_arg(args)?;
    let sort = aggregate_sort_arg(args)?;
    let limit = optional_bounded_usize_arg(args, "limit", MAX_AGENT_AGGREGATE_LIMIT)?
        .unwrap_or(DEFAULT_AGENT_AGGREGATE_LIMIT);
    let result = namespace.agent_aggregate_paths(NamespaceAggregateRequest {
        path: path.to_owned(),
        predicates,
        group_by,
        measures,
        sort,
        limit,
    })?;
    Ok(aggregate_result_json(&result))
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
        "facets": result.facets.iter().map(facet_summary_json).collect::<Vec<_>>(),
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
        object.insert("evidence".to_owned(), json!(card.evidence));
        object.insert("snapshot_id".to_owned(), json!(card.snapshot_id));
        object.insert("generation".to_owned(), json!(card.generation));
        object.insert(
            "values".to_owned(),
            projected_values_json(card, fields.unwrap_or(&[])),
        );
        object.insert(
            "field_values".to_owned(),
            projected_field_values_json(card, fields.unwrap_or(&[])),
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
        let values = projected_field_values(card, field)
            .into_iter()
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

fn projected_field_values_json(card: &NamespaceCard, fields: &[String]) -> Value {
    let mut output = Map::new();
    for field in fields {
        let values = projected_field_values(card, field)
            .into_iter()
            .map(|value| namespace_field_value_json(&value))
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

fn projected_field_values(card: &NamespaceCard, field: &str) -> Vec<NamespaceFieldValue> {
    let field_id = NamespaceFindField::new(field);
    let namespace_source = || NamespaceFieldSource {
        evidence: card.evidence.clone(),
        source_path: card.path.clone(),
        source_kind: NamespaceFieldSourceKind::Namespace,
    };
    let materialized_source = || NamespaceFieldSource {
        evidence: card.evidence.clone(),
        source_path: card.path.clone(),
        source_kind: NamespaceFieldSourceKind::MaterializedIndex,
    };
    let value = |value| NamespaceFieldValue {
        field: field_id.clone(),
        value,
        source: namespace_source(),
    };
    match field {
        "path" => vec![value(NamespacePredicateValue::String(card.path.clone()))],
        "name" => vec![value(NamespacePredicateValue::String(card.name.clone()))],
        "kind" => vec![value(NamespacePredicateValue::String(
            card_kind_name(&card.kind).to_owned(),
        ))],
        "size_bytes" => card
            .size_bytes
            .map(NamespacePredicateValue::U64)
            .map(value)
            .into_iter()
            .collect(),
        "body.content_type" => card
            .body
            .as_ref()
            .map(|body| NamespacePredicateValue::String(body.content_type.clone()))
            .map(value)
            .into_iter()
            .collect(),
        "body.producer" => card
            .body
            .as_ref()
            .map(|body| NamespacePredicateValue::String(body.producer.clone()))
            .map(value)
            .into_iter()
            .collect(),
        "body.manifest_id" => card
            .body
            .as_ref()
            .map(|body| NamespacePredicateValue::String(body.manifest_id.clone()))
            .map(value)
            .into_iter()
            .collect(),
        _ => card
            .indexed_values
            .iter()
            .filter(|value| value.field.id == field)
            .map(|value| NamespaceFieldValue {
                field: value.field.clone(),
                value: value.value.clone(),
                source: materialized_source(),
            })
            .collect(),
    }
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
        "facets": catalog.facets.iter().map(facet_summary_json).collect::<Vec<_>>(),
        "projections": catalog.projections.iter()
            .filter(|include| !matches!(include, NamespaceInclude::Catalog))
            .map(include_name)
            .collect::<Vec<_>>(),
    })
}

fn facet_summary_json(facet: &NamespaceFacetSummary) -> Value {
    json!({
        "field": facet.field.id,
        "values": facet.values.iter().map(facet_value_json).collect::<Vec<_>>(),
        "distinct_count": facet.distinct_count,
        "truncated": facet.truncated,
    })
}

fn facet_value_json(value: &NamespaceFacetValue) -> Value {
    json!({
        "value": predicate_value_json(&value.value),
        "count": value.count,
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

fn predicate_json(predicate: &NamespacePredicate) -> Value {
    let mut object = Map::new();
    object.insert("field".to_owned(), json!(predicate.field.id));
    object.insert("op".to_owned(), json!(predicate_op_name(&predicate.op)));
    if let Some(value) = &predicate.value {
        object.insert("value".to_owned(), predicate_value_json(value));
    }
    Value::Object(object)
}

fn predicate_value_json(value: &NamespacePredicateValue) -> Value {
    match value {
        NamespacePredicateValue::String(value) => json!(value),
        NamespacePredicateValue::U64(value) => json!(value),
        NamespacePredicateValue::F64(value) if value.is_finite() => json!(value),
        NamespacePredicateValue::F64(_) => Value::Null,
        NamespacePredicateValue::List(values) => {
            Value::Array(values.iter().map(predicate_value_json).collect())
        }
    }
}

fn aggregate_result_json(result: &NamespaceAggregateResult) -> Value {
    json!({
        "tool": "aggregate",
        "bytes_read": 0,
        "path": result.path,
        "evidence": result.evidence,
        "snapshot_id": result.snapshot_id,
        "predicates": result.predicates.iter().map(predicate_json).collect::<Vec<_>>(),
        "scope_note": Value::Null,
        "input_match_count": result.input_match_count,
        "row_count": result.row_count,
        "group_count": result.group_count,
        "groups": result.groups.iter().map(aggregate_group_json).collect::<Vec<_>>(),
        "truncated": result.truncated,
        "scanned_entries": result.scanned_entries,
    })
}

fn aggregate_group_json(group: &NamespaceAggregateGroup) -> Value {
    let key = group
        .key
        .iter()
        .map(|value| (value.field.id.clone(), predicate_value_json(&value.value)))
        .collect::<Map<_, _>>();
    let values = group
        .measures
        .iter()
        .map(|measure| (measure.name.clone(), aggregate_value_json(&measure.value)))
        .collect::<Map<_, _>>();
    json!({
        "key": key,
        "key_fields": group.key.iter().map(namespace_field_value_json).collect::<Vec<_>>(),
        "values": values,
        "measures": group.measures.iter().map(aggregate_output_measure_json).collect::<Vec<_>>(),
        "evidence": group.evidence,
        "sample_matches": group.sample_matches.iter().map(aggregate_sample_json).collect::<Vec<_>>(),
    })
}

fn aggregate_output_measure_json(measure: &NamespaceAggregateOutputMeasure) -> Value {
    json!({
        "name": measure.name,
        "op": aggregate_op_name(&measure.op),
        "field": measure.field.as_ref().map(|field| field.id.clone()),
        "value": aggregate_value_json(&measure.value),
    })
}

fn aggregate_value_json(value: &NamespaceAggregateValue) -> Value {
    match value {
        NamespaceAggregateValue::U64(value) => json!(value),
        NamespaceAggregateValue::F64(value) if value.is_finite() => json!(value),
        NamespaceAggregateValue::F64(_) | NamespaceAggregateValue::Null => Value::Null,
    }
}

fn namespace_field_value_json(value: &NamespaceFieldValue) -> Value {
    json!({
        "field": value.field.id,
        "value": predicate_value_json(&value.value),
        "evidence": value.source.evidence,
        "source_path": value.source.source_path,
        "source_kind": field_source_kind_name(&value.source.source_kind),
    })
}

fn field_source_kind_name(kind: &NamespaceFieldSourceKind) -> &'static str {
    match kind {
        NamespaceFieldSourceKind::Namespace => "namespace",
        NamespaceFieldSourceKind::MaterializedIndex => "materialized_index",
    }
}

fn aggregate_sample_json(sample: &nokvfs_meta::NamespaceAggregateSample) -> Value {
    json!({
        "path": sample.path,
        "evidence": sample.evidence,
        "generation": sample.generation,
    })
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

fn optional_bool_arg(args: &Value, name: &'static str) -> Result<Option<bool>, ClientError> {
    let Some(value) = object_args(args)?.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_bool()
        .map(Some)
        .ok_or_else(|| ClientError::Protocol(format!("{name} must be a boolean or null")))
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
    optional_bounded_usize_arg(args, name, MAX_AGENT_PAGE_LIMIT)
}

fn optional_bounded_usize_arg(
    args: &Value,
    name: &'static str,
    max: usize,
) -> Result<Option<usize>, ClientError> {
    optional_u64_arg(args, name)?
        .map(|value| {
            let value = usize::try_from(value)
                .map_err(|_| ClientError::Protocol(format!("{name} exceeds platform limit")))?;
            if value == 0 || value > max {
                return Err(ClientError::Protocol(format!(
                    "{name} must be between 1 and {max}"
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
    let op = predicate_op_arg(op)?;
    let raw_value = object.get("value").filter(|value| !value.is_null());
    let value = match op {
        NamespacePredicateOp::Exists | NamespacePredicateOp::NotExists => {
            if raw_value.is_some() {
                return Err(ClientError::Protocol(format!(
                    "predicate op {} must not include value",
                    predicate_op_name(&op)
                )));
            }
            None
        }
        NamespacePredicateOp::In => {
            let value = raw_value.ok_or_else(|| {
                ClientError::Protocol("predicate op in requires array value".to_owned())
            })?;
            if !value.is_array() {
                return Err(ClientError::Protocol(
                    "predicate op in requires array value".to_owned(),
                ));
            }
            Some(predicate_value_arg(value)?)
        }
        _ => {
            let value = raw_value.ok_or_else(|| {
                ClientError::Protocol(format!(
                    "predicate op {} requires value",
                    predicate_op_name(&op)
                ))
            })?;
            Some(predicate_value_arg(value)?)
        }
    };
    Ok(NamespacePredicate {
        field: NamespaceFindField::new(field),
        op,
        value,
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

fn facets_arg(args: &Value) -> Result<Vec<NamespaceFindField>, ClientError> {
    let Some(value) = object_args(args)?.get("facets") else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    value
        .as_array()
        .ok_or_else(|| ClientError::Protocol("facets must be an array".to_owned()))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(NamespaceFindField::new)
                .ok_or_else(|| ClientError::Protocol("facets entries must be strings".to_owned()))
        })
        .collect()
}

fn group_by_arg(args: &Value) -> Result<Vec<NamespaceFindField>, ClientError> {
    let Some(value) = object_args(args)?.get("group_by") else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    value
        .as_array()
        .ok_or_else(|| ClientError::Protocol("group_by must be an array".to_owned()))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(NamespaceFindField::new)
                .ok_or_else(|| ClientError::Protocol("group_by entries must be strings".to_owned()))
        })
        .collect()
}

fn aggregate_measures_arg(args: &Value) -> Result<Vec<NamespaceAggregateMeasure>, ClientError> {
    let value = object_args(args)?
        .get("measures")
        .ok_or_else(|| ClientError::Protocol("missing array argument measures".to_owned()))?;
    let measures = value
        .as_array()
        .ok_or_else(|| ClientError::Protocol("measures must be an array".to_owned()))?;
    if measures.is_empty() {
        return Err(ClientError::Protocol(
            "measures must contain at least one measure".to_owned(),
        ));
    }
    measures
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| ClientError::Protocol("measure must be an object".to_owned()))?;
            let name = string_property(object, "name")?.to_owned();
            if name.is_empty() {
                return Err(ClientError::Protocol(
                    "measure name must not be empty".to_owned(),
                ));
            }
            let op = aggregate_op_arg(string_property(object, "op")?)?;
            let field = object
                .get("field")
                .and_then(|value| (!value.is_null()).then_some(value))
                .map(|value| {
                    value.as_str().map(str::to_owned).ok_or_else(|| {
                        ClientError::Protocol("measure field must be a string or null".to_owned())
                    })
                })
                .transpose()?;
            if !matches!(op, NamespaceAggregateOp::Count) && field.is_none() {
                return Err(ClientError::Protocol(format!(
                    "measure {name} with op {} requires field",
                    aggregate_op_name(&op)
                )));
            }
            Ok(NamespaceAggregateMeasure {
                name,
                op,
                field: field.map(NamespaceFindField::new),
            })
        })
        .collect()
}

fn aggregate_op_arg(op: &str) -> Result<NamespaceAggregateOp, ClientError> {
    match op {
        "count" => Ok(NamespaceAggregateOp::Count),
        "sum" => Ok(NamespaceAggregateOp::Sum),
        "avg" => Ok(NamespaceAggregateOp::Avg),
        "min" => Ok(NamespaceAggregateOp::Min),
        "max" => Ok(NamespaceAggregateOp::Max),
        other => Err(ClientError::Protocol(format!(
            "unsupported aggregate op {other}"
        ))),
    }
}

fn aggregate_sort_arg(args: &Value) -> Result<Vec<NamespaceAggregateSort>, ClientError> {
    let Some(value) = object_args(args)?.get("sort") else {
        return Ok(Vec::new());
    };
    let sort = value
        .as_array()
        .ok_or_else(|| ClientError::Protocol("sort must be an array".to_owned()))?;
    sort.iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| ClientError::Protocol("sort item must be an object".to_owned()))?;
            let field = string_property(object, "field")?.to_owned();
            let direction = object
                .get("direction")
                .and_then(Value::as_str)
                .unwrap_or("asc");
            Ok(NamespaceAggregateSort {
                field,
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
        })
        .collect()
}

fn filtered_catalog(
    catalog: &NamespaceQueryCatalog,
    field_prefix: Option<&str>,
    include_facets: bool,
) -> NamespaceQueryCatalog {
    NamespaceQueryCatalog {
        filterable: catalog
            .filterable
            .iter()
            .filter(|capability| field_matches_prefix(capability.field.id.as_str(), field_prefix))
            .cloned()
            .collect(),
        sortable: catalog
            .sortable
            .iter()
            .filter(|field| field_matches_prefix(field.id.as_str(), field_prefix))
            .cloned()
            .collect(),
        facetable: catalog
            .facetable
            .iter()
            .filter(|field| field_matches_prefix(field.id.as_str(), field_prefix))
            .cloned()
            .collect(),
        facets: if include_facets {
            catalog
                .facets
                .iter()
                .filter(|facet| field_matches_prefix(facet.field.id.as_str(), field_prefix))
                .cloned()
                .collect()
        } else {
            Vec::new()
        },
        projections: catalog.projections.clone(),
    }
}

fn catalog_is_empty(catalog: &NamespaceQueryCatalog) -> bool {
    catalog.filterable.is_empty() && catalog.sortable.is_empty() && catalog.facetable.is_empty()
}

fn child_catalogs<T>(
    namespace: &T,
    path: &str,
    include_facets: bool,
) -> Result<Vec<Value>, ClientError>
where
    T: AgentNamespace + ?Sized,
{
    let page = namespace.agent_list_page(
        path,
        NamespaceListOptions {
            cursor: None,
            limit: 20,
        },
    )?;
    let mut children = Vec::new();
    for entry in page.entries {
        if !matches!(entry.kind, NamespaceCardKind::Directory) {
            continue;
        }
        let Some(card) = namespace.agent_stat_card(&entry.path)? else {
            continue;
        };
        let catalog = filtered_catalog(&card.catalog, None, include_facets);
        if catalog_is_empty(&catalog) {
            continue;
        }
        children.push(json!({
            "path": card.path,
            "evidence": card.evidence,
            "snapshot_id": card.snapshot_id,
            "catalog": catalog_json(&catalog),
        }));
        if children.len() == 5 {
            break;
        }
    }
    Ok(children)
}

fn field_matches_prefix(field: &str, prefix: Option<&str>) -> bool {
    prefix
        .map(|prefix| field.starts_with(prefix) || field.contains(prefix))
        .unwrap_or(true)
}

fn aggregate_op_name(op: &NamespaceAggregateOp) -> &'static str {
    match op {
        NamespaceAggregateOp::Count => "count",
        NamespaceAggregateOp::Sum => "sum",
        NamespaceAggregateOp::Avg => "avg",
        NamespaceAggregateOp::Min => "min",
        NamespaceAggregateOp::Max => "max",
    }
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
        "ne" | "not_equal" => Ok(NamespacePredicateOp::NotEqual),
        "in" => Ok(NamespacePredicateOp::In),
        "prefix" => Ok(NamespacePredicateOp::Prefix),
        "suffix" => Ok(NamespacePredicateOp::Suffix),
        "contains" => Ok(NamespacePredicateOp::Contains),
        "gt" | "greater_than" => Ok(NamespacePredicateOp::GreaterThan),
        "gte" | "greater_than_or_equal" => Ok(NamespacePredicateOp::GreaterThanOrEqual),
        "lt" | "less_than" => Ok(NamespacePredicateOp::LessThan),
        "lte" | "less_than_or_equal" => Ok(NamespacePredicateOp::LessThanOrEqual),
        "exists" => Ok(NamespacePredicateOp::Exists),
        "not_exists" => Ok(NamespacePredicateOp::NotExists),
        other => Err(ClientError::Protocol(format!(
            "unsupported predicate operator {other}"
        ))),
    }
}

fn predicate_value_arg(value: &Value) -> Result<NamespacePredicateValue, ClientError> {
    if let Some(values) = value.as_array() {
        return values
            .iter()
            .map(predicate_scalar_value_arg)
            .collect::<Result<Vec<_>, _>>()
            .map(NamespacePredicateValue::List);
    }
    predicate_scalar_value_arg(value)
}

fn predicate_scalar_value_arg(value: &Value) -> Result<NamespacePredicateValue, ClientError> {
    if let Some(value) = value.as_str() {
        return Ok(NamespacePredicateValue::String(value.to_owned()));
    }
    if let Some(value) = value.as_bool() {
        return Ok(NamespacePredicateValue::U64(u64::from(value)));
    }
    if let Some(value) = value.as_u64() {
        return Ok(NamespacePredicateValue::U64(value));
    }
    if let Some(value) = value.as_f64() {
        if value.is_finite() {
            return Ok(NamespacePredicateValue::F64(value));
        }
    }
    Err(ClientError::Protocol(
        "predicate value must be a string, boolean, finite number, or array of scalar values"
            .to_owned(),
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
        NamespacePredicateOp::NotEqual => "ne",
        NamespacePredicateOp::In => "in",
        NamespacePredicateOp::Prefix => "prefix",
        NamespacePredicateOp::Suffix => "suffix",
        NamespacePredicateOp::Contains => "contains",
        NamespacePredicateOp::GreaterThan => "gt",
        NamespacePredicateOp::GreaterThanOrEqual => "gte",
        NamespacePredicateOp::LessThan => "lt",
        NamespacePredicateOp::LessThanOrEqual => "lte",
        NamespacePredicateOp::Exists => "exists",
        NamespacePredicateOp::NotExists => "not_exists",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_meta::NamespaceRecordCount;
    use nokvfs_types::InodeId;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    struct FakeNamespace {
        last_find: RefCell<Option<NamespaceFindRequest>>,
        last_aggregate: RefCell<Option<NamespaceAggregateRequest>>,
        read_calls: RefCell<usize>,
        record_count: usize,
        find_matches: Vec<NamespaceCard>,
        aggregate_result: NamespaceAggregateResult,
        stat_cards: BTreeMap<String, NamespaceCard>,
        list_entries: Vec<NamespaceCard>,
    }

    impl FakeNamespace {
        fn new() -> Self {
            Self {
                last_find: RefCell::new(None),
                last_aggregate: RefCell::new(None),
                read_calls: RefCell::new(0),
                record_count: 1,
                find_matches: vec![sample_card("/runs/run-1", 1)],
                aggregate_result: sample_aggregate_result(),
                stat_cards: BTreeMap::new(),
                list_entries: vec![sample_card("/runs/run-1", 1)],
            }
        }

        fn with_record_count(record_count: usize) -> Self {
            Self {
                last_find: RefCell::new(None),
                last_aggregate: RefCell::new(None),
                read_calls: RefCell::new(0),
                record_count,
                find_matches: vec![sample_card("/runs/run-1", record_count)],
                aggregate_result: sample_aggregate_result(),
                stat_cards: BTreeMap::new(),
                list_entries: vec![sample_card("/runs/run-1", record_count)],
            }
        }

        fn with_aggregate_result(aggregate_result: NamespaceAggregateResult) -> Self {
            Self {
                last_find: RefCell::new(None),
                last_aggregate: RefCell::new(None),
                read_calls: RefCell::new(0),
                record_count: 1,
                find_matches: vec![sample_card("/runs/run-1", 1)],
                aggregate_result,
                stat_cards: BTreeMap::new(),
                list_entries: vec![sample_card("/runs/run-1", 1)],
            }
        }

        fn with_root_child_catalog() -> Self {
            let mut root = sample_card("/yanex", 1);
            root.kind = NamespaceCardKind::Directory;
            root.catalog = empty_catalog();
            let mut runs = sample_card("/yanex/runs", 1);
            runs.kind = NamespaceCardKind::Directory;
            runs.catalog.filterable.push(NamespaceFilterCapability {
                field: NamespaceFindField::new("run.script"),
                operators: vec![NamespacePredicateOp::Eq],
            });
            runs.catalog
                .sortable
                .push(NamespaceSortField::new("run.script"));
            let mut stat_cards = BTreeMap::new();
            stat_cards.insert(root.path.clone(), root);
            stat_cards.insert(runs.path.clone(), runs.clone());
            Self {
                last_find: RefCell::new(None),
                last_aggregate: RefCell::new(None),
                read_calls: RefCell::new(0),
                record_count: 1,
                find_matches: vec![sample_card("/yanex/runs/run-1", 1)],
                aggregate_result: sample_aggregate_result(),
                stat_cards,
                list_entries: vec![runs],
            }
        }
    }

    impl AgentNamespace for FakeNamespace {
        fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, ClientError> {
            Ok(Some(
                self.stat_cards
                    .get(path)
                    .cloned()
                    .unwrap_or_else(|| sample_card(path, self.record_count)),
            ))
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
                entry_count: self.list_entries.len(),
                entries: self.list_entries.clone(),
                next_cursor: None,
                truncated: false,
            })
        }

        fn agent_find_paths(
            &self,
            request: NamespaceFindRequest,
        ) -> Result<NamespaceFindResult, ClientError> {
            self.last_find.replace(Some(request));
            let matches = self.find_matches.clone();
            Ok(NamespaceFindResult {
                path: "/runs".to_owned(),
                evidence: "nokv-native:///runs".to_owned(),
                snapshot_id: Some(9),
                match_count: matches.len(),
                matches,
                facets: vec![NamespaceFacetSummary {
                    field: NamespaceFindField::new("run.script"),
                    values: vec![NamespaceFacetValue {
                        value: NamespacePredicateValue::String("train.py".to_owned()),
                        count: 1,
                    }],
                    distinct_count: 1,
                    truncated: false,
                }],
                next_cursor: None,
                truncated: false,
                scanned_entries: 1,
            })
        }

        fn agent_aggregate_paths(
            &self,
            request: NamespaceAggregateRequest,
        ) -> Result<NamespaceAggregateResult, ClientError> {
            let mut result = self.aggregate_result.clone();
            result.path = request.path.clone();
            result.predicates = request.predicates.clone();
            self.last_aggregate.replace(Some(request));
            Ok(result)
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

    fn empty_catalog() -> NamespaceQueryCatalog {
        NamespaceQueryCatalog {
            filterable: Vec::new(),
            sortable: Vec::new(),
            facetable: Vec::new(),
            facets: Vec::new(),
            projections: Vec::new(),
        }
    }

    fn sample_field_value(field: &str, value: NamespacePredicateValue) -> NamespaceFieldValue {
        NamespaceFieldValue {
            field: NamespaceFindField::new(field),
            value,
            source: NamespaceFieldSource {
                evidence: "nokv-native:///runs/run-1@generation:7".to_owned(),
                source_path: "/runs/run-1".to_owned(),
                source_kind: NamespaceFieldSourceKind::MaterializedIndex,
            },
        }
    }

    fn sample_aggregate_result() -> NamespaceAggregateResult {
        NamespaceAggregateResult {
            path: "/runs".to_owned(),
            evidence: "nokv-native:///runs".to_owned(),
            snapshot_id: Some(9),
            predicates: Vec::new(),
            input_match_count: 5,
            row_count: 3,
            group_count: 2,
            groups: vec![NamespaceAggregateGroup {
                key: vec![sample_field_value(
                    "param.lr",
                    NamespacePredicateValue::String("0.001".to_owned()),
                )],
                measures: vec![
                    NamespaceAggregateOutputMeasure {
                        name: "run_count".to_owned(),
                        op: NamespaceAggregateOp::Count,
                        field: Some(NamespaceFindField::new("metric.val_loss.min")),
                        value: NamespaceAggregateValue::U64(2),
                    },
                    NamespaceAggregateOutputMeasure {
                        name: "avg_min_val_loss".to_owned(),
                        op: NamespaceAggregateOp::Avg,
                        field: Some(NamespaceFindField::new("metric.val_loss.min")),
                        value: NamespaceAggregateValue::F64(0.3),
                    },
                    NamespaceAggregateOutputMeasure {
                        name: "stdout_available".to_owned(),
                        op: NamespaceAggregateOp::Sum,
                        field: Some(NamespaceFindField::new("artifact.stdout_available")),
                        value: NamespaceAggregateValue::U64(1),
                    },
                ],
                evidence: "nokv-native:///runs".to_owned(),
                sample_matches: vec![nokvfs_meta::NamespaceAggregateSample {
                    path: "/runs/run-1".to_owned(),
                    evidence: "nokv-native:///runs/run-1@generation:7".to_owned(),
                    generation: 7,
                }],
            }],
            truncated: false,
            scanned_entries: 5,
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
                facets: vec![NamespaceFacetSummary {
                    field: NamespaceFindField::new("run.status"),
                    values: vec![NamespaceFacetValue {
                        value: NamespacePredicateValue::String("completed".to_owned()),
                        count: 1,
                    }],
                    distinct_count: 1,
                    truncated: false,
                }],
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

        assert_eq!(
            names,
            vec!["ls", "stat", "catalog", "read", "aggregate", "find"]
        );
    }

    #[test]
    fn find_tool_schema_excludes_catalog_include_and_caps_limit_at_ten() {
        let tools = agent_tool_definitions();
        let find = tools
            .iter()
            .find(|tool| tool.name == "find")
            .expect("find tool must be registered");

        let include_enum = find.parameters["properties"]["include"]["items"]["enum"]
            .as_array()
            .expect("include enum must be present");
        assert_eq!(
            include_enum.as_slice(),
            json!(["body", "schema", "sample"]).as_array().unwrap()
        );
        assert_eq!(find.parameters["properties"]["limit"]["maximum"], 10);
    }

    #[test]
    fn stat_tool_exposes_catalog_without_advertising_catalog_find_include() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "stat",
            &json!({
                "path": "/runs",
            }),
        )
        .unwrap();

        assert_eq!(
            output["card"]["catalog"]["filterable"][0]["field"],
            "run.status"
        );
        assert_eq!(
            output["card"]["catalog"]["projections"],
            json!(["body", "schema", "sample"])
        );
        assert_eq!(
            output["card"]["catalog"]["facets"][0],
            json!({
                "field": "run.status",
                "values": [{"value": "completed", "count": 1}],
                "distinct_count": 1,
                "truncated": false
            })
        );
    }

    #[test]
    fn catalog_tool_returns_compact_filtered_catalog() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "catalog",
            &json!({
                "path": "/runs",
                "field_prefix": "run.",
                "include_facets": false
            }),
        )
        .unwrap();

        assert_eq!(output["tool"], "catalog");
        assert_eq!(output["path"], "/runs");
        assert_eq!(output["evidence"], "nokv-native:///runs@generation:7");
        assert_eq!(output["catalog"]["filterable"][0]["field"], "run.status");
        assert_eq!(output["catalog"]["sortable"], json!(["run.status"]));
        assert_eq!(output["catalog"]["facetable"], json!(["run.status"]));
        assert_eq!(output["catalog"]["facets"], json!([]));
        assert!(output.get("card").is_none());
        assert!(output.get("body").is_none());
    }

    #[test]
    fn catalog_tool_matches_field_prefix_inside_field_ids() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "catalog",
            &json!({
                "path": "/runs",
                "field_prefix": "status",
                "include_facets": false
            }),
        )
        .unwrap();

        assert_eq!(output["catalog"]["filterable"][0]["field"], "run.status");
        assert_eq!(output["catalog"]["sortable"], json!(["run.status"]));
    }

    #[test]
    fn catalog_tool_suggests_child_catalogs_when_current_scope_is_empty() {
        let namespace = FakeNamespace::with_root_child_catalog();

        let output = execute_agent_tool(
            &namespace,
            "catalog",
            &json!({
                "path": "/yanex",
                "field_prefix": "status",
                "include_facets": false
            }),
        )
        .unwrap();

        assert_eq!(output["catalog_empty"], true);
        assert_eq!(output["child_catalogs"][0]["path"], "/yanex/runs");
        assert_eq!(
            output["child_catalogs"][0]["catalog"]["filterable"][0]["field"],
            "run.status"
        );
        assert_eq!(
            output["child_catalogs"][0]["catalog"]["filterable"][1]["field"],
            "run.script"
        );
        assert_eq!(
            output["scope_note"],
            "catalogs are scoped to the path; call catalog on the directory you will search"
        );
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
                "limit": 5
            }),
        )
        .unwrap();

        let request = namespace.last_find.borrow().clone().unwrap();
        assert_eq!(request.predicates[0].field.id, "run.status");
        assert_eq!(
            request.predicates[0].value,
            Some(NamespacePredicateValue::String("completed".to_owned()))
        );
        assert_eq!(request.sort[0].field.id, "run.status");
        assert_eq!(request.include, Vec::<NamespaceInclude>::new());
        assert_eq!(output["match_count"], 1);
        assert_eq!(
            output["matches"][0]["indexed_values"][0]["field"],
            "run.status"
        );
        assert!(output.get("catalog").is_none() || output["catalog"].is_null());
        assert!(output["matches"][0].get("catalog").is_none());
    }

    #[test]
    fn find_tool_requests_and_returns_filtered_facets() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "run.status", "op": "eq", "value": "completed"}],
                "facets": ["run.script"],
                "limit": 1
            }),
        )
        .unwrap();

        let request = namespace.last_find.borrow().clone().unwrap();
        assert_eq!(request.facets, vec![NamespaceFindField::new("run.script")]);
        assert_eq!(
            output["facets"],
            json!([{
                "field": "run.script",
                "values": [{"value": "train.py", "count": 1}],
                "distinct_count": 1,
                "truncated": false
            }])
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
                "include": ["body", "schema", "sample"],
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
            ]
        );
        let match_ = &output["matches"][0];
        assert_eq!(match_["body"]["content_type"], "application/json");
        assert_eq!(match_["schema"]["record_type"], "json_object");
        assert_eq!(match_["sample"][0], r#"{"status":"completed"}"#);
        assert!(match_.get("catalog").is_none());
        assert!(output.get("catalog").is_none() || output["catalog"].is_null());
    }

    #[test]
    fn find_tool_rejects_catalog_include() {
        let namespace = FakeNamespace::new();

        let err = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "include": ["catalog"],
                "limit": 1
            }),
        )
        .unwrap_err();

        assert!(
            matches!(err, ClientError::Protocol(ref message) if message.contains("unsupported include projection catalog")),
            "unexpected error: {err:?}"
        );
        assert!(namespace.last_find.borrow().is_none());
    }

    #[test]
    fn find_tool_defaults_to_limit_ten() {
        let namespace = FakeNamespace::new();

        execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
            }),
        )
        .unwrap();

        let request = namespace.last_find.borrow().clone().unwrap();
        assert_eq!(request.limit, 10);
    }

    #[test]
    fn find_tool_rejects_limit_above_ten() {
        let namespace = FakeNamespace::new();

        let err = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "limit": 11
            }),
        )
        .unwrap_err();

        assert!(
            matches!(err, ClientError::Protocol(ref message) if message.contains("limit must be between 1 and 10")),
            "unexpected error: {err:?}"
        );
        assert!(namespace.last_find.borrow().is_none());
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
        assert_eq!(
            request.predicates[0].value,
            Some(NamespacePredicateValue::U64(1))
        );
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
        assert_eq!(
            output["matches"][0]["evidence"],
            "nokv-native:///runs/run-1@generation:7"
        );
        assert_eq!(output["matches"][0]["snapshot_id"], 9);
        assert_eq!(output["matches"][0]["generation"], 7);
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
        assert_eq!(
            match_["field_values"]["run.status"],
            json!({
                "field": "run.status",
                "value": "completed",
                "evidence": "nokv-native:///runs/run-1@generation:7",
                "source_path": "/runs/run-1",
                "source_kind": "materialized_index"
            })
        );
        assert_eq!(match_["schema"]["record_type"], "json_object");
        assert!(match_.get("indexed_values").is_none());
        assert!(match_.get("name").is_none());
        assert_eq!(match_["evidence"], "nokv-native:///runs/run-1@generation:7");
        assert_eq!(match_["generation"], 7);
    }

    #[test]
    fn aggregate_tool_groups_and_sorts_indexed_values() {
        let namespace = FakeNamespace::with_aggregate_result(sample_aggregate_result());

        let output = execute_agent_tool(
            &namespace,
            "aggregate",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "run.status", "op": "eq", "value": "completed"}],
                "group_by": ["param.lr"],
                "measures": [
                    {"name": "run_count", "op": "count", "field": "metric.val_loss.min"},
                    {"name": "avg_min_val_loss", "op": "avg", "field": "metric.val_loss.min"},
                    {"name": "stdout_available", "op": "sum", "field": "artifact.stdout_available"}
                ],
                "sort": [{"field": "avg_min_val_loss", "direction": "asc"}],
                "limit": 5
            }),
        )
        .unwrap();

        let request = namespace.last_aggregate.borrow().clone().unwrap();
        assert_eq!(request.predicates[0].field.id, "run.status");
        assert!(namespace.last_find.borrow().is_none());
        assert_eq!(output["tool"], "aggregate");
        assert_eq!(
            output["predicates"],
            json!([{"field": "run.status", "op": "eq", "value": "completed"}])
        );
        assert_eq!(output["scope_note"], Value::Null);
        assert_eq!(output["input_match_count"], 5);
        assert_eq!(output["row_count"], 3);
        assert_eq!(output["group_count"], 2);
        assert_eq!(output["groups"][0]["key"], json!({"param.lr": "0.001"}));
        assert_eq!(
            output["groups"][0]["key_fields"][0],
            json!({
                "field": "param.lr",
                "value": "0.001",
                "evidence": "nokv-native:///runs/run-1@generation:7",
                "source_path": "/runs/run-1",
                "source_kind": "materialized_index"
            })
        );
        assert_eq!(output["groups"][0]["values"]["run_count"], 2);
        assert_eq!(output["groups"][0]["values"]["avg_min_val_loss"], 0.3);
        assert_eq!(output["groups"][0]["values"]["stdout_available"], 1);
        assert_eq!(output["groups"][0]["evidence"], "nokv-native:///runs");
        assert_eq!(
            output["groups"][0]["sample_matches"][0]["evidence"],
            "nokv-native:///runs/run-1@generation:7"
        );
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
