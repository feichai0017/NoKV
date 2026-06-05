use super::*;

const DEFAULT_PAGE_LIMIT: usize = 100;
const DEFAULT_SAMPLE_LIMIT: usize = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceCardKind {
    File,
    Directory,
    Symlink,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceRecordType {
    DirectoryEntries,
    JsonArray,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordCountProvenance {
    LiveNamespace,
    StructuredBody,
    MaterializedIndex,
    Approximate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceRecordCount {
    pub count: usize,
    pub provenance: RecordCountProvenance,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceSchema {
    pub record_type: NamespaceRecordType,
    pub fields: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceBodyDescriptor {
    pub producer: String,
    pub digest_uri: String,
    pub size: u64,
    pub content_type: String,
    pub manifest_id: String,
    pub generation: u64,
    pub chunk_size: u64,
    pub block_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceInclude {
    Body,
    Schema,
    Sample,
    Catalog,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceFilterCapability {
    pub field: NamespaceFindField,
    pub operators: Vec<NamespacePredicateOp>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceQueryCatalog {
    pub filterable: Vec<NamespaceFilterCapability>,
    pub sortable: Vec<NamespaceSortField>,
    pub facetable: Vec<NamespaceFindField>,
    pub projections: Vec<NamespaceInclude>,
}

impl NamespaceQueryCatalog {
    fn empty() -> Self {
        Self {
            filterable: Vec::new(),
            sortable: Vec::new(),
            facetable: Vec::new(),
            projections: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceCard {
    pub path: String,
    pub name: String,
    pub kind: NamespaceCardKind,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub inode: InodeId,
    pub generation: u64,
    pub size_bytes: Option<u64>,
    pub entry_count: Option<usize>,
    pub record_count: Option<NamespaceRecordCount>,
    pub schema: Option<NamespaceSchema>,
    pub sample: Vec<String>,
    pub body: Option<NamespaceBodyDescriptor>,
    pub catalog: NamespaceQueryCatalog,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceListOptions {
    pub cursor: Option<String>,
    pub limit: usize,
}

impl Default for NamespaceListOptions {
    fn default() -> Self {
        Self {
            cursor: None,
            limit: DEFAULT_PAGE_LIMIT,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceListPage {
    pub path: String,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub entry_count: usize,
    pub entries: Vec<NamespaceCard>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceFindField {
    Path,
    FileName,
    FileType,
    SizeBytes,
    BodyContentType,
    BodyProducer,
    BodyManifestId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespacePredicateOp {
    Eq,
    Prefix,
    Suffix,
    Contains,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespacePredicateValue {
    String(String),
    U64(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespacePredicate {
    pub field: NamespaceFindField,
    pub op: NamespacePredicateOp,
    pub value: NamespacePredicateValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceSortField {
    Path,
    FileName,
    SizeBytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceSortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceSort {
    pub field: NamespaceSortField,
    pub direction: NamespaceSortDirection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceFindRequest {
    pub path: String,
    pub predicates: Vec<NamespacePredicate>,
    pub sort: Vec<NamespaceSort>,
    pub include: Vec<NamespaceInclude>,
    pub cursor: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceFindResult {
    pub path: String,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub matches: Vec<NamespaceCard>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub scanned_entries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceReadFormat {
    Structured,
    Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceReadOptions {
    pub format: NamespaceReadFormat,
    pub cursor: Option<String>,
    pub offset: u64,
    pub limit: usize,
    pub expected_generation: Option<u64>,
}

impl Default for NamespaceReadOptions {
    fn default() -> Self {
        Self {
            format: NamespaceReadFormat::Structured,
            cursor: None,
            offset: 0,
            limit: DEFAULT_PAGE_LIMIT,
            expected_generation: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceReadItem {
    pub index: usize,
    pub value_json: String,
    pub evidence: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceReadPage {
    pub path: String,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub generation: u64,
    pub total_size_bytes: u64,
    pub format: NamespaceReadFormat,
    pub record_count: Option<usize>,
    pub cursor: Option<String>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub items: Vec<NamespaceReadItem>,
    pub bytes: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct TraversalEntry {
    path: String,
    name: String,
    entry: DentryWithAttr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CardProjection {
    body: bool,
    schema: bool,
    sample: bool,
    catalog: bool,
}

impl CardProjection {
    fn full() -> Self {
        Self {
            body: true,
            schema: true,
            sample: true,
            catalog: true,
        }
    }

    fn find(includes: &[NamespaceInclude]) -> Self {
        Self {
            body: includes.contains(&NamespaceInclude::Body),
            schema: includes.contains(&NamespaceInclude::Schema),
            sample: includes.contains(&NamespaceInclude::Sample),
            catalog: includes.contains(&NamespaceInclude::Catalog),
        }
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, MetadError> {
        let path = normalize_card_path(path)?;
        let version = self.read_version()?;
        let metadata = self.stat_path_from_at_version(InodeId::root(), &path, version)?;
        metadata
            .map(|metadata| {
                self.card_for_metadata(
                    path_name_for_card(&path),
                    &path,
                    metadata,
                    version,
                    CardProjection::full(),
                )
            })
            .transpose()
    }

    pub fn list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, MetadError> {
        let path = normalize_card_path(path)?;
        let limit = bounded_limit(options.limit)?;
        let offset = parse_cursor(options.cursor.as_deref())?;
        let version = self.read_version()?;
        let parent = self.resolve_directory_path(&path)?;
        let mut entries = self.read_dir_plus_at_version(parent, version)?;
        entries.sort_by(|left, right| {
            left.dentry
                .name
                .as_bytes()
                .cmp(right.dentry.name.as_bytes())
        });
        let entry_count = entries.len();
        let page_entries = entries
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|entry| {
                let name = String::from_utf8_lossy(entry.dentry.name.as_bytes()).to_string();
                let child_path = join_card_path(&path, &name);
                self.card_for_entry(name, child_path, entry, version, CardProjection::full())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next_offset = offset.saturating_add(page_entries.len());
        let truncated = next_offset < entry_count;
        Ok(NamespaceListPage {
            evidence: namespace_evidence(&path, None),
            path,
            snapshot_id: Some(version.get()),
            entry_count,
            entries: page_entries,
            next_cursor: truncated.then(|| next_offset.to_string()),
            truncated,
        })
    }

    pub fn find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, MetadError> {
        validate_find_request(&request)?;
        let root = normalize_card_path(&request.path)?;
        let limit = bounded_limit(request.limit)?;
        let offset = parse_cursor(request.cursor.as_deref())?;
        let version = self.read_version()?;
        let root_inode = self.resolve_directory_path(&root)?;
        let mut entries = Vec::new();
        self.collect_entries(&root, root_inode, version, &mut entries)?;
        let scanned_entries = entries.len();
        let projection = CardProjection::find(&request.include);
        let mut cards = entries
            .into_iter()
            .filter(|entry| matches_predicates(entry, &request.predicates))
            .map(|entry| {
                self.card_for_entry(entry.name, entry.path, entry.entry, version, projection)
            })
            .collect::<Result<Vec<_>, _>>()?;
        apply_sort(&mut cards, &request.sort);
        let total_matches = cards.len();
        let matches = cards
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();
        let next_offset = offset.saturating_add(matches.len());
        let truncated = next_offset < total_matches;
        Ok(NamespaceFindResult {
            evidence: namespace_evidence(&root, None),
            path: root,
            snapshot_id: Some(version.get()),
            matches,
            next_cursor: truncated.then(|| next_offset.to_string()),
            truncated,
            scanned_entries,
        })
    }

    pub fn read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, MetadError> {
        let path = normalize_card_path(path)?;
        let metadata = self.stat_path(&path)?.ok_or(MetadError::NotFound)?;
        if metadata.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if let Some(expected) = options.expected_generation {
            if expected != metadata.attr.generation {
                return Err(MetadError::StaleBodyGeneration {
                    expected,
                    current: metadata.attr.generation,
                });
            }
        }
        match options.format {
            NamespaceReadFormat::Bytes => self.read_bytes_page(path, metadata, options),
            NamespaceReadFormat::Structured => self.read_structured_page(path, metadata, options),
        }
    }

    fn card_for_entry(
        &self,
        name: String,
        path: String,
        entry: DentryWithAttr,
        version: Version,
        projection: CardProjection,
    ) -> Result<NamespaceCard, MetadError> {
        self.card_for_metadata(
            name,
            &path,
            PathMetadata {
                attr: entry.attr,
                body: entry.body,
            },
            version,
            projection,
        )
    }

    fn card_for_metadata(
        &self,
        name: String,
        path: &str,
        metadata: PathMetadata,
        version: Version,
        projection: CardProjection,
    ) -> Result<NamespaceCard, MetadError> {
        let kind = card_kind(metadata.attr.file_type);
        let body_descriptor = metadata.body.as_ref().map(namespace_body_descriptor);
        let mut entry_count = None;
        let mut record_count = None;
        let mut schema = None;
        let mut sample = Vec::new();
        if metadata.attr.file_type == FileType::Directory {
            let entries = self.read_dir_plus_at_version(metadata.attr.inode, version)?;
            let count = entries.len();
            entry_count = Some(count);
            record_count = Some(NamespaceRecordCount {
                count,
                provenance: RecordCountProvenance::LiveNamespace,
            });
            if projection.schema {
                schema = Some(NamespaceSchema {
                    record_type: NamespaceRecordType::DirectoryEntries,
                    fields: vec![
                        "path".to_owned(),
                        "file.name".to_owned(),
                        "file.type".to_owned(),
                        "file.size_bytes".to_owned(),
                    ],
                });
            }
            if projection.sample {
                sample = entries
                    .iter()
                    .take(DEFAULT_SAMPLE_LIMIT)
                    .map(|entry| String::from_utf8_lossy(entry.dentry.name.as_bytes()).to_string())
                    .collect();
            }
        } else if metadata.attr.file_type == FileType::File
            && (projection.schema || projection.sample)
            && is_structured_json(path, body_descriptor.as_ref())
        {
            if let Some(summary) = self.json_array_summary(&metadata, DEFAULT_SAMPLE_LIMIT)? {
                record_count = Some(NamespaceRecordCount {
                    count: summary.count,
                    provenance: RecordCountProvenance::StructuredBody,
                });
                if projection.schema {
                    schema = Some(NamespaceSchema {
                        record_type: NamespaceRecordType::JsonArray,
                        fields: summary.fields,
                    });
                }
                if projection.sample {
                    sample = summary.sample;
                }
            }
        }
        let body = projection.body.then_some(body_descriptor).flatten();
        let catalog = if projection.catalog {
            namespace_query_catalog(kind.clone())
        } else {
            NamespaceQueryCatalog::empty()
        };
        Ok(NamespaceCard {
            evidence: namespace_evidence(path, Some(metadata.attr.generation)),
            path: path.to_owned(),
            name,
            kind,
            snapshot_id: Some(version.get()),
            inode: metadata.attr.inode,
            generation: metadata.attr.generation,
            size_bytes: (metadata.attr.file_type != FileType::Directory)
                .then_some(metadata.attr.size),
            entry_count,
            record_count,
            schema,
            sample,
            body,
            catalog,
        })
    }

    fn collect_entries(
        &self,
        root_path: &str,
        root_inode: InodeId,
        version: Version,
        out: &mut Vec<TraversalEntry>,
    ) -> Result<(), MetadError> {
        let mut entries = self.read_dir_plus_at_version(root_inode, version)?;
        entries.sort_by(|left, right| {
            left.dentry
                .name
                .as_bytes()
                .cmp(right.dentry.name.as_bytes())
        });
        for entry in entries {
            let name = String::from_utf8_lossy(entry.dentry.name.as_bytes()).to_string();
            let path = join_card_path(root_path, &name);
            if entry.attr.file_type == FileType::Directory {
                self.collect_entries(&path, entry.attr.inode, version, out)?;
            }
            out.push(TraversalEntry { path, name, entry });
        }
        Ok(())
    }

    fn json_array_summary(
        &self,
        metadata: &PathMetadata,
        sample_limit: usize,
    ) -> Result<Option<JsonArraySummary>, MetadError> {
        let body = metadata
            .body
            .as_ref()
            .ok_or(MetadError::MissingBodyDescriptor)?;
        let bytes = self.read_file_at_version(
            metadata.attr.inode,
            body,
            0,
            file_len(metadata.attr.size)?,
            self.read_version()?,
        )?;
        let value = serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|err| {
            MetadError::InvalidQuery(format!("structured JSON parse failed: {err}"))
        })?;
        let Some(items) = value.as_array() else {
            return Ok(None);
        };
        let fields = infer_json_array_fields(items);
        let sample = items
            .iter()
            .take(sample_limit)
            .map(json_value_string)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(JsonArraySummary {
            count: items.len(),
            fields,
            sample,
        }))
    }

    fn read_bytes_page(
        &self,
        path: String,
        metadata: PathMetadata,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, MetadError> {
        let limit = bounded_limit(options.limit)?;
        let bytes = self.read_file(metadata.attr.inode, options.offset, limit)?;
        let next_offset = options.offset.saturating_add(
            u64::try_from(bytes.len())
                .map_err(|_| MetadError::InvalidQuery("read length overflow".to_owned()))?,
        );
        let truncated = next_offset < metadata.attr.size;
        Ok(NamespaceReadPage {
            evidence: namespace_evidence(&path, Some(metadata.attr.generation)),
            path,
            snapshot_id: Some(self.read_version()?.get()),
            generation: metadata.attr.generation,
            total_size_bytes: metadata.attr.size,
            format: NamespaceReadFormat::Bytes,
            record_count: None,
            cursor: options.cursor,
            next_cursor: truncated.then(|| next_offset.to_string()),
            truncated,
            items: Vec::new(),
            bytes: Some(bytes),
        })
    }

    fn read_structured_page(
        &self,
        path: String,
        metadata: PathMetadata,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, MetadError> {
        let limit = bounded_limit(options.limit)?;
        let offset = parse_cursor(options.cursor.as_deref())?;
        let body = metadata
            .body
            .as_ref()
            .ok_or(MetadError::MissingBodyDescriptor)?;
        let bytes = self.read_file_at_version(
            metadata.attr.inode,
            body,
            0,
            file_len(metadata.attr.size)?,
            self.read_version()?,
        )?;
        let value = serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|err| {
            MetadError::InvalidQuery(format!("structured JSON parse failed: {err}"))
        })?;
        let items = value.as_array().ok_or_else(|| {
            MetadError::InvalidQuery("structured read currently supports JSON arrays".to_owned())
        })?;
        let page_items = items
            .iter()
            .enumerate()
            .skip(offset)
            .take(limit)
            .map(|(index, value)| {
                Ok(NamespaceReadItem {
                    index,
                    value_json: json_value_string(value)?,
                    evidence: format!(
                        "{}#item:{index}",
                        namespace_evidence(&path, Some(metadata.attr.generation))
                    ),
                })
            })
            .collect::<Result<Vec<_>, MetadError>>()?;
        let next_offset = offset.saturating_add(page_items.len());
        let truncated = next_offset < items.len();
        Ok(NamespaceReadPage {
            evidence: namespace_evidence(&path, Some(metadata.attr.generation)),
            path,
            snapshot_id: Some(self.read_version()?.get()),
            generation: metadata.attr.generation,
            total_size_bytes: metadata.attr.size,
            format: NamespaceReadFormat::Structured,
            record_count: Some(items.len()),
            cursor: options.cursor,
            next_cursor: truncated.then(|| next_offset.to_string()),
            truncated,
            items: page_items,
            bytes: None,
        })
    }
}

struct JsonArraySummary {
    count: usize,
    fields: Vec<String>,
    sample: Vec<String>,
}

fn validate_find_request(request: &NamespaceFindRequest) -> Result<(), MetadError> {
    bounded_limit(request.limit)?;
    for predicate in &request.predicates {
        validate_predicate(predicate)?;
    }
    for sort in &request.sort {
        match sort.field {
            NamespaceSortField::Path
            | NamespaceSortField::FileName
            | NamespaceSortField::SizeBytes => {}
        }
    }
    Ok(())
}

fn namespace_query_catalog(kind: NamespaceCardKind) -> NamespaceQueryCatalog {
    if kind != NamespaceCardKind::Directory {
        return NamespaceQueryCatalog::empty();
    }
    NamespaceQueryCatalog {
        filterable: vec![
            string_filter_capability(NamespaceFindField::Path),
            string_filter_capability(NamespaceFindField::FileName),
            string_filter_capability(NamespaceFindField::FileType),
            numeric_filter_capability(NamespaceFindField::SizeBytes),
            string_filter_capability(NamespaceFindField::BodyContentType),
            string_filter_capability(NamespaceFindField::BodyProducer),
            string_filter_capability(NamespaceFindField::BodyManifestId),
        ],
        sortable: vec![
            NamespaceSortField::Path,
            NamespaceSortField::FileName,
            NamespaceSortField::SizeBytes,
        ],
        facetable: vec![
            NamespaceFindField::FileType,
            NamespaceFindField::BodyContentType,
            NamespaceFindField::BodyProducer,
        ],
        projections: vec![
            NamespaceInclude::Body,
            NamespaceInclude::Schema,
            NamespaceInclude::Sample,
            NamespaceInclude::Catalog,
        ],
    }
}

fn string_filter_capability(field: NamespaceFindField) -> NamespaceFilterCapability {
    NamespaceFilterCapability {
        field,
        operators: vec![
            NamespacePredicateOp::Eq,
            NamespacePredicateOp::Prefix,
            NamespacePredicateOp::Suffix,
            NamespacePredicateOp::Contains,
        ],
    }
}

fn numeric_filter_capability(field: NamespaceFindField) -> NamespaceFilterCapability {
    NamespaceFilterCapability {
        field,
        operators: vec![
            NamespacePredicateOp::Eq,
            NamespacePredicateOp::GreaterThan,
            NamespacePredicateOp::GreaterThanOrEqual,
            NamespacePredicateOp::LessThan,
            NamespacePredicateOp::LessThanOrEqual,
        ],
    }
}

fn validate_predicate(predicate: &NamespacePredicate) -> Result<(), MetadError> {
    match (&predicate.field, &predicate.op, &predicate.value) {
        (
            NamespaceFindField::Path
            | NamespaceFindField::FileName
            | NamespaceFindField::FileType
            | NamespaceFindField::BodyContentType
            | NamespaceFindField::BodyProducer
            | NamespaceFindField::BodyManifestId,
            NamespacePredicateOp::Eq
            | NamespacePredicateOp::Prefix
            | NamespacePredicateOp::Suffix
            | NamespacePredicateOp::Contains,
            NamespacePredicateValue::String(_),
        ) => Ok(()),
        (
            NamespaceFindField::SizeBytes,
            NamespacePredicateOp::Eq
            | NamespacePredicateOp::GreaterThan
            | NamespacePredicateOp::GreaterThanOrEqual
            | NamespacePredicateOp::LessThan
            | NamespacePredicateOp::LessThanOrEqual,
            NamespacePredicateValue::U64(_),
        ) => Ok(()),
        _ => Err(MetadError::InvalidQuery(format!(
            "unsupported predicate {:?} {:?} {:?}",
            predicate.field, predicate.op, predicate.value
        ))),
    }
}

fn matches_predicates(entry: &TraversalEntry, predicates: &[NamespacePredicate]) -> bool {
    predicates
        .iter()
        .all(|predicate| matches_predicate(entry, predicate))
}

fn matches_predicate(entry: &TraversalEntry, predicate: &NamespacePredicate) -> bool {
    match &predicate.value {
        NamespacePredicateValue::String(expected) => string_field(entry, predicate)
            .map(|actual| matches_string(&actual, &predicate.op, expected))
            .unwrap_or(false),
        NamespacePredicateValue::U64(expected) => u64_field(entry, predicate)
            .map(|actual| matches_u64(actual, &predicate.op, *expected))
            .unwrap_or(false),
    }
}

fn string_field(entry: &TraversalEntry, predicate: &NamespacePredicate) -> Option<String> {
    match predicate.field {
        NamespaceFindField::Path => Some(entry.path.clone()),
        NamespaceFindField::FileName => Some(entry.name.clone()),
        NamespaceFindField::FileType => {
            Some(file_type_label(entry.entry.attr.file_type).to_owned())
        }
        NamespaceFindField::BodyContentType => entry
            .entry
            .body
            .as_ref()
            .map(|body| body.content_type.clone()),
        NamespaceFindField::BodyProducer => {
            entry.entry.body.as_ref().map(|body| body.producer.clone())
        }
        NamespaceFindField::BodyManifestId => entry
            .entry
            .body
            .as_ref()
            .map(|body| body.manifest_id.clone()),
        NamespaceFindField::SizeBytes => None,
    }
}

fn u64_field(entry: &TraversalEntry, predicate: &NamespacePredicate) -> Option<u64> {
    match predicate.field {
        NamespaceFindField::SizeBytes => Some(entry.entry.attr.size),
        _ => None,
    }
}

fn matches_string(actual: &str, op: &NamespacePredicateOp, expected: &str) -> bool {
    match op {
        NamespacePredicateOp::Eq => actual == expected,
        NamespacePredicateOp::Prefix => actual.starts_with(expected),
        NamespacePredicateOp::Suffix => actual.ends_with(expected),
        NamespacePredicateOp::Contains => actual.contains(expected),
        _ => false,
    }
}

fn matches_u64(actual: u64, op: &NamespacePredicateOp, expected: u64) -> bool {
    match op {
        NamespacePredicateOp::Eq => actual == expected,
        NamespacePredicateOp::GreaterThan => actual > expected,
        NamespacePredicateOp::GreaterThanOrEqual => actual >= expected,
        NamespacePredicateOp::LessThan => actual < expected,
        NamespacePredicateOp::LessThanOrEqual => actual <= expected,
        _ => false,
    }
}

fn apply_sort(cards: &mut [NamespaceCard], sort: &[NamespaceSort]) {
    for sort_key in sort.iter().rev() {
        cards.sort_by(|left, right| {
            let ordering = match sort_key.field {
                NamespaceSortField::Path => left.path.cmp(&right.path),
                NamespaceSortField::FileName => left.name.cmp(&right.name),
                NamespaceSortField::SizeBytes => left.size_bytes.cmp(&right.size_bytes),
            };
            match sort_key.direction {
                NamespaceSortDirection::Asc => ordering,
                NamespaceSortDirection::Desc => ordering.reverse(),
            }
        });
    }
}

fn bounded_limit(limit: usize) -> Result<usize, MetadError> {
    if limit == 0 {
        return Err(MetadError::InvalidQuery(
            "limit must be greater than zero".to_owned(),
        ));
    }
    Ok(limit.min(1000))
}

fn parse_cursor(cursor: Option<&str>) -> Result<usize, MetadError> {
    cursor
        .unwrap_or("0")
        .parse::<usize>()
        .map_err(|err| MetadError::InvalidQuery(format!("invalid cursor: {err}")))
}

fn normalize_card_path(path: &str) -> Result<String, MetadError> {
    let components = parse_absolute_path(path)?;
    let mut out = String::from("/");
    out.push_str(
        &components
            .iter()
            .map(|name| String::from_utf8_lossy(name.as_bytes()).to_string())
            .collect::<Vec<_>>()
            .join("/"),
    );
    if out.len() > 1 && out.ends_with('/') {
        out.pop();
    }
    Ok(out)
}

fn path_name_for_card(path: &str) -> String {
    if path == "/" {
        "/".to_owned()
    } else {
        path.rsplit('/').next().unwrap_or(path).to_owned()
    }
}

fn join_card_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn namespace_evidence(path: &str, generation: Option<u64>) -> String {
    match generation {
        Some(generation) => format!("nokv-native://{path}@generation:{generation}"),
        None => format!("nokv-native://{path}"),
    }
}

fn card_kind(file_type: FileType) -> NamespaceCardKind {
    match file_type {
        FileType::File => NamespaceCardKind::File,
        FileType::Directory => NamespaceCardKind::Directory,
        FileType::Symlink => NamespaceCardKind::Symlink,
    }
}

fn namespace_body_descriptor(body: &BodyDescriptor) -> NamespaceBodyDescriptor {
    NamespaceBodyDescriptor {
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

fn is_structured_json(path: &str, body: Option<&NamespaceBodyDescriptor>) -> bool {
    body.map(|body| body.content_type == "application/json")
        .unwrap_or(false)
        || path.ends_with(".json")
}

fn infer_json_array_fields(items: &[serde_json::Value]) -> Vec<String> {
    let mut fields = Vec::new();
    for item in items {
        if let Some(object) = item.as_object() {
            for key in object.keys() {
                if !fields.contains(key) {
                    fields.push(key.clone());
                }
            }
        }
    }
    fields.sort();
    fields
}

fn json_value_string(value: &serde_json::Value) -> Result<String, MetadError> {
    serde_json::to_string(value)
        .map_err(|err| MetadError::InvalidQuery(format!("structured JSON encode failed: {err}")))
}

fn file_len(size: u64) -> Result<usize, MetadError> {
    usize::try_from(size).map_err(|_| {
        MetadError::InvalidQuery(format!("file size {size} does not fit this platform"))
    })
}

fn file_type_label(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "directory",
        FileType::Symlink => "symlink",
    }
}
