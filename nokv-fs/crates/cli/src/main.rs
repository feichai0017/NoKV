//! Minimal NoKV-FS command line interface.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use nokv_fs_client::{ArtifactMetadata, NoKvFsClient};
use nokv_fs_holtstore::HoltMetadataStore;
use nokv_fs_metad::NoKvFs;
use nokv_fs_model::{FileType, MountId};
use nokv_fs_object::LocalObjectStore;

const DEFAULT_MODE_DIR: u32 = 0o755;
const DEFAULT_MODE_FILE: u32 = 0o644;
const DEFAULT_UID: u32 = 1000;
const DEFAULT_GID: u32 = 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Config {
    meta: PathBuf,
    objects: PathBuf,
    mount: MountId,
    uid: u32,
    gid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Init,
    Mkdir { path: String },
    PutArtifact { path: String, source: PathBuf },
    Ls { path: String },
    Cat { path: String },
    Help,
}

#[derive(Debug)]
enum CliError {
    MissingValue(&'static str),
    UnknownOption(String),
    UnknownCommand(String),
    MissingArgument(&'static str),
    TooManyArguments,
    InvalidMount(String),
    InvalidNumber { field: &'static str, value: String },
    Io(String),
    Client(String),
}

type LocalClient = NoKvFsClient<HoltMetadataStore, LocalObjectStore>;

fn main() {
    if let Err(err) = run(env::args().skip(1).collect()) {
        eprintln!("error: {err}");
        eprintln!();
        print_help(&mut io::stderr()).ok();
        std::process::exit(2);
    }
}

fn run(args: Vec<String>) -> Result<(), CliError> {
    let (config, command) = parse(args)?;
    match command {
        Command::Init => {
            let client = open_client(&config)?;
            client
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            println!("initialized mount {}", config.mount.get());
        }
        Command::Mkdir { path } => {
            let client = open_client(&config)?;
            client
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            let entry = client
                .mkdir(&path, DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            println!("dir {} inode={}", path, entry.attr.inode.get());
        }
        Command::PutArtifact { path, source } => {
            let bytes = fs::read(&source).map_err(from_io)?;
            let client = open_client(&config)?;
            client
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            let object_ref = default_object_ref(&path)?;
            let entry = client
                .put_artifact(
                    &path,
                    bytes,
                    ArtifactMetadata {
                        producer: "nokv-fs-cli".to_owned(),
                        digest_uri: "unknown".to_owned(),
                        content_type: "application/octet-stream".to_owned(),
                        object_ref,
                        generation: 1,
                        mode: DEFAULT_MODE_FILE,
                        uid: config.uid,
                        gid: config.gid,
                    },
                )
                .map_err(from_client)?;
            println!(
                "artifact {} inode={} size={}",
                path,
                entry.attr.inode.get(),
                entry.attr.size
            );
        }
        Command::Ls { path } => {
            let client = open_client(&config)?;
            client
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            for entry in client.list(&path).map_err(from_client)? {
                println!(
                    "{}\t{}\t{}\t{}",
                    file_type_label(entry.attr.file_type),
                    entry.attr.inode.get(),
                    entry.attr.size,
                    String::from_utf8_lossy(entry.dentry.name.as_bytes())
                );
            }
        }
        Command::Cat { path } => {
            let client = open_client(&config)?;
            let bytes = client.cat(&path).map_err(from_client)?;
            io::stdout().write_all(&bytes).map_err(from_io)?;
        }
        Command::Help => {
            print_help(&mut io::stdout()).map_err(from_io)?;
        }
    }
    Ok(())
}

fn open_client(config: &Config) -> Result<LocalClient, CliError> {
    let metadata = HoltMetadataStore::open_file(&config.meta).map_err(from_metadata)?;
    let objects = LocalObjectStore::new(&config.objects).map_err(from_object)?;
    let service = NoKvFs::open_existing(config.mount, metadata, objects)
        .map_err(|err| CliError::Client(err.to_string()))?;
    Ok(NoKvFsClient::new(service))
}

fn parse(args: Vec<String>) -> Result<(Config, Command), CliError> {
    let mut meta = PathBuf::from(".nokv-fs/meta");
    let mut objects = PathBuf::from(".nokv-fs/objects");
    let mut mount = MountId::new(1).expect("default mount id is non-zero");
    let mut uid = DEFAULT_UID;
    let mut gid = DEFAULT_GID;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--meta" => {
                index += 1;
                meta = PathBuf::from(value(&args, index, "--meta")?);
            }
            "--objects" => {
                index += 1;
                objects = PathBuf::from(value(&args, index, "--objects")?);
            }
            "--mount" => {
                index += 1;
                let raw = value(&args, index, "--mount")?;
                let parsed = raw
                    .parse::<u64>()
                    .map_err(|_| CliError::InvalidMount(raw.to_owned()))?;
                mount = MountId::new(parsed).map_err(|_| CliError::InvalidMount(raw.to_owned()))?;
            }
            "--uid" => {
                index += 1;
                uid = parse_u32(value(&args, index, "--uid")?, "uid")?;
            }
            "--gid" => {
                index += 1;
                gid = parse_u32(value(&args, index, "--gid")?, "gid")?;
            }
            "--help" | "-h" => {
                return Ok((
                    Config {
                        meta,
                        objects,
                        mount,
                        uid,
                        gid,
                    },
                    Command::Help,
                ));
            }
            option if option.starts_with('-') => {
                return Err(CliError::UnknownOption(option.to_owned()));
            }
            _ => break,
        }
        index += 1;
    }

    let command = parse_command(&args[index..])?;
    Ok((
        Config {
            meta,
            objects,
            mount,
            uid,
            gid,
        },
        command,
    ))
}

fn parse_command(args: &[String]) -> Result<Command, CliError> {
    let Some(command) = args.first().map(String::as_str) else {
        return Ok(Command::Help);
    };
    match command {
        "init" => exact_args(args, 1).map(|()| Command::Init),
        "mkdir" => exact_args(args, 2).map(|()| Command::Mkdir {
            path: args[1].clone(),
        }),
        "put-artifact" => exact_args(args, 3).map(|()| Command::PutArtifact {
            path: args[1].clone(),
            source: PathBuf::from(&args[2]),
        }),
        "ls" => exact_args(args, 2).map(|()| Command::Ls {
            path: args[1].clone(),
        }),
        "cat" => exact_args(args, 2).map(|()| Command::Cat {
            path: args[1].clone(),
        }),
        "help" => Ok(Command::Help),
        other => Err(CliError::UnknownCommand(other.to_owned())),
    }
}

fn exact_args(args: &[String], expected: usize) -> Result<(), CliError> {
    if args.len() < expected {
        return Err(CliError::MissingArgument(
            match args.first().map(String::as_str) {
                Some("mkdir") | Some("ls") | Some("cat") => "path",
                Some("put-artifact") => "path and source",
                _ => "argument",
            },
        ));
    }
    if args.len() > expected {
        return Err(CliError::TooManyArguments);
    }
    Ok(())
}

fn value<'a>(args: &'a [String], index: usize, option: &'static str) -> Result<&'a str, CliError> {
    args.get(index)
        .map(String::as_str)
        .ok_or(CliError::MissingValue(option))
}

fn parse_u32(raw: &str, field: &'static str) -> Result<u32, CliError> {
    raw.parse::<u32>().map_err(|_| CliError::InvalidNumber {
        field,
        value: raw.to_owned(),
    })
}

fn default_object_ref(path: &str) -> Result<String, CliError> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Err(CliError::MissingArgument("artifact path"));
    }
    Ok(format!("artifacts/{trimmed}"))
}

fn file_type_label(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "dir",
        FileType::Symlink => "symlink",
    }
}

fn from_io(err: impl Error) -> CliError {
    CliError::Io(err.to_string())
}

fn from_client(err: impl Error) -> CliError {
    CliError::Client(err.to_string())
}

fn from_metadata(err: impl Error) -> CliError {
    CliError::Client(err.to_string())
}

fn from_object(err: impl Error) -> CliError {
    CliError::Client(err.to_string())
}

fn print_help(out: &mut impl Write) -> io::Result<()> {
    writeln!(
        out,
        "NoKV-FS local metadata CLI\n\
\n\
Usage:\n\
  nokv-fs [--meta PATH] [--objects PATH] [--mount ID] init\n\
  nokv-fs [--meta PATH] [--objects PATH] mkdir PATH\n\
  nokv-fs [--meta PATH] [--objects PATH] put-artifact PATH SOURCE\n\
  nokv-fs [--meta PATH] [--objects PATH] ls PATH\n\
  nokv-fs [--meta PATH] [--objects PATH] cat PATH\n\
\n\
Defaults:\n\
  --meta .nokv-fs/meta\n\
  --objects .nokv-fs/objects\n\
  --mount 1"
    )
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue(option) => write!(f, "{option} requires a value"),
            Self::UnknownOption(option) => write!(f, "unknown option {option}"),
            Self::UnknownCommand(command) => write!(f, "unknown command {command}"),
            Self::MissingArgument(arg) => write!(f, "missing {arg}"),
            Self::TooManyArguments => write!(f, "too many arguments"),
            Self::InvalidMount(value) => write!(f, "invalid mount id {value}"),
            Self::InvalidNumber { field, value } => write!(f, "invalid {field} value {value}"),
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Client(err) => write!(f, "{err}"),
        }
    }
}

impl Error for CliError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(value: &str) -> String {
        value.to_owned()
    }

    #[test]
    fn parse_defaults_to_local_paths() {
        let (config, command) = parse(vec![s("ls"), s("/")]).unwrap();
        assert_eq!(config.meta, PathBuf::from(".nokv-fs/meta"));
        assert_eq!(config.objects, PathBuf::from(".nokv-fs/objects"));
        assert_eq!(config.mount.get(), 1);
        assert_eq!(command, Command::Ls { path: s("/") });
    }

    #[test]
    fn parse_global_options_before_command() {
        let (config, command) = parse(vec![
            s("--meta"),
            s("/tmp/meta"),
            s("--objects"),
            s("/tmp/objects"),
            s("--mount"),
            s("7"),
            s("mkdir"),
            s("/runs"),
        ])
        .unwrap();
        assert_eq!(config.meta, PathBuf::from("/tmp/meta"));
        assert_eq!(config.objects, PathBuf::from("/tmp/objects"));
        assert_eq!(config.mount.get(), 7);
        assert_eq!(command, Command::Mkdir { path: s("/runs") });
    }

    #[test]
    fn default_object_ref_is_relative_and_stable() {
        assert_eq!(
            default_object_ref("/runs/1/checkpoint").unwrap(),
            "artifacts/runs/1/checkpoint"
        );
        assert_eq!(
            default_object_ref("runs/1/checkpoint").unwrap(),
            "artifacts/runs/1/checkpoint"
        );
    }
}
