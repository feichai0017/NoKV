use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let repo_root = manifest_dir
        .ancestors()
        .nth(3)
        .ok_or("nokv-proto crate must live under raftstore/crates/proto")?;
    let proto_root = repo_root.join("pb");
    let files = [
        "error/error.proto",
        "meta/region.proto",
        "meta/descriptor.proto",
        "meta/recovery.proto",
        "meta/root.proto",
        "coordinator/coordinator.proto",
        "metadata/metadata.proto",
        "kv/kv.proto",
        "raft/cmd.proto",
        "admin/admin.proto",
        "fsmeta/fsmeta.proto",
    ];
    let protos: Vec<_> = files.iter().map(|file| proto_root.join(file)).collect();

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&protos, &[proto_root])?;
    Ok(())
}
