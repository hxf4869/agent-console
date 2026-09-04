//! 编译仓库根 `proto/agent_console/v1/` 下全部 .proto(proto workstream 维护)。
//!
//! 使用 protox(纯 Rust)解析,不依赖系统 protoc;生成代码写入 OUT_DIR,
//! 由 src/lib.rs include 为 `agent_console::v1` 模块。

use std::path::PathBuf;

fn main() {
    // build script 的 cwd 是本 crate 根,用 CARGO_MANIFEST_DIR 锚定仓库根 proto/。
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let proto_root = manifest_dir.join("../../proto");
    let proto_dir = proto_root.join("agent_console/v1");

    println!("cargo:rerun-if-changed={}", proto_root.display());

    let mut proto_files: Vec<PathBuf> = std::fs::read_dir(&proto_dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("proto"))
                .collect()
        })
        .unwrap_or_default();
    proto_files.sort();

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    if proto_files.is_empty() {
        // proto workstream 尚未就绪:生成空模块而不是 panic,保证 crate 可编译。
        println!(
            "cargo:warning=[agent_console_protocol] {} 下没有 .proto 文件,生成空协议模块",
            proto_dir.display()
        );
        std::fs::write(out_dir.join("agent_console.v1.rs"), "").unwrap();
        return;
    }

    let file_descriptors = protox::compile(proto_files, [proto_root]).unwrap();
    prost_build::Config::new()
        .file_descriptor_set_path(out_dir.join("descriptor.bin"))
        .compile_fds(file_descriptors)
        .unwrap();
}
