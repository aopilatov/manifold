use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the vendored protoc — the system may not have it.
    if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
        env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["../../proto/manifold.proto"], &["../../proto"])?;

    println!("cargo:rerun-if-changed=../../proto/manifold.proto");
    Ok(())
}
