use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Используем вендоренный protoc — система может его не иметь.
    if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
        env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["../../proto/socket.proto"], &["../../proto"])?;

    println!("cargo:rerun-if-changed=../../proto/socket.proto");
    Ok(())
}
