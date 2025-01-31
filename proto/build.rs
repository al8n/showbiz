use std::{env, io::Result, path::PathBuf};

fn main() -> Result<()> {
  // Get the manifest directory (directory containing Cargo.toml)
  let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

  // Create proto directory path relative to the manifest directory
  let proto_dir = manifest_dir.join("proto");
  let proto_file = proto_dir.join("message.proto");

  // Tell cargo to rerun this if the proto file changes
  println!("cargo:rerun-if-changed={}", proto_file.display());
  // Configure and run prost-build
  prost_build::Config::new()
    .out_dir(manifest_dir.join("src")) // Output the generated code to src/
    .bytes(["."])
    .format(true)
    .compile_protos(&[proto_file], &[proto_dir])?; // Assumes proto file is in proto/ directory

  // Tell cargo to rerun this if the proto file changes
  println!("cargo:rerun-if-changed=proto/proto/message.proto");

  Ok(())
}
