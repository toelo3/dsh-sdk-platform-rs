#[cfg(feature = "dsh-envelope")]
fn main() -> std::io::Result<()> {
    prost_build::compile_protos(&["./src/proto/dsh.proto"], &["src"])?;
    Ok(())
}

// Needs a main fn even if Kafka is disabled.
#[cfg(not(feature = "dsh-envelope"))]
fn main() {}
