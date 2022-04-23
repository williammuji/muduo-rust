fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile(
            &["proto/nqueens/nqueens.proto"],
            &["proto/nqueens"],
            )?;

    tonic_build::configure()
        .build_server(true)
        .compile(
            &["proto/median/median.proto"],
            &["proto/median"],
            )?;
    
    tonic_build::configure()
        .build_server(true)
        .compile(
            &["proto/wordfreq/wordfreq.proto"],
            &["proto/wordfreq"],
            )?;

    Ok(())
}
