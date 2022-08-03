fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/nqueens/nqueens.proto"], &["proto/nqueens"])?;

    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/median/median.proto"], &["proto/median"])?;

    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/wordfreq/wordfreq.proto"], &["proto/wordfreq"])?;

    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/logrecord/logrecord.proto"], &["proto/logrecord"])?;

    tonic_build::configure().build_server(true).compile(
        &["proto/protobuf/rpc/sudoku.proto"],
        &["proto/protobuf/rpc"],
    )?;

    tonic_build::configure().build_server(true).compile(
        &["proto/protobuf/resolver/resolver.proto"],
        &["proto/protobuf/resolver"],
    )?;

    tonic_build::configure().build_server(true).compile(
        &["proto/protobuf/rpcbench/echo.proto"],
        &["proto/protobuf/rpcbench"],
    )?;

    tonic_build::configure().build_server(true).compile(
        &["proto/protobuf/balancer/echo.proto"],
        &["proto/protobuf/balancer"],
    )?;

    Ok(())
}
