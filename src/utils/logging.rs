use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::writer::MakeWriterExt;

pub fn init_tracing(rust_log: &str) -> tracing_appender::non_blocking::WorkerGuard {
    let file_appender = tracing_appender::rolling::daily("logs", "server.log");
    let (non_blocking_file, guard) = tracing_appender::non_blocking(file_appender);

    let stdout = std::io::stdout.with_max_level(tracing::Level::INFO);

    let combined_writer = stdout.and(non_blocking_file);

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(rust_log))
        .with_writer(combined_writer)
        .with_target(false)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .with_span_events(FmtSpan::NONE)
        .with_file(true)
        .with_line_number(true)
        .compact()
        .init();

    guard
}
