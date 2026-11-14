use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tokio::spawn;
use tracing::info_span;
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub(crate) fn init_telemetry() -> anyhow::Result<()> {
    let meter_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .build()?;

    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(meter_exporter)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider);

    let meter = opentelemetry::global::meter("obsidian");
    let gauge = meter.u64_gauge("obsidian_hello_world").build();
    gauge.record(1, &[]);
    spawn(async {
        loop {
            let meter = opentelemetry::global::meter("obsidian");
            let counter = meter.u64_counter("obsidian_hello_world_ctr").build();
            counter.add(1, &[]);

            tokio::time::sleep(Duration::from_millis(500))
                .instrument(info_span!("test_span"))
                .await;
        }
    });

    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()?;

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(otlp_exporter)
        .build();

    let tracer = tracer_provider.tracer("obsidian");

    opentelemetry::global::set_tracer_provider(tracer_provider);

    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
                // The spans created by OtelGrpcLayer are TRACE.
                .add_directive("otel::tracing=trace".parse()?),
        )
        .with(telemetry_layer) // Export to OTel gRPC
        .init();

    Ok(())
}
