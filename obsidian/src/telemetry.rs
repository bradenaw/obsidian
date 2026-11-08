// use opentelemetry::KeyValue;
// use opentelemetry_otlp::WithExportConfig;
// use opentelemetry_sdk::trace as sdktrace;
// use opentelemetry_sdk::Resource;
// use tracing_subscriber::layer::SubscriberExt;
// use tracing_subscriber::util::SubscriberInitExt;

// use opentelemetry::trace::TracerProvider as _;

use std::time::Duration;

use opentelemetry_sdk::metrics::SdkMeterProvider;
use tokio::spawn;

pub(crate) fn init_telemetry() -> anyhow::Result<()> {
    let meter_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .build()?;

    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(meter_exporter)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider);

    let meter = opentelemetry::global::meter("foo");
    let gauge = meter.u64_gauge("obsidian_hello_world").build();
    gauge.record(1, &[]);
    spawn(async {
        loop {
            let meter = opentelemetry::global::meter("foo");
            let counter = meter.u64_counter("obsidian_hello_world_ctr").build();
            counter.add(1, &[]);

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()?;

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(otlp_exporter)
        .build();

    //let tracer = tracer_provider.tracer("obsidian");

    opentelemetry::global::set_tracer_provider(tracer_provider);

    //let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // tracing_subscriber::registry()
    //     .with(
    //         tracing_subscriber::EnvFilter::from_default_env()
    //             .add_directive(tracing::Level::INFO.into()),
    //     )
    //     //.with(tracing_subscriber::fmt::layer()) // Log to stdout
    //     .with(telemetry_layer) // Export to OTel gRPC
    //     .init();

    Ok(())
}
