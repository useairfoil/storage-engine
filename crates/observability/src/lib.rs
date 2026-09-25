use std::{borrow::Cow, sync::Arc, time::Duration};

use opentelemetry::{global, trace::TracerProvider as _, InstrumentationScope};
pub use opentelemetry::{
    metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter},
    KeyValue,
};
use opentelemetry_otlp::{ExporterBuildError, MetricExporter, SpanExporter};
use opentelemetry_sdk::{
    metrics::{MeterProviderBuilder, PeriodicReader},
    trace::SdkTracerProvider,
    Resource,
};
use se_common::clock::SystemClock;
use tracing::Subscriber;
use tracing_opentelemetry::MetricsLayer;
use tracing_subscriber::{prelude::*, registry::LookupSpan, EnvFilter, Layer};

use crate::format::StorageEngineFormat;

mod format;

const OTEL_SDK_DISABLED: &str = "OTEL_SDK_DISABLED";

pub type BoxedLayer<S> = Box<dyn Layer<S> + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum ObservabilityError {
    #[error("failed to build exporter: {0}")]
    Exporter(#[from] ExporterBuildError),
}

pub fn meter(name: &'static str) -> Meter {
    global::meter(name)
}

pub fn init_observability(
    package_name: impl Into<Cow<'static, str>>,
    package_version: impl Into<Cow<'static, str>>,
    clock: Arc<dyn SystemClock>,
) -> Result<(), ObservabilityError> {
    // The otel sdk doesn't follow the disabled env variable flag.
    // so we manually implement it to disable otel exports.
    // we diverge from the spec by defaulting to disabled.
    let sdk_disabled = std::env::var(OTEL_SDK_DISABLED)
        .map(|v| v == "true")
        .unwrap_or(true);

    if std::env::var("RUST_LOG").is_err() {
        unsafe {
            std::env::set_var("RUST_LOG", "info");
        }
    }

    let otel_layer = otel(package_name, package_version, !sdk_disabled)?;

    let layers = vec![stdout(clock), otel_layer];

    tracing_subscriber::registry().with(layers).init();

    Ok(())
}

fn stdout<S>(clock: Arc<dyn SystemClock>) -> BoxedLayer<S>
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    let log_env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));

    let json_fmt = std::env::var("RUST_LOG_FORMAT")
        .map(|val| val == "json")
        .unwrap_or(false);

    if json_fmt {
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_target(true)
            .json()
            .with_filter(log_env_filter)
            .boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .with_ansi(true)
            .event_format(StorageEngineFormat::new(Arc::clone(&clock)))
            .fmt_fields(StorageEngineFormat::new(clock))
            .with_filter(log_env_filter)
            .boxed()
    }
}

fn otel<S>(
    package_name: impl Into<Cow<'static, str>>,
    version: impl Into<Cow<'static, str>>,
    sdk_enabled: bool,
) -> Result<BoxedLayer<S>, ObservabilityError>
where
    S: Subscriber + Send + Sync,
    for<'a> S: LookupSpan<'a>,
{
    let package_name = package_name.into();
    let version = version.into();

    let resource = Resource::builder().build();

    // filter traces by crate/level
    let otel_env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));

    let instrumentation_lib = InstrumentationScope::builder(package_name.clone())
        .with_version(version.clone())
        .build();

    let mut trace_provider = SdkTracerProvider::builder().with_resource(resource.clone());

    if sdk_enabled {
        let span_exporter = SpanExporter::builder().with_tonic().build()?;

        trace_provider = trace_provider.with_batch_exporter(span_exporter);
    }

    let trace_provider = trace_provider.build();
    let tracer = trace_provider.tracer_with_scope(instrumentation_lib.clone());

    let mut meter_provider = MeterProviderBuilder::default().with_resource(resource);

    if sdk_enabled {
        let metrics_exporter = MetricExporter::builder().with_tonic().build()?;

        let metrics_reader = PeriodicReader::builder(metrics_exporter)
            .with_interval(Duration::from_secs(10))
            .build();

        meter_provider = meter_provider.with_reader(metrics_reader);
    }

    let meter_provider = meter_provider.build();

    global::set_meter_provider(meter_provider.clone());

    // export traces and metrics to otel
    let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let otel_metrics_layer = MetricsLayer::new(meter_provider);
    let otel_layer = otel_env_filter
        .and_then(otel_metrics_layer)
        .and_then(otel_trace_layer)
        .boxed();

    Ok(otel_layer)
}
