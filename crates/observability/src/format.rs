use std::{fmt, sync::Arc};

use nu_ansi_term::{Color, Style};
use se_common::clock::{DefaultSystemClock, SystemClock};
use tracing::{field, span, Event, Level, Subscriber};
use tracing_subscriber::{
    field::{RecordFields, VisitFmt, VisitOutput},
    fmt::{format::Writer, FmtContext, FormatEvent, FormatFields},
    registry::LookupSpan,
};

pub struct StorageEngineFormat {
    format: Vec<chrono::format::Item<'static>>,
    clock: Arc<dyn SystemClock>,
}

impl<S, N> FormatEvent<S, N> for StorageEngineFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let meta = event.metadata();

        write!(
            writer,
            "{}",
            FmtLevel::new(meta.level(), writer.has_ansi_escapes())
        )?;
        writer.write_char(' ')?;
        if self.format_time(&mut writer).is_err() {
            write!(writer, "[<unknown-timestamp>]")?;
        };
        writer.write_char(' ')?;

        ctx.format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

impl<'w> FormatFields<'w> for StorageEngineFormat {
    fn format_fields<R: RecordFields>(&self, writer: Writer<'w>, fields: R) -> fmt::Result {
        let mut v = StorageEngineFormatVisitor::new(writer, true);
        fields.record(&mut v);
        v.finish()
    }

    fn add_fields(
        &self,
        current: &'w mut tracing_subscriber::fmt::FormattedFields<Self>,
        fields: &span::Record<'_>,
    ) -> fmt::Result {
        let empty = current.is_empty();
        let writer = current.as_writer();
        let mut v = StorageEngineFormatVisitor::new(writer, empty);
        fields.record(&mut v);
        v.finish()
    }
}

struct StorageEngineFormatVisitor<'a> {
    writer: Writer<'a>,
    is_empty: bool,
    style: Style,
    result: std::fmt::Result,
}

impl<'a> StorageEngineFormatVisitor<'a> {
    fn new(writer: Writer<'a>, is_empty: bool) -> Self {
        Self {
            writer,
            is_empty,
            style: Style::new(),
            result: Ok(()),
        }
    }

    fn write_padded(&mut self, v: &impl fmt::Debug) {
        let padding = if self.is_empty {
            self.is_empty = false;
            ""
        } else {
            " "
        };

        self.result = write!(self.writer, "{}{:?}", padding, v);
    }
}

impl field::Visit for StorageEngineFormatVisitor<'_> {
    fn record_str(&mut self, field: &field::Field, value: &str) {
        if self.result.is_err() {
            return;
        }

        if field.name() == "message" {
            self.record_debug(field, &format_args!("{:<60}", value))
        } else {
            self.record_debug(field, &value)
        }
    }

    fn record_debug(&mut self, field: &field::Field, value: &dyn fmt::Debug) {
        if self.result.is_err() {
            return;
        }

        let value = format!("{:?}", value);
        match field.name() {
            "message" => {
                self.write_padded(&format_args!("{}{:<40}", self.style.prefix(), value));
            }
            name => {
                let color = if field.name() == "error" {
                    Color::Red
                } else {
                    Color::Blue
                };

                if self.writer.has_ansi_escapes() {
                    self.write_padded(&format_args!(
                        "{}{}={}",
                        self.style.prefix(),
                        name,
                        color.paint(value)
                    ));
                } else {
                    self.write_padded(&format_args!("{}{}={}", self.style.prefix(), name, value));
                }
            }
        }
    }
}

impl VisitOutput<std::fmt::Result> for StorageEngineFormatVisitor<'_> {
    fn finish(mut self) -> std::fmt::Result {
        write!(&mut self.writer, "{}", self.style.suffix())?;
        self.result
    }
}

impl VisitFmt for StorageEngineFormatVisitor<'_> {
    fn writer(&mut self) -> &mut dyn fmt::Write {
        &mut self.writer
    }
}

impl StorageEngineFormat {
    pub fn new(clock: Arc<dyn SystemClock>) -> Self {
        use chrono::format::StrftimeItems;

        let format = StrftimeItems::new(r#"[%m-%d|%H:%M:%S%.3f]"#)
            .parse_to_owned()
            .expect("failed to parse time format");

        Self { format, clock }
    }

    pub fn format_time(&self, writer: &mut Writer<'_>) -> std::fmt::Result {
        let now = self.clock.now();
        write!(
            writer,
            "{}",
            now.format_with_items(self.format.as_slice().iter())
        )
    }
}

struct FmtLevel<'a> {
    level: &'a Level,
    ansi: bool,
}

impl<'a> FmtLevel<'a> {
    fn new(level: &'a Level, ansi: bool) -> Self {
        Self { level, ansi }
    }
}

impl std::fmt::Display for FmtLevel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.ansi {
            match *self.level {
                Level::TRACE => write!(f, "[{}]", Color::Purple.paint("TRACE")),
                Level::DEBUG => write!(f, "[{}]", Color::Blue.paint("DEBUG")),
                Level::INFO => write!(f, "[{}]", Color::Green.paint("INFO")),
                Level::WARN => write!(f, "[{}]", Color::Yellow.paint("WARN")),
                Level::ERROR => write!(f, "[{}]", Color::Red.paint("ERROR")),
            }
        } else {
            match *self.level {
                Level::TRACE => write!(f, "[TRACE]"),
                Level::DEBUG => write!(f, "[DEBUG]"),
                Level::INFO => write!(f, "[INFO]"),
                Level::WARN => write!(f, "[WARN]"),
                Level::ERROR => write!(f, "[ERROR]"),
            }
        }
    }
}

impl Default for StorageEngineFormat {
    fn default() -> Self {
        Self::new(Arc::new(DefaultSystemClock::new()))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    use se_common::clock::{MockSystemClock, SystemClock};
    use tracing::{dispatcher, Dispatch};
    use tracing_subscriber::fmt::format::Writer;

    use super::StorageEngineFormat;

    #[derive(Clone, Debug)]
    struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for CapturedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn fixed_clock() -> Arc<dyn SystemClock> {
        Arc::new(MockSystemClock::with_time(1_738_553_106_789))
    }

    fn formatter() -> StorageEngineFormat {
        StorageEngineFormat::new(fixed_clock())
    }

    fn capture_event(ansi: bool, emit: impl FnOnce()) -> String {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer_buffer = Arc::clone(&buffer);
        let clock = fixed_clock();

        let subscriber = tracing_subscriber::fmt()
            .with_ansi(ansi)
            .with_writer(move || CapturedWriter(Arc::clone(&writer_buffer)))
            .event_format(StorageEngineFormat::new(Arc::clone(&clock)))
            .fmt_fields(StorageEngineFormat::new(clock))
            .finish();

        dispatcher::with_default(&Dispatch::new(subscriber), emit);

        let bytes = buffer.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn format_time_writes_configured_pattern() {
        let formatter = formatter();
        let mut output = String::new();

        {
            let mut writer = Writer::new(&mut output);
            formatter.format_time(&mut writer).unwrap();
        }

        insta::assert_snapshot!(output, @"[02-03|03:25:06.789]");
    }

    #[test]
    fn format_event_writes_plain_level_time_message_and_fields() {
        let output = capture_event(false, || {
            tracing::info!(count = 2, status = "ok", "hello");
        });

        insta::assert_snapshot!(output, @r#"[INFO] [02-03|03:25:06.789] hello                                    count=2 status="ok""#);
    }

    #[test]
    fn format_event_colors_level_and_named_fields_when_ansi_is_enabled() {
        let output = capture_event(true, || {
            tracing::error!(error = "boom", retry = false, "failed");
        });

        insta::assert_snapshot!(output, @r#"[[31mERROR[0m] [02-03|03:25:06.789] failed                                   error=[31m"boom"[0m retry=[34mfalse[0m"#);
    }

    #[test]
    fn format_event_pads_explicit_string_messages() {
        let output = capture_event(false, || {
            tracing::info!(message = "short", user = "sam");
        });

        insta::assert_snapshot!(output, @r#"[INFO] [02-03|03:25:06.789] short                                                        user="sam""#);
    }
}
