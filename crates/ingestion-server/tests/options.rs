use se_ingestion_server::IngestionOptions;

#[test]
fn ingestion_options_defaults() {
    let options = IngestionOptions::default();

    assert_eq!(options.max_in_flight_writes.get(), 32);
    assert_eq!(options.max_in_flight_bytes.get(), 256 * 1024 * 1024);
    assert_eq!(
        options.max_global_in_flight_bytes.get(),
        512 * 1024 * 1024 * 1024
    );
}
