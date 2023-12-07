// You can execute this example with `cargo run --example prometheus_metrics`

use chrono::Utc;
use lsmlite_rs::{DbConf, Disk, LsmCompressionLib, LsmDb, LsmHandleMode, LsmMetrics, LsmMode};
use prometheus::{Encoder, Histogram, HistogramOpts, Registry, TextEncoder, DEFAULT_BUCKETS};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // We first produce a file we can work on
    let now = Utc::now();
    let db_path = "/tmp".to_string();
    let db_base_name = format!(
        "{}-{}-{}",
        "example-prometheus-metrics",
        0,
        now.timestamp_nanos_opt().unwrap()
    );

    let buckets = DEFAULT_BUCKETS.to_vec();
    let opts_1 = HistogramOpts::new("write_times_s", "non_typed_write_times_s help")
        .buckets(buckets.clone());
    let opts_2 = HistogramOpts::new("work_kbs", "work_kbs help").buckets(buckets.clone());
    let opts_3 = HistogramOpts::new("work_times_s", "work_times_s help").buckets(buckets.clone());
    let opts_4 =
        HistogramOpts::new("checkpoint_kbs", "checkpoint_kbs help").buckets(buckets.clone());
    let opts_5 =
        HistogramOpts::new("checkpoint_times_s", "checkpoint_times_s help").buckets(buckets);

    let metrics = LsmMetrics {
        write_times_s: Histogram::with_opts(opts_1).unwrap(),
        work_kbs: Histogram::with_opts(opts_2).unwrap(),
        work_times_s: Histogram::with_opts(opts_3).unwrap(),
        checkpoint_kbs: Histogram::with_opts(opts_4).unwrap(),
        checkpoint_times_s: Histogram::with_opts(opts_5).unwrap(),
    };

    // This registry usually belongs to the upper layer using the engine.
    let registry = Registry::new();
    registry
        .register(Box::new(metrics.write_times_s.clone()))
        .unwrap();
    registry
        .register(Box::new(metrics.work_kbs.clone()))
        .unwrap();
    registry
        .register(Box::new(metrics.work_times_s.clone()))
        .unwrap();
    registry
        .register(Box::new(metrics.checkpoint_kbs.clone()))
        .unwrap();
    registry
        .register(Box::new(metrics.checkpoint_times_s.clone()))
        .unwrap();

    // One way to check that checkpoints are being done is by checking
    // that the size of the file increases over time. That is, data is made durable.
    // Checking metrics would be another way.
    let db_conf = DbConf::new_with_parameters(
        db_path,
        db_base_name,
        LsmMode::LsmBackgroundCheckpointer,
        LsmHandleMode::ReadWrite,
        Some(metrics),
        LsmCompressionLib::NoCompression,
    );
    let mut db: LsmDb = Default::default();

    let rc = db.initialize(db_conf);
    assert_eq!(rc, Ok(()));
    // Connect to the database. It is at this point that the file is produced.
    let rc = db.connect();
    assert_eq!(rc, Ok(()));

    // Insert data into the database, so that something gets traversed.
    // Persist numbers 1 to 100 with 1 KB zeroed payload.
    let value = vec![0; 1024];
    let max_n: usize = 100;
    for n in 1..=max_n {
        let key_serial = n.to_be_bytes();
        let rc = db.persist(&key_serial, &value);
        assert_eq!(rc, Ok(()))
    }

    // Gather the metrics.
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    // Output to the standard output.
    println!("{}", String::from_utf8(buffer).unwrap());

    Ok(())
}
