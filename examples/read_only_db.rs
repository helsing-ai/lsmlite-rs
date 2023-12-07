// You can execute this example with `cargo run --example read_only_db`

use chrono::Utc;
use lsmlite_rs::LsmCompressionLib::NoCompression;
use lsmlite_rs::{DbConf, Disk, LsmDb, LsmErrorCode, LsmHandleMode, LsmMode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // We first produce a file we can work on
    let now = Utc::now();
    let db_path = "/tmp".to_string();
    let db_base_name = format!(
        "{}-{}-{}",
        "example-read-only-mode",
        0,
        now.timestamp_nanos_opt().unwrap()
    );

    let db_conf = DbConf::new(db_path.clone(), db_base_name.clone());
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

    // Now let's open the database in read-only mode and try to write something (and fail).
    let db_conf = DbConf::new_with_parameters(
        db_path,
        db_base_name,
        LsmMode::LsmNoBackgroundThreads,
        LsmHandleMode::ReadOnly,
        None,
        NoCompression,
    );
    let mut db_ro: LsmDb = Default::default();
    let rc = db_ro.initialize(db_conf);
    assert_eq!(rc, Ok(()));

    let rc = db_ro.connect();
    assert_eq!(rc, Ok(()));

    // Construct another record and try to persist it.
    let key_serial = 101_usize.to_be_bytes();
    let rc = db_ro.persist(&key_serial, &value);
    assert_eq!(rc, Err(LsmErrorCode::LsmReadOnly));

    Ok(())
}
