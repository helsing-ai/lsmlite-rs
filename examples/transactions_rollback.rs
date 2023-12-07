// You can execute this example with `cargo run --example transactions_rollback`

use chrono::Utc;
use lsmlite_rs::{Cursor, DbConf, Disk, LsmDb};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // We first produce a file we can work on
    let now = Utc::now();
    let db_path = "/tmp".to_string();
    let db_base_name = format!(
        "{}-{}-{}",
        "example-transactions-rollback",
        0,
        now.timestamp_nanos_opt().unwrap()
    );

    let db_conf = DbConf::new(db_path, db_base_name);
    let mut db: LsmDb = Default::default();

    let rc = db.initialize(db_conf);
    assert_eq!(rc, Ok(()));

    // Connect to the database. It is at this point that the file is produced.
    let rc = db.connect();
    assert_eq!(rc, Ok(()));

    // Let's open a transaction
    let mut rc = db.begin_transaction();
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

    // Let's rollback the transaction (making all persisted blobs disappear).
    rc = db.rollback_transaction();
    assert_eq!(rc, Ok(()));

    // Open the cursor once we have written data (snapshot isolation).
    let mut cursor = db.cursor_open().unwrap();
    // Move the cursor to the very first record on the database.
    let rc = cursor.first();
    assert!(rc.is_ok());

    // Now traverse the database extracting the data we just added (nothing was added).
    let mut num_records = 0;
    while cursor.valid().is_ok() {
        num_records += 1;
        // Extract the key.
        let current_key = Cursor::get_key(&cursor).unwrap();
        // Parse it to an integer.
        assert!(current_key.len() == 8);
        let key = usize::from_be_bytes(current_key.try_into().unwrap());
        // Extract the value.
        let current_value = Cursor::get_value(&cursor).unwrap();
        // Everything should match.
        assert!(key == num_records);
        assert!(current_value.len() == 1024);
        // Move onto the next record.
        cursor.next().unwrap();
    }
    assert_eq!(num_records, 0);
    assert!(cursor.valid().is_err());
    Ok(())
}
