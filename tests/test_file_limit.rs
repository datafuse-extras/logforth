// cargo test --test test_file_limit --features rolling_file -- --nocapture
#![cfg(feature = "rolling_file")]
use logforth::append::rolling_file::{RollingFileWriterBuilder, Rotation};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_global_file_count_limit() {
    // Create a temporary directory for our test
    let temp_dir = TempDir::new().expect("failed to create a temporary directory");
    let max_files = 10; // Set maximum file count to 10

    println!("Starting test for log file limit across multiple dates and time periods");

    // Create log files with different date patterns
    create_log_files_with_different_dates(temp_dir.path(), max_files, "databend-query-default");

    // Count the total number of files with our prefix
    let files = count_log_files(temp_dir.path(), "databend-query-default");

    println!("Found {} files: {:?}", files.len(), files);

    // Assert that the total number of files is limited to max_files
    assert!(
        files.len() <= max_files,
        "Expected at most {} files, but found {}: {:?}",
        max_files,
        files.len(),
        files
    );

    println!("Test passed! File count is limited to {}", max_files);
}

// Create log files with different date patterns to test file limit across multiple dates
fn create_log_files_with_different_dates(dir: &Path, max_files: usize, prefix: &str) {
    // Create a RollingFileWriter to manage the files
    let mut writer = RollingFileWriterBuilder::new()
        .rotation(Rotation::Hourly)
        .filename_prefix(prefix)
        .max_log_files(max_files)
        .max_file_size(50) // Small size to trigger rotation quickly
        .build(dir)
        .unwrap();
    
    // Create files with different date patterns
    println!("Creating files with different date patterns");
    
    // Manually create files with different date patterns
    let dates = ["2025-03-18", "2025-03-19", "2025-03-20"];
    let hours = ["08", "09", "10", "14", "15", "16", "20", "21", "22"];
    
    // Create enough files to exceed the max_files limit
    for date in dates.iter() {
        for hour in hours.iter() {
            // Create a file with this date-hour pattern
            let filename = format!("{}.{}-{}", prefix, date, hour);
            println!("Creating file with pattern: {}", filename);
            
            // Write data to trigger file creation and rotation
            for i in 0..5 {
                let data = format!("Date: {}, Hour: {}, Entry: {}\n", date, hour, i);
                writer.write_all(data.as_bytes()).unwrap();
                writer.flush().unwrap();
                
                // Brief pause to ensure file system has time to process
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
    
    // Force a final rotation to ensure we've created enough files
    for i in 0..10 {
        let data = format!("Final entry: {}\n", i);
        writer.write_all(data.as_bytes()).unwrap();
        writer.flush().unwrap();
        thread::sleep(Duration::from_millis(10));
    }
}

// Count log files with the specified prefix
fn count_log_files(dir: &Path, prefix: &str) -> Vec<String> {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let filename = entry.file_name().to_str()?.to_string();
            if filename.starts_with(prefix) {
                Some(filename)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
}
