use anyhow::Result;
use serde::Serialize;
use serde_json;
use std::fs::{File, create_dir_all};
use std::io::{BufWriter, Write};
use std::path::Path;
use tracing::*;

use anyhow::Context;
use tokio::fs;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReadDirStream;

// Generic function to fetch and write a resource in json format to disk
pub async fn cleanup_resource_logs(
    mount_path: &str,
    cluster_name: &str,
    retention: &u16,
    tf: &i64,
) -> Result<(), anyhow::Error> {
    let file_path = Path::new(mount_path).join(cluster_name);
    let retention_secs = (*retention as i64) * 24 * 60 * 60;

    let read_dir = fs::read_dir(&file_path)
        .await
        .context(format!("Failed to read directory {:?}", file_path))?;

    let mut entries = ReadDirStream::new(read_dir);

    while let Some(entry_result) = entries.next().await {
        let entry = entry_result?;
        let path = entry.path();

        if let Some(folder_name) = path.file_name().and_then(|s| s.to_str()) {
            match folder_name.parse::<i64>() {
                Ok(folder_timestamp) => {
                    let age = tf - folder_timestamp;
                    if age > retention_secs {
                        info!("Removing old folder: {:?}", path);
                        fs::remove_dir_all(&path)
                            .await
                            .context(format!("Failed to delete folder {:?}", path))?;
                    }
                }
                Err(_) => {
                    error!("Skipping non-numeric folder: {:?}", folder_name);
                }
            }
        }
    }

    Ok(())
}

/// Writes a list of serializable items to a file in JSONL format.
pub async fn write_json_to_file<T>(items: &[T], file_name: &str) -> Result<(), anyhow::Error>
where
    T: Serialize,
{
    let file = create_file_with_dirs(file_name)?;
    let mut writer = BufWriter::new(file);

    for item in items {
        let json_line = serde_json::to_string(item)?;
        writeln!(writer, "{}", json_line)?;
    }

    info!("Items written to {}, one per line", file_name);
    Ok(())
}

fn create_file_with_dirs(file_name: &str) -> std::io::Result<File> {
    if let Some(parent) = Path::new(file_name).parent() {
        create_dir_all(parent)?;
    }

    File::create(file_name)
}
