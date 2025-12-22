use crate::crd::PersistentVolumeSync;

use anyhow::{Result, anyhow};
use tracing::info;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::ObjectMeta as StoreObjectMeta;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use std::time::Duration;

use tokio::{sync::mpsc, task, time::sleep};
use tracing::debug;

// --- Object Store Builder ---

pub async fn initialize_object_store(provider: &str) -> Result<Box<dyn ObjectStore>> {
    let store = match provider {
        "azure" => {
            let container_name = env::var("OBJECT_STORAGE_BUCKET")
                .map_err(|_| anyhow::anyhow!("OBJECT_STORAGE_BUCKET must be set for Azure"))?;

            initialize_azure_store(&container_name)?
        }
        "s3" => {
            let bucket_name = env::var("OBJECT_STORAGE_BUCKET")
                .map_err(|_| anyhow::anyhow!("OBJECT_STORAGE_BUCKET must be set for S3"))?;

            let endpoint = env::var("S3_ENDPOINT_URL").ok();

            initialize_s3_store(&bucket_name, endpoint.as_deref())?
        }
        _ => {
            anyhow::bail!(
                "Unsupported CLOUD_PROVIDER: {}. Use 'azure' or 's3'.",
                provider
            )
        }
    };
    Ok(Box::new(store))
}

/// Initializes and returns an Arc to the Azure Blob ObjectStore.
///
/// It relies on environment variables (AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_ACCESS_KEY)
/// and requires the container name.
pub fn initialize_azure_store(container_name: &str) -> Result<Arc<dyn ObjectStore>> {
    // Note: We use env::var() here. The caller (main) is responsible for loading the .env file.
    let account = env::var("OBJECT_STORAGE_ACCOUNT")
        .map_err(|_| anyhow!("OBJECT_STORAGE_ACCOUNT not found in environment"))?;

    let access_key = env::var("OBJECT_STORAGE_SECRET")
        .map_err(|_| anyhow!("OBJECT_STORAGE_SECRET not found in environment"))?;

    info!("Attempting to connect to Azure Blob Storage...");

    let store = MicrosoftAzureBuilder::new()
        .with_account(account)
        .with_access_key(access_key)
        .with_container_name(container_name)
        .build()
        .map_err(|e| anyhow!("Failed to build Azure Store: {}", e))?;

    Ok(Arc::new(store))
}

/// Initializes and returns an Arc to the S3 ObjectStore.
///
/// It can connect to:
/// 1. AWS S3 (if endpoint_url is None).
/// 2. S3-compatible vendors like Cloudian and MinIO (if endpoint_url is Some).
pub fn initialize_s3_store(
    bucket_name: &str,
    endpoint_url: Option<&str>,
) -> Result<Arc<dyn ObjectStore>> {
    // Note: We use env::var() for access keys, which is standard for both AWS and generic S3.
    let access_key = env::var("OBJECT_STORAGE_ACCOUNT")
        .map_err(|_| anyhow!("OBJECT_STORAGE_ACCOUNT (or equivalent S3 key) not found"))?;

    let secret_key = env::var("OBJECT_STORAGE_SECRET")
        .map_err(|_| anyhow!("OBJECT_STORAGE_SECRET (or equivalent S3 secret) not found"))?;

    info!("Attempting to connect to S3-compatible Storage...");

    let mut builder = AmazonS3Builder::new()
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_bucket_name(bucket_name);

    // CRITICAL: Configure for S3-compatible services (Cloudian/MinIO)
    if let Some(endpoint) = endpoint_url {
        info!("Using custom S3 endpoint: {}", endpoint);
        builder = builder
            .with_endpoint(endpoint)
            // S3-compatible vendors often run on HTTP without SSL, so we allow it.
            .with_allow_http(true);
    } else {
        info!("Using AWS S3 defaults (region must be configured via env or default).");
    }

    let store = builder
        .build()
        .map_err(|e| anyhow!("Failed to build S3 Store: {}", e))?;

    Ok(Arc::new(store))
}

/// Lists files at a prefix and returns the metadata of the most recently modified file.
pub async fn get_latest_file(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<Option<StoreObjectMeta>> {
    let path = Path::from(prefix);

    info!("Listing objects in store at prefix: {}", path);

    // Get the stream of objects
    let mut list_stream = store.list(Some(&path));

    let mut latest_object: Option<StoreObjectMeta> = None;

    // Using try_next() automatically handles the Result wrapping each item in the stream
    while let Some(meta) = list_stream
        .try_next()
        .await
        .map_err(|e| anyhow!("Error listing objects: {}", e))?
    {
        match &latest_object {
            None => {
                latest_object = Some(meta);
            }
            Some(current_latest) => {
                // Comparing chrono::DateTime<Utc> values
                if meta.last_modified > current_latest.last_modified {
                    latest_object = Some(meta);
                }
            }
        }
    }

    if let Some(ref obj) = latest_object {
        info!(
            "Latest file found: {} (Modified: {}, Size: {} bytes)",
            obj.location, obj.last_modified, obj.size
        );
    } else {
        info!("No files found at the specified prefix.");
    }

    Ok(latest_object)
}

/// Downloads the actual content of the latest file found in the store.
pub async fn get_latest_file_content(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<Option<Bytes>> {
    // 1. Get the metadata first (using the function we just wrote)
    let latest_meta = get_latest_file(store.clone(), prefix).await?;

    if let Some(meta) = latest_meta {
        info!("Downloading latest file: {}", meta.location);

        // 2. Fetch the actual object from the store
        let result = store
            .get(&meta.location)
            .await
            .map_err(|e| anyhow!("Failed to get object: {}", e))?;

        // 3. Stream the body into a Bytes buffer
        let bytes = result
            .bytes()
            .await
            .map_err(|e| anyhow!("Failed to read bytes: {}", e))?;

        Ok(Some(bytes))
    } else {
        Ok(None)
    }
}

// --- Write Function ---

/// Writes a slice of bytes to a specified path in the object store.
pub async fn write_data(store: Arc<dyn ObjectStore>, path: &str, data: &[u8]) -> Result<()> {
    let object_key = Path::from(path);
    let container_name =
        env::var("OBJECT_STORAGE_BUCKET").unwrap_or_else(|_| "Unknown Container".to_string());

    let bytes_payload = Bytes::from(data.to_vec());
    let payload: PutPayload = PutPayload::from(bytes_payload);

    info!(
        "Writing data to container '{}' at path: {}",
        container_name, object_key
    );

    // Perform the atomic write operation.
    let result = store
        .put(&object_key, payload)
        .await
        .map_err(|e| anyhow!("Failed to put object: {}", e))?;

    info!("File written successfully!");
    info!("Returned E-Tag: {:?}", result.e_tag);

    // Verification (HEAD request).
    let metadata = store
        .head(&object_key)
        .await
        .map_err(|e| anyhow!("Failed to read metadata: {}", e))?;
    info!("--- Verification ---");
    info!("Retrieved object size: {} bytes", metadata.size);

    // Cleanup.
    //store.delete(&object_key).await
    //    .map_err(|e| anyhow!("Failed to delete object: {}", e))?;
    //info!("Object deleted successfully for cleanup.");

    Ok(())
}

// --- Data Structures ---
// Tracks object paths/metadata to detect changes in the listing of a prefix.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
struct ObjectListing(Vec<String>);

/// Enum to represent the possible outcomes of the object store list poll.
enum PollResult {
    Changed { new_listing: ObjectListing },
}

// --- Object Store Watcher Core ---

/// Watcher for an object store prefix using listing comparison to detect changes.
pub async fn start_object_store_watcher(
    cr: &PersistentVolumeSync,
    polling_interval: Duration,
    tx: mpsc::Sender<()>,
) -> Result<(), anyhow::Error> {
    // Store the object paths/metadata from the last successful list
    let mut last_state: Option<ObjectListing> = None;

    // 1. INITIALIZE THE OBJECT STORE BASED ON THE CR SPECIFICATIONS
    let external_prefix: Path = cr.spec.protected_cluster.clone().into();
    let provider: &str = cr.spec.cloud_provider.as_str();
    let raw_store: Box<dyn ObjectStore> = initialize_object_store(provider).await?;

    // OBJECT STORE IN AN ARC TO ENABLE SHARING AND CLONING (Reference Counting)
    // We assume the type of raw_store is Box<dyn ObjectStore>.
    let object_store: Arc<dyn ObjectStore> = Arc::from(raw_store);

    info!(
        "Starting object_store-based watcher for prefix: {}",
        external_prefix
    );

    task::spawn(async move {
        loop {
            // Wait for the polling interval
            sleep(polling_interval).await;

            debug!(
                "Polling object store prefix. Last state size: {:?}",
                last_state.as_ref().map(|l| l.0.len())
            );

            match poll_object_store_prefix(object_store.clone(), external_prefix.clone()).await {
                Some(PollResult::Changed { new_listing }) => {
                    // 1. Change Detected (New listing data)
                    if last_state.as_ref() != Some(&new_listing) {
                        info!(
                            "Object Store Prefix Change Detected! New list size: {}",
                            new_listing.0.len()
                        );

                        last_state = Some(new_listing);

                        // Send signal to the controller/reconciler
                        if let Err(e) = tx.send(()).await {
                            info!(
                                "Failed to send change signal, receiver likely dropped: {}",
                                e
                            );
                            break;
                        }
                    } else {
                        debug!("Object list retrieved, but no effective change detected.");
                    }
                }
                None => {
                    // 2. Error during Poll
                    info!("Object Store Watcher Poll Failed (see logs above). Retrying...");
                }
            }
        }

        info!("Object store watcher task finished.");
    });

    Ok(())
}

// --- Polling Logic ---
/// Fetches the resource listing from the object store prefix.
async fn poll_object_store_prefix(store: Arc<dyn ObjectStore>, prefix: Path) -> Option<PollResult> {
    // FIX: Assign the stream directly. The compiler has confirmed store.list()
    // returns Pin<Box<dyn Stream<...>>> in this environment, not a Result wrapper.
    let listing_stream = store.list(Some(&prefix));

    // Consume the stream and collect object metadata.
    let all_objects: Vec<StoreObjectMeta> = match listing_stream
        .try_collect::<Vec<StoreObjectMeta>>() // Use the correct StoreObjectMeta type
        .await
    {
        Ok(objects) => objects,
        Err(e) => {
            // Handle the error that occurred while reading the stream (list collection failure).
            match e {
                ObjectStoreError::NotFound { path, source: _ } => {
                    info!("Object store prefix not found: {}", path);
                }
                _ => {
                    info!("Object store listing failed during collection: {}", e);
                }
            }
            return None;
        }
    };

    // Map the metadata into our simplified listing structure.
    let paths: Vec<String> = all_objects
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect();

    Some(PollResult::Changed {
        new_listing: ObjectListing(paths),
    })
}
