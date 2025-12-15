use crate::crd::PersistentVolumeSync;
use crate::resource;

use anyhow::{Result, anyhow};
use tracing::info;

use k8s_openapi::api::core::v1::{
    PersistentVolume, PersistentVolumeClaim, PersistentVolumeClaimSpec, PersistentVolumeSpec,
    VolumeResourceRequirements,
};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Client;
use serde::{Deserialize, Serialize};

use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload, Error as ObjectStoreError};
// Re-aliasing for the Kubernetes ObjectMeta conflict (if still needed)
use chrono::{Duration, Utc};
use futures::TryStreamExt;
use object_store::ObjectMeta as StoreObjectMeta;
use std::env;
use std::sync::Arc;

use tokio::{sync::mpsc, task, time::sleep};
use tracing::{debug}; 

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageObjectBundle {
    pub storage_classes: Vec<StorageClass>,
    pub persistent_volumes: Vec<PersistentVolume>,
    pub persistent_volume_claims: Vec<PersistentVolumeClaim>,
}

impl StorageObjectBundle {
    pub fn new() -> Self {
        Self {
            storage_classes: Vec::new(),
            persistent_volumes: Vec::new(),
            persistent_volume_claims: Vec::new(),
        }
    }

    pub fn add_storage_class(&mut self, sc: StorageClass) {
        self.storage_classes.push(sc);
    }

    pub fn add_persistent_volume(&mut self, pv: PersistentVolume) {
        self.persistent_volumes.push(pv);
    }

    pub fn add_persistent_volume_claim(&mut self, pvc: PersistentVolumeClaim) {
        self.persistent_volume_claims.push(pvc);
    }
}

pub async fn populate_storage_bundle(
    client: Client,
    label: &str, // "key=value"
) -> Result<StorageObjectBundle, anyhow::Error> {
    // Parse "key=value"
    let (label_key, label_value) = match label.split_once('=') {
        Some((key, value)) => (key, value),
        None => {
            return Err(anyhow::anyhow!(
                "Invalid label format '{}', expected 'key=value'",
                label
            ));
        }
    };

    let storage_classes_list = resource::get_resource_list::<StorageClass>(client.clone()).await?;
    let persistent_volumes_list =
        resource::get_resource_list::<PersistentVolume>(client.clone()).await?;
    let persistent_volume_claims_list =
        resource::get_resource_list::<PersistentVolumeClaim>(client.clone()).await?;

    // Filter PVs by parsed key and value
    let filtered_pvs: Vec<PersistentVolume> = persistent_volumes_list
        .items
        .into_iter()
        .filter(|pv| {
            pv.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(label_key))
                == Some(&label_value.to_string())
        })
        .collect();

    Ok(StorageObjectBundle {
        storage_classes: storage_classes_list.items,
        persistent_volumes: filtered_pvs,
        persistent_volume_claims: persistent_volume_claims_list.items,
    })
}

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

// acting as an orchestrator.
pub async fn write_objects_to_object_store(
    cr: &PersistentVolumeSync,
    timestamp: i64,
    storage_objects: StorageObjectBundle,
) -> anyhow::Result<()> {
    // 1. Environment Setup (Dev only)
    dotenvy::dotenv().ok();

    // 2. Configuration Extraction
    let provider = &cr.spec.cloud_provider.to_lowercase();
    let cluster = &cr.spec.protected_cluster.to_lowercase();
    let target_path = format!("{}/{}_test_file.json", &cluster, &timestamp);

    // 3. Data Preparation
    let data = serde_json::to_string_pretty(&storage_objects)?;
    info!("Selected Provider: {}", provider);

    // 4. Object Store Initialization (Delegated)
    let store = initialize_object_store(provider.as_str()).await?;

    // 5. Store Operations (Cloud Agnostic)
    write_data(store.into(), &target_path, &data.as_bytes()).await?;

    info!(
        "Application completed successfully. Object written to path: {}",
        target_path
    );

    Ok(())
}

// --- Object Store Builder ---

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

/// Cleans up log objects in the object store older than the specified retention period.
///
/// # Arguments
/// * `cr` - Reference to the PersistentVolumeSync Custom Resource for configuration.
/// * `retention_days` - The number of days to retain logs (logs older than this are deleted).
pub async fn cleanup_old_objects(cr: &PersistentVolumeSync) -> anyhow::Result<()> {
    // 1. Environment Setup (Dev only)
    dotenvy::dotenv().ok();

    // 2. Configuration Extraction
    let provider = &cr.spec.cloud_provider.to_lowercase();
    let cluster = &cr.spec.protected_cluster.to_lowercase();
    let retention_days = cr.spec.retention;

    // 3. Calculate Cutoff Timestamp
    // FIX E0425: This entire block must execute and define the variable *before* it's used.
    let retention_duration = Duration::days(retention_days as i64);
    let cutoff_time = Utc::now() - retention_duration;
    let cutoff_timestamp_sec = cutoff_time.timestamp(); // Variable is now in scope

    info!(
        // Variable is now in scope here:
        "Cleaning up logs for cluster: {}. Retention: {} days. Cutoff timestamp (seconds): {}",
        &cluster, retention_days, cutoff_timestamp_sec
    );

    // 4. Object Store Initialization
    let store: Arc<dyn ObjectStore> = initialize_object_store(provider.as_str()).await?.into();

    // Define Object Prefix
    let prefix = Path::from(format!("{}/", &cluster));

    // List, Filter, and Delete Objects

    // FIX E0277: ObjectStore::list returns the Stream directly
    let objects_stream = store.list(Some(&prefix));

    let objects_to_delete: Vec<Path> = objects_stream
        .try_filter_map(|meta: StoreObjectMeta| async move {
            // FIX E0599: Use the correct method name `filename()`
            if let Some(filename) = meta.location.filename() {
                // filename is likely a &str here, so split() should work
                if let Some(timestamp_str) = filename.split('_').next() {
                    if let Ok(file_timestamp) = timestamp_str.parse::<i64>() {
                        // FIX E0425: Variable is now in scope here:
                        if file_timestamp < cutoff_timestamp_sec {
                            // Object is older than retention, mark for deletion
                            return Ok(Some(meta.location));
                        }
                    }
                }
            }
            Ok(None)
        })
        .try_collect()
        .await?;

    if objects_to_delete.is_empty() {
        info!(
            "No log files found older than {} days for cluster {}.",
            retention_days, &cluster
        );
        return Ok(());
    }

    info!(
        "Found {} old log files to delete. Initiating sequential deletion.",
        objects_to_delete.len()
    );

    for path in objects_to_delete.iter() {
        // Delete is async, so we await each one.
        store.delete(path).await?;
    }

    info!(
        "Successfully deleted {} log files older than {} days for cluster {}.",
        objects_to_delete.len(),
        retention_days,
        &cluster
    );

    Ok(())
}

pub fn dummy_storage_bundle() -> StorageObjectBundle {
    let mut bundle = StorageObjectBundle::new();

    // -------- StorageClass --------
    let sc = StorageClass {
        metadata: ObjectMeta {
            name: Some("fast-storage".into()),
            ..Default::default()
        },
        provisioner: "example.com/dummy".into(),
        ..Default::default()
    };
    bundle.add_storage_class(sc);

    // -------- PersistentVolume --------
    let pv = PersistentVolume {
        metadata: ObjectMeta {
            name: Some("pv-fast-001".into()),
            ..Default::default()
        },
        spec: Some(PersistentVolumeSpec {
            storage_class_name: Some("fast-storage".into()),
            capacity: Some(
                [("storage".into(), Quantity("10Gi".into()))]
                    .into_iter()
                    .collect(),
            ),
            access_modes: Some(vec!["ReadWriteOnce".into()]),
            ..Default::default()
        }),
        ..Default::default()
    };
    bundle.add_persistent_volume(pv);

    // -------- PersistentVolumeClaim --------
    let pvc = PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some("pvc-fast-claim".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            storage_class_name: Some("fast-storage".into()),
            access_modes: Some(vec!["ReadWriteOnce".into()]),
            resources: Some(VolumeResourceRequirements {
                requests: Some(
                    [("storage".into(), Quantity("10Gi".into()))]
                        .into_iter()
                        .collect(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    bundle.add_persistent_volume_claim(pvc);

    bundle
}

/////////////////////////////////////////////////

// --- Data Structures ---

// Tracks object paths/metadata to detect changes in the listing of a prefix.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
struct ObjectListing(Vec<String>);

/// Enum to represent the possible outcomes of the object store list poll.
enum PollResult {
    Changed { new_listing: ObjectListing },
}

// --- Watcher Core ---

/// Watcher for an object store prefix using listing comparison to detect changes.
pub async fn start_object_store_watcher(
    cr: &PersistentVolumeSync,
    external_prefix: Path,
    polling_interval: Duration,
    tx: mpsc::Sender<()>,
) -> Result<(), anyhow::Error> {
    // Store the object paths/metadata from the last successful list
    let mut last_state: Option<ObjectListing> = None;

    let provider = cr.spec.cloud_provider.as_str();
    let raw_store = initialize_object_store(provider).await?;
    
    // 2. WRAP THE STORE IN AN ARC TO ENABLE SHARING AND CLONING (Reference Counting)
    // We assume the type of raw_store is Box<dyn ObjectStore>.
    let object_store: Arc<dyn ObjectStore> = Arc::from(raw_store);

    info!(
        "Starting object_store-based watcher for prefix: {}",
        external_prefix
    );

    task::spawn(async move {
        loop {
            // Wait for the polling interval
            sleep(polling_interval.to_std().unwrap_or_default()).await;

            debug!(
                "Polling object store prefix. Last state size: {:?}",
                last_state.as_ref().map(|l| l.0.len())
            );

            match poll_object_store_prefix(
                object_store.clone(),
                external_prefix.clone(),
            )
            .await
            {
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
                            info!("Failed to send change signal, receiver likely dropped: {}", e);
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
async fn poll_object_store_prefix(
    store: Arc<dyn ObjectStore>,
    prefix: Path,
) -> Option<PollResult> {
    
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