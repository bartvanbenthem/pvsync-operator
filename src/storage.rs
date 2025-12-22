use crate::crd::PersistentVolumeSync;
use crate::objectstorage;
use crate::resource;

use anyhow::Result;
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

use chrono::Duration as ChronoDuration;
use chrono::Utc;
use futures::TryStreamExt;
use object_store::ObjectMeta as StoreObjectMeta;
use object_store::ObjectStore;
use object_store::path::Path;
use std::sync::Arc;

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

    // Filter PVCs by parsed key and value
    let filtered_pvcs: Vec<PersistentVolumeClaim> = persistent_volume_claims_list
        .items
        .into_iter()
        .filter(|pvc| {
            pvc.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(label_key))
                == Some(&label_value.to_string())
        })
        .collect();

    Ok(StorageObjectBundle {
        storage_classes: storage_classes_list.items,
        persistent_volumes: filtered_pvs,
        persistent_volume_claims: filtered_pvcs,
    })
}

// acting as an orchestrator.
pub async fn write_bundle_to_object_store(
    cr: &PersistentVolumeSync,
    timestamp: i64,
    storage_objects: StorageObjectBundle,
) -> anyhow::Result<()> {
    // 2. Configuration Extraction
    let provider = &cr.spec.cloud_provider.to_lowercase();
    let cluster = &cr.spec.protected_cluster.to_lowercase();
    let target_path = format!("{}/{}_storage_objects.json", &cluster, &timestamp);

    // 3. Data Preparation
    let data = serde_json::to_string_pretty(&storage_objects)?;
    info!("Selected Provider: {}", provider);

    // 4. Object Store Initialization (Delegated)
    let store = objectstorage::initialize_object_store(provider.as_str()).await?;

    // 5. Store Operations (Cloud Agnostic)
    objectstorage::write_data(store.into(), &target_path, &data.as_bytes()).await?;

    info!(
        "Application completed successfully. Object written to path: {}",
        target_path
    );

    Ok(())
}

/// Cleans up log objects in the object store older than the specified retention period.
///
/// # Arguments
/// * `cr` - Reference to the PersistentVolumeSync Custom Resource for configuration.
/// * `retention_days` - The number of days to retain logs (logs older than this are deleted).
pub async fn cleanup_old_objects(cr: &PersistentVolumeSync) -> anyhow::Result<()> {
    // Configuration Extraction
    let provider = &cr.spec.cloud_provider.to_lowercase();
    let cluster = &cr.spec.protected_cluster.to_lowercase();
    let retention_days = cr.spec.retention;

    // Calculate Cutoff Timestamp
    // This entire block must execute and define the variable *before* it's used.
    let retention_duration = ChronoDuration::days(retention_days as i64);
    let cutoff_time = Utc::now() - retention_duration;
    let cutoff_timestamp_sec = cutoff_time.timestamp(); // Variable is now in scope

    info!(
        // Variable is now in scope here:
        "Cleaning up logs for cluster: {}. Retention: {} days. Cutoff timestamp (seconds): {}",
        &cluster, retention_days, cutoff_timestamp_sec
    );

    // Object Store Initialization
    let store: Arc<dyn ObjectStore> = objectstorage::initialize_object_store(provider.as_str())
        .await?
        .into();

    // Define Object Prefix
    let prefix = Path::from(format!("{}/", &cluster));

    // List, Filter, and Delete Objects

    // ObjectStore::list returns the Stream directly
    let objects_stream = store.list(Some(&prefix));

    let objects_to_delete: Vec<Path> = objects_stream
        .try_filter_map(|meta: StoreObjectMeta| async move {
            if let Some(filename) = meta.location.filename() {
                // filename is likely a &str here, so split() should work
                if let Some(timestamp_str) = filename.split('_').next() {
                    if let Ok(file_timestamp) = timestamp_str.parse::<i64>() {
                        // Variable is now in scope here:
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

// --- Dummy Data for Testing ---
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
