use crate::resource;

use anyhow::{Result, anyhow};

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
use object_store::{ObjectStore, PutPayload};
use std::env;
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

pub async fn populate_storage_bundle(client: Client) -> Result<StorageObjectBundle, anyhow::Error> {
    let storage_classes_list = resource::get_resource_list::<StorageClass>(client.clone()).await?;
    let persistent_volumes_list =
        resource::get_resource_list::<PersistentVolume>(client.clone()).await?;
    let persistent_volume_claims_list =
        resource::get_resource_list::<PersistentVolumeClaim>(client.clone()).await?;

    let bundle = StorageObjectBundle {
        // ObjectList<T> has a 'items' field which is Vec<T>
        storage_classes: storage_classes_list.items,
        persistent_volumes: persistent_volumes_list.items,
        persistent_volume_claims: persistent_volume_claims_list.items,
    };

    Ok(bundle)
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

    println!("Attempting to connect to Azure Blob Storage...");

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

    println!("Attempting to connect to S3-compatible Storage...");

    let mut builder = AmazonS3Builder::new()
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_bucket_name(bucket_name);

    // CRITICAL: Configure for S3-compatible services (Cloudian/MinIO)
    if let Some(endpoint) = endpoint_url {
        println!("Using custom S3 endpoint: {}", endpoint);
        builder = builder
            .with_endpoint(endpoint)
            // S3-compatible vendors often run on HTTP without SSL, so we allow it.
            .with_allow_http(true);
    } else {
        println!("Using AWS S3 defaults (region must be configured via env or default).");
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

    println!(
        "Writing data to container '{}' at path: {}",
        container_name, object_key
    );

    // Perform the atomic write operation.
    let result = store
        .put(&object_key, payload)
        .await
        .map_err(|e| anyhow!("Failed to put object: {}", e))?;

    println!("File written successfully!");
    println!("Returned E-Tag: {:?}", result.e_tag);

    // Verification (HEAD request).
    let metadata = store
        .head(&object_key)
        .await
        .map_err(|e| anyhow!("Failed to read metadata: {}", e))?;
    println!("--- Verification ---");
    println!("Retrieved object size: {} bytes", metadata.size);

    // Cleanup.
    //store.delete(&object_key).await
    //    .map_err(|e| anyhow!("Failed to delete object: {}", e))?;
    //println!("Object deleted successfully for cleanup.");

    Ok(())
}
