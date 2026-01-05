use anyhow::Result;
use serde::Serialize;
use serde_json;
use std::fs::{File, create_dir_all};
use std::io::{BufWriter, Write};
use std::path::Path;
use tracing::*;

use k8s_openapi::api::core::v1::{PersistentVolume, PersistentVolumeClaim};
use kube::Error;
use kube::core::ObjectMeta;

/// Generic metadata cleaner for any Kubernetes resource
fn clean_metadata(mut meta: ObjectMeta, keep_namespace: bool) -> ObjectMeta {
    ObjectMeta {
        name: meta.name.take(),
        namespace: if keep_namespace {
            meta.namespace.take()
        } else {
            None
        },
        labels: meta.labels.take(),
        annotations: meta.annotations.take().map(|ann| {
            ann.into_iter()
                .filter(|(k, _)| !k.contains("kubernetes.io/"))
                .collect()
        }),
        ..Default::default()
    }
}

/// Transforms an existing PV into a clean version ready for re-application.
pub fn sanitize_pv(input: &PersistentVolume) -> PersistentVolume {
    let mut pv = input.clone();

    // Clean Metadata using generic helper
    pv.metadata = clean_metadata(pv.metadata, false);

    // Unbind from any existing Claim
    if let Some(ref mut spec) = pv.spec {
        spec.claim_ref = None;
    }

    // Wipe Status
    pv.status = None;

    pv
}

/// Transforms an existing PVC into a clean version ready for re-application.
pub fn sanitize_pvc(input: &PersistentVolumeClaim) -> PersistentVolumeClaim {
    let mut pvc = input.clone();

    // Clean Metadata using generic helper
    pvc.metadata = clean_metadata(pvc.metadata, true);

    // Clean Spec
    if let Some(ref mut spec) = pvc.spec {
        spec.volume_name = None;
    }

    // Wipe Status
    pvc.status = None;

    pvc
}

pub async fn create_test_pv(name: &str) -> Result<PersistentVolume, Error> {
    let pv: PersistentVolume = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": name,
            "labels": {
                "type": "local",
                "storage-tier": "premium",
            },
        },
        "spec": {
            "capacity": {
                "storage": "5Gi",
            },
            "accessModes": [
                "ReadWriteOnce",
            ],
            "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": "manual",
            // Example for a simple HostPath volume for demonstration
            "hostPath": {
                "path": "/mnt/data/my-pv-data",
            }
        }
    }))
    .expect("Failed to create PV object from JSON");

    Ok(pv)
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
