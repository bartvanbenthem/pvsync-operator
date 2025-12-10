use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Error, ResourceExt};
use serde_json::{Value, json};
use tracing::*;

use crate::crd::{PersistentVolumeSync, PersistentVolumeSyncStatus};

/// Update PersistentVolumeSync status
pub async fn patch(
    client: Client,
    name: &str,
    status: PersistentVolumeSyncStatus,
) -> Result<PersistentVolumeSync, Error> {
    let api: Api<PersistentVolumeSync> = Api::all(client);

    let data: Value = json!({
        "status": status,
    });

    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&data))
        .await
}

/// Print PersistentVolumeSync status
pub async fn print(client: Client, name: &str) -> Result<(), Error> {
    let api: Api<PersistentVolumeSync> = Api::all(client);

    let cdb = api.get_status(name).await?;

    info!(
        "Got status succeeded {:?} for custom resource {}",
        cdb.clone()
            .status
            .unwrap_or(PersistentVolumeSyncStatus { succeeded: false })
            .succeeded,
        cdb.name_any()
    );

    Ok(())
}
