use kube::{
    Client, Error, Resource,
    api::{Api, Patch, PatchParams, TypeMeta},
    core::{NamespaceResourceScope, object::HasStatus},
};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

/// # Type Parameters
/// * `K`: The Kubernetes resource type (e.g., PersistentVolumeSync).
///   It must implement `Clone`, `Resource`, `DeserializeOwned`, `Serialize`,
///   and `HasStatus` (meaning it has a `.status` field).
/// * `S`: The Status type for the resource `K` (e.g., PersistentVolumeSyncStatus).
///   It must implement `Serialize` and `DeserializeOwned`.
pub async fn patch_status_cluster<K, S>(client: Client, name: &str, status: S) -> Result<K, Error>
where
    K: Clone
        + Resource<DynamicType = TypeMeta>
        + DeserializeOwned
        + Serialize
        + HasStatus<Status = S>,
    S: Serialize + DeserializeOwned,
{
    // 1. Create an API instance for the generic type `K`.
    // We use `Api::all` assuming the resource is cluster-scoped,
    // but you could easily adapt this to `Api::namespaced(client, "my-ns")`
    // if you prefer namespaced resources.
    let api: Api<K> = Api::all(client);

    // 2. Construct the JSON patch data.
    // The structure for patching a status subresource is always {"status": <new_status_object>}.
    let data: Value = json!({
        "status": status,
    });

    // 3. Apply the patch to the status subresource.
    // We use `Patch::Merge` (JSON Merge Patch) for simplicity.
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&data))
        .await
}

/// Updates the status subresource for a given Kubernetes Custom Resource within a specific namespace.
///
/// This function is generic over the resource type `K` and the status type `S`.
///
/// # Type Parameters
/// * `K`: The Kubernetes resource type (e.g., Deployment, PersistentVolumeSync).
/// * `S`: The Status type for the resource `K` (e.g., DeploymentStatus, PersistentVolumeSyncStatus).
pub async fn patch_status_namespaced<K, S>(
    client: Client,
    namespace: &str, // <--- New parameter for the namespace
    name: &str,
    status: S,
) -> Result<K, Error>
where
    K: Clone
        + Resource<DynamicType = TypeMeta, Scope = NamespaceResourceScope>
        + DeserializeOwned
        + Serialize
        + HasStatus<Status = S>,
    S: Serialize + DeserializeOwned,
{
    // 1. Create an API instance for the generic type `K`, scoped to a namespace.
    let api: Api<K> = Api::namespaced(client, namespace);

    // 2. Construct the JSON patch data.
    let data: Value = json!({
        "status": status,
    });

    // 3. Apply the patch to the status subresource.
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&data))
        .await
}
