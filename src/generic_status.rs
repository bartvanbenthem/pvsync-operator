use kube::{
    api::{Api, Patch, PatchParams, ResourceExt},
    core::{NamespaceResourceScope, object::HasStatus},
    Client, Error, Resource,
    // Note: We remove ClusterResourceScope import as it is unused and causes a warning
};

use k8s_openapi::{
    Metadata,
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use serde::{
    de::DeserializeOwned, 
    Serialize,
};
use serde_json::{
    json, Value,
};
use std::fmt::Debug;

/// Create an API object for either cluster-scoped or namespaced CRDs.
fn crd_scope<T>(client: Client, namespace: Option<&str>) -> Api<T>
where
    // THIS IS THE SOURCE OF TRUTH: T must be a NAMESPACED Resource with Default DynamicType
    T: Resource<Scope = NamespaceResourceScope> + DeserializeOwned, 
    T::DynamicType: Default, 
    T: Metadata<Ty = ObjectMeta>,
{
    match namespace {
        Some(ns) => Api::namespaced(client, ns),
        None => Api::all(client),
    }
}

/// Patch the `.status` field of any CRD
pub async fn patch_status_generic<T>(
    client: Client,
    name: &str,
    namespace: Option<&str>,
    status: &T::Status,
) -> Result<T, Error>
where
    // ENFORCE THE crd_scope BOUNDS:
// 1. RESOURCE/CRD BOUNDS: Must inherit the exact bounds from crd_scope (which includes Metadata implicitly):
    T: Resource<Scope = NamespaceResourceScope> + DeserializeOwned,

    // 2. REQUIRED FOR STATUS & ASYNC:
    T: HasStatus,              // For T::Status
    T: Clone + Debug + Serialize, // Required for Clone/Debug/Serialize/Api interactions
    T: Send + Sync + 'static,    // For async/Tokio runtime

    // 3. ASSOCIATED TYPE BOUNDS:
    T::DynamicType: Default,     // For Api::all/namespaced
    T::Status: Serialize + Clone + Debug,
{
    let api = crd_scope::<T>(client, namespace);

    let data: Value = json!({ "status": status });

    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&data))
        .await
}

/// Print the `.status` field of any CRD
pub async fn print_status_generic<T>(
    client: Client,
    name: &str,
    namespace: Option<&str>,
) -> Result<(), Error>
where
    // ENFORCE THE crd_scope BOUNDS:
    T: Resource<Scope = NamespaceResourceScope> + DeserializeOwned,
    T: Metadata<Ty = ObjectMeta>,

    // OTHER NECESSARY BOUNDS:
    T: Clone + Debug + Serialize,
    T: HasStatus,
    T: Send + Sync + 'static, 
    T::DynamicType: Default,
    T::Status: Default + Clone + Serialize + DeserializeOwned + Debug,
{
    let api = crd_scope::<T>(client, namespace);

    let obj = api.get_status(name).await?;

    // FIX FOR E0615: Call the method with parentheses
    let status = obj.status().cloned().unwrap_or_default(); 

    println!(
        "Got status {:?} for CRD {}",
        status,
        obj.name_any()
    );

    Ok(())
}