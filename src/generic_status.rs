use kube::{
    api::{Api, Patch, PatchParams, ResourceExt},
    Client, Error,
};
use k8s_openapi::Resource;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use std::fmt::Debug;

/// Create an API object for either cluster-scoped or namespaced CRDs.
fn generic_api<T>(client: Client, namespace: Option<&str>) -> Api<T>
where
    T: Resource,
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
    T: Resource + Clone + DeserializeOwned + Debug + Serialize,
    T::DynamicType: Default,
    T::Status: Serialize + Clone + Debug,
{
    let api = generic_api::<T>(client, namespace);

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
    T: Resource + Clone + DeserializeOwned + Debug + Serialize,
    T::DynamicType: Default,
    T::Status: Default + Clone + Serialize + DeserializeOwned + Debug,
{
    let api = generic_api::<T>(client, namespace);

    let obj = api.get_status(name).await?;

    let status = obj.status.clone().unwrap_or_default();

    println!(
        "Got status {:?} for CRD {}",
        status,
        obj.name_any()
    );

    Ok(())
}
