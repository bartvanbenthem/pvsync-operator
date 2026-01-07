use crate::utils;

use k8s_openapi::ClusterResourceScope;
use k8s_openapi::Metadata;
use k8s_openapi::NamespaceResourceScope;
use k8s_openapi::api::core::v1::Namespace;
use kube::Resource;
use kube::api::{DeleteParams, Patch, PatchParams};
use kube::{Api, Client, api::ListParams};
use kube_runtime::reflector::ObjectRef;
use serde::Serialize;
use serde::de::DeserializeOwned;

use anyhow::{Result, anyhow};
use kube::api::ObjectList;
use kube::core::ObjectMeta;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;
use tracing::*;

use futures::TryStreamExt;
use kube::ResourceExt;
use kube_runtime::WatchStreamExt;
use kube_runtime::watcher;
use kube_runtime::watcher::Config;
use tokio::sync::mpsc;
use tokio::task;

use std::sync::Arc;

use serde_json::{Value, json};

/// Ensures a namespace exists using Server-Side Apply.
///
/// # Arguments
/// * `client` - The Kubernetes client
/// * `name` - The name of the namespace to create/update
/// * `field_manager` - The name of the controller/service applying the change (for SSA ownership)
#[allow(dead_code)]
pub async fn ensure_namespace(
    client: Client,
    name: &str,
    field_manager: &str,
) -> Result<Namespace, kube::Error> {
    let namespaces: Api<Namespace> = Api::all(client);

    // Create the Namespace struct directly
    let ns_patch = Namespace {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    let params = PatchParams::apply(field_manager).force();

    info!("Applying Namespace with name '{}' using SSA...", name);
    // Use Patch::Apply with the struct reference
    namespaces
        .patch(name, &params, &Patch::Apply(&ns_patch))
        .await
}

/// Idempotently creates or updates any cluster-scoped Kubernetes resource using Server-Side Apply (SSA).
///
/// This function is generic over any type `T` that represents a cluster-scoped
/// Kubernetes resource (e.g., PersistentVolume, ClusterRole, CRD).
///
/// # Arguments:
/// - `client`: The Kubernetes client.
/// - `resource`: The desired state of the resource struct.
/// - `field_manager`: The unique name of the controller/tool managing this resource.
#[allow(dead_code)]
pub async fn apply_cluster_resource<T>(
    client: Client,
    resource: &T,
    field_manager: &str,
) -> Result<T, kube::Error>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Send
        + Sync
        + 'static
        + Default
        + Serialize,
{
    // Use Api::all() for cluster-scoped resources
    let api: Api<T> = Api::all(client);

    // Resource name must be derived from the resource's metadata
    let name = resource.metadata().name.as_deref().unwrap_or("[No Name]");

    // Configure the Server-Side Apply parameters
    let params = PatchParams::apply(field_manager).force();
    // Get the resource Kind for logging, using &()
    let kind = T::kind(&());
    info!(
        "Applying cluster-scoped resource {} with name '{}' using SSA...",
        kind, name
    );

    // Perform the Server-Side Apply (Patch::Apply)
    api.patch(name, &params, &Patch::Apply(resource)).await
}

/// Deletes an existing cluster-scoped Kubernetes resource by name.
///
/// This function is generic over any type `T` that represents a cluster-scoped
/// Kubernetes resource (e.g., PersistentVolume, ClusterRole, CRD).
///
/// # Arguments:
/// - `client`: A Kubernetes client.
/// - `name`: The name of the resource to delete.
#[allow(dead_code)]
pub async fn delete_cluster_resource<T>(client: Client, name: &str) -> Result<(), kube::Error>
where
    // T needs to be a resource, be debuggable, and allow deserialization (for the delete API response).
    // The Scope is implicitly ClusterResourceScope because we use Api::all().
    T: Resource<DynamicType = ()> + Debug + DeserializeOwned + Clone + Send + Sync + 'static,
{
    // Use Api::all() for cluster-scoped resources.
    let api: Api<T> = Api::all(client);

    // Get the resource Kind for logging, using &()
    let kind = T::kind(&());
    info!(
        "Deleting cluster-scoped resource {} with name '{}'...",
        kind, name
    );

    // Perform the deletion
    // The DeleteParams::default() ensures a standard, graceful deletion.
    api.delete(name, &DeleteParams::default()).await?;

    // We return Ok(()) regardless of the exact DeleteResponse (Status or T)
    Ok(())
}

/// Idempotently creates or updates any namespaced Kubernetes resource using Server-Side Apply (SSA).
///
/// This function is generic over any type `T` that represents a namespaced
/// Kubernetes resource (e.g., Deployment, Service, NetworkPolicy).
///
/// # Arguments:
/// - `client`: The Kubernetes client.
/// - `namespace`: The target namespace for the resource. (NEW)
/// - `resource`: The desired state of the resource struct.
/// - `field_manager`: The unique name of the controller/tool managing this resource.
#[allow(dead_code)]
pub async fn apply_namespaced_resource<T>(
    client: Client,
    namespace: &str,
    resource: &T,
    field_manager: &str,
) -> Result<T, kube::Error>
where
    // T must implement the NamespaceResourceScope trait implicitly (via Resource bound)
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Serialize
        + Default
        + Send
        + Sync
        + 'static,
{
    // Use Api::namespaced() for namespaced resources
    let api: Api<T> = Api::namespaced(client, namespace);

    // Resource name must be derived from the resource's metadata
    let name = resource.metadata().name.as_deref().unwrap_or("[No Name]");

    // Configure the Server-Side Apply parameters
    let params = PatchParams::apply(field_manager).force();

    // Get the resource Kind for logging, using &()
    let kind = T::kind(&());
    info!(
        "Applying namespaced resource {}/{} in namespace '{}' using SSA...",
        kind, name, namespace
    );

    // Perform the Server-Side Apply (Patch::Apply)
    api.patch(name, &params, &Patch::Apply(resource)).await
}

// Deletes an existing namespaced Kubernetes resource by name.
///
/// This function is generic over any type `T` that represents a namespaced
/// Kubernetes resource (e.g., Deployment, Service, NetworkPolicy).
///
/// # Arguments:
/// - `client`: A Kubernetes client.
/// - `namespace`: The namespace the resource resides in. (NEW)
/// - `name`: The name of the resource to delete.
#[allow(dead_code)]
pub async fn delete_namespaced_resource<T>(
    client: Client,
    namespace: &str,
    name: &str,
) -> Result<(), kube::Error>
where
    // T must implement the NamespaceResourceScope trait implicitly (via Resource bound)
    T: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Debug
        + DeserializeOwned
        + Clone
        + Send
        + Sync
        + 'static,
{
    // Use Api::namespaced() for namespaced resources.
    let api: Api<T> = Api::namespaced(client, namespace);

    // Get the resource Kind for logging, using &()
    let kind = T::kind(&());
    info!(
        "Deleting namespaced resource {}/{} in namespace '{}'...",
        kind, name, namespace
    );

    // Perform the deletion
    api.delete(name, &DeleteParams::default()).await?;

    Ok(())
}

///let _ = add_finalizer_namespaced_resource::<PersistentVolumeClaim>(
///    client.clone(),
///    "test-pvc",
///    "default",
///    "volumetrackers.cndev.nl/finalizer",
///)
///.await?;
pub async fn add_finalizer_namespaced_resource<T>(
    client: Client,
    name: &str,
    namespace: &str,
    value: &str,
) -> Result<T, kube::Error>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + DeserializeOwned,
{
    let api: Api<T> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": [value]
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    api.patch(name, &PatchParams::default(), &patch).await
}

///let _ = add_finalizer_cluster_resource::<PersistentVolume>(
///    client.clone(),
///    "test-pv",
///    "volumetrackers.cndev.nl/finalizer",
///)
///.await?;
pub async fn add_finalizer_cluster_resource<T>(
    client: Client,
    name: &str,
    value: &str,
) -> Result<T, kube::Error>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = kube::core::ClusterResourceScope>
        + DeserializeOwned,
{
    let api: Api<T> = Api::all(client);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": [value]
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    api.patch(name, &PatchParams::default(), &patch).await
}

///let _ = delete_finalizer_namespaced_resource::<PersistentVolumeClaim>(
///    client.clone(),
///    "test-pvc",
///    "default",
///)
///.await?;
#[allow(dead_code)]
pub async fn delete_finalizer_namespaced_resource<T>(
    client: Client,
    name: &str,
    namespace: &str,
) -> Result<T, kube::Error>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + DeserializeOwned,
{
    let api: Api<T> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": null
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    api.patch(name, &PatchParams::default(), &patch).await
}

///let _ = add_finalizer_cluster_resource::<PersistentVolume>(
///    client.clone(),
///    "test-pv",
///)
///.await?;
#[allow(dead_code)]
pub async fn delete_finalizer_cluster_resource<T>(
    client: Client,
    name: &str,
) -> Result<T, kube::Error>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = kube::core::ClusterResourceScope>
        + DeserializeOwned,
{
    let api: Api<T> = Api::all(client);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": null
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    api.patch(name, &PatchParams::default(), &patch).await
}

/// Garbage collects orphaned cluster-scoped resources.
///
/// This function lists all resources of type `T` matching the `label_selector`.
/// Resources found on the cluster that are not present in `desired_names` are deleted.
/// If a resource is already missing during deletion, the error is ignored.
///
/// # Arguments
/// * `client` - The Kubernetes [Client] used to interact with the API.
/// * `label_selector` - A label query (e.g., "app=pvsync") to filter owned resources.
/// * `desired_names` - The set of resource names that should currently exist.
///
/// # Errors
/// Returns [kube::Error] for API failures, excluding 404 Not Found during deletion.
#[allow(dead_code)]
pub async fn gc_cluster_resources<T>(
    client: Client,
    label_selector: &str,
    desired_names: &HashSet<String>,
) -> Result<(), kube::Error>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = ClusterResourceScope>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Send
        + Sync
        + 'static
        + Default
        + Serialize,
{
    let api: Api<T> = Api::all(client.clone());
    let lp = ListParams::default().labels(label_selector);
    let existing = api.list(&lp).await?;

    for resource in existing {
        let name = resource.name_any();
        if !desired_names.contains(&name) {
            // 1. If it has a deletion timestamp, it's already hanging.
            // We must clear finalizers to let it vanish.
            if resource.metadata().deletion_timestamp.is_some() {
                info!(resource = %name, "Resource is terminating; force-clearing all finalizers");
                let purge_patch = serde_json::json!({
                    "metadata": {
                        "finalizers": null
                    }
                });
                let _ = api
                    .patch(&name, &PatchParams::default(), &Patch::Merge(&purge_patch))
                    .await;
                continue;
            }

            // 2. Not terminating yet? Initiate deletion.
            info!(resource = %name, "Deleting orphaned cluster resource");

            // We use Foreground propagation to ensure children are handled
            let dp = DeleteParams::foreground();

            match api.delete(&name, &dp).await {
                Ok(_) => {
                    // 3. Immediately follow up with a finalizer wipe to prevent hanging
                    let purge_patch = serde_json::json!({
                        "metadata": {
                            "finalizers": null
                        }
                    });
                    let _ = api
                        .patch(&name, &PatchParams::default(), &Patch::Merge(&purge_patch))
                        .await;
                }
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    info!(resource = %name, "Resource already deleted, skipping");
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}

/// Garbage collects orphaned namespaced resources.
///
/// Performs a cross-namespace search for resources of type `T`. If a resource
/// matches the `label_selector` but is not in `desired_names`, it is deleted.
///
/// This function is robust against race conditions where resources are deleted
/// by external actors between the list and delete calls.
///
/// # Arguments
/// * `client` - The Kubernetes [Client].
/// * `label_selector` - Filter for resources belonging to this controller.
/// * `desired_names` - The source-of-truth list of resource names (Namespace, Name).
///
/// # Errors
/// Returns [kube::Error] if API operations fail (except 404s).
#[allow(dead_code)]
pub async fn gc_namespaced_resources<T>(
    client: Client,
    label_selector: &str,
    desired_resources: &HashSet<(String, String)>, // (Namespace, Name)
) -> Result<(), kube::Error>
where
    T: Clone
        + Debug
        + DeserializeOwned
        + Serialize
        + Default
        + Send
        + Sync
        + 'static
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Metadata<Ty = ObjectMeta>,
{
    let api: Api<T> = Api::all(client.clone());
    let lp = ListParams::default().labels(label_selector);
    let existing = api.list(&lp).await?;

    for resource in existing {
        let name = resource.name_any();
        let ns = resource.metadata().namespace.clone().unwrap_or_default();

        // Check if this specific instance (Namespace + Name) is in the desired list
        if !desired_resources.contains(&(ns.clone(), name.clone())) {
            let ns_api: Api<T> = Api::namespaced(client.clone(), &ns);

            // 1. Handle stuck terminations
            if resource.metadata().deletion_timestamp.is_some() {
                info!(resource = %name, namespace = %ns, "Orphaned resource is terminating; force-clearing finalizers");
                let purge_patch = serde_json::json!({ "metadata": { "finalizers": null } });
                let _ = ns_api
                    .patch(&name, &PatchParams::default(), &Patch::Merge(&purge_patch))
                    .await;
                continue;
            }

            // 2. Initiate Deletion
            info!(resource = %name, namespace = %ns, "Deleting orphaned namespaced resource");
            match ns_api.delete(&name, &DeleteParams::foreground()).await {
                Ok(_) => {
                    // 3. Force wipe finalizers to ensure cleanup
                    let purge_patch = serde_json::json!({ "metadata": { "finalizers": null } });
                    let _ = ns_api
                        .patch(&name, &PatchParams::default(), &Patch::Merge(&purge_patch))
                        .await;
                }
                Err(kube::Error::Api(e)) if e.code == 404 => continue,
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}

/// watcher for all resources of a certain type and label combination
#[allow(dead_code)]
pub async fn start_watcher_label<T>(
    client: Client,
    tx: mpsc::Sender<()>,
    label_selector: &str,
) -> Result<(), anyhow::Error>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let resource_api = Api::<T>::all(client.clone());

    // Create a Config with the label selector
    let config = Config::default().labels(label_selector);

    task::spawn(async move {
        // Pass the configured config to the watcher
        let result = watcher(resource_api, config)
            .applied_objects()
            .default_backoff()
            .try_for_each(|pv| {
                let tx = tx.clone();
                async move {
                    // This block will now *only* receive events for resources
                    // that match the label selector!
                    info!("Watched Resource: {}", pv.name_any());
                    tx.send(()).await.ok();
                    Ok(())
                }
            })
            .await;

        if let Err(err) = result {
            error!("Resource Watcher Failed: {:?}", err);
        }
    });

    Ok(())
}

/// watcher for all resources of a certain type
#[allow(dead_code)]
pub async fn start_watcher<T>(client: Client, tx: mpsc::Sender<()>) -> Result<(), anyhow::Error>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let resource_api = Api::<T>::all(client.clone());

    task::spawn(async move {
        let result = watcher(resource_api, Config::default())
            .applied_objects()
            .default_backoff()
            .try_for_each(|pv| {
                let tx = tx.clone();
                async move {
                    info!("Watched Resource: {}", pv.name_any());
                    tx.send(()).await.ok();
                    Ok(())
                }
            })
            .await;

        if let Err(err) = result {
            error!("Resource Watcher Failed: {:?}", err);
        }
    });

    Ok(())
}

// Generic function to get a specific resource type with specific label and return a list
#[allow(dead_code)]
pub async fn get_resource_list_label<T>(
    client: Client,
    label_selector: &str,
) -> Result<ObjectList<T>, anyhow::Error>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    // Access the Resource <T> API (it's cluster-scoped/all namespaces)
    let r: Api<T> = Api::all(client);

    // Create ListParams, setting the label_selector
    let lp = ListParams {
        label_selector: Some(label_selector.to_string()),
        ..ListParams::default() // Use the remaining default parameters
    };

    // List Resource <T> with the specified label selector
    let r_list = r.list(&lp).await?;

    Ok(r_list)
}

// Generic function to get a specific resource type and return a list
#[allow(dead_code)]
pub async fn get_resource_list<T>(client: Client) -> Result<ObjectList<T>, anyhow::Error>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    // Access the Resource <T> API (it's cluster-scoped)
    let r: Api<T> = Api::all(client);

    // List Resource <T>
    let lp = ListParams::default();
    let r_list = r.list(&lp).await?;

    Ok(r_list)
}

// Generic function to fetch and write a list of resources in json format to disk
#[allow(dead_code)]
pub async fn fetch_and_write_resources_to_file<T>(
    client: Client,
    mount_path: &str,
    cluster_name: &str,
    file_name: &str,
    tf: &i64,
) -> Result<(), anyhow::Error>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Serialize
        + Send
        + Sync
        + 'static,
{
    let file_path = Path::new(mount_path)
        .join(cluster_name)
        .join(tf.to_string())
        .join(file_name);

    let file_str = file_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in file path"))?;

    let resource_list: ObjectList<T> = get_resource_list(client).await?;
    utils::write_json_to_file(&resource_list.items, file_str).await
}

/// Generate `ObjectRef`s for all instances of a given Kubernetes resource type.
/// https://kube.rs/controllers/relations/#watched-relations
#[allow(dead_code)]
pub async fn make_object_refs<T>(
    client: Client,
    namespace: Option<&str>,
) -> Result<Vec<ObjectRef<T>>>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + DeserializeOwned
        + Serialize
        + Send
        + Sync
        + 'static,
{
    let api: Api<T> = match namespace {
        Some(ns) => Api::namespaced(client, ns),
        None => Api::all(client),
    };

    let mut refs: Vec<ObjectRef<T>> = Vec::new();
    let resources = api.list(&ListParams::default()).await?;

    for resource in resources.items {
        let metadata = resource.meta();
        let name = metadata
            .name
            .clone()
            .ok_or_else(|| anyhow!("Missing metadata.name"))?;
        let ns = metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        info!("Resource '{}' has namespace: {}", name, ns);

        refs.push(ObjectRef::new(&name).within(&ns));
    }

    Ok(refs)
}

//pub fn make_object_ref_mapper<T>(
//    refs: Arc<Vec<ObjectRef<VolumeTracker>>>,
//) -> impl Fn(T) -> Vec<ObjectRef<VolumeTracker>> {
//    move |_: T| (*refs).clone()
//}
#[allow(dead_code)]
pub fn make_object_ref_mapper<T, CR>(
    refs: Arc<Vec<ObjectRef<CR>>>,
) -> impl Fn(T) -> Vec<ObjectRef<CR>>
where
    CR: Clone + Resource<DynamicType = ()> + 'static,
    T: Metadata + 'static,
{
    move |_: T| (*refs).clone()
}
