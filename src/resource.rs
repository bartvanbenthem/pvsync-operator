use crate::utils;

use k8s_openapi::Metadata;
use k8s_openapi::NamespaceResourceScope;
use kube::Resource;
use kube::{Api, Client, api::ListParams};
use kube_runtime::reflector::ObjectRef;
use serde::Serialize;
use serde::de::DeserializeOwned;

use anyhow::{Result, anyhow};
use kube::api::ObjectList;
use kube::core::ObjectMeta;
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

/// watcher for all resources of a certain type and label combination
pub async fn start_watcher_label<T>(
    client: Client,
    tx: mpsc::Sender<()>,
    label_selector: &str, // Add an argument for the selector
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
