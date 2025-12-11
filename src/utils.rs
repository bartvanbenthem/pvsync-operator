use anyhow::{Result, anyhow};
use k8s_openapi::Metadata;
use k8s_openapi::NamespaceResourceScope;
use kube::Resource;
use kube::{Api, Client, api::ListParams};
use kube_runtime::reflector::ObjectRef;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use std::fs::{File, create_dir_all};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use tracing::*;

use std::fmt::Debug;

use futures::TryStreamExt;
use kube::ResourceExt;
use kube::api::ObjectMeta;
use kube_runtime::WatchStreamExt;
use kube_runtime::watcher;
use kube_runtime::watcher::Config;
use tokio::sync::mpsc;
use tokio::task;

pub async fn start_resource_watcher<T>(
    client: Client,
    tx: mpsc::Sender<()>,
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

pub async fn start_resource_watcher_label<T>(
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

/// Generate `ObjectRef`s for all instances of a given Kubernetes resource type.
/// https://kube.rs/controllers/relations/#watched-relations
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
pub fn make_object_ref_mapper<T, CR>(
    refs: Arc<Vec<ObjectRef<CR>>>,
) -> impl Fn(T) -> Vec<ObjectRef<CR>>
where
    CR: Clone + Resource<DynamicType = ()> + 'static,
    T: Metadata + 'static,
{
    move |_: T| (*refs).clone()
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