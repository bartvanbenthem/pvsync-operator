mod resource;

use pvsync::crd::PersistentVolumeSync;
use pvsync::crd::PersistentVolumeSyncStatus;
use pvsync::crd::SyncMode;
use pvsync::status;
use pvsync::storage;
use pvsync::utils;

use chrono::Utc;
use futures::stream::StreamExt;
use k8s_openapi::api::core::v1::PersistentVolume;
use kube::Config as KubeConfig;
use kube::ResourceExt;
use kube::api::ListParams;
use kube::runtime::watcher::Config;
use kube::{Api, client::Client, runtime::Controller, runtime::controller::Action};
use std::sync::Arc;
use tokio::time::Duration;
use tracing::*;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;


/// Context injected with each `reconcile` and `on_error` method invocation.
struct ContextData {
    /// Kubernetes client to make Kubernetes API requests with. Required for K8S resource management.
    client: Client,
}

impl ContextData {
    /// Constructs a new instance of ContextData.
    ///
    /// # Arguments:
    /// - `client`: A Kubernetes client to make Kubernetes REST API requests with. Resources
    /// will be created and deleted with this client.
    pub fn new(client: Client) -> Self {
        ContextData { client }
    }
}

pub static SYNC_LABEL: &str = "volumesyncs.storage.cndev.nl/sync=enabled";

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();
    // First, a Kubernetes client must be obtained using the `kube` crate
    // Attempt to infer KubeConfig
    let config = KubeConfig::infer()
        .await
        .map_err(|e| kube::Error::InferConfig(e))?;
    let client: Client = Client::try_from(config)?;

    let context: Arc<ContextData> = Arc::new(ContextData::new(client.clone()));

    // Preparation of resources used by the `kube_runtime::Controller`
    let crd_api: Api<PersistentVolumeSync> = Api::all(client.clone());
    let cr_list = crd_api.list(&ListParams::default()).await?;

    // 1. Check for a single CR
    if cr_list.items.len() != 1 {
        // <-- FIX applied here (L63)
        error!(
            "Expected exactly one PersistentVolumeSync resource for global mode configuration, found {}. Shutting down.",
            cr_list.items.len()
        ); // <-- FIX applied here (L64)
        // Return an error to stop the application gracefully
        return Err(Error::UserInputError(format!(
            "Configuration Error: Expected exactly one PersistentVolumeSync resource, found {}",
            cr_list.items.len() // <-- FIX applied here (L68, inferred)
        )));
    }

    let cr_to_watch = cr_list.items.into_iter().next().unwrap();
    let mode = cr_to_watch.spec.mode;

    if mode == SyncMode::Protected {
        // channel to trigger global reconciles
        let (tx, rx) = mpsc::channel::<()>(16);
        // converts mpsc into a stream
        let signal_stream = ReceiverStream::new(rx);
        // Start the Persistant Volume watcher in background
        resource::start_watcher_label::<PersistentVolume>(client.clone(), tx, SYNC_LABEL).await?;
        // The controller comes from the `kube_runtime` crate and manages the reconciliation process.
        // It requires the following information:
        // - `kube::Api<T>` this controller "owns". In this case, `T = PersistentVolumeSync`, as this controller owns the `PersistentVolumeSync` resource,
        // - `kube::runtime::watcher::Config` can be adjusted for precise filtering of `PersistentVolumeSync` resources before the actual reconciliation, e.g. by label,
        // - `reconcile` function with reconciliation logic to be called each time a resource of `PersistentVolumeSync` kind is created/updated/deleted,
        // - `on_error` function to call whenever reconciliation fails.
        Controller::new(crd_api.clone(), Config::default())
            .shutdown_on_signal()
            .reconcile_all_on(signal_stream)
            .run(reconcile_protected, on_error, context)
            .for_each(|reconciliation_result| async move {
                match reconciliation_result {
                    Ok(custom_resource) => {
                        info!("Reconciliation successful. Resource: {:?}", custom_resource);
                    }
                    Err(reconciliation_err) => {
                        warn!("Reconciliation error: {:?}", reconciliation_err)
                    }
                }
            })
            .await;
    } else if mode == SyncMode::Recovery {
        // Setup HTTP client for external object store access
        //let polling_interval = Duration::from_secs(30);
        // channel to trigger global reconciles
        //let (tx, rx) = mpsc::channel::<()>(16);
        // converts mpsc into a stream
        //let signal_stream = ReceiverStream::new(rx);
        // Start the Persistant Volume watcher in background

        // The controller comes from the `kube_runtime` crate and manages the reconciliation process.
        // It requires the following information:
        // - `kube::Api<T>` this controller "owns". In this case, `T = PersistentVolumeSync`, as this controller owns the `PersistentVolumeSync` resource,
        // - `kube::runtime::watcher::Config` can be adjusted for precise filtering of `PersistentVolumeSync` resources before the actual reconciliation, e.g. by label,
        // - `reconcile` function with reconciliation logic to be called each time a resource of `PersistentVolumeSync` kind is created/updated/deleted,
        // - `on_error` function to call whenever reconciliation fails.
        Controller::new(crd_api.clone(), Config::default())
            .shutdown_on_signal()
            //.reconcile_all_on(signal_stream)
            .run(reconcile_recovery, on_error, context)
            .for_each(|reconciliation_result| async move {
                match reconciliation_result {
                    Ok(custom_resource) => {
                        info!("Reconciliation successful. Resource: {:?}", custom_resource);
                    }
                    Err(reconciliation_err) => {
                        warn!("Reconciliation error: {:?}", reconciliation_err)
                    }
                }
            })
            .await;
    }

    Ok(())
}

async fn reconcile_recovery(
    cr: Arc<PersistentVolumeSync>,
    context: Arc<ContextData>,
) -> Result<Action, Error> {
    let _ = cr;
    let _ = context;

    info!("Reconcile Recovery");

    Ok(Action::requeue(Duration::from_secs(1000)))
}

async fn reconcile_protected(
    cr: Arc<PersistentVolumeSync>,
    context: Arc<ContextData>,
) -> Result<Action, Error> {
    let client: Client = context.client.clone(); // The `Client` is shared -> a clone from the reference is obtained

    let name = cr.name_any(); // Name of the PersistentVolumeSync resource is used to name the subresources as well.
    let pvsync = cr.as_ref();

    let now = Utc::now();
    //let tf = now.format("%Y-%m-%d-%H%M%S");
    let tf = now.timestamp();

    // populate bundle, only add pvs with correct label
    let storage_bundle = storage::populate_storage_bundle(client.clone(), SYNC_LABEL).await?;

    // upload the storage objects bundle to the object storage backend
    storage::write_objects_to_object_store(pvsync, tf, storage_bundle).await?;

    // cleanup old log folders based on the given retention in days in the CR spec.
    storage::cleanup_old_objects(pvsync).await?;

    //update status
    let status = PersistentVolumeSyncStatus {
        succeeded: true,
        ..Default::default()
    };
    //let updated_cr = status::patch(client.clone(), &name, status.clone()).await?;
    let updated_cr: PersistentVolumeSync =
        status::patch_cr_cluster(client.clone(), &name, status.clone()).await?;
    info!("{:?}", updated_cr.status.unwrap_or(status.clone()));

    Ok(Action::requeue(Duration::from_secs(32000)))
}

fn on_error(cr: Arc<PersistentVolumeSync>, error: &Error, context: Arc<ContextData>) -> Action {
    let client = context.client.clone();
    let name = cr.name_any();
    let namespace = cr.namespace().unwrap_or_else(|| "default".to_string());

    error!(
        error = ?error,
        name = %name,
        namespace = %namespace,
        "Reconciliation error occurred"
    );

    // Spawn async patch inside sync function
    tokio::spawn(async move {
        let status = PersistentVolumeSyncStatus {
            succeeded: false,
            ..Default::default()
        };
        if let Err(e) =
            status::patch_cr_cluster::<PersistentVolumeSync, _>(client, &name, status).await
        {
            error!("Failed to update status: {:?}", e);
        }
    });

    // Requeue to try again later
    Action::requeue(Duration::from_secs(5))
}

/// All errors possible to occur during reconciliation
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Any error originating from the `kube-rs` crate
    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
    },

    /// Any error originating from the watcher
    #[error("Watcher reported error: {source}")]
    WatcherError {
        #[from]
        source: kube_runtime::watcher::Error,
    },

    /// Error making an HTTP request to the external endpoint.
    /*
    #[error("HTTP request error to external resource: {source}")]
    ReqwestError {
        #[from]
        source: reqwest::Error,
    },
    */

    /// Error in user input or PersistentVolumeSync resource definition, typically missing fields.
    #[error("Invalid PersistentVolumeSync CRD: {0}")]
    UserInputError(String),

    /// Catch-all for any other error.
    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
