mod resource;

use kube::Resource;
use pvsync::crd::PersistentVolumeSync;
use pvsync::crd::PersistentVolumeSyncStatus;
use pvsync::crd::SyncMode;
use pvsync::status;
use pvsync::storage;
use pvsync::utils;

use chrono::Utc;
use futures::stream::StreamExt;
use k8s_openapi::api::core::v1::PersistentVolume;
use kube::api::ListParams;
use kube::Config as KubeConfig;
use kube::ResourceExt;
use kube::runtime::watcher::Config;
use kube::{Api, client::Client, runtime::Controller, runtime::controller::Action};
use std::sync::Arc;
use tokio::time::Duration;
use tracing::*;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio::time::sleep;

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

    // Environment Setup trough .env file
    dotenvy::dotenv().ok();

    // BLOCKING CONFIGURATION CHECK
    // This call will pause until the configuration is available or a fatal error occurs.
    // This call will pause the execution of main until the CR is available.
    let cr_to_watch = wait_for_initial_cr(client.clone()).await?;
    // Create the Api for the CRD
    let crd_api: Api<PersistentVolumeSync> = Api::all(client.clone());
    // Shared context for the reconciler functions
    let context: Arc<ContextData> = Arc::new(ContextData::new(client.clone()));
    let mode = cr_to_watch.spec.mode.clone();

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
        // get the single CR
        let cr = cr_to_watch.clone();
        let polling_interval: Duration;
        if cr.spec.polling_interval.is_none() {
            polling_interval = Duration::from_secs(30);
        } else {
            polling_interval = Duration::from_secs(cr.spec.polling_interval.unwrap_or_default())
        }
        // channel to trigger global reconciles
        let (tx, rx) = mpsc::channel::<()>(16);
        // converts mpsc into a stream
        let signal_stream = ReceiverStream::new(rx);
        // Start the Persistant Volume watcher in background
        storage::start_object_store_watcher(&cr, polling_interval, tx).await?;
        // The controller comes from the `kube_runtime` crate and manages the reconciliation process.
        // It requires the following information:
        // - `kube::Api<T>` this controller "owns". In this case, `T = PersistentVolumeSync`, as this controller owns the `PersistentVolumeSync` resource,
        // - `kube::runtime::watcher::Config` can be adjusted for precise filtering of `PersistentVolumeSync` resources before the actual reconciliation, e.g. by label,
        // - `reconcile` function with reconciliation logic to be called each time a resource of `PersistentVolumeSync` kind is created/updated/deleted,
        // - `on_error` function to call whenever reconciliation fails.
        Controller::new(crd_api.clone(), Config::default())
            .shutdown_on_signal()
            .reconcile_all_on(signal_stream)
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

    // If the CR has a deletion timestamp, it has been marked for removal.
    // For a critical, single-instance CR, we should force an exit/failure.
    let name = cr.name_any();
    if cr.meta().deletion_timestamp.is_some() {
        // Return the new fatal error
        return Err(Error::DeletedCriticalResource(name)); 
    }

    info!("Reconcile Recovery");

    Ok(Action::requeue(Duration::from_secs(32000)))
}

async fn reconcile_protected(
    cr: Arc<PersistentVolumeSync>,
    context: Arc<ContextData>,
) -> Result<Action, Error> {
    // The `Client` is shared -> a clone from the reference is obtained
    let client: Client = context.client.clone();
    // Name of the PersistentVolumeSync resource is used to name the subresources as well.
    let name = cr.name_any();
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

/// Waits indefinitely for a single PersistentVolumeSync resource to be applied.
/// Polls the Kubernetes API for a list of resources every 10 seconds.
async fn wait_for_initial_cr(client: Client) -> Result<PersistentVolumeSync, Error> {
    // Api::all(client) creates a cluster-scoped API client
    let api: Api<PersistentVolumeSync> = Api::all(client);
    // Log waiting message
    info!("Waiting for the single required PersistentVolumeSync resource to be applied...");
    // We can use default ListParams as we are not applying labels/fields filtering
    let lp = ListParams::default(); 
    // Start polling loop
    loop {
        match api.list(&lp).await {
            Ok(list) => {
                match list.items.len() {
                    0 => {
                        // Case 1: No CR found
                        info!("No PersistentVolumeSync resource found. Retrying in 10 seconds...");
                    }
                    1 => {
                        // Case 2: Exactly one CR found (Success case)
                        let cr = list.items.into_iter().next().unwrap();

                        // ** Perform Validation Here **
                        // If validation fails, log error and continue (or return a specific error)

                        info!("PersistentVolumeSync resource found: '{}'. Starting controller.", cr.metadata.name.as_deref().unwrap_or("[unknown]"));
                        return Ok(cr);
                    }
                    _ => {
                        // Case 3: More than one CR found (Ambiguous/Error state for a single-config operator)
                        error!("Found {} PersistentVolumeSync resources. Expected exactly one for global configuration. Retrying in 60 seconds...", list.items.len());
                        sleep(Duration::from_secs(60)).await;
                    }
                }
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                // This typically means the CRD itself hasn't been applied yet.
                error!("Custom Resource Definition (CRD) for PersistentVolumeSync not found (404). Retrying in 60 seconds...");
                sleep(Duration::from_secs(60)).await;
            }
            Err(e) => {
                // Other API error (e.g., connection issue, permission denied)
                error!("API error while checking for config: {:?}. Retrying in 30 seconds...", e);
                sleep(Duration::from_secs(30)).await;
            }
        }
        // Short sleep before next polling attempt
        sleep(Duration::from_secs(10)).await; // Short sleep for the main polling loop
    }
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

    /// The critical configuration resource was deleted, requiring operator restart.
    #[error("Critical configuration resource was deleted: {0}")]
    DeletedCriticalResource(String),

    /// Error in user input or PersistentVolumeSync resource definition, typically missing fields.
    #[error("Invalid PersistentVolumeSync CRD: {0}")]
    UserInputError(String),

    /// Catch-all for any other error.
    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

fn exit_process_on_delete(name: &str) -> Result<Action, Error> {
    error!("FATAL: The required PersistentVolumeSync CR '{}' was deleted. Exiting to trigger operator restart.", name);
    
    // We can't exit the process directly here because the reconciler runs in a thread.
    // Instead, we return a specific error that the main error handling can catch, 
    // OR we rely on the controller eventually shutting down after deletion.
    
    // **A cleaner method is to use a specific, unrecoverable error.**
    // Let's modify our Error enum slightly.
    
    Err(Error::DeletedCriticalResource(name.to_string()))
}