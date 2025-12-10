use pvsync::crd::PersistentVolumeSync;
use pvsync::crd::SyncMode;
use pvsync::status;
use pvsync::storage;
use pvsync::storage::StorageBundle;
use pvsync::utils;

use anyhow::anyhow;
use std::env;
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

    let mut mode = SyncMode::default();
    for cr in cr_list {
        mode = cr.spec.mode;
    }

    if mode == SyncMode::Protected {
        let (tx, rx) = mpsc::channel::<()>(16); // channel to trigger global reconciles
        let signal_stream = ReceiverStream::new(rx); // converts mpsc into a stream
        // Start the Persistant Volume watcher in background
        utils::start_resource_watcher::<PersistentVolume>(client.clone(), tx).await?;

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
        println!("Recovery");
    }

    Ok(())
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

    let cluster_name =
        utils::get_most_common_cluster_name(client.clone(), &pvsync.spec.cluster_name_key).await?;

    // populate bundle
    let storage_bundle = storage::populate_storage_bundle(client.clone()).await?;

    // upload to object storage
    object_storage_logic(tf, &cluster_name, storage_bundle).await?;

    // cleanup old log folders based on the given retention in days in the CR spec.
    // TODO()

    //update status
    status::patch(client.clone(), &name, true).await?;
    status::print(client.clone(), &name).await?;

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
        if let Err(e) = status::patch(client, &name, false).await {
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

    /// Error in user input or PersistentVolumeSync resource definition, typically missing fields.
    #[error("Invalid PersistentVolumeSync CRD: {0}")]
    UserInputError(String),

    /// Catch-all for any other error.
    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

async fn object_storage_logic(
    timestamp: i64,
    cluster_name: &str,
    storage_bundle: StorageBundle,
) -> anyhow::Result<()> {
    // 1. Load the environment file once at startup.
    dotenvy::dotenv().ok();

    // --- Core Logic: Select Provider and Initialize Store ---

    // 2. Determine the cloud provider from environment variable.
    let provider = env::var("CLOUD_PROVIDER")
        .map_err(|_| anyhow!("CLOUD_PROVIDER must be set (e.g., 'azure' or 's3')"))?
        .to_lowercase();

    // 3. Define common application parameters.
    let target_path = format!("{}/{}_test_file.json", cluster_name, timestamp);

    // test data
    let test_data = serde_json::to_string_pretty(&storage_bundle)?;

    println!("Selected Provider: {}", provider);

    // 4. Initialize the Object Store dynamically.
    let store = match provider.as_str() {
        "azure" => {
            let container_name = env::var("OBJECT_STORAGE_BUCKET")
                .map_err(|_| anyhow::anyhow!("OBJECT_STORAGE_BUCKET must be set for Azure"))?;

            storage::initialize_azure_store(&container_name)?
        }
        "s3" => {
            // Note: For S3, the bucket name is required. We'll use AWS_BUCKET_NAME convention.
            let bucket_name = env::var("OBJECT_STORAGE_BUCKET")
                .map_err(|_| anyhow::anyhow!("OBJECT_STORAGE_BUCKET must be set for S3"))?;

            // Allow an optional custom endpoint for S3-compatible systems (like Cloudian or MinIO)
            let endpoint = env::var("S3_ENDPOINT_URL").ok();

            storage::initialize_s3_store(&bucket_name, endpoint.as_deref())?
        }
        _ => {
            anyhow::bail!(
                "Unsupported CLOUD_PROVIDER: {}. Use 'azure' or 's3'.",
                provider
            )
        }
    };

    // --- Store Operations (Cloud Agnostic) ---

    // 5. Perform the write and verification using the reusable library function.
    // This call works seamlessly with either the Azure or S3 store.
    storage::write_data(store, &target_path, &test_data.as_bytes()).await?;

    println!(
        "Application completed successfully. Object written to path: {}",
        target_path
    );

    Ok(())
}
