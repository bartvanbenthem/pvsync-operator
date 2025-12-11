use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Error, Resource, core::NamespaceResourceScope};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

/// Generic status patcher for *cluster-scoped* custom resources.
///
/// `K` must be a cluster-scoped CRD type that has a `status` field.
pub async fn patch_crd_cluster<K, S>(client: Client, name: &str, status: S) -> Result<K, Error>
where
    K: Resource + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
    S: Serialize,
{
    // Api::all is required for cluster-scoped CRDs
    let api: Api<K> = Api::all(client);

    let data: Value = json!({
        "status": status,
    });

    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&data))
        .await
}

/// Generic status patcher for *namespace-scoped* custom resources.
///
/// `K` must be a namespace-scoped CRD type that has a `status` field.
pub async fn patch_crd_namespaced<K, S>(
    client: Client,
    namespace: &str,
    name: &str,
    status: S,
) -> Result<K, Error>
where
    K: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
    S: Serialize,
{
    let api: Api<K> = Api::namespaced(client, namespace);

    let data: Value = json!({
        "status": status,
    });

    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&data))
        .await
}
