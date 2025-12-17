use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Error, Resource, core::NamespaceResourceScope};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;

/// Generic status patcher for *cluster-scoped* custom resources.
/// Server-Side Apply (SSA), prefered by Kubernetes.
/// `K` must be a cluster-scoped CRD type that has a `status` field.
pub async fn patch_cr_cluster<K, S>(client: Client, name: &str, status: S) -> Result<K, Error>
where
    K: Resource + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
    S: Serialize,
{
    // Api::all is used for cluster-scoped resources
    let api: Api<K> = Api::all(client);
    // We use Default::default() as the argument for api_version and kind
    let dynamic_context = K::DynamicType::default();
    // Construct the SSA payload
    // SSA requires the full object structure, but for /status it only cares about the status block
    let patch = json!({
        "apiVersion": K::api_version(&dynamic_context),
        "kind": K::kind(&dynamic_context),
        "status": status,
    });
    // We apply as a specific manager to own the status fields
    let params = PatchParams::apply("pvsync-controller").force();
    // Perform the patch
    api.patch_status(name, &params, &Patch::Apply(&patch)).await
}

/// Generic status patcher for *namespace-scoped* custom resources.
///
/// `K` must be a namespace-scoped CRD type that has a `status` field.
pub async fn patch_cr_namespaced<K, S>(
    client: Client,
    namespace: &str,
    name: &str,
    status: S,
) -> Result<K, Error>
where
    K: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default, // This allows us to call Default::default()
    S: Serialize,
{
    // Api::namespaced is used for namespace-scoped resources
    let api: Api<K> = Api::namespaced(client, namespace);
    // We use Default::default() as the argument for api_version and kind
    let dynamic_context = K::DynamicType::default();
    // Construct the SSA payload
    // SSA requires the full object structure, but for /status it only cares about the status block
    let patch = json!({
        "apiVersion": K::api_version(&dynamic_context),
        "kind": K::kind(&dynamic_context),
        "status": status,
    });
    // We apply as a specific manager to own the status fields
    let params = PatchParams::apply("pvsync-controller").force();
    // Perform the patch
    api.patch_status(name, &params, &Patch::Apply(&patch)).await
}
