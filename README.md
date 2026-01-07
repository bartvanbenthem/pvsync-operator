# PersistentVolume Sync Operator
The PersistentVolume Sync Operator provides automated, cluster-wide replication of Kubernetes PersistentVolumes (PV) that share a common backend (e.g., NFS, RWX file storage).
It enables disaster-recovery scenarios by exporting PV metadata from a Protected cluster and restoring identical PV definitions into a Recovery cluster.

## Operator Logic: Metadata Synchronization and Storage Mapping
The VolumeSync Operator facilitates cross-cluster disaster recovery by synchronizing the state and specifications of Kubernetes storage resources. Its behavior is divided into two distinct logical paths:

### Resource Specification Capture (Sync)
The operator performs a point-in-time capture of the Kubernetes resource specifications for all labeled PersistentVolumes (PV) and PersistentVolumeClaims (PVC).

- Scope: It monitors resources across all StorageClasses to ensure a complete map of the source cluster's storage requirements is maintained.
- Mechanism: The operator captures the metadata, labels, and specific volume requirements (size, access modes) into a portable format. This ensures that the intent of the storage configuration is preserved and ready for recreation on the recovery cluster.

### StorageClass Decoupling (Skip by Design)
While the operator captures the metadata for PVCs tied to various StorageClasses, it explicitly does not recover or create StorageClass objects on the recovery cluster.

- Architectural Intent: This is a deliberate design choice to support Heterogeneous Storage Environments. In many recovery scenarios, the storage backend on the recovery site differs from the source. For example, the recovery cluster may need to point to a localized file cache or a different storage endpoint (e.g., a regional vs. local SSD) to optimize performance or adhere to site-specific infrastructure.
- Binding Logic: By synchronizing only the PVC/PV metadata and not the StorageClass, the operator allows the recovery cluster's local storage controller to bind the restored claims to the appropriate local backend.

### Recovery Workflow

- Metadata Extraction: The operator scans the source cluster for labeled storage resources and captures their YAML specifications.
- Transformation: Using the clean_metadata logic, the operator strips environment-specific internal annotations while preserving the core volume requirements.
- Local Re-Binding: On the recovery cluster, the operator recreates the PVCs. These claims then automatically target the pre-existing local StorageClasses defined on the recovery site, ensuring the data is served via the correct local file cache or storage provider.

Key Benefit: This approach provides a "Clean Slate" recovery where storage logic is kept local to the cluster, preventing the migration of invalid or incompatible storage provider configurations from the primary site.

### Features
- 🔄 Cluster-wide PV discovery (no namespace restrictions)
- ☁️ Backend-agnostic object storage support (Azure Blob, S3, MinIO, Cloudian)
- 📤 Export storage definitions from the Protected cluster to object storage
- 📥 Recreate PV objects on the Recovery cluster pointing to the same shared storage
- 🏷 Cluster identity detection via configurable value
- 🧹 Automatic retention-based cleanup of historical exports
- 📡 Event-driven + periodic sync using Kubernetes watches and optional scheduling

### Use Cases
- 🌐 Multi-cluster DR for shared RWX storage
- 💾 PV metadata backup and restore
- 🔁 Migration of PVs between clusters
- 🧭 Stateless failover for NFS-backed workloads

## upcoming release
Features in currently in development for the upcoming release:
* Validating admission webhook for a max of one pvsync custom resource per cluster
* Helm chart for kubernetes native deployment
* update cr status with more information: 
  - pub error_message: Option<String>,
  - pub last_run: Option<chrono::DateTime<chrono::Utc>>,
  - pub managed_volumes: Vec<String>,
* instead of an external watcher (s3) on Polling/Listing Comparison, investigate an alternative based on ETAG

## Build container
```bash
source ../00-ENV/env.sh
CVERSION="v0.6.1"

docker login ghcr.io -u bartvanbenthem -p $CR_PAT

docker build -t pvsync:$CVERSION .

docker tag pvsync:$CVERSION ghcr.io/bartvanbenthem/pvsync:$CVERSION
docker push ghcr.io/bartvanbenthem/pvsync:$CVERSION

# test image
docker run --rm -it --entrypoint /bin/sh pvsync:$CVERSION

/# ls -l /usr/local/bin/pvsync
/# /usr/local/bin/pvsync
```

## Deploy CRD
```bash
kubectl apply -f ./config/crd/pvsync.storage.cndev.nl.yaml
# kubectl delete -f ./config/crd/pvsync.storage.cndev.nl.yaml
```

## create secret
```bash
# secret containing object storage
source ../00-ENV/env.sh
kubectl create ns pvsync-operator
kubectl -n pvsync-operator create secret generic pvsync \
  --from-literal=OBJECT_STORAGE_ACCOUNT=$OBJECT_STORAGE_ACCOUNT \
  --from-literal=OBJECT_STORAGE_SECRET=$OBJECT_STORAGE_SECRET \
  --from-literal=OBJECT_STORAGE_BUCKET=$OBJECT_STORAGE_BUCKET \
  --from-literal=S3_ENDPOINT_URL=""
```

## Deploy Operator
```bash
helm install pvsync ./config/operator/chart --create-namespace --namespace pvsync-operator
# helm -n pvsync-operator uninstall pvsync
```

## Sample volume sync resource on protected cluster
```bash
# use label: volumesyncs.storage.cndev.nl/sync: "enabled"
# to enable a sync on a persistant volume
kubectl apply -f ./config/samples/pvsync-protected-example.yaml
kubectl describe persistentvolumesyncs.storage.cndev.nl example-protected-cluster
# kubectl delete -f ./config/samples/pvsync-protected-example.yaml
```

## Sample volume sync resource on recovery cluster
```bash
kubectl apply -f ./config/samples/pvsync-recovery-example.yaml
kubectl describe persistentvolumesyncs.storage.cndev.nl example-recovery-cluster
# kubectl delete -f ./config/samples/pvsync-recovery-example.yaml
```

## Test Watchers & Reconciler on Create Persistant Volumes
```bash
kubectl apply -f ./config/samples/test-pv-nolabel.yaml
kubectl apply -f ./config/samples/test-pv.yaml
# kubectl delete -f ./config/samples/test-pv.yaml

# scripted test
while true; do 
  kubectl apply -f ./config/samples/test-pv.yaml; 
  sleep 5; 
  kubectl delete -f ./config/samples/test-pv.yaml; 
  sleep 5; 
done

```

## CR Spec
```yaml
apiVersion: storage.cndev.nl/v1alpha1
kind: PersistentVolumeSync
metadata:
  name: example-protected-cluster
  labels:
    volumesyncs.storage.cndev.nl/name: example-protected-cluster
    volumesyncs.storage.cndev.nl/part-of: pvsync-operator
  annotations:
    description: "Disaster Recovery solution for Persistent Volumes"
spec:
  protectedCluster: mylocalcluster # name or id of the protected cluster
  mode: Protected # Protected | Recovery
  cloudProvider: azure # azure | s3
  retention: 5 # retention in days
---
apiVersion: storage.cndev.nl/v1alpha1
kind: PersistentVolumeSync
metadata:
  name: example-recovery-cluster
  labels:
    volumesyncs.storage.cndev.nl/name: example-recovery-cluster
    volumesyncs.storage.cndev.nl/part-of: pvsync-operator
  annotations:
    description: "Disaster Recovery solution for Persistent Volumes"
spec:
  protectedCluster: mylocalcluster # name or id of the recovery cluster
  mode: Recovery # Protected | Recovery
  cloudProvider: azure # azure | s3
  retention: 5 # retention in days
  pollingInterval: 25 # polling interval to object store in seconds
```