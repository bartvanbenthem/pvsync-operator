# PersistentVolume Sync Operator
The PersistentVolume Sync Operator provides automated, cluster-wide replication of Kubernetes PersistentVolumes (PV) that share a common backend (e.g., NFS, RWX file storage).
It enables disaster-recovery scenarios by exporting PV metadata from a Protected cluster and restoring identical PV definitions into a Recovery cluster.

## Features
- 🔄 Cluster-wide PV discovery (no namespace restrictions)
- ☁️ Backend-agnostic object storage support (Azure Blob, S3, MinIO, Cloudian)
- 📤 Export storage definitions from the Protected cluster to object storage
- 📥 Recreate PV objects on the Recovery cluster pointing to the same shared storage
- 🏷 Cluster identity detection via configurable value
- 🧹 Automatic retention-based cleanup of historical exports
- 📡 Event-driven + periodic sync using Kubernetes watches and optional scheduling

## Use Cases
- 🌐 Multi-cluster DR for shared RWX storage
- 💾 PV metadata backup and restore
- 🔁 Migration of PVs between clusters
- 🧭 Stateless failover for NFS-backed workloads

### upcoming release
Features in currently in development for the upcoming release:
* auto rebuild pv on Recovery cluster
* update status with more information
* instead of an external watcher on Polling/Listing Comparison, investigate an alternative based on ETAG

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