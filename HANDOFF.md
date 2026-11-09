# Handoff: KubeVirt DataVolume namespace-move PoC

## Goal and agreed behavior

Build a small Go PoC that demonstrates one DataVolume moving between namespaces while keeping the same PV and data:

1. Create a blank DataVolume in the tenant namespace.
2. Transfer it to subnet A, attach it to VM A, and write a marker from inside the guest.
3. Stop VM A, detach the volume, transfer it to subnet B, attach it to VM B, and verify the marker from inside the guest.
4. Stop VM B, detach the volume, and return the DataVolume to the tenant namespace.

The user confirmed that an unattached DataVolume belongs in the tenant namespace and that an attached DataVolume must be bound to a single VM in one namespace. The VMs can use a PoC test image, and resources should remain after success by default.

## ObjectTransfer decision

CDI documents `ObjectTransfer` as an API that cluster administrators can use directly. It is a CDI custom resource submitted through the Kubernetes API, not a private endpoint or an undocumented workaround. It moves the DataVolume/PVC association without copying the physical volume data. The API is cluster scoped, so the PoC needs cluster-level RBAC. See [CDI Namespace Transfer](https://github.com/kubevirt/containerized-data-importer/blob/main/doc/namespace-transfer.md).

## Files created

- [`main.go`](./main.go): CLI and workflow implementation. Required flags are `--tenant`, `--subnet-a`, `--subnet-b`, and `--storage-class`. Optional flags include `--kubeconfig`, `--vm-image`, `--disk-size`, `--memory`, `--timeout`, and `--cleanup`.
- [`console.go`](./console.go): authenticated KubeVirt serial-console WebSocket connection over `client-go`. It requests the `plain.kubevirt.io` stream and retries HTTP 400 responses for up to 30 seconds while the console becomes ready.
- [`go.mod`](./go.mod) and [`go.sum`](./go.sum): Kubernetes `v0.35.0` dependencies and the direct WebSocket dependency. The KubeVirt `client-go` module was removed because its dependency graph fails resolution and its generated mock imports an API package absent from `client-go v0.35.0`.
- [`README.md`](./README.md): prerequisites and run instructions.

The default guest image is `quay.io/containerdisks/fedora:latest`. The app uses cloud-init scripts and the KubeVirt serial console to write and verify a unique marker. On success, both VMs are halted with the test disk detached, and the DataVolume is back in the tenant namespace. `--cleanup` deletes the created VMs, DataVolume, and ObjectTransfer records after verification. The PV reclaim policy is restored to its original value.

## Cluster prerequisites

- The three namespaces already exist and are distinct.
- KubeVirt and CDI are installed, including the CDI `ObjectTransfer` CRD.
- The `--storage-class` StorageClass can provision a filesystem PVC.
- The configured VM containerDisk image is pullable by the cluster.
- The app identity can create CDI ObjectTransfers at cluster scope, inspect and patch PVs, manage DataVolumes/VMs/PVCs in the involved namespaces, and access the KubeVirt serial console.

The test VMs do not configure a network interface; guest file I/O and verification use the attached disk and serial console.

## Validation status and next steps

- Completed: dependency references causing the reported `go get move-dv` errors were removed. The previous graph requested the nonexistent `k8s.io/kube-openapi v0.31.0` and expected `k8s.io/client-go/kubernetes/typed/storagemigration/v1alpha1` while the selected Kubernetes client version did not provide it.
- Completed: `gofmt` ran on `main.go` and `console.go`.
- Completed: kubeconfig loading now uses client-go's default loading rules when `--kubeconfig` is omitted, so it reads `KUBECONFIG` (including a path list) or the standard `~/.kube/config`. Previously, passing an empty path to `BuildConfigFromFlags` attempted in-cluster config and then used empty loading rules, so the exported `KUBECONFIG` was ignored. Explicit `--kubeconfig` and in-cluster credentials remain supported.
- Completed: `go build` succeeded for the updated source. The build cache and output were directed to `/private/tmp` because the default Go build cache location was not writable in the build environment.
- Not completed: `go mod tidy` status has not been rechecked in this environment. `go.sum` currently has direct module checksums; run `go mod tidy` if the target environment reports missing checksum entries.
- Not completed: a cluster run. The direct WebSocket console path and marker read still need runtime validation against a cluster with KubeVirt/CDI and the required RBAC.

Continue from the module root in an environment with Go module network access:

```sh
go mod tidy
go build ./...
```

Then verify dependency resolution from a separate consumer module with `go get move-dv` (or the published module path), address any compile errors, and run the PoC against a cluster with KubeVirt/CDI and the required RBAC. Confirm that the cluster's CDI version supports direct DataVolume transfers through `ObjectTransfer`.

Example invocation:

```sh
go run . --tenant tenant-ns --subnet-a subnet-a-ns --subnet-b subnet-b-ns --storage-class my-storage-class
```
