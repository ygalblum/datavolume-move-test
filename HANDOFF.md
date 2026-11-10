# Handoff: KubeVirt DataVolume namespace-move PoC

## Goal and agreed behavior

Build a small Go PoC that demonstrates one DataVolume moving between namespaces while keeping the same PV and data:

1. Create a blank DataVolume in the tenant namespace.
2. Transfer it to subnet A, attach it to VM A, and write a marker from inside the guest.
3. Stop VM A, detach the volume, transfer it to subnet B, attach it to VM B, and verify the marker from inside the guest.
4. Stop VM B, detach the volume, and return the DataVolume to the tenant namespace.

The user confirmed that an unattached DataVolume belongs in the tenant namespace and that an attached DataVolume must be bound to a single VM in one namespace. The VMs can use a PoC test image, and resources should remain after success by default.

## Namespace move implementation

The PoC implements the namespace handoff directly because CDI `ObjectTransfer` is deprecated and has no replacement API. For each move it verifies the DataVolume's PVC and PV claimRef, sets the PV reclaim policy to `Retain`, deletes the source DataVolume/PVC, clears the PV claimRef, creates a matching PVC in the target namespace with `spec.volumeName` set, then creates a DataVolume that adopts the pre-populated PVC. This changes Kubernetes bindings only; the PV data is not copied.

CDI's `DataVolumeClaimAdoption` feature gate must be enabled in `spec.config.featureGates`. The replacement DataVolume and PVC carry CDI adoption/pre-populated annotations so CDI recognizes the existing disk without initializing it again. The PV remains `Retain` after success and cleanup.

## Files created

- [`main.go`](./main.go): CLI and workflow implementation. Required flags are `--tenant`, `--subnet-a`, `--subnet-b`, and `--storage-class`. Optional flags include `--kubeconfig`, `--vm-image`, `--disk-size`, `--memory`, `--timeout`, `--guest-timeout`, and `--cleanup`. The overall timeout defaults to 20 minutes; each VM boot and guest-marker check has a four-minute timeout. The guest repeats its success marker for up to 120 seconds, and the serial-console reader reconnects after a dropped stream while the VMI remains running. Waits now log periodic state summaries; console stalls log heartbeats and collect best-effort VMI, launcher pod, event, and `compute` container diagnostics before recovery.
- [`console.go`](./console.go): authenticated KubeVirt serial-console WebSocket connection over `client-go`. It requests the `plain.kubevirt.io` stream and retries HTTP 400 responses for up to 30 seconds while the console becomes ready.
- [`go.mod`](./go.mod) and [`go.sum`](./go.sum): Kubernetes `v0.35.0` dependencies and the direct WebSocket dependency. The KubeVirt `client-go` module was removed because its dependency graph fails resolution and its generated mock imports an API package absent from `client-go v0.35.0`.
- [`README.md`](./README.md): prerequisites and run instructions.

The default guest image is `quay.io/containerdisks/fedora:latest`. The app uses cloud-init scripts and the KubeVirt serial console to write and verify a unique marker. On success, both VMs are halted with the test disk detached, and the DataVolume is back in the tenant namespace. `--cleanup` deletes the created VMs and DataVolume; the PV is left protected with reclaim policy `Retain`.

## Cluster prerequisites

- The three namespaces already exist and are distinct.
- KubeVirt and CDI are installed, including the DataVolume API, and CDI's `DataVolumeClaimAdoption` feature gate is enabled.
- The `--storage-class` StorageClass can provision a filesystem PVC.
- The configured VM containerDisk image is pullable by the cluster.
- The app identity can list CDI resources, inspect and patch PVs, manage DataVolumes/VMs/PVCs in the involved namespaces, and access the KubeVirt serial console.

The test VMs do not configure a network interface; guest file I/O and verification use the attached disk and serial console.

## Latest VM A console failure (2026-11-09)

The VM A check reached guest boot and read cloud-init SSH host-key generation output (through roughly 26 seconds of guest uptime), then the console stream ended with `websocket: close 1006 (abnormal closure): unexpected EOF`. The expected `MOVE_DV_A_OK:move-dv-912690dcb92fe671161fc839` marker and a guest `MOVE_DV_A_ERROR` marker were both absent.

This establishes that the serial-console WebSocket ended before the check saw its marker. The output alone does not establish whether the VMI stopped or restarted, another console client displaced this connection, an API/network proxy interrupted it, or the guest script had not reached its marker yet. The VM name shown in the log was `mvdv-4e2737c3-vm-a`; the namespace was not included in the report.

After this failure, `main.go` was changed to wait for the VMI and reconnect one second after a console read error. It retains captured output across connections and reports VMI or reconnect errors. VM A and VM B repeat their success markers once per second for up to 120 seconds, allowing a reconnect to catch a marker emitted during a brief disconnect. The current diagnostics add ten-second heartbeats for long Kubernetes waits and serial-console reads. On a console disconnect, guest error marker, or timeout, they capture the VMI status and conditions, matching launcher pod state, recent related namespace events, and the last 100 lines of the `compute` container logs before automatic recovery starts. Pod lookup first uses the KubeVirt domain label and falls back to matching VMI events, owner references, and launcher pod names. Pod, event, and pod-log reads are best effort; grant `list pods`, `list events`, and `get pods/log` in the VM namespaces to include them. Without those permissions the run logs which snapshots it could not read. Guest user-data now reports stages for script startup, disk discovery, formatting, mounting, and marker verification. These changes have built locally but have not yet been run against a cluster.

### VM A wait (2026-11-09, run `mvdv-56e87c35`)

The PV and DataVolume move to subnet A completed, and VMI `subnet-a/mvdv-56e87c35-vm-a` reached `Running` with `Ready=True`, `DataVolumesReady=True`, and `AgentConnected=True`. The console received about 111 KB of boot output through guest uptime 24.96 seconds, then made no further progress. At 18:19:05 the WebSocket closed with code 1006; the VMI was still running, and reconnect succeeded one second later. The run was manually stopped at 18:20:07 without a guest success or error marker.

The `LiveMigratable=False` condition says the RWO PVC does not support live migration; this PoC does not live-migrate the VM, so it does not explain the missing guest marker. The diagnostic pod lookup found no pod using its original label selector, while namespace events named `virt-launcher-mvdv-56e87c35-vm-a-7rn7w`. The lookup now falls back to those event-referenced launcher names so the next run can capture pod state and `compute` logs. The console output does not establish whether cloud-init reached the user script or whether the script stalled while locating the disk; the new guest stage markers will distinguish those cases.

### Later VM A wait (2026-11-09, run `mvdv-26f72c82`)

The VMI again reached `Running`; guest agent connectivity became true. The console showed `MOVE_DV_A_STAGE:script-start` at guest uptime 20.6 seconds, then later output stopped after SSH host-key generation at about 29.1 seconds. No `waiting-for-disk` stage appeared in the supplied log excerpt. The only command between `script-start` and `waiting-for-disk` was the script's fixed eight-second sleep. That sleep has been removed because the disk discovery loop already waits for the device; the next run should report disk discovery immediately. This run's excerpt ends before a disconnect snapshot, so it provides no launcher `compute` logs.

On an operation error, the existing deferred recovery path sets both VMs to `Halted` and attempts to return the volume to the tenant. This can remove the running VMI and its launcher pod after the new snapshot is collected. Ensure no other client is attached to the VM's serial console while reproducing the failure, since another client may disrupt or consume the same stream.

## Validation status and next steps

- Completed: dependency references causing the reported `go get move-dv` errors were removed. The previous graph requested the nonexistent `k8s.io/kube-openapi v0.31.0` and expected `k8s.io/client-go/kubernetes/typed/storagemigration/v1alpha1` while the selected Kubernetes client version did not provide it.
- Completed: `gofmt` ran on `main.go` and `console.go`.
- Completed: kubeconfig loading now uses client-go's default loading rules when `--kubeconfig` is omitted, so it reads `KUBECONFIG` (including a path list) or the standard `~/.kube/config`. Previously, passing an empty path to `BuildConfigFromFlags` attempted in-cluster config and then used empty loading rules, so the exported `KUBECONFIG` was ignored. Explicit `--kubeconfig` and in-cluster credentials remain supported.
- Completed for the current changes: `gofmt`, `git diff --check`, and `GOCACHE=/private/tmp/move-dv-gocache go build -mod=readonly ./...`.
- Not completed: runtime validation of the updated launcher pod lookup, guest stage markers, direct WebSocket path, reconnect behavior, and marker read against a cluster with KubeVirt/CDI and the required RBAC.

Next, verify dependency resolution from a separate consumer module with `go get move-dv` (or the published module path), then rerun the PoC against a cluster with KubeVirt/CDI, the `DataVolumeClaimAdoption` feature gate, and the required RBAC. During the VM check, the app now attempts to capture VMI status/events and virt-launcher logs before automatic recovery halts the VM; grant the optional diagnostic permissions above for the full snapshot.

Example invocation:

```sh
go run . --tenant tenant-ns --subnet-a subnet-a-ns --subnet-b subnet-b-ns --storage-class my-storage-class
```
