# move-dv PoC

This small Go program exercises moving one CDI DataVolume between namespaces while preserving its underlying PV:

1. Create a blank DataVolume in the tenant namespace.
2. Set the PV reclaim policy to `Retain`, delete the source DataVolume and PVC, clear the PV claimRef, and create a matching PVC in subnet A with `spec.volumeName` set. Create a DataVolume that adopts the existing PVC, then attach it to VM A and write a marker from inside the guest.
3. Stop VM A, detach the volume, and repeat the PV/PVC/DataVolume handoff to subnet B.
4. Attach it to VM B and verify the marker from inside the guest.
5. Stop VM B, detach the volume, and return the DataVolume to the tenant namespace.

The test VMs use a Fedora containerDisk image with cloud-init. They do not need network access for the disk check; the program reads the guest result over the KubeVirt serial console.

## Requirements

- Existing tenant, subnet A, and subnet B namespaces.
- KubeVirt and CDI installed, with CDI's `DataVolumeClaimAdoption` feature gate enabled in `spec.config.featureGates`.
- A StorageClass that can provision a filesystem PVC.
- Permissions to list CDI resources to check the adoption feature gate, inspect and patch PVs, manage DataVolumes/VMs/PVCs in the involved namespaces, and access the KubeVirt serial console.
- The selected VM containerDisk image must be pullable by the cluster.

The handoff changes Kubernetes bindings only; it does not copy the physical volume data. The replacement DataVolume and PVC are marked as pre-populated and eligible for adoption so CDI does not initialize the disk again. The PV remains set to `Retain`, including after `--cleanup` deletes the test DataVolume.

## Run

```sh
go run . --tenant tenant-ns --subnet-a subnet-a-ns --subnet-b subnet-b-ns --storage-class my-storage-class
```

The program uses the active kubeconfig context, or in-cluster credentials when run in a Pod. `--storage-class` is required and selects the StorageClass for the test DataVolume. Optional settings include `--kubeconfig`, `--vm-image`, `--disk-size`, `--memory`, and `--timeout`.

By default, successful runs leave both VMs halted and the detached DataVolume back in the tenant namespace for inspection. Add `--cleanup` to delete the created VMs and DataVolume after verification. The PV remains protected with reclaim policy `Retain`.
