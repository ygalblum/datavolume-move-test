# move-dv PoC

This small Go program exercises moving one CDI DataVolume between namespaces while preserving its underlying PV:

1. Create a blank DataVolume in the tenant namespace.
2. Transfer it to subnet A with CDI `ObjectTransfer`, attach it to VM A, and write a marker from inside the guest.
3. Stop VM A, detach the volume, and transfer it to subnet B.
4. Attach it to VM B and verify the marker from inside the guest.
5. Stop VM B, detach the volume, and return the DataVolume to the tenant namespace.

The test VMs use a Fedora containerDisk image with cloud-init. They do not need network access for the disk check; the program reads the guest result over the KubeVirt serial console.

## Requirements

- Existing tenant, subnet A, and subnet B namespaces.
- KubeVirt and CDI installed, including CDI's `ObjectTransfer` CRD.
- A StorageClass that can provision a filesystem PVC.
- Cluster-level permissions to create CDI ObjectTransfers and inspect/update PVs, plus namespace permissions to create and manage DataVolumes and VMs and read the VM serial console.
- The selected VM containerDisk image must be pullable by the cluster.

CDI documents direct ObjectTransfer use for cluster administrators. The transfer is cluster scoped and moves the PVC/DataVolume binding; it does not copy the physical volume data.

CDI currently serves ObjectTransfer as `cdi.kubevirt.io/v1beta1` and marks it deprecated for removal in v1. Its current transfer guide has no replacement API, so Kubernetes may emit a deprecation warning when the program creates transfer records. Keep using the served version until CDI publishes a replacement.

## Run

```sh
go run . --tenant tenant-ns --subnet-a subnet-a-ns --subnet-b subnet-b-ns --storage-class my-storage-class
```

The program uses the active kubeconfig context, or in-cluster credentials when run in a Pod. `--storage-class` is required and selects the StorageClass for the test DataVolume. Optional settings include `--kubeconfig`, `--vm-image`, `--disk-size`, `--memory`, and `--timeout`.

By default, successful runs leave both VMs halted and the detached DataVolume back in the tenant namespace for inspection. Add `--cleanup` to delete the created VMs and DataVolume after verification. The PV reclaim policy is restored to its original value after the transfers.
