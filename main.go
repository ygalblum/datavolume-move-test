package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	cdiAPIGroupVersion    = "cdi.kubevirt.io/v1beta1"
	kubeVirtGroupVersion  = "kubevirt.io/v1"
	appLabel              = "app.kubernetes.io/name"
	appLabelValue         = "move-dv-poc"
	runLabel              = "app.kubernetes.io/instance"
	dataVolumeSizeDefault = "1Gi"
	vmImageDefault        = "quay.io/containerdisks/fedora:latest"
)

var (
	namespacesGVR              = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	pvcsGVR                    = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}
	pvsGVR                     = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}
	dataVolumesGVR             = schema.GroupVersionResource{Group: "cdi.kubevirt.io", Version: "v1beta1", Resource: "datavolumes"}
	objectTransfersGVR         = schema.GroupVersionResource{Group: "cdi.kubevirt.io", Version: "v1beta1", Resource: "objecttransfers"}
	virtualMachinesGVR         = schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachines"}
	virtualMachineInstancesGVR = schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachineinstances"}
)

type options struct {
	tenant       string
	subnetA      string
	subnetB      string
	kubeconfig   string
	vmImage      string
	diskSize     string
	storageClass string
	memory       string
	timeout      time.Duration
	cleanup      bool
}

type runner struct {
	client                dynamic.Interface
	config                *rest.Config
	discovery             discovery.DiscoveryInterface
	opts                  options
	runID                 string
	dvName                string
	vmA                   string
	vmB                   string
	marker                string
	transferNames         []string
	pvName                string
	originalReclaimPolicy string
}

func main() {
	var opts options
	flag.StringVar(&opts.tenant, "tenant", "", "tenant namespace where an unattached DataVolume is kept (required)")
	flag.StringVar(&opts.subnetA, "subnet-a", "", "namespace for the first test VM (required)")
	flag.StringVar(&opts.subnetB, "subnet-b", "", "namespace for the second test VM (required)")
	flag.StringVar(&opts.kubeconfig, "kubeconfig", "", "kubeconfig path; defaults to KUBECONFIG or the standard kubeconfig")
	flag.StringVar(&opts.vmImage, "vm-image", vmImageDefault, "bootable containerDisk image with cloud-init support")
	flag.StringVar(&opts.diskSize, "disk-size", dataVolumeSizeDefault, "size of the test DataVolume")
	flag.StringVar(&opts.storageClass, "storage-class", "", "StorageClass for the test DataVolume (required)")
	flag.StringVar(&opts.memory, "memory", "1Gi", "memory requested by each test VM")
	flag.DurationVar(&opts.timeout, "timeout", 20*time.Minute, "overall operation timeout")
	flag.BoolVar(&opts.cleanup, "cleanup", false, "delete the test VMs and DataVolume after successful verification")
	flag.Parse()

	if err := validateOptions(opts); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	if err := execute(ctx, opts); err != nil {
		log.Fatal(err)
	}
}

func validateOptions(opts options) error {
	for label, value := range map[string]string{
		"--tenant":   opts.tenant,
		"--subnet-a": opts.subnetA,
		"--subnet-b": opts.subnetB,
	} {
		if value == "" {
			return fmt.Errorf("%s is required", label)
		}
		if problems := validation.IsDNS1123Label(value); len(problems) > 0 {
			return fmt.Errorf("invalid namespace %q for %s: %s", value, label, strings.Join(problems, "; "))
		}
	}
	if opts.tenant == opts.subnetA || opts.tenant == opts.subnetB || opts.subnetA == opts.subnetB {
		return errors.New("tenant, subnet-a, and subnet-b must be three different namespaces")
	}
	if strings.TrimSpace(opts.vmImage) == "" {
		return errors.New("--vm-image must not be empty")
	}
	if strings.TrimSpace(opts.memory) == "" || strings.TrimSpace(opts.diskSize) == "" {
		return errors.New("--memory and --disk-size must not be empty")
	}
	if strings.TrimSpace(opts.storageClass) == "" {
		return errors.New("--storage-class is required")
	}
	if opts.timeout <= 0 {
		return errors.New("--timeout must be positive")
	}
	return nil
}

func execute(ctx context.Context, opts options) (retErr error) {
	config, err := kubeConfig(opts.kubeconfig)
	if err != nil {
		return err
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes dynamic client: %w", err)
	}
	dc, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes discovery client: %w", err)
	}
	runID, err := randomHex(4)
	if err != nil {
		return fmt.Errorf("generate run ID: %w", err)
	}
	marker, err := randomHex(12)
	if err != nil {
		return fmt.Errorf("generate verification marker: %w", err)
	}
	r := &runner{
		client:    client,
		config:    config,
		discovery: dc,
		opts:      opts,
		runID:     "mvdv-" + runID,
		marker:    "move-dv-" + marker,
	}
	r.dvName = r.runID + "-disk"
	r.vmA = r.runID + "-vm-a"
	r.vmB = r.runID + "-vm-b"

	if err := r.preflight(ctx); err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			recoveryCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			if recoveryErr := r.returnVolumeToTenant(recoveryCtx); recoveryErr != nil {
				log.Printf("recovery note: %v", recoveryErr)
			}
		}
	}()
	return r.run(ctx)
}

func kubeConfig(path string) (*rest.Config, error) {
	if path == "" && os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		config, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("load in-cluster credentials: %w", err)
		}
		return config, nil
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if path != "" {
		loadingRules = &clientcmd.ClientConfigLoadingRules{ExplicitPath: path}
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	return config, nil
}

func (r *runner) preflight(ctx context.Context) error {
	for _, namespace := range []string{r.opts.tenant, r.opts.subnetA, r.opts.subnetB} {
		if _, err := r.client.Resource(namespacesGVR).Get(ctx, namespace, metav1.GetOptions{}); err != nil {
			return fmt.Errorf("check namespace %q: %w", namespace, err)
		}
	}
	if err := r.requireAPI(cdiAPIGroupVersion, "datavolumes"); err != nil {
		return err
	}
	if err := r.requireAPI(cdiAPIGroupVersion, "objecttransfers"); err != nil {
		return fmt.Errorf("CDI ObjectTransfer API is required: %w", err)
	}
	if err := r.requireAPI(kubeVirtGroupVersion, "virtualmachines"); err != nil {
		return err
	}
	if err := r.requireAPI(kubeVirtGroupVersion, "virtualmachineinstances"); err != nil {
		return err
	}
	log.Printf("preflight complete; run ID %s", r.runID)
	return nil
}

func (r *runner) requireAPI(groupVersion, resource string) error {
	apiResources, err := r.discovery.ServerResourcesForGroupVersion(groupVersion)
	if err != nil {
		return fmt.Errorf("discover %s API: %w", groupVersion, err)
	}
	for _, item := range apiResources.APIResources {
		if item.Name == resource {
			return nil
		}
	}
	return fmt.Errorf("%s does not serve resource %q", groupVersion, resource)
}

func (r *runner) run(ctx context.Context) error {
	log.Printf("creating halted VMs %s/%s and %s/%s", r.opts.subnetA, r.vmA, r.opts.subnetB, r.vmB)
	if err := r.createVM(ctx, r.opts.subnetA, r.vmA, "write"); err != nil {
		return err
	}
	if err := r.createVM(ctx, r.opts.subnetB, r.vmB, "verify"); err != nil {
		return err
	}

	log.Printf("creating blank DataVolume %s/%s", r.opts.tenant, r.dvName)
	if err := r.createDataVolume(ctx, r.opts.tenant); err != nil {
		return err
	}
	if err := r.waitForDataVolume(ctx, r.opts.tenant); err != nil {
		return err
	}
	pvName, reclaimPolicy, err := r.boundPVAndPolicy(ctx, r.opts.tenant)
	if err != nil {
		return err
	}
	r.pvName, r.originalReclaimPolicy = pvName, reclaimPolicy
	log.Printf("DataVolume is bound to PV %s (original reclaim policy %s)", r.pvName, r.originalReclaimPolicy)

	if err := r.transfer(ctx, r.opts.tenant, r.opts.subnetA, "tenant-to-a"); err != nil {
		return err
	}
	if err := r.assertPVClaim(ctx, r.opts.subnetA); err != nil {
		return err
	}
	if err := r.attachDataVolume(ctx, r.opts.subnetA, r.vmA); err != nil {
		return err
	}
	if err := r.runVMAndWaitForSignal(ctx, r.opts.subnetA, r.vmA, "MOVE_DV_A_OK:"); err != nil {
		return fmt.Errorf("VM A did not write the marker: %w", err)
	}
	if err := r.stopAndDetach(ctx, r.opts.subnetA, r.vmA); err != nil {
		return err
	}

	if err := r.transfer(ctx, r.opts.subnetA, r.opts.subnetB, "a-to-b"); err != nil {
		return err
	}
	if err := r.assertPVClaim(ctx, r.opts.subnetB); err != nil {
		return err
	}
	if err := r.attachDataVolume(ctx, r.opts.subnetB, r.vmB); err != nil {
		return err
	}
	if err := r.runVMAndWaitForSignal(ctx, r.opts.subnetB, r.vmB, "MOVE_DV_B_OK:"); err != nil {
		return fmt.Errorf("VM B could not verify the marker: %w", err)
	}
	if err := r.stopAndDetach(ctx, r.opts.subnetB, r.vmB); err != nil {
		return err
	}

	// The test VM is done with the disk, so return it to the tenant namespace.
	if err := r.transfer(ctx, r.opts.subnetB, r.opts.tenant, "b-to-tenant"); err != nil {
		return err
	}
	if err := r.assertPVClaim(ctx, r.opts.tenant); err != nil {
		return err
	}
	if err := r.restoreReclaimPolicy(ctx); err != nil {
		return err
	}

	if r.opts.cleanup {
		if err := r.cleanup(ctx); err != nil {
			return err
		}
		log.Printf("verification succeeded; test VMs, DataVolume, and transfer records were cleaned up")
		return nil
	}

	log.Printf("verification succeeded: marker %q was written by VM A and read by VM B", r.marker)
	log.Printf("DataVolume returned to tenant namespace %s and is detached", r.opts.tenant)
	log.Printf("resources retained: VMs %s/%s and %s/%s; DataVolume %s/%s; PV %s", r.opts.subnetA, r.vmA, r.opts.subnetB, r.vmB, r.opts.tenant, r.dvName, r.pvName)
	return nil
}

func (r *runner) createDataVolume(ctx context.Context, namespace string) error {
	dv := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": cdiAPIGroupVersion,
		"kind":       "DataVolume",
		"metadata": map[string]interface{}{
			"name":        r.dvName,
			"namespace":   namespace,
			"labels":      map[string]interface{}{appLabel: appLabelValue, runLabel: r.runID},
			"annotations": map[string]interface{}{"cdi.kubevirt.io/storage.bind.immediate.requested": "true"},
		},
		"spec": map[string]interface{}{
			"source": map[string]interface{}{"blank": map[string]interface{}{}},
			"storage": map[string]interface{}{
				"accessModes":      []interface{}{"ReadWriteOnce"},
				"storageClassName": r.opts.storageClass,
				"volumeMode":       "Filesystem",
				"resources":        map[string]interface{}{"requests": map[string]interface{}{"storage": r.opts.diskSize}},
			},
		},
	}}
	if _, err := r.client.Resource(dataVolumesGVR).Namespace(namespace).Create(ctx, dv, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create blank DataVolume %s/%s: %w", namespace, r.dvName, err)
	}
	return nil
}

func (r *runner) waitForDataVolume(ctx context.Context, namespace string) error {
	return poll(ctx, 2*time.Second, func() (bool, error) {
		dv, err := r.client.Resource(dataVolumesGVR).Namespace(namespace).Get(ctx, r.dvName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		phase, _, _ := unstructured.NestedString(dv.Object, "status", "phase")
		if strings.EqualFold(phase, "Failed") {
			return false, fmt.Errorf("DataVolume %s/%s failed", namespace, r.dvName)
		}
		claim, _, _ := unstructured.NestedString(dv.Object, "status", "claimName")
		if claim == "" {
			claim = r.dvName
		}
		pvc, err := r.client.Resource(pvcsGVR).Namespace(namespace).Get(ctx, claim, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		pvcPhase, _, _ := unstructured.NestedString(pvc.Object, "status", "phase")
		if pvcPhase != "Bound" {
			return false, nil
		}
		if strings.EqualFold(phase, "Succeeded") || hasReadyCondition(dv) {
			return true, nil
		}
		return false, nil
	})
}

func hasReadyCondition(obj *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, raw := range conditions {
		condition, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		typ, _, _ := unstructured.NestedString(condition, "type")
		status, _, _ := unstructured.NestedString(condition, "status")
		if typ == "Ready" && status == "True" {
			return true
		}
	}
	return false
}

func (r *runner) boundPVAndPolicy(ctx context.Context, namespace string) (string, string, error) {
	dv, err := r.client.Resource(dataVolumesGVR).Namespace(namespace).Get(ctx, r.dvName, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}
	claim, _, _ := unstructured.NestedString(dv.Object, "status", "claimName")
	if claim == "" {
		claim = r.dvName
	}
	pvc, err := r.client.Resource(pvcsGVR).Namespace(namespace).Get(ctx, claim, metav1.GetOptions{})
	if err != nil {
		return "", "", fmt.Errorf("get PVC %s/%s: %w", namespace, claim, err)
	}
	pvName, _, _ := unstructured.NestedString(pvc.Object, "spec", "volumeName")
	if pvName == "" {
		return "", "", fmt.Errorf("PVC %s/%s is bound without a PV name", namespace, claim)
	}
	pv, err := r.client.Resource(pvsGVR).Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return "", "", fmt.Errorf("get PV %s: %w", pvName, err)
	}
	policy, _, _ := unstructured.NestedString(pv.Object, "spec", "persistentVolumeReclaimPolicy")
	if policy == "" {
		policy = "Delete"
	}
	return pvName, policy, nil
}

func (r *runner) transfer(ctx context.Context, sourceNS, targetNS, suffix string) error {
	name := r.runID + "-" + suffix
	transfer := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": cdiAPIGroupVersion,
		"kind":       "ObjectTransfer",
		"metadata":   map[string]interface{}{"name": name, "labels": map[string]interface{}{appLabel: appLabelValue, runLabel: r.runID}},
		"spec": map[string]interface{}{
			"source": map[string]interface{}{"kind": "DataVolume", "namespace": sourceNS, "name": r.dvName},
			"target": map[string]interface{}{"namespace": targetNS, "name": r.dvName},
		},
	}}
	if _, err := r.client.Resource(objectTransfersGVR).Create(ctx, transfer, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create ObjectTransfer %s from %s to %s (requires cluster-scoped CDI permissions): %w", name, sourceNS, targetNS, err)
	}
	r.transferNames = append(r.transferNames, name)
	log.Printf("transferring DataVolume %s from %s to %s using CDI ObjectTransfer", r.dvName, sourceNS, targetNS)
	return poll(ctx, 2*time.Second, func() (bool, error) {
		ot, err := r.client.Resource(objectTransfersGVR).Get(ctx, name, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		if err == nil {
			phase, _, _ := unstructured.NestedString(ot.Object, "status", "phase")
			if strings.EqualFold(phase, "Failed") {
				message, _, _ := unstructured.NestedString(ot.Object, "status", "message")
				return false, fmt.Errorf("ObjectTransfer %s failed: %s", name, message)
			}
		}
		source, sourceErr := r.client.Resource(dataVolumesGVR).Namespace(sourceNS).Get(ctx, r.dvName, metav1.GetOptions{})
		target, targetErr := r.client.Resource(dataVolumesGVR).Namespace(targetNS).Get(ctx, r.dvName, metav1.GetOptions{})
		if sourceErr != nil && !apierrors.IsNotFound(sourceErr) {
			return false, sourceErr
		}
		if targetErr != nil && !apierrors.IsNotFound(targetErr) {
			return false, targetErr
		}
		if source != nil || target == nil {
			return false, nil
		}
		claim, _, _ := unstructured.NestedString(target.Object, "status", "claimName")
		if claim == "" {
			claim = r.dvName
		}
		pvc, err := r.client.Resource(pvcsGVR).Namespace(targetNS).Get(ctx, claim, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		phase, _, _ := unstructured.NestedString(pvc.Object, "status", "phase")
		pvName, _, _ := unstructured.NestedString(pvc.Object, "spec", "volumeName")
		return phase == "Bound" && pvName == r.pvName, nil
	})
}

func (r *runner) assertPVClaim(ctx context.Context, namespace string) error {
	dv, err := r.client.Resource(dataVolumesGVR).Namespace(namespace).Get(ctx, r.dvName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("expected DataVolume in %s: %w", namespace, err)
	}
	claim, _, _ := unstructured.NestedString(dv.Object, "status", "claimName")
	if claim == "" {
		claim = r.dvName
	}
	pvc, err := r.client.Resource(pvcsGVR).Namespace(namespace).Get(ctx, claim, metav1.GetOptions{})
	if err != nil {
		return err
	}
	pvName, _, _ := unstructured.NestedString(pvc.Object, "spec", "volumeName")
	phase, _, _ := unstructured.NestedString(pvc.Object, "status", "phase")
	if phase != "Bound" || pvName != r.pvName {
		return fmt.Errorf("DataVolume claim in %s is not bound to original PV %s (phase=%s, PV=%s)", namespace, r.pvName, phase, pvName)
	}
	return nil
}

func (r *runner) createVM(ctx context.Context, namespace, name, role string) error {
	userData := cloudInitUserData(role, r.marker, r.dataDiskSerial())
	vm := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": kubeVirtGroupVersion,
		"kind":       "VirtualMachine",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": namespace,
			"labels":    map[string]interface{}{appLabel: appLabelValue, runLabel: r.runID},
		},
		"spec": map[string]interface{}{
			"running": false,
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{"labels": map[string]interface{}{"kubevirt.io/vm": name, appLabel: appLabelValue, runLabel: r.runID}},
				"spec": map[string]interface{}{
					"terminationGracePeriodSeconds": int64(0),
					"domain": map[string]interface{}{
						"resources": map[string]interface{}{"requests": map[string]interface{}{"memory": r.opts.memory}},
						"devices": map[string]interface{}{
							"disks":    baseDisks(),
							"consoles": []interface{}{map[string]interface{}{"name": "serial0", "type": "serial"}},
						},
					},
					"volumes": baseVolumes(r.opts.vmImage, userData),
				},
			},
		},
	}}
	if _, err := r.client.Resource(virtualMachinesGVR).Namespace(namespace).Create(ctx, vm, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create VirtualMachine %s/%s: %w", namespace, name, err)
	}
	return nil
}

func baseDisks() []interface{} {
	return []interface{}{
		map[string]interface{}{"name": "rootdisk", "disk": map[string]interface{}{"bus": "virtio"}},
		map[string]interface{}{"name": "cloudinitdisk", "disk": map[string]interface{}{"bus": "virtio"}},
	}
}

func baseVolumes(image, userData string) []interface{} {
	return []interface{}{
		map[string]interface{}{"name": "rootdisk", "containerDisk": map[string]interface{}{"image": image}},
		map[string]interface{}{"name": "cloudinitdisk", "cloudInitNoCloud": map[string]interface{}{"userData": userData}},
	}
}

func cloudInitUserData(role, marker, diskSerial string) string {
	var script string
	if role == "write" {
		script = fmt.Sprintf(`#!/bin/bash
set -euo pipefail
exec > /dev/ttyS0 2>&1
trap 'echo MOVE_DV_A_ERROR' ERR
sleep 8
disk=/dev/disk/by-id/virtio-%s
for i in $(seq 1 120); do
  [ -b "$disk" ] && break
  sleep 1
done
if [ ! -b "$disk" ]; then echo MOVE_DV_A_ERROR; exit 1; fi
mkfs.ext4 -F "$disk"
mkdir -p /mnt/move-dv
mount "$disk" /mnt/move-dv
printf '%%s\n' '%s' > /mnt/move-dv/marker.txt
sync
echo 'MOVE_DV_A_OK:%s'
`, diskSerial, marker, marker)
	} else {
		script = fmt.Sprintf(`#!/bin/bash
set -euo pipefail
exec > /dev/ttyS0 2>&1
trap 'echo MOVE_DV_B_ERROR' ERR
sleep 8
disk=/dev/disk/by-id/virtio-%s
for i in $(seq 1 120); do
  [ -b "$disk" ] && break
  sleep 1
done
if [ ! -b "$disk" ]; then echo MOVE_DV_B_ERROR; exit 1; fi
mkdir -p /mnt/move-dv
mount -o ro "$disk" /mnt/move-dv
actual=$(cat /mnt/move-dv/marker.txt)
if [ "$actual" = '%s' ]; then
  echo 'MOVE_DV_B_OK:%s'
else
  echo MOVE_DV_B_ERROR
  exit 1
fi
`, diskSerial, marker, marker)
	}

	var indented strings.Builder
	for _, line := range strings.Split(strings.TrimSuffix(script, "\n"), "\n") {
		indented.WriteString("      ")
		indented.WriteString(line)
		indented.WriteByte('\n')
	}
	return "#cloud-config\nwrite_files:\n  - path: /usr/local/sbin/move-dv-poc.sh\n    owner: root:root\n    permissions: '0755'\n    content: |\n" + indented.String() + "runcmd:\n  - [\"/usr/local/sbin/move-dv-poc.sh\"]\n"
}

func (r *runner) dataDiskSerial() string {
	return r.runID + "data"
}

func (r *runner) attachDataVolume(ctx context.Context, namespace, vmName string) error {
	dataDisk := map[string]interface{}{
		"name":   "testdata",
		"serial": r.dataDiskSerial(),
		"disk":   map[string]interface{}{"bus": "virtio"},
	}
	disks := append(baseDisks(), dataDisk)
	volumes := append(baseVolumes(r.opts.vmImage, cloudInitUserData("write", r.marker, r.dataDiskSerial())), map[string]interface{}{
		"name":       "testdata",
		"dataVolume": map[string]interface{}{"name": r.dvName},
	})
	if vmName == r.vmB {
		volumes[len(volumes)-2] = baseVolumes(r.opts.vmImage, cloudInitUserData("verify", r.marker, r.dataDiskSerial()))[1]
	}
	return r.patchVMTemplate(ctx, namespace, vmName, disks, volumes)
}

func (r *runner) detachDataVolume(ctx context.Context, namespace, vmName string) error {
	return r.patchVMTemplate(ctx, namespace, vmName, baseDisks(), baseVolumes(r.opts.vmImage, cloudInitUserData(map[bool]string{true: "verify", false: "write"}[vmName == r.vmB], r.marker, r.dataDiskSerial())))
}

func (r *runner) patchVMTemplate(ctx context.Context, namespace, vmName string, disks, volumes []interface{}) error {
	patch := map[string]interface{}{"spec": map[string]interface{}{"template": map[string]interface{}{"spec": map[string]interface{}{
		"domain":  map[string]interface{}{"devices": map[string]interface{}{"disks": disks}},
		"volumes": volumes,
	}}}}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	if _, err := r.client.Resource(virtualMachinesGVR).Namespace(namespace).Patch(ctx, vmName, types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("update VM %s/%s disk attachment: %w", namespace, vmName, err)
	}
	return nil
}

func (r *runner) setVMRunning(ctx context.Context, namespace, name string, running bool) error {
	data, _ := json.Marshal(map[string]interface{}{"spec": map[string]interface{}{"running": running}})
	if _, err := r.client.Resource(virtualMachinesGVR).Namespace(namespace).Patch(ctx, name, types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("set VM %s/%s running=%t: %w", namespace, name, running, err)
	}
	return nil
}

func (r *runner) runVMAndWaitForSignal(ctx context.Context, namespace, vmName, signal string) error {
	if err := r.setVMRunning(ctx, namespace, vmName, true); err != nil {
		return err
	}
	if err := r.waitForVMI(ctx, namespace, vmName, true); err != nil {
		return err
	}
	return r.waitForConsoleSignal(ctx, namespace, vmName, signal)
}

func (r *runner) waitForVMI(ctx context.Context, namespace, vmName string, running bool) error {
	return poll(ctx, 2*time.Second, func() (bool, error) {
		vmi, err := r.client.Resource(virtualMachineInstancesGVR).Namespace(namespace).Get(ctx, vmName, metav1.GetOptions{})
		if running {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			phase, _, _ := unstructured.NestedString(vmi.Object, "status", "phase")
			if phase == "Failed" || phase == "Succeeded" {
				return false, fmt.Errorf("VMI %s/%s entered phase %s", namespace, vmName, phase)
			}
			return phase == "Running", nil
		}
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})
}

func (r *runner) waitForConsoleSignal(ctx context.Context, namespace, vmName, signal string) error {
	conn, err := openSerialConsole(ctx, r.config, namespace, vmName)
	if err != nil {
		return fmt.Errorf("open serial console for %s/%s: %w", namespace, vmName, err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(deadline)
	}
	reader := bufio.NewReader(conn)
	var output strings.Builder
	for {
		line, readErr := reader.ReadString('\n')
		output.WriteString(line)
		if strings.Contains(line, signal+r.marker) {
			return nil
		}
		if strings.Contains(line, "MOVE_DV_A_ERROR") || strings.Contains(line, "MOVE_DV_B_ERROR") {
			return fmt.Errorf("guest reported failure; console output: %s", tail(output.String(), 2000))
		}
		if readErr != nil {
			return fmt.Errorf("read serial console; expected %q; console output: %s: %w", signal+r.marker, tail(output.String(), 2000), readErr)
		}
	}
}

func (r *runner) stopAndDetach(ctx context.Context, namespace, vmName string) error {
	if err := r.setVMRunning(ctx, namespace, vmName, false); err != nil {
		return err
	}
	if err := r.waitForVMI(ctx, namespace, vmName, false); err != nil {
		return fmt.Errorf("wait for VM %s/%s to stop: %w", namespace, vmName, err)
	}
	if err := r.detachDataVolume(ctx, namespace, vmName); err != nil {
		return err
	}
	log.Printf("VM %s/%s stopped and DataVolume detached", namespace, vmName)
	return nil
}

func (r *runner) restoreReclaimPolicy(ctx context.Context) error {
	if r.pvName == "" || r.originalReclaimPolicy == "" {
		return nil
	}
	patch, _ := json.Marshal(map[string]interface{}{"spec": map[string]interface{}{"persistentVolumeReclaimPolicy": r.originalReclaimPolicy}})
	if _, err := r.client.Resource(pvsGVR).Patch(ctx, r.pvName, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("restore reclaim policy %s on PV %s: %w", r.originalReclaimPolicy, r.pvName, err)
	}
	return nil
}

func (r *runner) cleanup(ctx context.Context) error {
	for _, item := range []struct{ namespace, name string }{{r.opts.subnetA, r.vmA}, {r.opts.subnetB, r.vmB}} {
		if err := r.setVMRunning(ctx, item.namespace, item.name, false); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		_ = r.waitForVMI(ctx, item.namespace, item.name, false)
		if err := r.client.Resource(virtualMachinesGVR).Namespace(item.namespace).Delete(ctx, item.name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete VM %s/%s: %w", item.namespace, item.name, err)
		}
	}
	if err := r.client.Resource(dataVolumesGVR).Namespace(r.opts.tenant).Delete(ctx, r.dvName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete tenant DataVolume %s/%s: %w", r.opts.tenant, r.dvName, err)
	}
	for _, name := range r.transferNames {
		if err := r.client.Resource(objectTransfersGVR).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete ObjectTransfer %s: %w", name, err)
		}
	}
	return nil
}

func (r *runner) returnVolumeToTenant(ctx context.Context) error {
	if r.runID == "" {
		return nil
	}
	var problems []string
	for _, item := range []struct{ namespace, name string }{{r.opts.subnetA, r.vmA}, {r.opts.subnetB, r.vmB}} {
		if item.namespace == "" || item.name == "" {
			continue
		}
		if err := r.setVMRunning(ctx, item.namespace, item.name, false); err != nil && !apierrors.IsNotFound(err) {
			problems = append(problems, err.Error())
			continue
		}
		if err := r.waitForVMI(ctx, item.namespace, item.name, false); err != nil {
			problems = append(problems, err.Error())
			continue
		}
		if err := r.detachDataVolume(ctx, item.namespace, item.name); err != nil && !apierrors.IsNotFound(err) {
			problems = append(problems, err.Error())
		}
	}

	locations := []string{r.opts.tenant, r.opts.subnetA, r.opts.subnetB}
	current := ""
	for _, namespace := range locations {
		if namespace == "" {
			continue
		}
		if _, err := r.client.Resource(dataVolumesGVR).Namespace(namespace).Get(ctx, r.dvName, metav1.GetOptions{}); err == nil {
			current = namespace
			if namespace == r.opts.tenant {
				break
			}
		} else if !apierrors.IsNotFound(err) {
			problems = append(problems, err.Error())
		}
	}
	if current != "" && current != r.opts.tenant {
		if err := r.transfer(ctx, current, r.opts.tenant, "recovery-to-tenant"); err != nil {
			problems = append(problems, err.Error())
		} else if r.pvName != "" {
			if err := r.assertPVClaim(ctx, r.opts.tenant); err != nil {
				problems = append(problems, err.Error())
			}
		}
	}
	if err := r.restoreReclaimPolicy(ctx); err != nil {
		problems = append(problems, err.Error())
	}
	if len(problems) > 0 {
		return fmt.Errorf("could not fully restore the test resources: %s", strings.Join(problems, "; "))
	}
	return nil
}

func poll(ctx context.Context, interval time.Duration, check func() (bool, error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		done, err := check()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func randomHex(bytes int) (string, error) {
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func tail(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[len(value)-max:]
}
