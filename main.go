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
	"net"
	"os"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
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
	cdisGVR                    = schema.GroupVersionResource{Group: "cdi.kubevirt.io", Version: "v1beta1", Resource: "cdis"}
	dataVolumesGVR             = schema.GroupVersionResource{Group: "cdi.kubevirt.io", Version: "v1beta1", Resource: "datavolumes"}
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
	guestTimeout time.Duration
	cleanup      bool
}

type runner struct {
	client             dynamic.Interface
	coreClient         kubernetes.Interface
	config             *rest.Config
	discovery          discovery.DiscoveryInterface
	opts               options
	runID              string
	dvName             string
	vmA                string
	vmB                string
	marker             string
	pvName             string
	pvcTemplate        *unstructured.Unstructured
	dvTemplate         *unstructured.Unstructured
	lastTransferSource string
	lastTransferTarget string
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
	flag.DurationVar(&opts.guestTimeout, "guest-timeout", 4*time.Minute, "maximum time for each VM to boot and report its guest marker")
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
	if opts.guestTimeout <= 0 {
		return errors.New("--guest-timeout must be positive")
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
	coreClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes core client: %w", err)
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
		client:     client,
		coreClient: coreClient,
		config:     config,
		discovery:  dc,
		opts:       opts,
		runID:      "mvdv-" + runID,
		marker:     "move-dv-" + marker,
	}
	r.dvName = r.runID + "-disk"
	r.vmA = r.runID + "-vm-a"
	r.vmB = r.runID + "-vm-b"

	if err := r.preflight(ctx); err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			log.Printf("operation failed (%v); starting best effort recovery before exit", retErr)
			recoveryCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			if recoveryErr := r.returnVolumeToTenant(recoveryCtx); recoveryErr != nil {
				log.Printf("recovery note: %v", recoveryErr)
			} else {
				log.Printf("recovery completed")
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
	if err := r.requireAPI(cdiAPIGroupVersion, "cdis"); err != nil {
		return fmt.Errorf("CDI configuration API is required to check PVC adoption support: %w", err)
	}
	if err := r.requireClaimAdoption(ctx); err != nil {
		return err
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

func (r *runner) requireClaimAdoption(ctx context.Context) error {
	items, err := r.client.Resource(cdisGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("check CDI DataVolumeClaimAdoption feature gate (requires permission to list CDI resources): %w", err)
	}
	for _, cdi := range items.Items {
		gates, _, _ := unstructured.NestedStringSlice(cdi.Object, "spec", "config", "featureGates")
		for _, gate := range gates {
			if gate == "DataVolumeClaimAdoption" {
				return nil
			}
		}
	}
	return errors.New("CDI DataVolumeClaimAdoption feature gate must be enabled in spec.config.featureGates to safely adopt the transferred PVC")
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
	pvName, err := r.boundPV(ctx, r.opts.tenant)
	if err != nil {
		return err
	}
	r.pvName = pvName
	log.Printf("DataVolume is bound to PV %s; it will be retained during each namespace move", r.pvName)

	if err := r.transfer(ctx, r.opts.tenant, r.opts.subnetA, "tenant-to-a"); err != nil {
		return err
	}
	if err := r.assertPVClaim(ctx, r.opts.subnetA); err != nil {
		return err
	}
	if err := r.attachDataVolume(ctx, r.opts.subnetA, r.vmA); err != nil {
		return err
	}
	log.Printf("starting VM A %s/%s and waiting for guest marker %q", r.opts.subnetA, r.vmA, "MOVE_DV_A_OK:"+r.marker)
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
	log.Printf("starting VM B %s/%s and waiting for guest marker %q", r.opts.subnetB, r.vmB, "MOVE_DV_B_OK:"+r.marker)
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
	if r.opts.cleanup {
		if err := r.cleanup(ctx); err != nil {
			return err
		}
		log.Printf("verification succeeded; test VMs and DataVolume were cleaned up; PV %s remains protected with reclaim policy Retain", r.pvName)
		return nil
	}

	log.Printf("verification succeeded: marker %q was written by VM A and read by VM B", r.marker)
	log.Printf("DataVolume returned to tenant namespace %s and is detached", r.opts.tenant)
	log.Printf("resources retained: VMs %s/%s and %s/%s; DataVolume %s/%s; PV %s (reclaim policy Retain)", r.opts.subnetA, r.vmA, r.opts.subnetB, r.vmB, r.opts.tenant, r.dvName, r.pvName)
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
	label := fmt.Sprintf("DataVolume %s/%s to become ready", namespace, r.dvName)
	return pollWithProgress(ctx, 2*time.Second, label, func() (bool, string, error) {
		dv, err := r.client.Resource(dataVolumesGVR).Namespace(namespace).Get(ctx, r.dvName, metav1.GetOptions{})
		if err != nil {
			return false, "DataVolume not available", err
		}
		phase, _, _ := unstructured.NestedString(dv.Object, "status", "phase")
		if strings.EqualFold(phase, "Failed") {
			return false, statusSummary(dv), fmt.Errorf("DataVolume %s/%s failed: %s", namespace, r.dvName, statusSummary(dv))
		}
		claim, _, _ := unstructured.NestedString(dv.Object, "status", "claimName")
		if claim == "" {
			claim = r.dvName
		}
		pvc, err := r.client.Resource(pvcsGVR).Namespace(namespace).Get(ctx, claim, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, fmt.Sprintf("phase=%q, PVC %s not created", phase, claim), nil
			}
			return false, fmt.Sprintf("phase=%q, PVC %s status unavailable", phase, claim), err
		}
		pvcPhase, _, _ := unstructured.NestedString(pvc.Object, "status", "phase")
		if pvcPhase != "Bound" {
			return false, fmt.Sprintf("%s, PVC %s phase=%q", statusSummary(dv), claim, pvcPhase), nil
		}
		if strings.EqualFold(phase, "Succeeded") || hasReadyCondition(dv) {
			return true, fmt.Sprintf("%s, PVC %s Bound", statusSummary(dv), claim), nil
		}
		return false, fmt.Sprintf("%s, PVC %s Bound", statusSummary(dv), claim), nil
	})
}

func statusSummary(obj *unstructured.Unstructured) string {
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	reason, _, _ := unstructured.NestedString(obj.Object, "status", "reason")
	message, _, _ := unstructured.NestedString(obj.Object, "status", "message")
	parts := make([]string, 0, 3)
	if phase != "" {
		parts = append(parts, "phase="+phase)
	}
	if reason != "" {
		parts = append(parts, "reason="+reason)
	}
	if message != "" {
		parts = append(parts, "message="+message)
	}
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, raw := range conditions {
		condition, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		typ, _, _ := unstructured.NestedString(condition, "type")
		state, _, _ := unstructured.NestedString(condition, "status")
		reason, _, _ := unstructured.NestedString(condition, "reason")
		message, _, _ := unstructured.NestedString(condition, "message")
		conditionText := fmt.Sprintf("condition=%s:%s", typ, state)
		if reason != "" {
			conditionText += " reason=" + reason
		}
		if message != "" {
			conditionText += " message=" + message
		}
		parts = append(parts, conditionText)
	}
	if len(parts) == 0 {
		return "status pending"
	}
	return strings.Join(parts, "; ")
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

func (r *runner) boundPV(ctx context.Context, namespace string) (string, error) {
	dv, err := r.client.Resource(dataVolumesGVR).Namespace(namespace).Get(ctx, r.dvName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	claim, _, _ := unstructured.NestedString(dv.Object, "status", "claimName")
	if claim == "" {
		claim = r.dvName
	}
	if claim != r.dvName {
		return "", fmt.Errorf("DataVolume %s/%s uses PVC %q; this PoC requires the PVC name to match the DataVolume name", namespace, r.dvName, claim)
	}
	pvc, err := r.client.Resource(pvcsGVR).Namespace(namespace).Get(ctx, claim, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get PVC %s/%s: %w", namespace, claim, err)
	}
	phase, _, _ := unstructured.NestedString(pvc.Object, "status", "phase")
	if phase != "Bound" {
		return "", fmt.Errorf("PVC %s/%s is not Bound (phase=%s)", namespace, claim, phase)
	}
	pvName, _, _ := unstructured.NestedString(pvc.Object, "spec", "volumeName")
	if pvName == "" {
		return "", fmt.Errorf("PVC %s/%s is bound without a PV name", namespace, claim)
	}
	pv, err := r.client.Resource(pvsGVR).Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get PV %s: %w", pvName, err)
	}
	claimRef, found, _ := unstructured.NestedMap(pv.Object, "spec", "claimRef")
	if !found || claimRef["namespace"] != namespace || claimRef["name"] != claim {
		return "", fmt.Errorf("PV %s is not claimed by PVC %s/%s", pvName, namespace, claim)
	}
	pvcUID, _, _ := unstructured.NestedString(pvc.Object, "metadata", "uid")
	claimUID, _ := claimRef["uid"].(string)
	if pvcUID != "" && claimUID != "" && pvcUID != claimUID {
		return "", fmt.Errorf("PV %s claimRef UID does not match PVC %s/%s", pvName, namespace, claim)
	}
	return pvName, nil
}

func (r *runner) transfer(ctx context.Context, sourceNS, targetNS, suffix string) error {
	r.lastTransferSource, r.lastTransferTarget = sourceNS, targetNS
	log.Printf("moving DataVolume %s from %s to %s by rebinding PV %s (%s)", r.dvName, sourceNS, targetNS, r.pvName, suffix)

	sourceDV, err := r.getDataVolume(ctx, sourceNS)
	if err != nil {
		return err
	}
	targetDV, err := r.getDataVolume(ctx, targetNS)
	if err != nil {
		return err
	}
	if sourceDV != nil && targetDV != nil {
		return fmt.Errorf("DataVolume %s exists in both %s and %s", r.dvName, sourceNS, targetNS)
	}
	if sourceDV == nil && targetDV != nil {
		if err := r.waitForDataVolume(ctx, targetNS); err != nil {
			return err
		}
		return r.assertPVClaim(ctx, targetNS)
	}

	if sourceDV != nil {
		sourcePVC, err := r.getDataVolumePVC(ctx, sourceNS, sourceDV)
		if err != nil {
			return err
		}
		pvName, _, _ := unstructured.NestedString(sourcePVC.Object, "spec", "volumeName")
		if r.pvName == "" {
			r.pvName = pvName
		}
		if pvName != r.pvName {
			return fmt.Errorf("source PVC %s/%s is bound to PV %s; expected PV %s", sourceNS, r.dvName, pvName, r.pvName)
		}
		if err := r.assertPVClaimReference(ctx, sourceNS, sourcePVC); err != nil {
			return err
		}
		r.pvcTemplate = sourcePVC.DeepCopy()
		r.dvTemplate = sourceDV.DeepCopy()
	}
	if r.pvcTemplate == nil || r.dvTemplate == nil || r.pvName == "" {
		return errors.New("cannot resume namespace move without the original DataVolume and PVC templates")
	}

	log.Printf("protecting PV %s with reclaim policy Retain", r.pvName)
	if err := r.protectPV(ctx); err != nil {
		return err
	}
	log.Printf("deleting source DataVolume and PVC %s/%s", sourceNS, r.dvName)
	if err := r.deleteSourceVolume(ctx, sourceNS); err != nil {
		return err
	}
	log.Printf("releasing PV %s claimRef from %s", r.pvName, sourceNS)
	if err := r.releasePVClaimRef(ctx, sourceNS, targetNS); err != nil {
		return err
	}
	log.Printf("creating target PVC %s/%s for existing PV %s", targetNS, r.dvName, r.pvName)
	if err := r.createTransferredPVC(ctx, targetNS); err != nil {
		return err
	}
	if err := r.waitForPVCBound(ctx, targetNS); err != nil {
		return err
	}
	if err := r.createTransferredDataVolume(ctx, targetNS); err != nil {
		return err
	}
	log.Printf("waiting for adopted DataVolume %s/%s", targetNS, r.dvName)
	if err := r.waitForDataVolume(ctx, targetNS); err != nil {
		return fmt.Errorf("wait for adopted DataVolume %s/%s: %w", targetNS, r.dvName, err)
	}
	return r.assertPVClaim(ctx, targetNS)
}

func (r *runner) getDataVolume(ctx context.Context, namespace string) (*unstructured.Unstructured, error) {
	dv, err := r.client.Resource(dataVolumesGVR).Namespace(namespace).Get(ctx, r.dvName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get DataVolume %s/%s: %w", namespace, r.dvName, err)
	}
	return dv, nil
}

func (r *runner) getDataVolumePVC(ctx context.Context, namespace string, dv *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	claim, _, _ := unstructured.NestedString(dv.Object, "status", "claimName")
	if claim == "" {
		claim = r.dvName
	}
	if claim != r.dvName {
		return nil, fmt.Errorf("DataVolume %s/%s uses PVC %q; this PoC requires matching DataVolume and PVC names", namespace, r.dvName, claim)
	}
	pvc, err := r.client.Resource(pvcsGVR).Namespace(namespace).Get(ctx, claim, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get source PVC %s/%s: %w", namespace, claim, err)
	}
	return pvc, nil
}

func (r *runner) assertPVClaimReference(ctx context.Context, namespace string, pvc *unstructured.Unstructured) error {
	pv, err := r.client.Resource(pvsGVR).Get(ctx, r.pvName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get PV %s: %w", r.pvName, err)
	}
	claimRef, found, _ := unstructured.NestedMap(pv.Object, "spec", "claimRef")
	if !found || claimRef["namespace"] != namespace || claimRef["name"] != pvc.GetName() {
		return fmt.Errorf("PV %s is not claimed by PVC %s/%s", r.pvName, namespace, pvc.GetName())
	}
	pvcUID := string(pvc.GetUID())
	claimUID, _ := claimRef["uid"].(string)
	if pvcUID != "" && claimUID != "" && pvcUID != claimUID {
		return fmt.Errorf("PV %s claimRef UID does not match PVC %s/%s", r.pvName, namespace, pvc.GetName())
	}
	return nil
}

func (r *runner) protectPV(ctx context.Context) error {
	pv, err := r.client.Resource(pvsGVR).Get(ctx, r.pvName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get PV %s before protecting it: %w", r.pvName, err)
	}
	policy, _, _ := unstructured.NestedString(pv.Object, "spec", "persistentVolumeReclaimPolicy")
	if policy == "Retain" {
		return nil
	}
	patch, _ := json.Marshal(map[string]interface{}{"spec": map[string]interface{}{"persistentVolumeReclaimPolicy": "Retain"}})
	if _, err := r.client.Resource(pvsGVR).Patch(ctx, r.pvName, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("set PV %s reclaim policy to Retain: %w", r.pvName, err)
	}
	return nil
}

func (r *runner) deleteSourceVolume(ctx context.Context, namespace string) error {
	if err := r.client.Resource(dataVolumesGVR).Namespace(namespace).Delete(ctx, r.dvName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete source DataVolume %s/%s: %w", namespace, r.dvName, err)
	}
	if err := pollWithProgress(ctx, 2*time.Second, "source DataVolume deletion", func() (bool, string, error) {
		_, err := r.client.Resource(dataVolumesGVR).Namespace(namespace).Get(ctx, r.dvName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, "deleted", nil
		}
		return false, "still present", err
	}); err != nil {
		return fmt.Errorf("wait for source DataVolume %s/%s deletion: %w", namespace, r.dvName, err)
	}
	if err := r.client.Resource(pvcsGVR).Namespace(namespace).Delete(ctx, r.dvName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete source PVC %s/%s: %w", namespace, r.dvName, err)
	}
	if err := pollWithProgress(ctx, 2*time.Second, "source PVC deletion", func() (bool, string, error) {
		_, err := r.client.Resource(pvcsGVR).Namespace(namespace).Get(ctx, r.dvName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, "deleted", nil
		}
		return false, "still present", err
	}); err != nil {
		return fmt.Errorf("wait for source PVC %s/%s deletion: %w", namespace, r.dvName, err)
	}
	return nil
}

func (r *runner) releasePVClaimRef(ctx context.Context, sourceNS, targetNS string) error {
	pv, err := r.client.Resource(pvsGVR).Get(ctx, r.pvName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get PV %s before releasing it: %w", r.pvName, err)
	}
	claimRef, found, _ := unstructured.NestedMap(pv.Object, "spec", "claimRef")
	claimNamespace, _ := claimRef["namespace"].(string)
	claimName, _ := claimRef["name"].(string)
	if claimNamespace == targetNS && claimName == r.dvName {
		return nil
	}
	if found && len(claimRef) > 0 && (claimNamespace != sourceNS || claimName != r.dvName) {
		return fmt.Errorf("refusing to clear PV %s claimRef for unexpected PVC %s/%s", r.pvName, claimNamespace, claimName)
	}
	if found && len(claimRef) > 0 {
		patch, _ := json.Marshal(map[string]interface{}{"spec": map[string]interface{}{"claimRef": nil}})
		if _, err := r.client.Resource(pvsGVR).Patch(ctx, r.pvName, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("clear claimRef from PV %s: %w", r.pvName, err)
		}
	}
	return pollWithProgress(ctx, 2*time.Second, fmt.Sprintf("PV %s to become Available", r.pvName), func() (bool, string, error) {
		pv, err := r.client.Resource(pvsGVR).Get(ctx, r.pvName, metav1.GetOptions{})
		if err != nil {
			return false, "PV status unavailable", err
		}
		claimRef, found, _ := unstructured.NestedMap(pv.Object, "spec", "claimRef")
		phase, _, _ := unstructured.NestedString(pv.Object, "status", "phase")
		claim := "claimRef cleared"
		if found && len(claimRef) > 0 {
			claim = fmt.Sprintf("claimRef=%s/%s", claimRef["namespace"], claimRef["name"])
		}
		state := fmt.Sprintf("phase=%s, %s", phase, claim)
		return (!found || len(claimRef) == 0) && phase == "Available", state, nil
	})
}

func (r *runner) createTransferredPVC(ctx context.Context, namespace string) error {
	pvc, err := r.client.Resource(pvcsGVR).Namespace(namespace).Get(ctx, r.dvName, metav1.GetOptions{})
	if err == nil {
		pvName, _, _ := unstructured.NestedString(pvc.Object, "spec", "volumeName")
		if pvName != r.pvName || pvc.GetLabels()[runLabel] != r.runID {
			return fmt.Errorf("target PVC %s/%s already exists and does not belong to this run on PV %s", namespace, r.dvName, r.pvName)
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("check target PVC %s/%s: %w", namespace, r.dvName, err)
	}

	pvc = r.pvcTemplate.DeepCopy()
	sanitizeForCreate(pvc)
	pvc.SetName(r.dvName)
	pvc.SetNamespace(namespace)
	pvcSpec, found, err := unstructured.NestedMap(pvc.Object, "spec")
	if err != nil || !found {
		return errors.New("source PVC template has no spec")
	}
	pvcSpec["volumeName"] = r.pvName
	removePopulationSources(pvcSpec)
	if err := unstructured.SetNestedMap(pvc.Object, pvcSpec, "spec"); err != nil {
		return fmt.Errorf("set target PVC PV name: %w", err)
	}
	annotations := pvc.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	delete(annotations, "cdi.kubevirt.io/createdForDataVolume")
	delete(annotations, "cdi.kubevirt.io/storage.ownerRef")
	delete(annotations, "cdi.kubevirt.io/storage.populator.kind")
	delete(annotations, "cdi.kubevirt.io/storage.populator.pvcPrime")
	delete(annotations, "pv.kubernetes.io/bind-completed")
	delete(annotations, "pv.kubernetes.io/bound-by-controller")
	delete(annotations, "volume.kubernetes.io/selected-node")
	annotations["cdi.kubevirt.io/allowClaimAdoption"] = "true"
	annotations["cdi.kubevirt.io/storage.populatedFor"] = r.dvName
	pvc.SetAnnotations(annotations)
	labels := pvc.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[appLabel], labels[runLabel] = appLabelValue, r.runID
	pvc.SetLabels(labels)
	if _, err := r.client.Resource(pvcsGVR).Namespace(namespace).Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create target PVC %s/%s for PV %s: %w", namespace, r.dvName, r.pvName, err)
	}
	return nil
}

func (r *runner) waitForPVCBound(ctx context.Context, namespace string) error {
	return pollWithProgress(ctx, 2*time.Second, fmt.Sprintf("PVC %s/%s to bind to PV %s", namespace, r.dvName, r.pvName), func() (bool, string, error) {
		pvc, err := r.client.Resource(pvcsGVR).Namespace(namespace).Get(ctx, r.dvName, metav1.GetOptions{})
		if err != nil {
			return false, "PVC status unavailable", err
		}
		phase, _, _ := unstructured.NestedString(pvc.Object, "status", "phase")
		pvName, _, _ := unstructured.NestedString(pvc.Object, "spec", "volumeName")
		if pvName != r.pvName {
			state := fmt.Sprintf("phase=%s, volumeName=%s", phase, pvName)
			return false, state, fmt.Errorf("PVC %s/%s references PV %s; expected %s", namespace, r.dvName, pvName, r.pvName)
		}
		return phase == "Bound", fmt.Sprintf("phase=%s, volumeName=%s", phase, pvName), nil
	})
}

func (r *runner) createTransferredDataVolume(ctx context.Context, namespace string) error {
	existing, err := r.getDataVolume(ctx, namespace)
	if err != nil {
		return err
	}
	if existing != nil {
		if existing.GetLabels()[runLabel] == r.runID {
			return nil
		}
		return fmt.Errorf("target DataVolume %s/%s already exists and does not belong to this run", namespace, r.dvName)
	}

	dv := r.dvTemplate.DeepCopy()
	sanitizeForCreate(dv)
	dv.SetName(r.dvName)
	dv.SetNamespace(namespace)
	annotations := dv.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations["cdi.kubevirt.io/allowClaimAdoption"] = "true"
	annotations["cdi.kubevirt.io/storage.prePopulated"] = r.dvName
	dv.SetAnnotations(annotations)
	labels := dv.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[appLabel], labels[runLabel] = appLabelValue, r.runID
	dv.SetLabels(labels)
	pvcSpec, found, err := unstructured.NestedMap(r.pvcTemplate.Object, "spec")
	if err != nil || !found {
		return errors.New("source PVC template has no spec")
	}
	pvcSpec["volumeName"] = r.pvName
	removePopulationSources(pvcSpec)
	dvSpec, found, err := unstructured.NestedMap(dv.Object, "spec")
	if err != nil || !found {
		return errors.New("source DataVolume template has no spec")
	}
	delete(dvSpec, "storage")
	delete(dvSpec, "sourceRef")
	dvSpec["pvc"] = pvcSpec
	if _, found := dvSpec["source"]; !found {
		dvSpec["source"] = map[string]interface{}{"blank": map[string]interface{}{}}
	}
	if err := unstructured.SetNestedMap(dv.Object, dvSpec, "spec"); err != nil {
		return fmt.Errorf("set target DataVolume PVC spec: %w", err)
	}
	if _, err := r.client.Resource(dataVolumesGVR).Namespace(namespace).Create(ctx, dv, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create target DataVolume %s/%s for existing PVC: %w", namespace, r.dvName, err)
	}
	return nil
}

func removePopulationSources(pvcSpec map[string]interface{}) {
	delete(pvcSpec, "dataSource")
	delete(pvcSpec, "dataSourceRef")
}

func sanitizeForCreate(obj *unstructured.Unstructured) {
	delete(obj.Object, "status")
	for _, field := range []string{"uid", "resourceVersion", "generation", "creationTimestamp", "deletionTimestamp", "deletionGracePeriodSeconds", "managedFields", "ownerReferences", "finalizers", "selfLink"} {
		unstructured.RemoveNestedField(obj.Object, "metadata", field)
	}
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
			"runStrategy": "Halted",
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{"labels": map[string]interface{}{"kubevirt.io/vm": name, appLabel: appLabelValue, runLabel: r.runID}},
				"spec": map[string]interface{}{
					"terminationGracePeriodSeconds": int64(0),
					"domain": map[string]interface{}{
						"resources": map[string]interface{}{"requests": map[string]interface{}{"memory": r.opts.memory}},
						"devices": map[string]interface{}{
							"disks":                   baseDisks(),
							"autoattachSerialConsole": true,
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
trap 'rc=$?; echo "MOVE_DV_A_ERROR: rc=$rc line=$LINENO command=$BASH_COMMAND"; exit "$rc"' ERR
echo MOVE_DV_A_STAGE:script-start
echo MOVE_DV_A_STAGE:waiting-for-disk path=/dev/disk/by-id/virtio-%s
disk=/dev/disk/by-id/virtio-%s
for i in $(seq 1 120); do
  [ -b "$disk" ] && break
  sleep 1
  if [ "$((i %% 10))" -eq 0 ]; then echo "MOVE_DV_A_STAGE:waiting-for-disk elapsed=${i}s path=$disk"; fi
done
if [ ! -b "$disk" ]; then echo "MOVE_DV_A_ERROR: disk-not-found path=$disk"; exit 1; fi
echo "MOVE_DV_A_STAGE:disk-found path=$disk"
echo MOVE_DV_A_STAGE:formatting-disk
mkfs.ext4 -F "$disk"
mkdir -p /mnt/move-dv
echo MOVE_DV_A_STAGE:mounting-disk
mount "$disk" /mnt/move-dv
echo MOVE_DV_A_STAGE:writing-marker
printf '%%s\n' '%s' > /mnt/move-dv/marker.txt
sync
echo MOVE_DV_A_STAGE:marker-written
for i in $(seq 1 120); do
  echo 'MOVE_DV_A_OK:%s'
  sleep 1
done
`, diskSerial, diskSerial, marker, marker)
	} else {
		script = fmt.Sprintf(`#!/bin/bash
set -euo pipefail
exec > /dev/ttyS0 2>&1
trap 'rc=$?; echo "MOVE_DV_B_ERROR: rc=$rc line=$LINENO command=$BASH_COMMAND"; exit "$rc"' ERR
echo MOVE_DV_B_STAGE:script-start
echo MOVE_DV_B_STAGE:waiting-for-disk path=/dev/disk/by-id/virtio-%s
disk=/dev/disk/by-id/virtio-%s
for i in $(seq 1 120); do
  [ -b "$disk" ] && break
  sleep 1
  if [ "$((i %% 10))" -eq 0 ]; then echo "MOVE_DV_B_STAGE:waiting-for-disk elapsed=${i}s path=$disk"; fi
done
if [ ! -b "$disk" ]; then echo "MOVE_DV_B_ERROR: disk-not-found path=$disk"; exit 1; fi
echo "MOVE_DV_B_STAGE:disk-found path=$disk"
mkdir -p /mnt/move-dv
echo MOVE_DV_B_STAGE:mounting-disk-read-only
mount -o ro "$disk" /mnt/move-dv
echo MOVE_DV_B_STAGE:reading-marker
actual=$(cat /mnt/move-dv/marker.txt)
if [ "$actual" = '%s' ]; then
  echo MOVE_DV_B_STAGE:marker-verified
  for i in $(seq 1 120); do
    echo 'MOVE_DV_B_OK:%s'
    sleep 1
  done
else
  echo MOVE_DV_B_ERROR: marker-mismatch
  exit 1
fi
`, diskSerial, diskSerial, marker, marker)
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

func (r *runner) setVMRunStrategy(ctx context.Context, namespace, name, runStrategy string) error {
	data, _ := json.Marshal(map[string]interface{}{"spec": map[string]interface{}{"runStrategy": runStrategy}})
	if _, err := r.client.Resource(virtualMachinesGVR).Namespace(namespace).Patch(ctx, name, types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("set VM %s/%s runStrategy=%s: %w", namespace, name, runStrategy, err)
	}
	return nil
}

func (r *runner) runVMAndWaitForSignal(ctx context.Context, namespace, vmName, signal string) error {
	guestCtx, cancel := context.WithTimeout(ctx, r.opts.guestTimeout)
	defer cancel()
	log.Printf("starting guest check for VM %s/%s with timeout %s", namespace, vmName, r.opts.guestTimeout)
	log.Printf("setting VM %s/%s runStrategy=Always", namespace, vmName)
	if err := r.setVMRunStrategy(guestCtx, namespace, vmName, "Always"); err != nil {
		if guestCtx.Err() != nil {
			r.captureVMDiagnostics(namespace, vmName)
		}
		return err
	}
	if err := r.waitForVMI(guestCtx, namespace, vmName, true); err != nil {
		if guestCtx.Err() != nil {
			r.captureVMDiagnostics(namespace, vmName)
		}
		return err
	}
	log.Printf("VMI %s/%s is running; opening serial console", namespace, vmName)
	return r.waitForConsoleSignal(guestCtx, namespace, vmName, signal)
}

func (r *runner) waitForVMI(ctx context.Context, namespace, vmName string, running bool) error {
	target := fmt.Sprintf("VMI %s/%s to be Running", namespace, vmName)
	if !running {
		target = fmt.Sprintf("VMI %s/%s to stop", namespace, vmName)
	}
	return pollWithProgress(ctx, 2*time.Second, target, func() (bool, string, error) {
		vmi, err := r.client.Resource(virtualMachineInstancesGVR).Namespace(namespace).Get(ctx, vmName, metav1.GetOptions{})
		if running {
			if apierrors.IsNotFound(err) {
				return false, "VMI not created yet", nil
			}
			if err != nil {
				return false, "VMI status unavailable", err
			}
			phase, _, _ := unstructured.NestedString(vmi.Object, "status", "phase")
			if phase == "Failed" || phase == "Succeeded" {
				return false, statusSummary(vmi), fmt.Errorf("VMI %s/%s entered phase %s (%s)", namespace, vmName, phase, statusSummary(vmi))
			}
			return phase == "Running", statusSummary(vmi), nil
		}
		if apierrors.IsNotFound(err) {
			return true, "VMI removed", nil
		}
		if err != nil {
			return false, "VMI status unavailable", err
		}
		return false, statusSummary(vmi), nil
	})
}

func (r *runner) waitForConsoleSignal(ctx context.Context, namespace, vmName, signal string) error {
	started := time.Now()
	log.Printf("opening serial console for %s/%s", namespace, vmName)
	conn, err := openSerialConsole(ctx, r.config, namespace, vmName)
	if err != nil {
		r.captureVMDiagnostics(namespace, vmName)
		return fmt.Errorf("open serial console for %s/%s: %w", namespace, vmName, err)
	}
	log.Printf("serial console connected to %s/%s; waiting for %q", namespace, vmName, signal+r.marker)
	var output strings.Builder
	for {
		if deadline, ok := ctx.Deadline(); ok {
			_ = conn.SetReadDeadline(deadline)
		}
		readCh := make(chan consoleReadResult, 32)
		readDone := make(chan struct{})
		go readConsoleLines(conn, readCh, readDone)
		progressTicker := time.NewTicker(10 * time.Second)
		var readErr error
		streamEnded := false
		for {
			select {
			case result := <-readCh:
				if result.line != "" {
					output.WriteString(result.line)
					if strings.Contains(result.line, signal+r.marker) {
						close(readDone)
						progressTicker.Stop()
						_ = conn.Close()
						log.Printf("received guest marker from %s/%s after %s (%d console bytes)", namespace, vmName, time.Since(started).Round(time.Second), output.Len())
						return nil
					}
					if strings.Contains(result.line, "MOVE_DV_A_ERROR") || strings.Contains(result.line, "MOVE_DV_B_ERROR") {
						close(readDone)
						progressTicker.Stop()
						_ = conn.Close()
						r.captureVMDiagnostics(namespace, vmName)
						return fmt.Errorf("guest reported failure; console output: %s", tail(output.String(), 2000))
					}
				}
				if result.err != nil {
					readErr = result.err
					streamEnded = true
				}
			case <-progressTicker.C:
				vmiState := r.currentVMIState(ctx, namespace, vmName)
				lastOutput := tail(strings.TrimSpace(output.String()), 240)
				if lastOutput == "" {
					lastOutput = "no serial output yet"
				}
				log.Printf("still waiting for guest marker from %s/%s after %s (console_bytes=%d, VMI=%s, last_output=%q)", namespace, vmName, time.Since(started).Round(time.Second), output.Len(), vmiState, lastOutput)
			case <-ctx.Done():
				close(readDone)
				progressTicker.Stop()
				_ = conn.Close()
				r.captureVMDiagnostics(namespace, vmName)
				return fmt.Errorf("read serial console; expected %q after %s; console bytes=%d; console output: %s: %w", signal+r.marker, time.Since(started).Round(time.Second), output.Len(), tail(output.String(), 2000), ctx.Err())
			}
			if streamEnded {
				break
			}
		}
		close(readDone)
		progressTicker.Stop()
		_ = conn.Close()
		log.Printf("serial console disconnected from %s/%s after %s (error=%v, console_bytes=%d); checking VMI and launcher state", namespace, vmName, time.Since(started).Round(time.Second), readErr, output.Len())
		r.captureVMDiagnostics(namespace, vmName)
		if ctx.Err() != nil {
			return fmt.Errorf("read serial console; expected %q after %s; console bytes=%d; console output: %s: %w", signal+r.marker, time.Since(started).Round(time.Second), output.Len(), tail(output.String(), 2000), ctx.Err())
		}
		if err := r.waitForVMI(ctx, namespace, vmName, true); err != nil {
			return fmt.Errorf("serial console disconnected and VMI %s/%s is not running; expected %q after %s; console bytes=%d; console output: %s; console read error: %v: %w", namespace, vmName, signal+r.marker, time.Since(started).Round(time.Second), output.Len(), tail(output.String(), 2000), readErr, err)
		}

		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			r.captureVMDiagnostics(namespace, vmName)
			return fmt.Errorf("read serial console; expected %q after %s; console bytes=%d; console output: %s: %w", signal+r.marker, time.Since(started).Round(time.Second), output.Len(), tail(output.String(), 2000), ctx.Err())
		case <-timer.C:
		}

		conn, err = openSerialConsole(ctx, r.config, namespace, vmName)
		if err != nil {
			r.captureVMDiagnostics(namespace, vmName)
			return fmt.Errorf("reopen serial console for %s/%s after disconnect; expected %q after %s; console bytes=%d; console output: %s; previous stream error: %v: %w", namespace, vmName, signal+r.marker, time.Since(started).Round(time.Second), output.Len(), tail(output.String(), 2000), readErr, err)
		}
		log.Printf("serial console reconnected to %s/%s after %s; continuing to wait for %q", namespace, vmName, time.Since(started).Round(time.Second), signal+r.marker)
	}
}

type consoleReadResult struct {
	line string
	err  error
}

func readConsoleLines(conn net.Conn, results chan<- consoleReadResult, done <-chan struct{}) {
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			select {
			case results <- consoleReadResult{line: line}:
			case <-done:
				return
			}
		}
		if err != nil {
			select {
			case results <- consoleReadResult{err: err}:
			case <-done:
			}
			return
		}
	}
}

func (r *runner) currentVMIState(ctx context.Context, namespace, vmName string) string {
	statusCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	vmi, err := r.client.Resource(virtualMachineInstancesGVR).Namespace(namespace).Get(statusCtx, vmName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "not found"
	}
	if err != nil {
		return "unavailable: " + err.Error()
	}
	return statusSummary(vmi)
}

func (r *runner) captureVMDiagnostics(namespace, vmName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	log.Printf("collecting best effort diagnostics for VM %s/%s before recovery", namespace, vmName)

	vmiUID := ""
	vmi, err := r.client.Resource(virtualMachineInstancesGVR).Namespace(namespace).Get(ctx, vmName, metav1.GetOptions{})
	if err != nil {
		log.Printf("diagnostic VMI lookup for %s/%s failed: %v", namespace, vmName, err)
	} else {
		vmiUID, _, _ = unstructured.NestedString(vmi.Object, "metadata", "uid")
		nodeName, _, _ := unstructured.NestedString(vmi.Object, "status", "nodeName")
		log.Printf("diagnostic VMI %s/%s: %s, node=%q", namespace, vmName, statusSummary(vmi), nodeName)
	}

	events, eventErr := r.coreClient.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if eventErr != nil {
		log.Printf("diagnostic namespace event lookup for %s failed (requires events list permission): %v", namespace, eventErr)
	}
	var eventItems []corev1.Event
	if events != nil {
		eventItems = events.Items
	}
	eventPodNames := launcherPodNamesFromEvents(eventItems, vmName)

	pods, err := r.coreClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: "kubevirt.io/domain=" + vmName})
	if err == nil && len(pods.Items) == 0 {
		log.Printf("diagnostic pod label lookup for %s/%s found none; searching namespace pods and VMI events", namespace, vmName)
		pods, err = r.coreClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	}
	podNames := make(map[string]struct{})
	if err != nil {
		log.Printf("diagnostic launcher pod lookup for %s/%s failed (requires pods list permission): %v", namespace, vmName, err)
	} else {
		for i := range pods.Items {
			pod := &pods.Items[i]
			if !isLauncherPodForVMI(pod, vmName, vmiUID, eventPodNames) {
				continue
			}
			podNames[pod.Name] = struct{}{}
			log.Printf("diagnostic pod %s/%s: phase=%s node=%q reason=%q message=%q", namespace, pod.Name, pod.Status.Phase, pod.Spec.NodeName, pod.Status.Reason, pod.Status.Message)
			for _, status := range pod.Status.ContainerStatuses {
				log.Printf("diagnostic pod %s/%s container %s: ready=%t restarts=%d state=%s", namespace, pod.Name, status.Name, status.Ready, status.RestartCount, containerStateSummary(status.State))
			}
			lines := int64(100)
			logs, logErr := r.coreClient.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: "compute", TailLines: &lines, Timestamps: true}).Do(ctx).Raw()
			if logErr != nil {
				log.Printf("diagnostic compute logs for %s/%s unavailable (requires pods/log get permission): %v", namespace, pod.Name, logErr)
			} else if len(logs) == 0 {
				log.Printf("diagnostic compute logs for %s/%s: no output", namespace, pod.Name)
			} else {
				log.Printf("diagnostic compute logs for %s/%s (tail, max 12000 bytes):\n%s", namespace, pod.Name, tail(string(logs), 12000))
			}
		}
	}
	if len(podNames) == 0 {
		log.Printf("diagnostic launcher pod lookup for %s/%s: no matching pods (event candidates=%v)", namespace, vmName, sortedNames(eventPodNames))
	}

	if eventErr != nil {
		return
	}
	relevant := make([]corev1.Event, 0, len(eventItems))
	for _, event := range eventItems {
		object := event.InvolvedObject
		if (vmiUID != "" && string(object.UID) == vmiUID) || object.Name == vmName {
			relevant = append(relevant, event)
			continue
		}
		if _, found := podNames[object.Name]; found {
			relevant = append(relevant, event)
		}
	}
	sort.SliceStable(relevant, func(i, j int) bool {
		return eventTimestamp(relevant[i]).After(eventTimestamp(relevant[j]))
	})
	if len(relevant) == 0 {
		log.Printf("diagnostic events for %s/%s: no VM, VMI, or launcher pod events found", namespace, vmName)
		return
	}
	if len(relevant) > 20 {
		relevant = relevant[:20]
	}
	for _, event := range relevant {
		log.Printf("diagnostic event %s object=%s/%s type=%s reason=%s count=%d message=%q", eventTimestamp(event).Format(time.RFC3339), event.InvolvedObject.Kind, event.InvolvedObject.Name, event.Type, event.Reason, event.Count, tail(event.Message, 1000))
	}
}

func launcherPodNamesFromEvents(events []corev1.Event, vmName string) map[string]struct{} {
	names := make(map[string]struct{})
	prefix := "virt-launcher-" + vmName + "-"
	for _, event := range events {
		if event.InvolvedObject.Kind == "Pod" && strings.HasPrefix(event.InvolvedObject.Name, prefix) {
			names[event.InvolvedObject.Name] = struct{}{}
		}
		for _, field := range strings.Fields(event.Message) {
			candidate := strings.Trim(field, "\"'.,:;()[]")
			if strings.HasPrefix(candidate, prefix) {
				names[candidate] = struct{}{}
			}
		}
	}
	return names
}

func isLauncherPodForVMI(pod *corev1.Pod, vmName, vmiUID string, eventPodNames map[string]struct{}) bool {
	if _, found := eventPodNames[pod.Name]; found {
		return true
	}
	if strings.HasPrefix(pod.Name, "virt-launcher-"+vmName+"-") || pod.Labels["kubevirt.io/domain"] == vmName {
		return true
	}
	if vmiUID != "" && pod.Labels["kubevirt.io/created-by"] == vmiUID {
		return true
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "VirtualMachineInstance" && (owner.Name == vmName || (vmiUID != "" && string(owner.UID) == vmiUID)) {
			return true
		}
	}
	return false
}

func sortedNames(names map[string]struct{}) []string {
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func containerStateSummary(state corev1.ContainerState) string {
	switch {
	case state.Running != nil:
		return "running since " + state.Running.StartedAt.Format(time.RFC3339)
	case state.Waiting != nil:
		return fmt.Sprintf("waiting reason=%q message=%q", state.Waiting.Reason, state.Waiting.Message)
	case state.Terminated != nil:
		return fmt.Sprintf("terminated reason=%q exitCode=%d message=%q", state.Terminated.Reason, state.Terminated.ExitCode, state.Terminated.Message)
	default:
		return "unknown"
	}
}

func eventTimestamp(event corev1.Event) time.Time {
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}
	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.Time
	}
	return event.CreationTimestamp.Time
}

func (r *runner) stopAndDetach(ctx context.Context, namespace, vmName string) error {
	if err := r.setVMRunStrategy(ctx, namespace, vmName, "Halted"); err != nil {
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

func (r *runner) cleanup(ctx context.Context) error {
	for _, item := range []struct{ namespace, name string }{{r.opts.subnetA, r.vmA}, {r.opts.subnetB, r.vmB}} {
		if err := r.setVMRunStrategy(ctx, item.namespace, item.name, "Halted"); err != nil && !apierrors.IsNotFound(err) {
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
		log.Printf("recovery: halting VM %s/%s before restoring the DataVolume", item.namespace, item.name)
		if err := r.setVMRunStrategy(ctx, item.namespace, item.name, "Halted"); err != nil && !apierrors.IsNotFound(err) {
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
			log.Printf("recovery: found DataVolume %s/%s", namespace, r.dvName)
			current = namespace
			if namespace == r.opts.tenant {
				break
			}
		} else if !apierrors.IsNotFound(err) {
			problems = append(problems, err.Error())
		}
	}
	if current != "" && current != r.opts.tenant {
		log.Printf("recovery: moving DataVolume from %s back to tenant namespace %s", current, r.opts.tenant)
		if err := r.transfer(ctx, current, r.opts.tenant, "recovery-to-tenant"); err != nil {
			problems = append(problems, err.Error())
		} else if r.pvName != "" {
			if err := r.assertPVClaim(ctx, r.opts.tenant); err != nil {
				problems = append(problems, err.Error())
			}
		}
	}
	if current == "" && r.lastTransferTarget != "" && r.pvName != "" && r.pvcTemplate != nil {
		log.Printf("recovery: resuming incomplete move %s -> %s", r.lastTransferSource, r.lastTransferTarget)
		if err := r.transfer(ctx, r.lastTransferSource, r.lastTransferTarget, "recovery-resume"); err != nil {
			problems = append(problems, err.Error())
		} else if r.lastTransferTarget != r.opts.tenant {
			if err := r.transfer(ctx, r.lastTransferTarget, r.opts.tenant, "recovery-to-tenant"); err != nil {
				problems = append(problems, err.Error())
			} else if err := r.assertPVClaim(ctx, r.opts.tenant); err != nil {
				problems = append(problems, err.Error())
			}
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("could not fully restore the test resources: %s", strings.Join(problems, "; "))
	}
	return nil
}

func pollWithProgress(ctx context.Context, interval time.Duration, target string, check func() (bool, string, error)) error {
	const progressInterval = 10 * time.Second
	started := time.Now()
	nextProgress := started.Add(progressInterval)
	var lastState string
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Printf("waiting for %s", target)
	for {
		done, state, err := check()
		if state != "" {
			lastState = state
		}
		if err != nil {
			return err
		}
		if done {
			log.Printf("completed wait for %s after %s (state: %s)", target, time.Since(started).Round(time.Second), fallbackState(lastState))
			return nil
		}
		if time.Now().After(nextProgress) {
			log.Printf("still waiting for %s after %s (state: %s)", target, time.Since(started).Round(time.Second), fallbackState(lastState))
			nextProgress = time.Now().Add(progressInterval)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %s after %s (last state: %s): %w", target, time.Since(started).Round(time.Second), fallbackState(lastState), ctx.Err())
		case <-ticker.C:
		}
	}
}

func fallbackState(state string) string {
	if state == "" {
		return "unknown"
	}
	return state
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
