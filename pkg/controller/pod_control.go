// Copyright 2018 infinivision, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/label"
	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
)

// PodControlInterface manages Pods used in HyenaCluster
type PodControlInterface interface {
	// TODO change this to UpdatePod
	UpdateMetaInfo(*v1alpha1.HyenaCluster, *corev1.Pod) (*corev1.Pod, error)
	DeletePod(*v1alpha1.HyenaCluster, *corev1.Pod) error
	UpdatePod(*v1alpha1.HyenaCluster, *corev1.Pod) (*corev1.Pod, error)
}

type realPodControl struct {
	kubeCli        kubernetes.Interface
	prophetControl ProphetControlInterface
	podLister      corelisters.PodLister
	recorder       record.EventRecorder
}

// NewRealPodControl creates a new PodControlInterface
func NewRealPodControl(
	kubeCli kubernetes.Interface,
	prophetControl ProphetControlInterface,
	podLister corelisters.PodLister,
	recorder record.EventRecorder,
) PodControlInterface {
	return &realPodControl{
		kubeCli:        kubeCli,
		prophetControl: prophetControl,
		podLister:      podLister,
		recorder:       recorder,
	}
}

func (rpc *realPodControl) UpdatePod(hc *v1alpha1.HyenaCluster, pod *corev1.Pod) (*corev1.Pod, error) {
	ns := hc.GetNamespace()
	hcName := hc.GetName()
	podName := pod.GetName()

	labels := pod.GetLabels()
	ann := pod.GetAnnotations()

	var updatePod *corev1.Pod
	// don't wait due to limited number of clients, but backoff after the default number of steps
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var updateErr error
		updatePod, updateErr = rpc.kubeCli.CoreV1().Pods(ns).Update(pod)
		if updateErr == nil {
			glog.Infof("Pod: [%s/%s] updated successfully, HyenaCluster: [%s/%s]", ns, podName, ns, hcName)
			return nil
		}
		glog.Errorf("failed to update Pod: [%s/%s], error: %v", ns, podName, updateErr)

		if updated, err := rpc.podLister.Pods(ns).Get(podName); err == nil {
			// make a copy so we don't mutate the shared cache
			pod = updated.DeepCopy()
			pod.Labels = labels
			pod.Annotations = ann
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated Pod %s/%s from lister: %v", ns, podName, err))
		}

		return updateErr
	})
	rpc.recordPodEvent("update", hc, podName, err)
	return updatePod, err
}

func (rpc *realPodControl) UpdateMetaInfo(hc *v1alpha1.HyenaCluster, pod *corev1.Pod) (*corev1.Pod, error) {
	ns := pod.GetNamespace()
	podName := pod.GetName()
	labels := pod.GetLabels()
	hcName := hc.GetName()
	if labels == nil {
		return pod, fmt.Errorf("pod %s/%s has empty labels, HyenaCluster: %s", ns, podName, hcName)
	}
	_, ok := labels[label.InstanceLabelKey]
	if !ok {
		return pod, fmt.Errorf("pod %s/%s doesn't have %s label, HyenaCluster: %s", ns, podName, label.InstanceLabelKey, hcName)
	}
	clusterID := labels[label.ClusterIDLabelKey]
	memberID := labels[label.MemberIDLabelKey]
	storeID := labels[label.StoreIDLabelKey]
	prophetClient := rpc.prophetControl.GetProphetClient(hc)
	if labels[label.ClusterIDLabelKey] == "" {
		cluster, err := prophetClient.GetCluster()
		if err != nil {
			return pod, fmt.Errorf("failed to get hyena cluster info from prophet, HyenaCluster: %s/%s, err: %v", ns, hcName, err)
		}
		clusterID = cluster
	}

	component := labels[label.ComponentLabelKey]
	switch component {
	case label.ProphetLabelVal:
		if labels[label.MemberIDLabelKey] == "" {
			// get member id
			members, err := prophetClient.GetMembers()
			if err != nil {
				return pod, fmt.Errorf("failed to get prophet members info from prophet, HyenaCluster: %s/%s, err: %v", ns, hcName, err)
			}
			for _, member := range members.Members {
				if member.Name == podName {
					memberID = strconv.FormatUint(member.MemberId, 10)
					break
				}
			}
		}
	case label.StoreLabelVal:
		if labels[label.StoreIDLabelKey] == "" {
			// get store id
			// storesInfo, err := prophetClient.GetStores()
			// if err != nil {
			// 	return pod, fmt.Errorf("failed to get stores info from prophet, HyenaCluster: %s/%s, err: %v", ns, hcName, err)
			// }
			// for _, store := range storesInfo.Stores {
			// 	addr := store.Meta.Address
			// 	if strings.Split(addr, ":")[0] == podName {
			// 		storeID = strconv.FormatUint(store.Meta.ID, 10)
			// 		break
			// 	}
			// }
		}
	}
	if labels[label.ClusterIDLabelKey] == clusterID &&
		labels[label.MemberIDLabelKey] == memberID &&
		labels[label.StoreIDLabelKey] == storeID {
		glog.V(4).Infof("pod %s/%s already has cluster labels set, skipping. HyenaCluster: %s", ns, podName, hcName)
		return pod, nil
	}
	// labels is a pointer, modify labels will modify pod.Labels
	setIfNotEmpty(labels, label.ClusterIDLabelKey, clusterID)
	setIfNotEmpty(labels, label.MemberIDLabelKey, memberID)
	setIfNotEmpty(labels, label.StoreIDLabelKey, storeID)

	var updatePod *corev1.Pod
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var updateErr error
		updatePod, updateErr = rpc.kubeCli.CoreV1().Pods(ns).Update(pod)
		if updateErr == nil {
			glog.V(4).Infof("update pod %s/%s with cluster labels %v successfully, HyenaCluster: %s", ns, podName, labels, hcName)
			return nil
		}
		glog.Errorf("failed to update pod %s/%s with cluster labels %v, HyenaCluster: %s, err: %v", ns, podName, labels, hcName, updateErr)

		if updated, err := rpc.podLister.Pods(ns).Get(podName); err == nil {
			// make a copy so we don't mutate the shared cache
			pod = updated.DeepCopy()
			pod.Labels = labels
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated Pod %s/%s from lister: %v", ns, podName, err))
		}
		return updateErr
	})

	rpc.recordPodEvent("update", hc, podName, err)
	return updatePod, err
}

func (rpc *realPodControl) DeletePod(hc *v1alpha1.HyenaCluster, pod *corev1.Pod) error {
	ns := hc.GetNamespace()
	hcName := hc.GetName()
	podName := pod.GetName()
	err := rpc.kubeCli.CoreV1().Pods(ns).Delete(podName, nil)
	if err != nil {
		glog.Errorf("failed to delete Pod: [%s/%s], HyenaCluster: %s, %v", ns, podName, hcName, err)
	} else {
		glog.V(4).Infof("delete Pod: [%s/%s] successfully, HyenaCluster: %s", ns, podName, hcName)
	}
	rpc.recordPodEvent("delete", hc, podName, err)
	return err
}

func (rpc *realPodControl) recordPodEvent(verb string, hc *v1alpha1.HyenaCluster, podName string, err error) {
	hcName := hc.GetName()
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		msg := fmt.Sprintf("%s Pod %s in HyenaCluster %s successful",
			strings.ToLower(verb), podName, hcName)
		rpc.recorder.Event(hc, corev1.EventTypeNormal, reason, msg)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		msg := fmt.Sprintf("%s Pod %s in HyenaCluster %s failed error: %s",
			strings.ToLower(verb), podName, hcName, err)
		rpc.recorder.Event(hc, corev1.EventTypeWarning, reason, msg)
	}
}

var _ PodControlInterface = &realPodControl{}

var (
	TestStoreID       string = "000"
	TestMemberID      string = "111"
	TestClusterID     string = "222"
	TestName          string = "hyena-cluster"
	TestComponentName string = "store"
	TestPodName       string = "pod-1"
	TestManagedByName string = "hyena-operator"
	TestClusterName   string = "test"
)

// FakePodControl is a fake PodControlInterface
type FakePodControl struct {
	PodIndexer        cache.Indexer
	updatePodTracker  requestTracker
	deletePodTracker  requestTracker
	getClusterTracker requestTracker
	getMemberTracker  requestTracker
	getStoreTracker   requestTracker
}

// NewFakePodControl returns a FakePodControl
func NewFakePodControl(podInformer coreinformers.PodInformer) *FakePodControl {
	return &FakePodControl{
		podInformer.Informer().GetIndexer(),
		requestTracker{0, nil, 0},
		requestTracker{0, nil, 0},
		requestTracker{0, nil, 0},
		requestTracker{0, nil, 0},
		requestTracker{0, nil, 0},
	}
}

// SetUpdatePodError sets the error attributes of updatePodTracker
func (fpc *FakePodControl) SetUpdatePodError(err error, after int) {
	fpc.updatePodTracker.err = err
	fpc.updatePodTracker.after = after
}

// SetDeletePodError sets the error attributes of deletePodTracker
func (fpc *FakePodControl) SetDeletePodError(err error, after int) {
	fpc.deletePodTracker.err = err
	fpc.deletePodTracker.after = after
}

// SetGetClusterError sets the error attributes of getClusterTracker
func (fpc *FakePodControl) SetGetClusterError(err error, after int) {
	fpc.getClusterTracker.err = err
	fpc.getClusterTracker.after = after
}

// SetGetMemberError sets the error attributes of getMemberTracker
func (fpc *FakePodControl) SetGetMemberError(err error, after int) {
	fpc.getMemberTracker.err = err
	fpc.getMemberTracker.after = after
}

// SetGetStoreError sets the error attributes of getStoreTracker
func (fpc *FakePodControl) SetGetStoreError(err error, after int) {
	fpc.getStoreTracker.err = err
	fpc.getStoreTracker.after = after
}

// UpdateMetaInfo update the meta info of Pod
func (fpc *FakePodControl) UpdateMetaInfo(_ *v1alpha1.HyenaCluster, pod *corev1.Pod) (*corev1.Pod, error) {
	defer fpc.updatePodTracker.inc()
	if fpc.updatePodTracker.errorReady() {
		defer fpc.updatePodTracker.reset()
		return nil, fpc.updatePodTracker.err
	}

	defer fpc.getClusterTracker.inc()
	if fpc.getClusterTracker.errorReady() {
		defer fpc.getClusterTracker.reset()
		return nil, fpc.getClusterTracker.err
	}

	defer fpc.getMemberTracker.inc()
	if fpc.getMemberTracker.errorReady() {
		defer fpc.getMemberTracker.reset()
		return nil, fpc.getMemberTracker.err
	}

	defer fpc.getStoreTracker.inc()
	if fpc.getStoreTracker.errorReady() {
		defer fpc.getStoreTracker.reset()
		return nil, fpc.getStoreTracker.err
	}

	setIfNotEmpty(pod.Labels, label.NameLabelKey, TestName)
	setIfNotEmpty(pod.Labels, label.ComponentLabelKey, TestComponentName)
	setIfNotEmpty(pod.Labels, label.ManagedByLabelKey, TestManagedByName)
	setIfNotEmpty(pod.Labels, label.InstanceLabelKey, TestClusterName)
	setIfNotEmpty(pod.Labels, label.ClusterIDLabelKey, TestClusterID)
	setIfNotEmpty(pod.Labels, label.MemberIDLabelKey, TestMemberID)
	setIfNotEmpty(pod.Labels, label.StoreIDLabelKey, TestStoreID)
	return pod, fpc.PodIndexer.Update(pod)
}

func (fpc *FakePodControl) DeletePod(_ *v1alpha1.HyenaCluster, pod *corev1.Pod) error {
	defer fpc.deletePodTracker.inc()
	if fpc.deletePodTracker.errorReady() {
		defer fpc.deletePodTracker.reset()
		return fpc.deletePodTracker.err
	}

	return fpc.PodIndexer.Delete(pod)
}

func (fpc *FakePodControl) UpdatePod(_ *v1alpha1.HyenaCluster, pod *corev1.Pod) (*corev1.Pod, error) {
	defer fpc.updatePodTracker.inc()
	if fpc.updatePodTracker.errorReady() {
		defer fpc.updatePodTracker.reset()
		return nil, fpc.updatePodTracker.err
	}

	return pod, fpc.PodIndexer.Update(pod)
}

var _ PodControlInterface = &FakePodControl{}
