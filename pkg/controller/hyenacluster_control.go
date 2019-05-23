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
	"strings"

	"github.com/golang/glog"
	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/client/clientset/versioned"
	cinformers "github.com/infinivision/hyena-operator/pkg/client/informers/externalversions/infinivision.com/v1alpha1"
	listers "github.com/infinivision/hyena-operator/pkg/client/listers/infinivision.com/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
)

// HyenaClusterControlInterface manages HyenaClusters
type HyenaClusterControlInterface interface {
	UpdateHyenaCluster(*v1alpha1.HyenaCluster, *v1alpha1.HyenaClusterStatus, *v1alpha1.HyenaClusterStatus) (*v1alpha1.HyenaCluster, error)
}

type realHyenaClusterControl struct {
	cli      versioned.Interface
	hcLister listers.HyenaClusterLister
	recorder record.EventRecorder
}

// NewRealHyenaClusterControl creates a new HyenaClusterControlInterface
func NewRealHyenaClusterControl(cli versioned.Interface,
	hcLister listers.HyenaClusterLister,
	recorder record.EventRecorder) HyenaClusterControlInterface {
	return &realHyenaClusterControl{
		cli,
		hcLister,
		recorder,
	}
}

func (rtc *realHyenaClusterControl) UpdateHyenaCluster(hc *v1alpha1.HyenaCluster, newStatus *v1alpha1.HyenaClusterStatus, oldStatus *v1alpha1.HyenaClusterStatus) (*v1alpha1.HyenaCluster, error) {
	ns := hc.GetNamespace()
	hcName := hc.GetName()

	status := hc.Status.DeepCopy()
	var updateHC *v1alpha1.HyenaCluster

	// don't wait due to limited number of clients, but backoff after the default number of steps
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var updateErr error
		updateHC, updateErr = rtc.cli.InfinivisionV1alpha1().HyenaClusters(ns).Update(hc)
		if updateErr == nil {
			glog.Infof("HyenaCluster: [%s/%s] updated successfully", ns, hcName)
			return nil
		}
		glog.Errorf("failed to update HyenaCluster: [%s/%s], error: %v", ns, hcName, updateErr)
		if updated, err := rtc.hcLister.HyenaClusters(ns).Get(hcName); err == nil {
			// make a copy so we don't mutate the shared cache
			hc = updated.DeepCopy()
			hc.Status = *status
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated HyenaCluster %s/%s from lister: %v", ns, hcName, err))
		}

		return updateErr
	})
	if !deepEqualExceptHeartbeatTime(newStatus.DeepCopy(), oldStatus.DeepCopy()) {
		rtc.recordHyenaClusterEvent("update", hc, err)
	}
	return updateHC, err
}

func (rtc *realHyenaClusterControl) recordHyenaClusterEvent(verb string, hc *v1alpha1.HyenaCluster, err error) {
	hcName := hc.GetName()
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		msg := fmt.Sprintf("%s HyenaCluster %s successful",
			strings.ToLower(verb), hcName)
		rtc.recorder.Event(hc, corev1.EventTypeNormal, reason, msg)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		msg := fmt.Sprintf("%s HyenaCluster %s failed error: %s",
			strings.ToLower(verb), hcName, err)
		rtc.recorder.Event(hc, corev1.EventTypeWarning, reason, msg)
	}
}

func deepEqualExceptHeartbeatTime(newStatus *v1alpha1.HyenaClusterStatus, oldStatus *v1alpha1.HyenaClusterStatus) bool {
	sweepHeartbeatTime(newStatus.Store.Stores)
	sweepHeartbeatTime(newStatus.Store.TombstoneStores)
	sweepHeartbeatTime(oldStatus.Store.Stores)
	sweepHeartbeatTime(oldStatus.Store.TombstoneStores)

	return apiequality.Semantic.DeepEqual(newStatus, oldStatus)
}

func sweepHeartbeatTime(stores map[string]v1alpha1.Store) {
	for id, store := range stores {
		store.LastHeartbeatTime = metav1.Time{}
		stores[id] = store
	}
}

// FakeHyenaClusterControl is a fake HyenaClusterControlInterface
type FakeHyenaClusterControl struct {
	HcLister                  listers.HyenaClusterLister
	HcIndexer                 cache.Indexer
	updateHyenaClusterTracker requestTracker
}

// NewFakeHyenaClusterControl returns a FakeHyenaClusterControl
func NewFakeHyenaClusterControl(hcInformer cinformers.HyenaClusterInformer) *FakeHyenaClusterControl {
	return &FakeHyenaClusterControl{
		hcInformer.Lister(),
		hcInformer.Informer().GetIndexer(),
		requestTracker{0, nil, 0},
	}
}

// SetUpdateHyenaClusterError sets the error attributes of updateHyenaClusterTracker
func (ssc *FakeHyenaClusterControl) SetUpdateHyenaClusterError(err error, after int) {
	ssc.updateHyenaClusterTracker.err = err
	ssc.updateHyenaClusterTracker.after = after
}

// UpdateHyenaCluster updates the HyenaCluster
func (ssc *FakeHyenaClusterControl) UpdateHyenaCluster(hc *v1alpha1.HyenaCluster, _ *v1alpha1.HyenaClusterStatus, _ *v1alpha1.HyenaClusterStatus) (*v1alpha1.HyenaCluster, error) {
	defer ssc.updateHyenaClusterTracker.inc()
	if ssc.updateHyenaClusterTracker.errorReady() {
		defer ssc.updateHyenaClusterTracker.reset()
		return hc, ssc.updateHyenaClusterTracker.err
	}

	return hc, ssc.HcIndexer.Update(hc)
}
