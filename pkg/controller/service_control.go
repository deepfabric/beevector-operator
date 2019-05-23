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
	hcinformers "github.com/infinivision/hyena-operator/pkg/client/informers/externalversions/infinivision.com/v1alpha1"
	v1listers "github.com/infinivision/hyena-operator/pkg/client/listers/infinivision.com/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
)

// ExternalTrafficPolicy denotes if this Service desires to route external traffic to node-local or cluster-wide endpoints.
var ExternalTrafficPolicy string

// ServiceControlInterface manages Services used in HyenaCluster
type ServiceControlInterface interface {
	CreateService(*v1alpha1.HyenaCluster, *corev1.Service) error
	UpdateService(*v1alpha1.HyenaCluster, *corev1.Service) (*corev1.Service, error)
	DeleteService(*v1alpha1.HyenaCluster, *corev1.Service) error
}

type realServiceControl struct {
	kubeCli   kubernetes.Interface
	svcLister corelisters.ServiceLister
	recorder  record.EventRecorder
}

// NewRealServiceControl creates a new ServiceControlInterface
func NewRealServiceControl(kubeCli kubernetes.Interface, svcLister corelisters.ServiceLister, recorder record.EventRecorder) ServiceControlInterface {
	return &realServiceControl{
		kubeCli,
		svcLister,
		recorder,
	}
}

func (sc *realServiceControl) CreateService(hc *v1alpha1.HyenaCluster, svc *corev1.Service) error {
	_, err := sc.kubeCli.CoreV1().Services(hc.Namespace).Create(svc)
	if apierrors.IsAlreadyExists(err) {
		return err
	}
	sc.recordServiceEvent("create", hc, svc, err)
	return err
}

func (sc *realServiceControl) UpdateService(hc *v1alpha1.HyenaCluster, svc *corev1.Service) (*corev1.Service, error) {
	ns := hc.GetNamespace()
	hcName := hc.GetName()
	svcName := svc.GetName()
	svcSpec := svc.Spec.DeepCopy()

	var updateSvc *corev1.Service
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var updateErr error
		updateSvc, updateErr = sc.kubeCli.CoreV1().Services(ns).Update(svc)
		if updateErr == nil {
			glog.Infof("update Service: [%s/%s] successfully, HyenaCluster: %s", ns, svcName, hcName)
			return nil
		}

		if updated, err := sc.svcLister.Services(hc.Namespace).Get(svcName); err != nil {
			svc = updated.DeepCopy()
			svc.Spec = *svcSpec
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated Service %s/%s from lister: %v", ns, svcName, err))
		}

		return updateErr
	})
	sc.recordServiceEvent("update", hc, svc, err)
	return updateSvc, err
}

func (sc *realServiceControl) DeleteService(hc *v1alpha1.HyenaCluster, svc *corev1.Service) error {
	err := sc.kubeCli.CoreV1().Services(hc.Namespace).Delete(svc.Name, nil)
	sc.recordServiceEvent("delete", hc, svc, err)
	return err
}

func (sc *realServiceControl) recordServiceEvent(verb string, hc *v1alpha1.HyenaCluster, svc *corev1.Service, err error) {
	hcName := hc.Name
	svcName := svc.Name
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		msg := fmt.Sprintf("%s Service %s in HyenaCluster %s successful",
			strings.ToLower(verb), svcName, hcName)
		sc.recorder.Event(hc, corev1.EventTypeNormal, reason, msg)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		msg := fmt.Sprintf("%s Service %s in HyenaCluster %s failed error: %s",
			strings.ToLower(verb), svcName, hcName, err)
		sc.recorder.Event(hc, corev1.EventTypeWarning, reason, msg)
	}
}

var _ ServiceControlInterface = &realServiceControl{}

// FakeServiceControl is a fake ServiceControlInterface
type FakeServiceControl struct {
	SvcLister                corelisters.ServiceLister
	SvcIndexer               cache.Indexer
	HcLister                 v1listers.HyenaClusterLister
	HcIndexer                cache.Indexer
	createServiceTracker     requestTracker
	updateServiceTracker     requestTracker
	deleteStatefulSetTracker requestTracker
}

// NewFakeServiceControl returns a FakeServiceControl
func NewFakeServiceControl(svcInformer coreinformers.ServiceInformer, tcInformer hcinformers.HyenaClusterInformer) *FakeServiceControl {
	return &FakeServiceControl{
		svcInformer.Lister(),
		svcInformer.Informer().GetIndexer(),
		tcInformer.Lister(),
		tcInformer.Informer().GetIndexer(),
		requestTracker{0, nil, 0},
		requestTracker{0, nil, 0},
		requestTracker{0, nil, 0},
	}
}

// SetCreateServiceError sets the error attributes of createServiceTracker
func (ssc *FakeServiceControl) SetCreateServiceError(err error, after int) {
	ssc.createServiceTracker.err = err
	ssc.createServiceTracker.after = after
}

// SetUpdateServiceError sets the error attributes of updateServiceTracker
func (ssc *FakeServiceControl) SetUpdateServiceError(err error, after int) {
	ssc.updateServiceTracker.err = err
	ssc.updateServiceTracker.after = after
}

// SetDeleteServiceError sets the error attributes of deleteServiceTracker
func (ssc *FakeServiceControl) SetDeleteServiceError(err error, after int) {
	ssc.deleteStatefulSetTracker.err = err
	ssc.deleteStatefulSetTracker.after = after
}

// CreateService adds the service to SvcIndexer
func (ssc *FakeServiceControl) CreateService(_ *v1alpha1.HyenaCluster, svc *corev1.Service) error {
	defer ssc.createServiceTracker.inc()
	if ssc.createServiceTracker.errorReady() {
		defer ssc.createServiceTracker.reset()
		return ssc.createServiceTracker.err
	}

	return ssc.SvcIndexer.Add(svc)
}

// UpdateService updates the service of SvcIndexer
func (ssc *FakeServiceControl) UpdateService(_ *v1alpha1.HyenaCluster, svc *corev1.Service) (*corev1.Service, error) {
	defer ssc.updateServiceTracker.inc()
	if ssc.updateServiceTracker.errorReady() {
		defer ssc.updateServiceTracker.reset()
		return nil, ssc.updateServiceTracker.err
	}

	return svc, ssc.SvcIndexer.Update(svc)
}

// DeleteService deletes the service of SvcIndexer
func (ssc *FakeServiceControl) DeleteService(_ *v1alpha1.HyenaCluster, _ *corev1.Service) error {
	return nil
}

var _ ServiceControlInterface = &FakeServiceControl{}
