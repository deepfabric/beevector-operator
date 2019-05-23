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

package meta

import (
	"errors"

	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/controller"
	"github.com/infinivision/hyena-operator/pkg/label"
	"github.com/infinivision/hyena-operator/pkg/manager"
	corev1 "k8s.io/api/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
)

var errPVCNotFound = errors.New("PVC is not found")

type metaManager struct {
	pvcLister  corelisters.PersistentVolumeClaimLister
	pvcControl controller.PVCControlInterface
	pvLister   corelisters.PersistentVolumeLister
	pvControl  controller.PVControlInterface
	podLister  corelisters.PodLister
	podControl controller.PodControlInterface
}

// NewMetaManager returns a *metaManager
func NewMetaManager(
	pvcLister corelisters.PersistentVolumeClaimLister,
	pvcControl controller.PVCControlInterface,
	pvLister corelisters.PersistentVolumeLister,
	pvControl controller.PVControlInterface,
	podLister corelisters.PodLister,
	podControl controller.PodControlInterface,
) manager.Manager {
	return &metaManager{
		pvcLister:  pvcLister,
		pvcControl: pvcControl,
		pvLister:   pvLister,
		pvControl:  pvControl,
		podLister:  podLister,
		podControl: podControl,
	}
}

func (pdmm *metaManager) Sync(hc *v1alpha1.HyenaCluster) error {
	ns := hc.GetNamespace()
	instanceName := hc.GetLabels()[label.InstanceLabelKey]

	l, err := label.New().Instance(instanceName).Selector()
	if err != nil {
		return err
	}
	pods, err := pdmm.podLister.Pods(ns).List(l)
	if err != nil {
		return err
	}

	for _, pod := range pods {
		// update meta info for pod
		_, err := pdmm.podControl.UpdateMetaInfo(hc, pod)
		if err != nil {
			return err
		}
		if component := pod.Labels[label.ComponentLabelKey]; component != label.ProphetLabelVal && component != label.StoreLabelVal {
			// Skip syncing meta info for pod that doesn't use PV
			// Currently only PD/Store uses PV
			continue
		}
		// update meta info for pvc
		pvc, err := pdmm.resolvePVCFromPod(pod)
		if err != nil {
			return err
		}
		_, err = pdmm.pvcControl.UpdateMetaInfo(hc, pvc, pod)
		if err != nil {
			return err
		}
		if pvc.Spec.VolumeName == "" {
			continue
		}
		// update meta info for pv
		pv, err := pdmm.pvLister.Get(pvc.Spec.VolumeName)
		if err != nil {
			return err
		}
		_, err = pdmm.pvControl.UpdateMetaInfo(hc, pv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pdmm *metaManager) resolvePVCFromPod(pod *corev1.Pod) (*corev1.PersistentVolumeClaim, error) {
	var pvcName string
	for _, vol := range pod.Spec.Volumes {
		switch vol.Name {
		case v1alpha1.ProphetMemberType.String(), v1alpha1.StoreMemberType.String():
			if vol.PersistentVolumeClaim != nil {
				pvcName = vol.PersistentVolumeClaim.ClaimName
				break
			}
		default:
			continue
		}
	}
	if len(pvcName) == 0 {
		return nil, errPVCNotFound
	}

	pvc, err := pdmm.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(pvcName)
	if err != nil {
		return nil, err
	}
	return pvc, nil
}

var _ manager.Manager = &metaManager{}

type FakeMetaManager struct {
	err error
}

func NewFakeMetaManager() *FakeMetaManager {
	return &FakeMetaManager{}
}

func (fmm *FakeMetaManager) SetSyncError(err error) {
	fmm.err = err
}

func (fmm *FakeMetaManager) Sync(_ *v1alpha1.HyenaCluster) error {
	if fmm.err != nil {
		return fmm.err
	}
	return nil
}
