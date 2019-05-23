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

package member

import (
	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/controller"
	apps "k8s.io/api/apps/v1beta1"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type storeScaler struct {
	generalScaler
	podLister corelisters.PodLister
}

// NewStoreScaler returns a store Scaler
func NewStoreScaler(prophetControl controller.ProphetControlInterface,
	pvcLister corelisters.PersistentVolumeClaimLister,
	pvcControl controller.PVCControlInterface,
	podLister corelisters.PodLister) Scaler {
	return &storeScaler{generalScaler{prophetControl, pvcLister, pvcControl}, podLister}
}

func (csd *storeScaler) ScaleOut(hc *v1alpha1.HyenaCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	if hc.StoreUpgrading() {
		resetReplicas(newSet, oldSet)
		return nil
	}

	_, err := csd.deleteDeferDeletingPVC(hc, oldSet.GetName(), v1alpha1.StoreMemberType, *oldSet.Spec.Replicas)
	if err != nil {
		resetReplicas(newSet, oldSet)
		return err
	}

	increaseReplicas(newSet, oldSet)
	return nil
}

// unsupport scale in now
func (csd *storeScaler) ScaleIn(hc *v1alpha1.HyenaCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	// decreaseReplicas(newSet, oldSet)
	return nil
}

type fakeStoreScaler struct{}

// NewFakeStoreScaler returns a fake store Scaler
func NewFakeStoreScaler() Scaler {
	return &fakeStoreScaler{}
}

func (fsd *fakeStoreScaler) ScaleOut(_ *v1alpha1.HyenaCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	increaseReplicas(newSet, oldSet)
	return nil
}

func (fsd *fakeStoreScaler) ScaleIn(_ *v1alpha1.HyenaCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	decreaseReplicas(newSet, oldSet)
	return nil
}
