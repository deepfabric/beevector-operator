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
	"time"

	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/controller"
)

type storeFailover struct {
	prophetControl controller.ProphetControlInterface
}

// NewStoreFailover returns a store Failover
func NewStoreFailover(prophetControl controller.ProphetControlInterface) Failover {
	return &storeFailover{prophetControl}
}

func (sf *storeFailover) Failover(hc *v1alpha1.HyenaCluster) error {
	maxStoreDownTimeDuration, err := sf.prophetControl.GetProphetClient(hc).GetConfig()
	if err != nil {
		return err
	}

	for storeID, store := range hc.Status.Store.Stores {
		podName := store.PodName
		if store.LastTransitionTime.IsZero() {
			continue
		}
		deadline := store.LastTransitionTime.Add(maxStoreDownTimeDuration)
		exist := false
		for _, failureStore := range hc.Status.Store.FailureStores {
			if failureStore.PodName == podName {
				exist = true
				break
			}
		}
		if store.State == v1alpha1.StoreStateDown && time.Now().After(deadline) && !exist {
			if hc.Status.Store.FailureStores == nil {
				hc.Status.Store.FailureStores = map[string]v1alpha1.FailureStore{}
			}
			hc.Status.Store.FailureStores[storeID] = v1alpha1.FailureStore{
				PodName: podName,
				StoreID: store.ID,
			}
		}
	}

	return nil
}

func (sf *storeFailover) Recover(_ *v1alpha1.HyenaCluster) {
	// Do nothing now
}

type fakeStoreFailover struct{}

// NewFakeStoreFailover returns a fake Failover
func NewFakeStoreFailover() Failover {
	return &fakeStoreFailover{}
}

func (ftf *fakeStoreFailover) Failover(_ *v1alpha1.HyenaCluster) error {
	return nil
}

func (ftf *fakeStoreFailover) Recover(_ *v1alpha1.HyenaCluster) {
	return
}
