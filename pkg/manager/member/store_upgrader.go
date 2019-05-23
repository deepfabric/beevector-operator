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
	"fmt"

	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/controller"
	apps "k8s.io/api/apps/v1beta1"
	corev1 "k8s.io/api/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type storeUpgrader struct {
	prophetControl controller.ProphetControlInterface
	podControl     controller.PodControlInterface
	podLister      corelisters.PodLister
}

// NewStoreUpgrader returns a store Upgrader
func NewStoreUpgrader(prophetControl controller.ProphetControlInterface,
	podControl controller.PodControlInterface,
	podLister corelisters.PodLister) Upgrader {
	return &storeUpgrader{
		prophetControl: prophetControl,
		podControl:     podControl,
		podLister:      podLister,
	}
}

func (csu *storeUpgrader) Upgrade(hc *v1alpha1.HyenaCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	ns := hc.GetNamespace()
	hcName := hc.GetName()
	if hc.Status.Prophet.Phase == v1alpha1.UpgradePhase {
		_, podSpec, err := GetLastAppliedConfig(oldSet)
		if err != nil {
			return err
		}
		newSet.Spec.Template.Spec = *podSpec
		return nil
	}

	if !hc.Status.Store.Synced {
		return fmt.Errorf("Hyenacluster: [%s/%s]'s store status sync failed, can not to be upgraded", ns, hcName)
	}

	hc.Status.Store.Phase = v1alpha1.UpgradePhase
	if !templateEqual(newSet.Spec.Template, oldSet.Spec.Template) {
		return nil
	}

	setUpgradePartition(newSet, *oldSet.Spec.UpdateStrategy.RollingUpdate.Partition)
	for i := hc.Status.Store.StatefulSet.Replicas - 1; i >= 0; i-- {
		store := csu.getStoreByOrdinal(hc, i)
		if store == nil {
			continue
		}
		podName := storePodName(hcName, i)
		pod, err := csu.podLister.Pods(ns).Get(podName)
		if err != nil {
			return err
		}
		revision, exist := pod.Labels[apps.ControllerRevisionHashLabelKey]
		if !exist {
			return controller.RequeueErrorf("hyenacluster: [%s/%s]'s store pod: [%s] has no label: %s", ns, hcName, podName, apps.ControllerRevisionHashLabelKey)
		}

		if revision == hc.Status.Store.StatefulSet.UpdateRevision {

			if pod.Status.Phase != corev1.PodRunning {
				return controller.RequeueErrorf("hyenacluster: [%s/%s]'s upgraded store pod: [%s] is not running", ns, hcName, podName)
			}
			if store.State != v1alpha1.StoreStateUp {
				return controller.RequeueErrorf("hyenacluster: [%s/%s]'s upgraded store pod: [%s] is not all ready", ns, hcName, podName)
			}
			continue
		}

		return csu.upgradeStorePod(hc, i, newSet)
	}

	return nil
}

func (csu *storeUpgrader) upgradeStorePod(hc *v1alpha1.HyenaCluster, ordinal int32, newSet *apps.StatefulSet) error {
	ns := hc.GetNamespace()
	hcName := hc.GetName()
	upgradePodName := storePodName(hcName, ordinal)
	// upgradePod, err := csu.podLister.Pods(ns).Get(upgradePodName)
	// if err != nil {
	// 	return err
	// }

	for _, store := range hc.Status.Store.Stores {
		if store.PodName == upgradePodName {
			// storeID, err := strconv.ParseUint(store.ID, 10, 64)
			// if err != nil {
			// 	return err
			// }
			setUpgradePartition(newSet, ordinal)
			return nil
		}
	}

	return controller.RequeueErrorf("hyenacluster: [%s/%s] no store status found for store pod: [%s]", ns, hcName, upgradePodName)
}

func (csu *storeUpgrader) getStoreByOrdinal(hc *v1alpha1.HyenaCluster, ordinal int32) *v1alpha1.Store {
	podName := storePodName(hc.GetName(), ordinal)
	for _, store := range hc.Status.Store.Stores {
		if store.PodName == podName {
			return &store
		}
	}
	return nil
}

type fakeStoreUpgrader struct{}

// NewFakeStoreUpgrader returns a fake store upgrader
func NewFakeStoreUpgrader() Upgrader {
	return &fakeStoreUpgrader{}
}

func (csu *fakeStoreUpgrader) Upgrade(hc *v1alpha1.HyenaCluster, _ *apps.StatefulSet, _ *apps.StatefulSet) error {
	hc.Status.Store.Phase = v1alpha1.UpgradePhase
	return nil
}
