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
	"github.com/infinivision/hyena-operator/pkg/label"
	apps "k8s.io/api/apps/v1beta1"
	corelisters "k8s.io/client-go/listers/core/v1"
)

type prophetUpgrader struct {
	prophetControl controller.ProphetControlInterface
	podControl     controller.PodControlInterface
	podLister      corelisters.PodLister
}

// NewProphetUpgrader returns a prophetUpgrader
func NewProphetUpgrader(prophetControl controller.ProphetControlInterface,
	podControl controller.PodControlInterface,
	podLister corelisters.PodLister) Upgrader {
	return &prophetUpgrader{
		prophetControl: prophetControl,
		podControl:     podControl,
		podLister:      podLister,
	}
}

func (pu *prophetUpgrader) Upgrade(hc *v1alpha1.HyenaCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	force, err := pu.needForceUpgrade(hc)
	if err != nil {
		return err
	}
	if force {
		return pu.forceUpgrade(hc, oldSet, newSet)
	}

	return pu.gracefulUpgrade(hc, oldSet, newSet)
}

func (pu *prophetUpgrader) forceUpgrade(hc *v1alpha1.HyenaCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	hc.Status.Prophet.Phase = v1alpha1.UpgradePhase
	setUpgradePartition(newSet, 0)
	return nil
}

func (pu *prophetUpgrader) gracefulUpgrade(hc *v1alpha1.HyenaCluster, oldSet *apps.StatefulSet, newSet *apps.StatefulSet) error {
	ns := hc.GetNamespace()
	ccName := hc.GetName()
	if !hc.Status.Prophet.Synced {
		return fmt.Errorf("hyenacluster: [%s/%s]'s prophet status sync failed,can not to be upgraded", ns, ccName)
	}

	hc.Status.Prophet.Phase = v1alpha1.UpgradePhase
	if !templateEqual(newSet.Spec.Template, oldSet.Spec.Template) {
		return nil
	}

	setUpgradePartition(newSet, *oldSet.Spec.UpdateStrategy.RollingUpdate.Partition)
	for i := hc.Status.Prophet.StatefulSet.Replicas - 1; i >= 0; i-- {
		podName := prophetPodName(ccName, i)
		pod, err := pu.podLister.Pods(ns).Get(podName)
		if err != nil {
			return err
		}

		revision, exist := pod.Labels[apps.ControllerRevisionHashLabelKey]
		if !exist {
			return controller.RequeueErrorf("hyenacluster: [%s/%s]'s prophet pod: [%s] has no label: %s", ns, ccName, podName, apps.ControllerRevisionHashLabelKey)
		}

		if revision == hc.Status.Prophet.StatefulSet.UpdateRevision {
			if member, exist := hc.Status.Prophet.Members[podName]; !exist || !member.Health {
				return controller.RequeueErrorf("hyenacluster: [%s/%s]'s pd upgraded pod: [%s] is not ready", ns, ccName, podName)
			}
			continue
		}

		return pu.upgradeProphetPod(hc, i, newSet)
	}

	return nil
}

func (pu *prophetUpgrader) needForceUpgrade(hc *v1alpha1.HyenaCluster) (bool, error) {
	ns := hc.GetNamespace()
	ccName := hc.GetName()
	instanceName := hc.GetLabels()[label.InstanceLabelKey]
	selector, err := label.New().Instance(instanceName).Prophet().Selector()
	if err != nil {
		return false, err
	}
	prophetPods, err := pu.podLister.Pods(ns).List(selector)
	if err != nil {
		return false, err
	}

	imagePullFailedCount := 0
	for _, pod := range prophetPods {
		revisionHash, exist := pod.Labels[apps.ControllerRevisionHashLabelKey]
		if !exist {
			return false, fmt.Errorf("hyenacluster: [%s/%s]'s pod:[%s] doesn't have label: %s", ns, ccName, pod.GetName(), apps.ControllerRevisionHashLabelKey)
		}
		if revisionHash == hc.Status.Prophet.StatefulSet.CurrentRevision {
			if imagePullFailed(pod) {
				imagePullFailedCount++
			}
		}
	}

	return imagePullFailedCount >= int(hc.Status.Prophet.StatefulSet.Replicas)/2+1, nil
}

func (pu *prophetUpgrader) upgradeProphetPod(hc *v1alpha1.HyenaCluster, ordinal int32, newSet *apps.StatefulSet) error {
	/*
		ns := cc.GetNamespace()
		ccName := cc.GetName()

		upgradePodName := pdPodName(ccName, ordinal)
		if cc.Status.PD.Leader.Name == upgradePodName && cc.Status.PD.StatefulSet.Replicas > 1 {
			lastOrdinal := cc.Status.PD.StatefulSet.Replicas - 1
			var targetName string
			if ordinal == lastOrdinal {
				targetName = pdPodName(ccName, 0)
			} else {
				targetName = pdPodName(ccName, lastOrdinal)
			}
			err := pu.transferPDLeaderTo(cc, targetName)
			if err != nil {
				return err
			}
			return controller.RequeueErrorf("hyenacluster: [%s/%s]'s prophet member: [%s] is transferring leader to prophet member: [%s]", ns, ccName, upgradePodName, targetName)
		}
	*/

	setUpgradePartition(newSet, ordinal)
	return nil
}

func (pu *prophetUpgrader) transferProphetLeaderTo(hc *v1alpha1.HyenaCluster, targetName string) error {
	return nil
}

type fakeProphetUpgrader struct{}

// NewFakeProphetUpgrader returns a fakeProphetUpgrader
func NewFakeProphetUpgrader() Upgrader {
	return &fakeProphetUpgrader{}
}

func (fpu *fakeProphetUpgrader) Upgrade(cc *v1alpha1.HyenaCluster, _ *apps.StatefulSet, _ *apps.StatefulSet) error {
	cc.Status.Prophet.Phase = v1alpha1.UpgradePhase
	return nil
}
