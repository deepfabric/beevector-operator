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
	"testing"

	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/controller"
	"github.com/infinivision/hyena-operator/pkg/label"
	. "github.com/onsi/gomega"
	apps "k8s.io/api/apps/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubeinformers "k8s.io/client-go/informers"
	podinformers "k8s.io/client-go/informers/core/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func TestProphetUpgraderUpgrade(t *testing.T) {
	g := NewGomegaWithT(t)

	type testcase struct {
		name              string
		changeFn          func(*v1alpha1.HyenaCluster)
		changePods        func(pods []*corev1.Pod)
		changeOldSet      func(set *apps.StatefulSet)
		transferLeaderErr bool
		errExpectFn       func(*GomegaWithT, error)
		expectFn          func(g *GomegaWithT, cc *v1alpha1.HyenaCluster, newSet *apps.StatefulSet)
	}

	testFn := func(test *testcase) {
		t.Log(test.name)
		upgrader, pdControl, _, podInformer := newProphetUpgrader()
		pdClient := controller.NewFakeProphetClient()
		cc := newHyenaClusterForProphetUpgrader()
		pdControl.SetPDClient(cc, pdClient)

		if test.changeFn != nil {
			test.changeFn(cc)
		}

		if test.transferLeaderErr {
			pdClient.AddReaction(controller.TransferPDLeaderActionType, func(action *controller.Action) (interface{}, error) {
				return nil, fmt.Errorf("failed to transfer leader")
			})
		} else {
			pdClient.AddReaction(controller.TransferPDLeaderActionType, func(action *controller.Action) (interface{}, error) {
				return nil, nil
			})
		}

		pods := getPods()
		if test.changePods != nil {
			test.changePods(pods)
		}
		for i := range pods {
			podInformer.Informer().GetIndexer().Add(pods[i])
		}

		newSet := newStatefulSetForPDUpgrader()
		oldSet := newSet.DeepCopy()
		if test.changeOldSet != nil {
			test.changeOldSet(oldSet)
		}
		SetLastAppliedConfigAnnotation(oldSet)

		newSet.Spec.UpdateStrategy.RollingUpdate.Partition = func() *int32 { i := int32(3); return &i }()

		err := upgrader.Upgrade(cc, oldSet, newSet)
		test.errExpectFn(g, err)
		test.expectFn(g, cc, newSet)
	}

	tests := []testcase{
		{
			name: "normal upgrade",
			changeFn: func(cc *v1alpha1.HyenaCluster) {
				cc.Status.PD.Synced = true
			},
			changePods:        nil,
			transferLeaderErr: false,
			errExpectFn: func(g *GomegaWithT, err error) {
				g.Expect(err).NotTo(HaveOccurred())
			},
			expectFn: func(g *GomegaWithT, cc *v1alpha1.HyenaCluster, newSet *apps.StatefulSet) {
				g.Expect(cc.Status.PD.Phase).To(Equal(v1alpha1.UpgradePhase))
				g.Expect(newSet.Spec.UpdateStrategy.RollingUpdate.Partition).To(Equal(func() *int32 { i := int32(1); return &i }()))
			},
		},
		{
			name: "newSet template changed",
			changeFn: func(cc *v1alpha1.HyenaCluster) {
				cc.Status.PD.Synced = true
			},
			changePods: nil,
			changeOldSet: func(set *apps.StatefulSet) {
				set.Spec.Template.Spec.Containers[0].Image = "prophet-test-image:old"
			},
			transferLeaderErr: false,
			errExpectFn: func(g *GomegaWithT, err error) {
				g.Expect(err).NotTo(HaveOccurred())
			},
			expectFn: func(g *GomegaWithT, cc *v1alpha1.HyenaCluster, newSet *apps.StatefulSet) {
				g.Expect(cc.Status.Prophet.Phase).To(Equal(v1alpha1.UpgradePhase))
				g.Expect(newSet.Spec.UpdateStrategy.RollingUpdate.Partition).To(Equal(func() *int32 { i := int32(3); return &i }()))
			},
		},
		{
			name: "skip to wait all members health",
			changeFn: func(cc *v1alpha1.HyenaCluster) {
				cc.Status.Prophet.Synced = true
				cc.Status.Prophet.Members[prophetPodName(upgradeCcName, 2)] = v1alpha1.ProphetMember{Name: prophetPodName(upgradeCcName, 2), Health: false}
			},
			changePods:        nil,
			transferLeaderErr: false,
			errExpectFn: func(g *GomegaWithT, err error) {
				g.Expect(err.Error()).To(Equal(fmt.Sprintf("HyenaCluster: [default/upgrader]'s pd upgraded pod: [%s] is not ready", pdPodName(upgradeCcName, 2))))
			},
			expectFn: func(g *GomegaWithT, cc *v1alpha1.HyenaCluster, newSet *apps.StatefulSet) {
				g.Expect(cc.Status.PD.Phase).To(Equal(v1alpha1.UpgradePhase))
				g.Expect(newSet.Spec.UpdateStrategy.RollingUpdate.Partition).To(Equal(func() *int32 { i := int32(2); return &i }()))
			},
		},
		/*
			{
				name: "transfer leader",
				changeFn: func(cc *v1alpha1.HyenaCluster) {
					cc.Status.PD.Synced = true
					cc.Status.PD.Leader = v1alpha1.PDMember{Name: pdPodName(upgradeCcName, 1), Health: true}
				},
				changePods:        nil,
				transferLeaderErr: false,
				errExpectFn: func(g *GomegaWithT, err error) {
					g.Expect(err).To(HaveOccurred())
				},
				expectFn: func(g *GomegaWithT, cc *v1alpha1.HyenaCluster, newSet *apps.StatefulSet) {
					g.Expect(cc.Status.PD.Phase).To(Equal(v1alpha1.UpgradePhase))
					g.Expect(newSet.Spec.UpdateStrategy.RollingUpdate.Partition).To(Equal(func() *int32 { i := int32(2); return &i }()))
				},
			},
		*/
		{
			name: "prophet sync failed",
			changeFn: func(cc *v1alpha1.HyenaCluster) {
				cc.Status.PD.Synced = false
			},
			changePods:        nil,
			transferLeaderErr: false,
			errExpectFn: func(g *GomegaWithT, err error) {
				g.Expect(err).To(HaveOccurred())
			},
			expectFn: func(g *GomegaWithT, cc *v1alpha1.HyenaCluster, newSet *apps.StatefulSet) {
				g.Expect(cc.Status.PD.Phase).To(Equal(v1alpha1.NormalPhase))
				g.Expect(newSet.Spec.UpdateStrategy.RollingUpdate.Partition).To(Equal(func() *int32 { i := int32(3); return &i }()))
			},
		},
		{
			name: "force upgrade",
			changeFn: func(cc *v1alpha1.HyenaCluster) {
				cc.Status.PD.Synced = false
			},
			changePods: func(pods []*corev1.Pod) {
				pods[1].Status = corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: ErrImagePull},
						},
					},
				}}
				pods[0].Status = corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: ErrImagePull},
						},
					},
				}}
			},
			transferLeaderErr: false,
			errExpectFn: func(g *GomegaWithT, err error) {
				g.Expect(err).NotTo(HaveOccurred())
			},
			expectFn: func(g *GomegaWithT, cc *v1alpha1.HyenaCluster, newSet *apps.StatefulSet) {
				g.Expect(cc.Status.PD.Phase).To(Equal(v1alpha1.UpgradePhase))
				g.Expect(newSet.Spec.UpdateStrategy.RollingUpdate.Partition).To(Equal(func() *int32 { i := int32(0); return &i }()))
			},
		},
		/*
			{
				name: "error when transfer leader",
				changeFn: func(cc *v1alpha1.HyenaCluster) {
					cc.Status.PD.Synced = true
					cc.Status.PD.Leader = v1alpha1.PDMember{Name: pdPodName(upgradeCcName, 1), Health: true}
				},
				changePods:        nil,
				transferLeaderErr: true,
				errExpectFn: func(g *GomegaWithT, err error) {
					g.Expect(err).To(HaveOccurred())
				},
				expectFn: func(g *GomegaWithT, cc *v1alpha1.HyenaCluster, newSet *apps.StatefulSet) {
					g.Expect(cc.Status.PD.Phase).To(Equal(v1alpha1.UpgradePhase))
					g.Expect(newSet.Spec.UpdateStrategy.RollingUpdate.Partition).To(Equal(func() *int32 { i := int32(2); return &i }()))
				},
			},
		*/
	}

	for _, test := range tests {
		testFn(&test)
	}

}

func newPDUpgrader() (Upgrader, *controller.FakePDControl, *controller.FakePodControl, podinformers.PodInformer) {
	kubeCli := kubefake.NewSimpleClientset()
	podInformer := kubeinformers.NewSharedInformerFactory(kubeCli, 0).Core().V1().Pods()
	pdControl := controller.NewFakePDControl()
	podControl := controller.NewFakePodControl(podInformer)
	return &pdUpgrader{
			pdControl:  pdControl,
			podControl: podControl,
			podLister:  podInformer.Lister()},
		pdControl, podControl, podInformer
}

func newStatefulSetForPDUpgrader() *apps.StatefulSet {
	return &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      controller.PDMemberName(upgradeCcName),
			Namespace: metav1.NamespaceDefault,
		},
		Spec: apps.StatefulSetSpec{
			Replicas: int32Pointer(3),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "prophet",
							Image: "rophet-test-image",
						},
					},
				},
			},
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type:          apps.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &apps.RollingUpdateStatefulSetStrategy{Partition: func() *int32 { i := int32(2); return &i }()},
			},
		},
		Status: apps.StatefulSetStatus{
			CurrentRevision: "1",
			UpdateRevision:  "2",
			ReadyReplicas:   3,
			Replicas:        3,
			CurrentReplicas: 2,
			UpdatedReplicas: 1,
		},
	}
}

func newHyenaClusterForPDUpgrader() *v1alpha1.HyenaCluster {
	podName0 := pdPodName(upgradeCcName, 0)
	podName1 := pdPodName(upgradeCcName, 1)
	podName2 := pdPodName(upgradeCcName, 2)
	return &v1alpha1.HyenaCluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       "HyenaCluster",
			APIVersion: "deepfabric.com/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      upgradeCcName,
			Namespace: corev1.NamespaceDefault,
			UID:       types.UID(upgradeCcName),
			Labels:    label.New().Instance(upgradeInstanceName),
		},
		Spec: v1alpha1.HyenaClusterSpec{
			PD: v1alpha1.PDSpec{
				ContainerSpec: v1alpha1.ContainerSpec{
					Image: "prophet-test-image",
				},
				Replicas:         3,
				StorageClassName: "my-storage-class",
			},
		},
		Status: v1alpha1.HyenaClusterStatus{
			PD: v1alpha1.PDStatus{
				Phase: v1alpha1.NormalPhase,
				StatefulSet: &apps.StatefulSetStatus{
					CurrentRevision: "1",
					UpdateRevision:  "2",
					ReadyReplicas:   3,
					Replicas:        3,
					CurrentReplicas: 2,
					UpdatedReplicas: 1,
				},
				Members: map[string]v1alpha1.ProphetMember{
					podName0: {Name: podName0, Health: true},
					podName1: {Name: podName1, Health: true},
					podName2: {Name: podName2, Health: true},
				},
				Leader: v1alpha1.ProphetMember{Name: podName2, Health: true},
			},
		},
	}
}

func getPods() []*corev1.Pod {
	lc := label.New().Instance(upgradeInstanceName).PD().Labels()
	lc[apps.ControllerRevisionHashLabelKey] = "1"
	lu := label.New().Instance(upgradeInstanceName).PD().Labels()
	lu[apps.ControllerRevisionHashLabelKey] = "2"
	pods := []*corev1.Pod{
		{
			TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      pdPodName(upgradeCcName, 0),
				Namespace: corev1.NamespaceDefault,
				Labels:    lc,
			},
		},
		{
			TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      pdPodName(upgradeCcName, 1),
				Namespace: corev1.NamespaceDefault,
				Labels:    lc,
			},
		},
		{
			TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      pdPodName(upgradeCcName, 2),
				Namespace: corev1.NamespaceDefault,
				Labels:    lu,
			},
		},
	}
	return pods
}
