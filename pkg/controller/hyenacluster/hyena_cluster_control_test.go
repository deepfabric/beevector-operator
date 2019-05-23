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

package hyenacluster

import (
	"fmt"
	"strings"
	"testing"

	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/client/clientset/versioned/fake"
	informers "github.com/infinivision/hyena-operator/pkg/client/informers/externalversions"
	"github.com/infinivision/hyena-operator/pkg/controller"
	mm "github.com/infinivision/hyena-operator/pkg/manager/member"
	"github.com/infinivision/hyena-operator/pkg/manager/meta"
	. "github.com/onsi/gomega"
	apps "k8s.io/api/apps/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
)

func TestHyenaClusterControlUpdateHyenaCluster(t *testing.T) {
	g := NewGomegaWithT(t)

	type testcase struct {
		name                        string
		update                      func(cluster *v1alpha1.HyenaCluster)
		syncReclaimPolicyErr        bool
		syncProphetMemberManagerErr bool
		syncStoreMemberManagerErr   bool
		syncProxyMemberManagerErr   bool
		syncMetaManagerErr          bool
		errExpectFn                 func(*GomegaWithT, error)
	}
	testFn := func(test *testcase, t *testing.T) {
		t.Log(test.name)

		cc := newHyenaClusterForHyenaClusterControl()
		if test.update != nil {
			test.update(cc)
		}
		control, reclaimPolicyManager, pdMemberManager, storeMemberManager, metaManager := newFakeHyenaClusterControl()

		if test.syncReclaimPolicyErr {
			reclaimPolicyManager.SetSyncError(fmt.Errorf("reclaim policy sync error"))
		}
		if test.syncProphetMemberManagerErr {
			pdMemberManager.SetSyncError(fmt.Errorf("pd member manager sync error"))
		}
		if test.syncStoreMemberManagerErr {
			storeMemberManager.SetSyncError(fmt.Errorf("store member manager sync error"))
		}
		if test.syncMetaManagerErr {
			metaManager.SetSyncError(fmt.Errorf("meta manager sync error"))
		}
		err := control.UpdateHyenaCluster(cc)
		if test.errExpectFn != nil {
			test.errExpectFn(g, err)
		}
	}
	tests := []testcase{
		{
			name:                        "reclaim policy sync error",
			update:                      nil,
			syncReclaimPolicyErr:        true,
			syncProphetMemberManagerErr: false,
			syncStoreMemberManagerErr:   false,
			syncMetaManagerErr:          false,
			errExpectFn: func(g *GomegaWithT, err error) {
				g.Expect(err).To(HaveOccurred())
				g.Expect(strings.Contains(err.Error(), "reclaim policy sync error")).To(Equal(true))
			},
		},
		{
			name:                        "pd member manager sync error",
			update:                      nil,
			syncReclaimPolicyErr:        false,
			syncProphetMemberManagerErr: true,
			syncStoreMemberManagerErr:   false,
			syncMetaManagerErr:          false,
			errExpectFn: func(g *GomegaWithT, err error) {
				g.Expect(err).To(HaveOccurred())
				g.Expect(strings.Contains(err.Error(), "pd member manager sync error")).To(Equal(true))
			},
		},
		{
			name: "store member manager sync error",
			update: func(cluster *v1alpha1.HyenaCluster) {
				cluster.Status.Prophet.Members = map[string]v1alpha1.ProphetMember{
					"prophet-0": {Name: "prophet-0", Health: true},
					"prophet-1": {Name: "prophet-1", Health: true},
					"prophet-2": {Name: "prophet-2", Health: true},
				}
				cluster.Status.Prophet.StatefulSet = &apps.StatefulSetStatus{ReadyReplicas: 3}
			},
			syncReclaimPolicyErr:        false,
			syncProphetMemberManagerErr: false,
			syncStoreMemberManagerErr:   true,
			syncMetaManagerErr:          false,
			errExpectFn: func(g *GomegaWithT, err error) {
				g.Expect(err).To(HaveOccurred())
				g.Expect(strings.Contains(err.Error(), "store member manager sync error")).To(Equal(true))
			},
		},
		{
			name: "proxy member manager sync error",
			update: func(cluster *v1alpha1.HyenaCluster) {
				cluster.Status.Prophet.Members = map[string]v1alpha1.ProphetMember{
					"prophet-0": {Name: "prophet-0", Health: true},
					"prophet-1": {Name: "prophet-1", Health: true},
					"prophet-2": {Name: "prophet-2", Health: true},
				}
				cluster.Status.Prophet.StatefulSet = &apps.StatefulSetStatus{ReadyReplicas: 3}
				cluster.Status.Store.Stores = map[string]v1alpha1.Store{
					"store-0": {PodName: "store-0", State: v1alpha1.StoreStateUp},
					"store-1": {PodName: "store-1", State: v1alpha1.StoreStateUp},
					"store-2": {PodName: "store-2", State: v1alpha1.StoreStateUp},
				}
				cluster.Status.Store.StatefulSet = &apps.StatefulSetStatus{ReadyReplicas: 3}
			},
			syncReclaimPolicyErr:        false,
			syncProphetMemberManagerErr: false,
			syncStoreMemberManagerErr:   false,
			syncMetaManagerErr:          false,
			errExpectFn: func(g *GomegaWithT, err error) {
				g.Expect(err).To(HaveOccurred())
				g.Expect(strings.Contains(err.Error(), "proxy member manager sync error")).To(Equal(true))
			},
		},
		{
			name: "meta manager sync error",
			update: func(cluster *v1alpha1.HyenaCluster) {
				cluster.Status.Prophet.Members = map[string]v1alpha1.ProphetMember{
					"prophet-0": {Name: "prophet-0", Health: true},
					"prophet-1": {Name: "prophet-1", Health: true},
					"prophet-2": {Name: "prophet-2", Health: true},
				}
				cluster.Status.Prophet.StatefulSet = &apps.StatefulSetStatus{ReadyReplicas: 3}
				cluster.Status.Store.Stores = map[string]v1alpha1.Store{
					"store-0": {PodName: "store-0", State: v1alpha1.StoreStateUp},
					"store-1": {PodName: "store-1", State: v1alpha1.StoreStateUp},
					"store-2": {PodName: "store-2", State: v1alpha1.StoreStateUp},
				}
				cluster.Status.Store.StatefulSet = &apps.StatefulSetStatus{ReadyReplicas: 3}
			},
			syncReclaimPolicyErr:        false,
			syncProphetMemberManagerErr: false,
			syncStoreMemberManagerErr:   false,
			syncMetaManagerErr:          true,
			errExpectFn: func(g *GomegaWithT, err error) {
				g.Expect(err).To(HaveOccurred())
				g.Expect(strings.Contains(err.Error(), "meta manager sync error")).To(Equal(true))
			},
		},
		{
			name: "normal",
			update: func(cluster *v1alpha1.HyenaCluster) {
				cluster.Status.Prophet.Members = map[string]v1alpha1.ProphetMember{
					"prophet-0": {Name: "prophet-0", Health: true},
					"prophet-1": {Name: "prophet-1", Health: true},
					"prophet-2": {Name: "prophet-2", Health: true},
				}
				cluster.Status.Prophet.StatefulSet = &apps.StatefulSetStatus{ReadyReplicas: 3}
				cluster.Status.Store.Stores = map[string]v1alpha1.Store{
					"store-0": {PodName: "store-0", State: v1alpha1.StoreStateUp},
					"store-1": {PodName: "store-1", State: v1alpha1.StoreStateUp},
					"store-2": {PodName: "store-2", State: v1alpha1.StoreStateUp},
				}
				cluster.Status.Store.StatefulSet = &apps.StatefulSetStatus{ReadyReplicas: 3}
			},
			syncReclaimPolicyErr:        false,
			syncProphetMemberManagerErr: false,
			syncStoreMemberManagerErr:   false,
			syncMetaManagerErr:          false,
			errExpectFn: func(g *GomegaWithT, err error) {
				g.Expect(err).NotTo(HaveOccurred())
			},
		},
	}

	for i := range tests {
		testFn(&tests[i], t)
	}
}

func TestHyenaClusterStatusEquality(t *testing.T) {
	g := NewGomegaWithT(t)
	tcStatus := v1alpha1.HyenaClusterStatus{}

	tcStatusCopy := tcStatus.DeepCopy()
	tcStatusCopy.Prophet = v1alpha1.ProphetStatus{}
	g.Expect(apiequality.Semantic.DeepEqual(&tcStatus, tcStatusCopy)).To(Equal(true))

	tcStatusCopy = tcStatus.DeepCopy()
	tcStatusCopy.Prophet.Phase = v1alpha1.NormalPhase
	g.Expect(apiequality.Semantic.DeepEqual(&tcStatus, tcStatusCopy)).To(Equal(false))
}

func newFakeHyenaClusterControl() (ControlInterface, *meta.FakeReclaimPolicyManager, *mm.FakeProphetMemberManager, *mm.FakeStoreMemberManager, *meta.FakeMetaManager) {
	cli := fake.NewSimpleClientset()
	tcInformer := informers.NewSharedInformerFactory(cli, 0).Infinivision().V1alpha1().HyenaClusters()
	recorder := record.NewFakeRecorder(10)

	hcControl := controller.NewFakeHyenaClusterControl(tcInformer)
	prophetMemberManager := mm.NewFakeProphetMemberManager()
	storeMemberManager := mm.NewFakeStoreMemberManager()
	reclaimPolicyManager := meta.NewFakeReclaimPolicyManager()
	metaManager := meta.NewFakeMetaManager()
	opc := mm.NewFakeOrphanPodsCleaner()
	control := NewDefaultHyenaClusterControl(hcControl, prophetMemberManager, storeMemberManager, reclaimPolicyManager, metaManager, opc, recorder)

	return control, reclaimPolicyManager, prophetMemberManager, storeMemberManager, metaManager
}

func newHyenaClusterForHyenaClusterControl() *v1alpha1.HyenaCluster {
	return &v1alpha1.HyenaCluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       "HyenaCluster",
			APIVersion: "infinivision.com/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-prophet",
			Namespace: corev1.NamespaceDefault,
			UID:       types.UID("test"),
		},
		Spec: v1alpha1.HyenaClusterSpec{
			Prophet: v1alpha1.ProphetSpec{
				Replicas: 3,
			},
			Store: v1alpha1.StoreSpec{
				Replicas: 3,
			},
		},
	}
}
