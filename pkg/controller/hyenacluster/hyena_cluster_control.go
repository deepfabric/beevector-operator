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
	"github.com/golang/glog"
	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/controller"
	"github.com/infinivision/hyena-operator/pkg/manager"
	"github.com/infinivision/hyena-operator/pkg/manager/member"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	errorutils "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/record"
)

// ControlInterface implements the control logic for updating HyenaClusters and their children StatefulSets.
// It is implemented as an interface to allow for extensions that provide different semantics.
// Currently, there is only one implementation.
type ControlInterface interface {
	// UpdateHyenaCluster implements the control logic for StatefulSet creation, update, and deletion
	UpdateHyenaCluster(*v1alpha1.HyenaCluster) error
}

// NewDefaultHyenaClusterControl returns a new instance of the default implementation HyenaClusterControlInterface that
// implements the documented semantics for HyenaClusters.
func NewDefaultHyenaClusterControl(
	ccControl controller.HyenaClusterControlInterface,
	prophetMemberManager manager.Manager,
	storeMemberManager manager.Manager,
	reclaimPolicyManager manager.Manager,
	metaManager manager.Manager,
	orphanPodsCleaner member.OrphanPodsCleaner,
	recorder record.EventRecorder) ControlInterface {
	return &defaultHyenaClusterControl{
		ccControl,
		prophetMemberManager,
		storeMemberManager,
		reclaimPolicyManager,
		metaManager,
		orphanPodsCleaner,
		recorder,
	}
}

type defaultHyenaClusterControl struct {
	ccControl            controller.HyenaClusterControlInterface
	prophetMemberManager manager.Manager
	storeMemberManager   manager.Manager
	reclaimPolicyManager manager.Manager
	metaManager          manager.Manager
	orphanPodsCleaner    member.OrphanPodsCleaner
	recorder             record.EventRecorder
}

// UpdateStatefulSet executes the core logic loop for a hyenacluster.
func (hcc *defaultHyenaClusterControl) UpdateHyenaCluster(hc *v1alpha1.HyenaCluster) error {
	var errs []error
	oldStatus := hc.Status.DeepCopy()

	if err := hcc.updateHyenaCluster(hc); err != nil {
		errs = append(errs, err)
	}
	if apiequality.Semantic.DeepEqual(&hc.Status, oldStatus) {
		return errorutils.NewAggregate(errs)
	}
	if _, err := hcc.ccControl.UpdateHyenaCluster(hc.DeepCopy(), &hc.Status, oldStatus); err != nil {
		errs = append(errs, err)
	}

	return errorutils.NewAggregate(errs)
}

func (hcc *defaultHyenaClusterControl) updateHyenaCluster(hc *v1alpha1.HyenaCluster) error {
	// syncing all PVs managed by operator's reclaim policy to Retain
	if err := hcc.reclaimPolicyManager.Sync(hc); err != nil {
		glog.Errorf("reclaimPolicyManager failed")
		return err
	}

	// cleaning all orphan pods(prophet or store which don't have a related PVC) managed by operator
	if _, err := hcc.orphanPodsCleaner.Clean(hc); err != nil {
		glog.Errorf("orphanPodsCleaner failed")
		return err
	}

	// works that should do to making the prophet cluster current state match the desired state:
	//   - create or update the prophet service
	//   - create or update the prophet headless service
	//   - create the prophet statefulset
	//   - sync prophet cluster status from prophet to HyenaCluster object
	//   - set two annotations to the first prophet member:
	// 	   - label.Bootstrapping
	// 	   - label.Replicas
	//   - upgrade the prophet cluster
	//   - scale out/in the prophet cluster
	//   - failover the prophet cluster
	if err := hcc.prophetMemberManager.Sync(hc); err != nil {
		glog.Errorf("prophetMemberManager failed")
		return err
	}

	// works that should do to making the store cluster current state match the desired state:
	//   - waiting for the prophet cluster available(prophet cluster is in quorum)
	//   - create or update store headless service
	//   - create the store statefulset
	//   - sync store cluster status from prophet to HyenaCluster object
	//   - set scheduler labels to store stores
	//   - upgrade the store cluster
	//   - scale out/in the store cluster
	//   - failover the store cluster
	if err := hcc.storeMemberManager.Sync(hc); err != nil {
		glog.Errorf("storeMemberManager failed")
		return err
	}

	// syncing the labels from Pod to PVC and PV, these labels include:
	//   - label.StoreIDLabelKey
	//   - label.MemberIDLabelKey
	//   - label.NamespaceLabelKey

	if err := hcc.metaManager.Sync(hc); err != nil {
		glog.Errorf("metaManager failed")
		return err
	}

	return nil
}
