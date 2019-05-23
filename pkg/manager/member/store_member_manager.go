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

	"github.com/golang/glog"
	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/controller"
	"github.com/infinivision/hyena-operator/pkg/label"
	"github.com/infinivision/hyena-operator/pkg/manager"
	"github.com/infinivision/hyena-operator/pkg/util"
	apps "k8s.io/api/apps/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/listers/apps/v1beta1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/apis"
)

// storeMemberManager implements manager.Manager.
type storeMemberManager struct {
	setControl                    controller.StatefulSetControlInterface
	svcControl                    controller.ServiceControlInterface
	prophetControl                controller.ProphetControlInterface
	setLister                     v1beta1.StatefulSetLister
	svcLister                     corelisters.ServiceLister
	podLister                     corelisters.PodLister
	nodeLister                    corelisters.NodeLister
	autoFailover                  bool
	storeFailover                 Failover
	storeScaler                   Scaler
	storeUpgrader                 Upgrader
	storeStatefulSetIsUpgradingFn func(corelisters.PodLister, controller.ProphetControlInterface, *apps.StatefulSet, *v1alpha1.HyenaCluster) (bool, error)
}

// NewStoreMemberManager returns a *storeMemberManager
func NewStoreMemberManager(prophetControl controller.ProphetControlInterface,
	setControl controller.StatefulSetControlInterface,
	svcControl controller.ServiceControlInterface,
	setLister v1beta1.StatefulSetLister,
	svcLister corelisters.ServiceLister,
	podLister corelisters.PodLister,
	nodeLister corelisters.NodeLister,
	autoFailover bool,
	storeFailover Failover,
	storeScaler Scaler,
	storeUpgrader Upgrader) manager.Manager {
	kvmm := storeMemberManager{
		prophetControl: prophetControl,
		podLister:      podLister,
		nodeLister:     nodeLister,
		setControl:     setControl,
		svcControl:     svcControl,
		setLister:      setLister,
		svcLister:      svcLister,
		autoFailover:   autoFailover,
		storeFailover:  storeFailover,
		storeScaler:    storeScaler,
		storeUpgrader:  storeUpgrader,
	}
	kvmm.storeStatefulSetIsUpgradingFn = storeStatefulSetIsUpgrading
	return &kvmm
}

// Sync fulfills the manager.Manager interface
func (stmm *storeMemberManager) Sync(hc *v1alpha1.HyenaCluster) error {
	ns := hc.GetNamespace()
	hcName := hc.GetName()

	if !hc.ProphetIsAvailable() {
		return controller.RequeueErrorf("HyenaCluster: [%s/%s], waiting for Prophet cluster running", ns, hcName)
	}

	// Sync Store Headless Service
	if err := stmm.syncStoreHeadlessServiceForHyenaCluster(hc); err != nil {
		return err
	}

	return stmm.syncStatefulSetForHyenaCluster(hc)
}

func (stmm *storeMemberManager) syncStoreHeadlessServiceForHyenaCluster(hc *v1alpha1.HyenaCluster) error {
	ns := hc.GetNamespace()
	hcName := hc.GetName()

	newSvc := stmm.getNewStoreHeadlessServiceForHyenaCluster(hc)
	oldSvc, err := stmm.svcLister.Services(ns).Get(controller.StorePeerName(hcName))
	if errors.IsNotFound(err) {
		err = SetServiceLastAppliedConfigAnnotation(newSvc)
		if err != nil {
			return err
		}
		return stmm.svcControl.CreateService(hc, newSvc)
	}
	if err != nil {
		return err
	}

	equal, err := serviceEqual(newSvc, oldSvc)
	if err != nil {
		return err
	}
	if !equal {
		svc := *oldSvc
		svc.Spec = newSvc.Spec
		// TODO add unit test
		svc.Spec.ClusterIP = oldSvc.Spec.ClusterIP
		err = SetServiceLastAppliedConfigAnnotation(newSvc)
		if err != nil {
			return err
		}
		_, err = stmm.svcControl.UpdateService(hc, &svc)
		return err
	}

	return nil
}

func (stmm *storeMemberManager) getNewStoreHeadlessServiceForHyenaCluster(hc *v1alpha1.HyenaCluster) *corev1.Service {
	ns := hc.Namespace
	hcName := hc.Name
	svcName := controller.StoreMemberName(hcName)
	instanceName := hc.GetLabels()[label.InstanceLabelKey]
	storeLabel := label.New().Instance(instanceName).Store().Labels()

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            svcName,
			Namespace:       ns,
			Labels:          storeLabel,
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(hc)},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Ports: []corev1.ServicePort{
				{
					Name:       "hyena-port",
					Port:       9527,
					TargetPort: intstr.FromInt(9527),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "raft-port",
					Port:       9528,
					TargetPort: intstr.FromInt(9528),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "prophet-port",
					Port:       9529,
					TargetPort: intstr.FromInt(9529),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Selector: storeLabel,
		},
	}
}

func (stmm *storeMemberManager) syncStatefulSetForHyenaCluster(hc *v1alpha1.HyenaCluster) error {
	ns := hc.GetNamespace()
	hcName := hc.GetName()

	newSet, err := stmm.getNewSetForHyenaCluster(hc)
	if err != nil {
		return err
	}

	oldSet, err := stmm.setLister.StatefulSets(ns).Get(controller.StoreMemberName(hcName))
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if errors.IsNotFound(err) {
		err = SetLastAppliedConfigAnnotation(newSet)
		if err != nil {
			return err
		}
		err = stmm.setControl.CreateStatefulSet(hc, newSet)
		if err != nil {
			return err
		}
		hc.Status.Store.StatefulSet = &apps.StatefulSetStatus{}
		return nil
	}

	if err := stmm.syncHyenaClusterStatus(hc, oldSet); err != nil {
		glog.Errorf("syncHyenaClusterStatus failed")
		return err
	}

	if !templateEqual(newSet.Spec.Template, oldSet.Spec.Template) || hc.Status.Store.Phase == v1alpha1.UpgradePhase {
		if err := stmm.storeUpgrader.Upgrade(hc, oldSet, newSet); err != nil {
			return err
		}
	}

	if *newSet.Spec.Replicas > *oldSet.Spec.Replicas {
		if err := stmm.storeScaler.ScaleOut(hc, oldSet, newSet); err != nil {
			return err
		}
	}

	if *newSet.Spec.Replicas < *oldSet.Spec.Replicas {
		if err := stmm.storeScaler.ScaleIn(hc, oldSet, newSet); err != nil {
			return err
		}
	}

	if stmm.autoFailover {
		if hc.StoreAllPodsStarted() && !hc.StoreAllStoresReady() {
			if err := stmm.storeFailover.Failover(hc); err != nil {
				return err
			}
		}
	}

	if !statefulSetEqual(*newSet, *oldSet) {
		set := *oldSet
		set.Spec.Template = newSet.Spec.Template
		*set.Spec.Replicas = *newSet.Spec.Replicas
		set.Spec.UpdateStrategy = newSet.Spec.UpdateStrategy
		err := SetLastAppliedConfigAnnotation(&set)
		if err != nil {
			return err
		}
		_, err = stmm.setControl.UpdateStatefulSet(hc, &set)
		glog.Errorf("UpdateStatefulSet failed")
		return err
	}

	return nil
}

func (stmm *storeMemberManager) getNewSetForHyenaCluster(hc *v1alpha1.HyenaCluster) (*apps.StatefulSet, error) {
	ns := hc.GetNamespace()
	hcName := hc.GetName()
	storeConfigMap := controller.StoreMemberName(hcName)
	annMount, annVolume := annotationsMountVolume()
	volMounts := []corev1.VolumeMount{
		annMount,
		{Name: controller.StoreMemberName(hcName), MountPath: "/data"},
		{Name: "config", ReadOnly: true, MountPath: "/etc/store"},
		{Name: "startup-script", ReadOnly: true, MountPath: "/usr/local/bin/startup"},
	}
	vols := []corev1.Volume{
		annVolume,
		{Name: "config", VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: storeConfigMap,
				},
				Items: []corev1.KeyToPath{{Key: "config-file", Path: "store.toml"}},
			}},
		},
		{Name: "startup-script", VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: storeConfigMap,
				},
				Items: []corev1.KeyToPath{{Key: "startup-script", Path: "store_start_script.sh"}},
			}},
		},
	}

	var q resource.Quantity
	var err error

	if hc.Spec.Store.Requests != nil {
		size := hc.Spec.Store.Requests.Storage
		q, err = resource.ParseQuantity(size)
		if err != nil {
			return nil, fmt.Errorf("cant' get storage size: %s for HyenaCluster: %s/%s, %v", size, ns, hcName, err)
		}
	}

	storeLabel := stmm.labelStore(hc)
	setName := controller.StoreMemberName(hcName)
	capacity := controller.StoreCapacity(hc.Spec.Store.Limits)
	storageClassName := hc.Spec.Store.StorageClassName
	if storageClassName == "" {
		storageClassName = controller.DefaultStorageClassName
	}

	storeset := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            setName,
			Namespace:       ns,
			Labels:          storeLabel.Labels(),
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(hc)},
		},
		Spec: apps.StatefulSetSpec{
			Replicas: func() *int32 { r := hc.StoreRealReplicas(); return &r }(),
			Selector: storeLabel.LabelSelector(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: storeLabel.Labels(),
				},
				Spec: corev1.PodSpec{
					SchedulerName: hc.Spec.SchedulerName,
					Affinity: util.AffinityForNodeSelector(
						ns,
						hc.Spec.Store.NodeSelectorRequired,
						storeLabel,
						hc.Spec.Store.NodeSelector,
					),
					Containers: []corev1.Container{
						{
							Name:            v1alpha1.StoreMemberType.String(),
							Image:           hc.Spec.Store.Image,
							Command:         []string{"/bin/sh", "/usr/local/bin/startup/store_start_script.sh"},
							ImagePullPolicy: hc.Spec.Store.ImagePullPolicy,
							Ports: []corev1.ContainerPort{
								{
									Name:          "hyena-port",
									ContainerPort: int32(9527),
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "raft-port",
									ContainerPort: int32(9528),
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "prophet-port",
									ContainerPort: int32(9529),
									Protocol:      corev1.ProtocolTCP,
								},
							},
							VolumeMounts: volMounts,
							Resources:    util.ResourceRequirement(hc.Spec.Store.ContainerSpec),
							Env: []corev1.EnvVar{
								{
									Name: "NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
								{
									Name:  "SERVICE_NAME",
									Value: controller.StoreMemberName(hcName),
								},
								{
									Name:  "CLUSTER_NAME",
									Value: hcName,
								},
								{
									Name:  "CAPACITY",
									Value: capacity,
								},
								{
									Name:  "TZ",
									Value: hc.Spec.Timezone,
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
					Tolerations:   hc.Spec.Store.Tolerations,
					Volumes:       vols,
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				stmm.volumeClaimTemplate(q, v1alpha1.StoreMemberType.String(), &storageClassName),
			},
			ServiceName:         controller.StoreMemberName(hcName),
			PodManagementPolicy: apps.ParallelPodManagement,
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &apps.RollingUpdateStatefulSetStrategy{
					Partition: func() *int32 { r := hc.StoreRealReplicas(); return &r }(),
				},
			},
		},
	}
	return storeset, nil
}

func (stmm *storeMemberManager) volumeClaimTemplate(q resource.Quantity, metaName string, storageClassName *string) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: metaName},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			StorageClassName: storageClassName,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: q,
				},
			},
		},
	}
}

func (stmm *storeMemberManager) labelStore(hc *v1alpha1.HyenaCluster) label.Label {
	instanceName := hc.GetLabels()[label.InstanceLabelKey]
	return label.New().Instance(instanceName).Store()
}

func (stmm *storeMemberManager) syncHyenaClusterStatus(hc *v1alpha1.HyenaCluster, set *apps.StatefulSet) error {
	hc.Status.Store.StatefulSet = &set.Status
	upgrading, err := stmm.storeStatefulSetIsUpgradingFn(stmm.podLister, stmm.prophetControl, set, hc)
	if err != nil {
		glog.Errorf("storeStatefulSetIsUpgradingFn failed")
		return err
	}
	if upgrading && hc.Status.Prophet.Phase != v1alpha1.UpgradePhase {
		hc.Status.Store.Phase = v1alpha1.UpgradePhase
	} else {
		hc.Status.Store.Phase = v1alpha1.NormalPhase
	}

	// previousStores := hc.Status.Store.Stores
	// stores := map[string]v1alpha1.Store{}
	// tombstoneStores := map[string]v1alpha1.Store{}

	// pdCli := stmm.pdControl.GetPDClient(cc)
	// // This only returns Up/Down/Offline stores
	// storeInfo, err := pdCli.GetStores()
	// if err != nil {
	// 	glog.Errorf("storeMemberManager GetStores failed")
	// 	cc.Status.Store.Synced = false
	// 	return err
	// }

	// for _, store := range storeInfo.Stores {
	// 	status := stmm.getKVStore(store)
	// 	if status == nil {
	// 		continue
	// 	}
	// 	// avoid LastHeartbeatTime be overwrite by zero time when pd lost LastHeartbeatTime
	// 	if status.LastHeartbeatTime.IsZero() {
	// 		if oldStatus, ok := previousStores[status.ID]; ok {
	// 			glog.V(4).Infof("the pod:%s's store LastHeartbeatTime is zero,so will keep in %v", status.IP, oldStatus.LastHeartbeatTime)
	// 			status.LastHeartbeatTime = oldStatus.LastHeartbeatTime
	// 		}
	// 	}

	// 	oldStore, exist := previousStores[status.ID]
	// 	if exist {
	// 		status.LastTransitionTime = oldStore.LastTransitionTime
	// 	}
	// 	if !exist || status.State != oldStore.State {
	// 		status.LastTransitionTime = metav1.Now()
	// 	}

	// 	stores[status.ID] = *status
	// }

	// //this returns all tombstone stores
	// tombstoneStoresInfo, err := pdCli.GetTombStoneStores()
	// if err != nil {
	// 	cc.Status.Store.Synced = false
	// 	return err
	// }
	// for _, store := range tombstoneStoresInfo.Stores {
	// 	status := stmm.getKVStore(store)
	// 	if status == nil {
	// 		continue
	// 	}
	// 	tombstoneStores[status.ID] = *status
	// }

	hc.Status.Store.Synced = true
	// hc.Status.Store.Stores = stores
	// cc.Status.Store.TombstoneStores = tombstoneStores
	return nil
}

// func (stmm *storeMemberManager) getStore(store *pdapi.StoreInfo) *v1alpha1.Store {
// 	if store == nil || store.Status == nil {
// 		return nil
// 	}
// 	storeID := fmt.Sprintf("%d", store.Meta.ID)
// 	ip := strings.Split(store.Meta.Address, ":")[0]
// 	// podName := strings.Split(ip, ".")[0]

// 	heartBeatTime := time.Time{}
// 	if store.Status.LastHeartbeatTS > 0 {
// 		heartBeatTime = time.Unix(store.Status.LastHeartbeatTS, 0)
// 	}

// 	state := v1alpha1.StoreStateUp
// 	if store.Meta.State == 1 {
// 		state = v1alpha1.StoreStateDown
// 	} else if store.Meta.State == 2 {
// 		state = v1alpha1.StoreStateTombstone
// 	}

// 	return &v1alpha1.Store{
// 		ID: storeID,
// 		// PodName:           podName,
// 		IP:                ip,
// 		LeaderCount:       int32(store.Status.LeaderCount),
// 		State:             state,
// 		LastHeartbeatTime: metav1.Time{Time: heartBeatTime},
// 	}
// }

func (stmm *storeMemberManager) setStoreLabelsForStore(hc *v1alpha1.HyenaCluster) (int, error) {
	// ns := hc.GetNamespace()
	// for unit test
	setCount := 0

	// pdCli := stmm.pdControl.GetPDClient(cc)
	// storesInfo, err := pdCli.GetStores()
	// if err != nil {
	// 	return setCount, err
	// }

	// for _, store := range storesInfo.Stores {
	// 	glog.Infof("store: %v", store)
	// 	status := stmm.getKVStore(store)
	// 	glog.Infof("status: %v", status)
	// 	if status == nil {
	// 		continue
	// 	}
	// 	podName := status.PodName

	// 	pod, err := stmm.podLister.Pods(ns).Get(podName)
	// 	if err != nil {
	// 		return setCount, err
	// 	}

	// 	nodeName := pod.Spec.NodeName
	// 	ls, err := stmm.getNodeLabels(nodeName)
	// 	if err != nil {
	// 		glog.Warningf("node: [%s] has no node labels, skipping set store labels for Pod: [%s/%s]", nodeName, ns, podName)
	// 		continue
	// 	}
	// 	if !stmm.storeLabelsEqualNodeLabels(store.Meta.Lables, ls) {
	// 		set, err := pdCli.SetStoreLabels(store.Meta.ID, ls)
	// 		if err != nil {
	// 			glog.Warningf("failed to set pod: [%s/%s]'s store labels: %v", ns, podName, ls)
	// 			continue
	// 		}
	// 		if set {
	// 			setCount++
	// 			glog.Infof("pod: [%s/%s] set labels: %v successfully", ns, podName, ls)
	// 		}
	// 	}
	// }

	return setCount, nil
}

func (stmm *storeMemberManager) getNodeLabels(nodeName string) (map[string]string, error) {
	node, err := stmm.nodeLister.Get(nodeName)
	if err != nil {
		return nil, err
	}
	if ls := node.GetLabels(); ls != nil {
		labels := map[string]string{}
		if region, found := ls["region"]; found {
			labels["region"] = region
		}
		if zone, found := ls["zone"]; found {
			labels["zone"] = zone
		}
		if rack, found := ls["rack"]; found {
			labels["rack"] = rack
		}
		if host, found := ls[apis.LabelHostname]; found {
			labels["host"] = host
		}
		return labels, nil
	}
	return nil, fmt.Errorf("labels not found")
}

// storeLabelsEqualNodeLabels compares store labels with node labels
// for historic reasons, PD stores Store labels as []*StoreLabel which is a key-value pair slice
// func (stmm *storeMemberManager) storeLabelsEqualNodeLabels(storeLabels []metapb.Label, nodeLabels map[string]string) bool {
// 	ls := map[string]string{}
// 	for _, label := range storeLabels {
// 		key := label.GetKey()
// 		if _, ok := nodeLabels[key]; ok {
// 			val := label.GetValue()
// 			ls[key] = val
// 		}
// 	}
// 	return reflect.DeepEqual(ls, nodeLabels)
// }

func storeStatefulSetIsUpgrading(podLister corelisters.PodLister, prophetControl controller.ProphetControlInterface, set *apps.StatefulSet, hc *v1alpha1.HyenaCluster) (bool, error) {
	if statefulSetIsUpgrading(set) {
		return true, nil
	}
	instanceName := hc.GetLabels()[label.InstanceLabelKey]
	selector, err := label.New().Instance(instanceName).Store().Selector()
	if err != nil {
		return false, err
	}
	storePods, err := podLister.Pods(hc.GetNamespace()).List(selector)
	if err != nil {
		return false, err
	}
	for _, pod := range storePods {
		revisionHash, exist := pod.Labels[apps.ControllerRevisionHashLabelKey]
		if !exist {
			return false, nil
		}
		if revisionHash != hc.Status.Store.StatefulSet.UpdateRevision {
			return true, nil
		}
	}
	return false, nil
}

type FakeStoreMemberManager struct {
	err error
}

func NewFakeStoreMemberManager() *FakeStoreMemberManager {
	return &FakeStoreMemberManager{}
}

func (ftmm *FakeStoreMemberManager) SetSyncError(err error) {
	ftmm.err = err
}

func (ftmm *FakeStoreMemberManager) Sync(_ *v1alpha1.HyenaCluster) error {
	if ftmm.err != nil {
		return ftmm.err
	}
	return nil
}
