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
)

type prophetMemberManager struct {
	prophetControl  controller.ProphetControlInterface
	setControl      controller.StatefulSetControlInterface
	svcControl      controller.ServiceControlInterface
	setLister       v1beta1.StatefulSetLister
	svcLister       corelisters.ServiceLister
	podLister       corelisters.PodLister
	podControl      controller.PodControlInterface
	pvcLister       corelisters.PersistentVolumeClaimLister
	prophetUpgrader Upgrader
}

// NewProphetMemberManager returns a *pdMemberManager
func NewProphetMemberManager(prophetControl controller.ProphetControlInterface,
	setControl controller.StatefulSetControlInterface,
	svcControl controller.ServiceControlInterface,
	setLister v1beta1.StatefulSetLister,
	svcLister corelisters.ServiceLister,
	podLister corelisters.PodLister,
	podControl controller.PodControlInterface,
	pvcLister corelisters.PersistentVolumeClaimLister,
	prophetUpgrader Upgrader) manager.Manager {
	return &prophetMemberManager{
		prophetControl,
		setControl,
		svcControl,
		setLister,
		svcLister,
		podLister,
		podControl,
		pvcLister,
		prophetUpgrader}
}

func (pdmm *prophetMemberManager) Sync(hc *v1alpha1.HyenaCluster) error {
	// // Sync Prophet Service
	// if err := pdmm.syncProphetServiceForHyenaCluster(hc); err != nil {
	// 	return err
	// }

	// Sync Prophet Headless Service
	if err := pdmm.syncProphetHeadlessServiceForHyenaCluster(hc); err != nil {
		return err
	}

	// Sync Prophet StatefulSet
	return pdmm.syncProphetStatefulSetForHyenaCluster(hc)
}

// func (pdmm *prophetMemberManager) syncProphetServiceForHyenaCluster(hc *v1alpha1.HyenaCluster) error {
// 	ns := hc.GetNamespace()
// 	hcName := hc.GetName()

// 	newSvc := pdmm.getNewProphetServiceForHyenaCluster(hc)
// 	oldSvc, err := pdmm.svcLister.Services(ns).Get(controller.ProphetMemberName(hcName))
// 	if errors.IsNotFound(err) {
// 		err = SetServiceLastAppliedConfigAnnotation(newSvc)
// 		if err != nil {
// 			return err
// 		}
// 		return pdmm.svcControl.CreateService(hc, newSvc)
// 	}
// 	if err != nil {
// 		return err
// 	}

// 	equal, err := serviceEqual(newSvc, oldSvc)
// 	if err != nil {
// 		return err
// 	}
// 	if !equal {
// 		svc := *oldSvc
// 		svc.Spec = newSvc.Spec
// 		// TODO add unit test
// 		svc.Spec.ClusterIP = oldSvc.Spec.ClusterIP
// 		err = SetServiceLastAppliedConfigAnnotation(&svc)
// 		if err != nil {
// 			return err
// 		}
// 		_, err = pdmm.svcControl.UpdateService(hc, &svc)
// 		return err
// 	}

// 	return nil
// }

func (pdmm *prophetMemberManager) syncProphetHeadlessServiceForHyenaCluster(hc *v1alpha1.HyenaCluster) error {
	ns := hc.GetNamespace()
	hcName := hc.GetName()

	newSvc := pdmm.getNewProphetHeadlessServiceForHyenaCluster(hc)
	oldSvc, err := pdmm.svcLister.Services(ns).Get(controller.ProphetMemberName(hcName))
	if errors.IsNotFound(err) {
		err = SetServiceLastAppliedConfigAnnotation(newSvc)
		if err != nil {
			return err
		}
		return pdmm.svcControl.CreateService(hc, newSvc)
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
		_, err = pdmm.svcControl.UpdateService(hc, &svc)
		return err
	}

	return nil
}

func (pdmm *prophetMemberManager) syncProphetStatefulSetForHyenaCluster(hc *v1alpha1.HyenaCluster) error {
	ns := hc.GetNamespace()
	hcName := hc.GetName()

	newProphetSet, err := pdmm.getNewProphetSetForHyenaCluster(hc)
	if err != nil {
		return err
	}

	oldProphetSet, err := pdmm.setLister.StatefulSets(ns).Get(controller.ProphetMemberName(hcName))
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if errors.IsNotFound(err) {
		err = SetLastAppliedConfigAnnotation(newProphetSet)
		if err != nil {
			return err
		}
		if err := pdmm.setControl.CreateStatefulSet(hc, newProphetSet); err != nil {
			return err
		}
		hc.Status.Prophet.StatefulSet = &apps.StatefulSetStatus{}
		return controller.RequeueErrorf("hyenaCluster: [%s/%s], waiting for prophet cluster running", ns, hcName)
	}

	if err := pdmm.syncHyenaClusterStatus(hc, oldProphetSet); err != nil {
		glog.Errorf("failed to sync HyenaCluster: [%s/%s]'s status, error: %v", ns, hcName, err)
	}

	if !templateEqual(newProphetSet.Spec.Template, oldProphetSet.Spec.Template) || hc.Status.Prophet.Phase == v1alpha1.UpgradePhase {
		if err := pdmm.prophetUpgrader.Upgrade(hc, oldProphetSet, newProphetSet); err != nil {
			return err
		}
	}

	if *newProphetSet.Spec.Replicas != *oldProphetSet.Spec.Replicas {
		glog.Errorf("failed to sync HyenaCluster: [%s/%s]'s status, prophet doesn't support scale now! ", ns, hcName)
	}

	// TODO FIXME equal is false every time
	if !statefulSetEqual(*newProphetSet, *oldProphetSet) {
		set := *oldProphetSet
		set.Spec.Template = newProphetSet.Spec.Template
		*set.Spec.Replicas = *newProphetSet.Spec.Replicas
		set.Spec.UpdateStrategy = newProphetSet.Spec.UpdateStrategy
		err := SetLastAppliedConfigAnnotation(&set)
		if err != nil {
			return err
		}
		_, err = pdmm.setControl.UpdateStatefulSet(hc, &set)
		return err
	}

	return nil
}

func (pdmm *prophetMemberManager) syncHyenaClusterStatus(hc *v1alpha1.HyenaCluster, set *apps.StatefulSet) error {
	// ns := hc.GetNamespace()
	// hcName := hc.GetName()

	hc.Status.Prophet.StatefulSet = &set.Status
	upgrading, err := pdmm.prophetStatefulSetIsUpgrading(set, hc)
	if err != nil {
		return err
	}
	if upgrading {
		hc.Status.Prophet.Phase = v1alpha1.UpgradePhase
	} else {
		hc.Status.Prophet.Phase = v1alpha1.NormalPhase
	}

	// prophetStatus := map[string]v1alpha1.ProphetMember{}
	// for _, memberHealth := range healthInfo.Healths {
	// 	id := memberHealth.MemberID
	// 	memberID := fmt.Sprintf("%d", id)
	// 	var clientURL string
	// 	if len(memberHealth.ClientUrls) > 0 {
	// 		clientURL = memberHealth.ClientUrls[0]
	// 	}
	// 	name := memberHealth.Name
	// 	if len(name) == 0 {
	// 		glog.Warningf("Prophet member: [%d] doesn't have a name, and can't get it from clientUrls: [%s], memberHealth Info: [%v] in [%s/%s]",
	// 			id, memberHealth.ClientUrls, memberHealth, ns, hcName)
	// 		continue
	// 	}

	// 	status := v1alpha1.ProphetMember{
	// 		Name:      name,
	// 		ID:        memberID,
	// 		ClientURL: clientURL,
	// 		Health:    memberHealth.Health,
	// 	}

	// 	oldProphetMember, exist := hc.Status.Prophet.Members[name]
	// 	if exist {
	// 		status.LastTransitionTime = oldProphetMember.LastTransitionTime
	// 	}
	// 	if !exist || status.Health != oldProphetMember.Health {
	// 		status.LastTransitionTime = metav1.Now()
	// 	}

	// 	prophetStatus[name] = status
	// }

	hc.Status.Prophet.Synced = true
	// hc.Status.Prophet.Members = prophetStatus
	// hc.Status.Prophet.Leader = v1alpha1.ProphetMember{}

	return nil
}

func (pdmm *prophetMemberManager) getNewProphetHeadlessServiceForHyenaCluster(hc *v1alpha1.HyenaCluster) *corev1.Service {
	ns := hc.Namespace
	hcName := hc.Name
	svcName := controller.ProphetMemberName(hcName)
	instanceName := hc.GetLabels()[label.InstanceLabelKey]
	prophetLabel := label.New().Instance(instanceName).Prophet().Labels()

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            svcName,
			Namespace:       ns,
			Labels:          prophetLabel,
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(hc)},
		},
		Spec: corev1.ServiceSpec{
			Type: controller.GetServiceType(hc.Spec.Services, v1alpha1.ProphetMemberType.String()),
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
				{
					Name:       "prophet-client-port",
					Port:       2371,
					TargetPort: intstr.FromInt(2371),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "prophet-peer-port",
					Port:       2381,
					TargetPort: intstr.FromInt(2381),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Selector: prophetLabel,
		},
	}
}

func (pdmm *prophetMemberManager) prophetStatefulSetIsUpgrading(set *apps.StatefulSet, hc *v1alpha1.HyenaCluster) (bool, error) {
	if statefulSetIsUpgrading(set) {
		return true, nil
	}
	selector, err := label.New().
		Instance(hc.GetLabels()[label.InstanceLabelKey]).
		Prophet().
		Selector()
	if err != nil {
		return false, err
	}
	prophetPods, err := pdmm.podLister.Pods(hc.GetNamespace()).List(selector)
	if err != nil {
		return false, err
	}
	for _, pod := range prophetPods {
		revisionHash, exist := pod.Labels[apps.ControllerRevisionHashLabelKey]
		if !exist {
			return false, nil
		}
		if revisionHash != hc.Status.Prophet.StatefulSet.UpdateRevision {
			return true, nil
		}
	}
	return false, nil
}

func (pdmm *prophetMemberManager) getNewProphetSetForHyenaCluster(hc *v1alpha1.HyenaCluster) (*apps.StatefulSet, error) {
	ns := hc.Namespace
	hcName := hc.Name
	instanceName := hc.GetLabels()[label.InstanceLabelKey]
	pdConfigMap := controller.ProphetMemberName(hcName)

	annMount, annVolume := annotationsMountVolume()
	volMounts := []corev1.VolumeMount{
		annMount,
		{Name: controller.ProphetMemberName(hcName), MountPath: "/data"},
		{Name: "config", ReadOnly: true, MountPath: "/etc/prophet"},
		{Name: "startup-script", ReadOnly: true, MountPath: "/usr/local/bin/startup"},
	}
	vols := []corev1.Volume{
		annVolume,
		{Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: pdConfigMap,
					},
					Items: []corev1.KeyToPath{{Key: "config-file", Path: "prophet.toml"}},
				},
			},
		},
		{Name: "startup-script",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: pdConfigMap,
					},
					Items: []corev1.KeyToPath{{Key: "startup-script", Path: "prophet_start_script.sh"}},
				},
			},
		},
	}

	var q resource.Quantity
	var err error
	if hc.Spec.Prophet.Requests != nil {
		size := hc.Spec.Prophet.Requests.Storage
		q, err = resource.ParseQuantity(size)
		if err != nil {
			return nil, fmt.Errorf("cant' get storage size: %s for HyenaCluster: %s/%s, %v", size, ns, hcName, err)
		}
	}
	pdLabel := label.New().Instance(instanceName).Prophet()
	setName := controller.ProphetMemberName(hcName)
	storageClassName := hc.Spec.Prophet.StorageClassName
	if storageClassName == "" {
		storageClassName = controller.DefaultStorageClassName
	}
	failureReplicas := 0
	for _, failureMember := range hc.Status.Prophet.FailureMembers {
		if failureMember.MemberDeleted {
			failureReplicas++
		}
	}

	pdSet := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            setName,
			Namespace:       ns,
			Labels:          pdLabel.Labels(),
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(hc)},
		},
		Spec: apps.StatefulSetSpec{
			Replicas: func() *int32 { r := hc.Spec.Prophet.Replicas + int32(failureReplicas); return &r }(),
			Selector: pdLabel.LabelSelector(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: pdLabel.Labels(),
				},
				Spec: corev1.PodSpec{
					SchedulerName: hc.Spec.SchedulerName,
					Affinity: util.AffinityForNodeSelector(
						ns,
						hc.Spec.Prophet.NodeSelectorRequired,
						label.New().Instance(instanceName).Prophet(),
						hc.Spec.Prophet.NodeSelector,
					),
					Containers: []corev1.Container{
						{
							Name:            v1alpha1.ProphetMemberType.String(),
							Image:           hc.Spec.Prophet.Image,
							Command:         []string{"/bin/sh", "/usr/local/bin/startup/prophet_start_script.sh"},
							ImagePullPolicy: hc.Spec.Prophet.ImagePullPolicy,
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
								{
									Name:          "prophet-client-port",
									ContainerPort: int32(2371),
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "prophet-peer-port",
									ContainerPort: int32(2381),
									Protocol:      corev1.ProtocolTCP,
								},
							},
							VolumeMounts: volMounts,
							Resources:    util.ResourceRequirement(hc.Spec.Prophet.ContainerSpec),
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
									Value: controller.ProphetMemberName(hcName),
								},
								{
									Name:  "TZ",
									Value: hc.Spec.Timezone,
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
					Tolerations:   hc.Spec.Prophet.Tolerations,
					Volumes:       vols,
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: v1alpha1.ProphetMemberType.String(),
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						StorageClassName: &storageClassName,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: q,
							},
						},
					},
				},
			},
			ServiceName:         controller.ProphetMemberName(hcName),
			PodManagementPolicy: apps.ParallelPodManagement,
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &apps.RollingUpdateStatefulSetStrategy{
					Partition: func() *int32 { r := hc.Spec.Prophet.Replicas + int32(failureReplicas); return &r }(),
				}},
		},
	}

	return pdSet, nil
}

type FakeProphetMemberManager struct {
	err error
}

func NewFakeProphetMemberManager() *FakeProphetMemberManager {
	return &FakeProphetMemberManager{}
}

func (fpmm *FakeProphetMemberManager) SetSyncError(err error) {
	fpmm.err = err
}

func (fpmm *FakeProphetMemberManager) Sync(_ *v1alpha1.HyenaCluster) error {
	if fpmm.err != nil {
		return fpmm.err
	}
	return nil
}
