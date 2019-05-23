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

package v1alpha1

import (
	apps "k8s.io/api/apps/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// StoreStateUp represents status of Up of Hyena Store
	StoreStateUp string = "Up"
	// StoreStateDown represents status of Down of Hyena Store
	StoreStateDown string = "Down"
	// StoreStateOffline represents status of Offline of Hyena Store
	StoreStateOffline string = "Offline"
	// StoreStateTombstone represents status of Tombstone of Hyena Store
	StoreStateTombstone string = "Tombstone"
)

// MemberType represents member type
type MemberType string

const (
	// ProphetMemberType is hyena prophet+store container type
	ProphetMemberType MemberType = "prophet"
	// StoreMemberType is hyena store container type
	StoreMemberType MemberType = "store"
	// UnknownMemberType is unknown container type
	UnknownMemberType MemberType = "unknown"
)

// MemberPhase is the current state of member
type MemberPhase string

const (
	// NormalPhase represents normal state of hyena cluster.
	NormalPhase MemberPhase = "Normal"
	// UpgradePhase represents the upgrade state of hyena cluster.
	UpgradePhase MemberPhase = "Upgrade"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HyenaCluster is the control script's spec
type HyenaCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	// Spec defines the behavior of a hyena cluster
	Spec HyenaClusterSpec `json:"spec"`

	// Most recently observed status of the hyena cluster
	Status HyenaClusterStatus `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HyenaClusterList is HyenaCluster list
type HyenaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []HyenaCluster `json:"items"`
}

// HyenaClusterSpec describes the attributes that a user creates on a hyena cluster
type HyenaClusterSpec struct {
	SchedulerName string      `json:"schedulerName,omitempty"`
	Prophet       ProphetSpec `json:"prophet,omitempty"`
	Store         StoreSpec   `json:"store,omitempty"`
	// Services list non-headless services type used in HyenaCluster
	Services        []Service                            `json:"services,omitempty"`
	PVReclaimPolicy corev1.PersistentVolumeReclaimPolicy `json:"pvReclaimPolicy,omitempty"`
	Timezone        string                               `json:"timezone,omitempty"`
}

// HyenaClusterStatus represents the current status of a hyena cluster.
type HyenaClusterStatus struct {
	ClusterID string        `json:"clusterID,omitempty"`
	Prophet   ProphetStatus `json:"prophet,omitempty"`
	Store     StoreStatus   `json:"store,omitempty"`
}

// ProphetSpec contains details of Prophet member
type ProphetSpec struct {
	ContainerSpec
	Replicas             int32               `json:"replicas"`
	NodeSelector         map[string]string   `json:"nodeSelector,omitempty"`
	NodeSelectorRequired bool                `json:"nodeSelectorRequired,omitempty"`
	StorageClassName     string              `json:"storageClassName,omitempty"`
	Tolerations          []corev1.Toleration `json:"tolerations,omitempty"`
}

// StoreSpec contains details of hyena store member
type StoreSpec struct {
	ContainerSpec
	Replicas             int32               `json:"replicas"`
	NodeSelector         map[string]string   `json:"nodeSelector,omitempty"`
	NodeSelectorRequired bool                `json:"nodeSelectorRequired,omitempty"`
	StorageClassName     string              `json:"storageClassName,omitempty"`
	Tolerations          []corev1.Toleration `json:"tolerations,omitempty"`
}

// ContainerSpec is the container spec of a pod
type ContainerSpec struct {
	Image           string               `json:"image"`
	ImagePullPolicy corev1.PullPolicy    `json:"imagePullPolicy,omitempty"`
	Requests        *ResourceRequirement `json:"requests,omitempty"`
	Limits          *ResourceRequirement `json:"limits,omitempty"`
}

// Service represent service type used in HyenaCluster
type Service struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
}

// ResourceRequirement is resource requirements for a pod
type ResourceRequirement struct {
	// CPU is how many cores a pod requires
	CPU string `json:"cpu,omitempty"`
	// Memory is how much memory a pod requires
	Memory string `json:"memory,omitempty"`
	// Storage is storage size a pod requires
	Storage string `json:"storage,omitempty"`
}

// ProphetStatus is Prophet status
type ProphetStatus struct {
	Synced         bool                            `json:"synced,omitempty"`
	Phase          MemberPhase                     `json:"phase,omitempty"`
	StatefulSet    *apps.StatefulSetStatus         `json:"statefulSet,omitempty"`
	Members        map[string]ProphetMember        `json:"members,omitempty"`
	Leader         ProphetMember                   `json:"leader,omitempty"`
	FailureMembers map[string]ProphetFailureMember `json:"failureMembers,omitempty"`
}

// ProphetMember is hyena prophet member
type ProphetMember struct {
	Name string `json:"name"`
	// member id is actually a uint64, but apimachinery's json only treats numbers as int64/float64
	// so uint64 may overflow int64 and thus convert to float64
	ID        string `json:"id"`
	ClientURL string `json:"clientURL"`
	Health    bool   `json:"health"`
	// Last time the health transitioned from one to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}

// ProphetFailureMember is the hyena prophet failure member information
type ProphetFailureMember struct {
	PodName       string    `json:"podName,omitempty"`
	MemberID      string    `json:"memberID,omitempty"`
	PVCUID        types.UID `json:"pvcUID,omitempty"`
	MemberDeleted bool      `json:"memberDeleted,omitempty"`
}

// StoreStatus is hyena store status
type StoreStatus struct {
	Synced          bool                    `json:"synced,omitempty"`
	Phase           MemberPhase             `json:"phase,omitempty"`
	StatefulSet     *apps.StatefulSetStatus `json:"statefulSet,omitempty"`
	Stores          map[string]Store        `json:"stores,omitempty"`
	TombstoneStores map[string]Store        `json:"tombstoneStores,omitempty"`
	FailureStores   map[string]FailureStore `json:"failureStores,omitempty"`
}

// Store is either Up/Down/Offline
type Store struct {
	// store id is also uint64, due to the same reason as pd id, we store id as string
	ID                string      `json:"id"`
	PodName           string      `json:"podName"`
	IP                string      `json:"ip"`
	LeaderCount       int32       `json:"leaderCount"`
	State             string      `json:"state"`
	LastHeartbeatTime metav1.Time `json:"lastHeartbeatTime"`
	// Last time the health transitioned from one to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}

// FailureStore is the store failure store information
type FailureStore struct {
	PodName string `json:"podName,omitempty"`
	StoreID string `json:"storeID,omitempty"`
}
