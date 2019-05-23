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

func (mt MemberType) String() string {
	return string(mt)
}

func (c *HyenaCluster) ProphetUpgrading() bool {
	return c.Status.Prophet.Phase == UpgradePhase
}

func (c *HyenaCluster) StoreUpgrading() bool {
	return c.Status.Store.Phase == UpgradePhase
}

func (c *HyenaCluster) ProphetAllPodsStarted() bool {
	return c.ProphetRealReplicas() == c.Status.Prophet.StatefulSet.Replicas
}

func (c *HyenaCluster) ProphetAllMembersReady() bool {
	if int(c.ProphetRealReplicas()) != len(c.Status.Prophet.Members) {
		return false
	}

	for _, member := range c.Status.Prophet.Members {
		if !member.Health {
			return false
		}
	}
	return true
}

func (c *HyenaCluster) ProphetAutoFailovering() bool {
	if len(c.Status.Prophet.FailureMembers) == 0 {
		return false
	}

	for _, failureMember := range c.Status.Prophet.FailureMembers {
		if !failureMember.MemberDeleted {
			return true
		}
	}
	return false
}

func (c *HyenaCluster) ProphetRealReplicas() int32 {
	return c.Spec.Prophet.Replicas + int32(len(c.Status.Prophet.FailureMembers))
}

func (c *HyenaCluster) StoreAllPodsStarted() bool {
	return c.StoreRealReplicas() == c.Status.Store.StatefulSet.Replicas
}

func (c *HyenaCluster) StoreAllStoresReady() bool {
	if int(c.StoreRealReplicas()) != len(c.Status.Store.Stores) {
		return false
	}

	for _, store := range c.Status.Store.Stores {
		if store.State != StoreStateUp {
			return false
		}
	}

	return true
}

func (c *HyenaCluster) StoreRealReplicas() int32 {
	return c.Spec.Store.Replicas + int32(len(c.Status.Store.FailureStores))
}

func (c *HyenaCluster) ProphetIsAvailable() bool {
	lowerLimit := c.Spec.Prophet.Replicas/2 + 1
	if int32(len(c.Status.Prophet.Members)) < lowerLimit {
		return false
	}

	var availableNum int32
	for _, prophetMember := range c.Status.Prophet.Members {
		if prophetMember.Health {
			availableNum++
		}
	}

	if availableNum < lowerLimit {
		return false
	}

	if c.Status.Prophet.StatefulSet == nil || c.Status.Prophet.StatefulSet.ReadyReplicas < lowerLimit {
		return false
	}

	return true
}

func (c *HyenaCluster) StoreIsAvailable() bool {
	var lowerLimit int32 = 1
	if int32(len(c.Status.Store.Stores)) < lowerLimit {
		return false
	}

	var availableNum int32
	for _, store := range c.Status.Store.Stores {
		if store.State == StoreStateUp {
			availableNum++
		}
	}

	if availableNum < lowerLimit {
		return false
	}

	if c.Status.Store.StatefulSet == nil || c.Status.Store.StatefulSet.ReadyReplicas < lowerLimit {
		return false
	}

	return true
}

func (c *HyenaCluster) GetClusterID() string {
	return c.Status.ClusterID
}
