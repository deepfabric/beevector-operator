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

package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
)

const (
	timeout = 5 * time.Second
	// to do check time.Second
	MaxStoreDownTimeDuration = 3600 * 1000 * 1000 * 1000
	// to do which state is tombstone
	TombStoneStoreState = 1
)

// PDControlInterface is an interface that knows how to manage and get hyena cluster's Prophet client
type ProphetControlInterface interface {
	// GetProphetClient provides ProphetClient of the hyena cluster.
	GetProphetClient(hc *v1alpha1.HyenaCluster) ProphetClient
}

// defaultProphetControl is the default implementation of ProphetControlInterface.
type defaultProphetControl struct {
	mutex          sync.Mutex
	prophetClients map[string]ProphetClient
}

// NewDefaultProphetControl returns a defaultProphetControl instance
func NewDefaultProphetControl() ProphetControlInterface {
	return &defaultProphetControl{prophetClients: map[string]ProphetClient{}}
}

// GetProphetClient provides a GetProphetClient of real prophet cluster,if the GetProphetClient not existing, it will create new one.
func (pdc *defaultProphetControl) GetProphetClient(hc *v1alpha1.HyenaCluster) ProphetClient {
	pdc.mutex.Lock()
	defer pdc.mutex.Unlock()
	namespace := hc.GetNamespace()
	hcName := hc.GetName()
	key := prophetClientKey(namespace, hcName)
	if _, ok := pdc.prophetClients[key]; !ok {
		pdc.prophetClients[key] = NewProphetClient(prophetClientUrl(namespace, hcName), timeout)
	}
	return pdc.prophetClients[key]
}

// prophetClientKey returns the prophet client key
func prophetClientKey(namespace, clusterName string) string {
	return fmt.Sprintf("%s.%s", clusterName, namespace)
}

// prophetClientUrl builds the url of prophet client
func prophetClientUrl(namespace, clusterName string) string {
	return fmt.Sprintf("http://%s-prophet.%s:2379", clusterName, namespace)
}

// ProphetClient provides prophet server's api
type ProphetClient interface {
	// GetHealth returns the Prophet's health info
	GetHealth() (*HealthInfo, error)
	// GetConfig returns PD's config
	GetConfig() (time.Duration, error)
	// GetCluster returns used when syncing pod labels.
	GetCluster() (string, error)
	// GetMembers returns all Prophet members from cluster
	GetMembers() (*MembersInfo, error)
	// DeleteMember deletes a Prophet member from cluster
	DeleteMember(name string) error
	// DeleteMemberByID deletes a Prophet member from cluster
	DeleteMemberByID(memberID uint64) error
	// GetStores lists all stores from cluster
	GetStores() (*StoresInfo, error)
	// GetTombStoneStores lists all tombstone stores from cluster
	GetTombStoneStores() (*StoresInfo, error)
	// GetStore gets a store for a specific store id from cluster
	// GetStore(storeID uint64) (*pdapi.StoreInfo, error)
	// storeLabelsEqualNodeLabels compares store labels with node labels
	SetStoreLabels(storeID uint64, labels map[string]string) (bool, error)
	// DeleteStore deletes a store from cluster
	DeleteStore(storeID uint64) error

	// BeginEvictLeader initiates leader eviction for a storeID.
	// This is used when upgrading a pod.
	BeginEvictLeader(storeID uint64) error
	// EndEvictLeader is used at the end of pod upgrade.
	EndEvictLeader(storeID uint64) error
	// GetEvictLeaderSchedulers gets schedulers of evict leader
	GetEvictLeaderSchedulers() ([]string, error)
	// GetPDLeader returns pd leader
	GetProphetLeader() (*ProphetMember, error)
	// TransferProphetLeader transfers prophet leader to specified member
	TransferProphetLeader(name string) error
}

var (
	healthPrefix  = "pd/health"
	membersPrefix = "pd/api/v1/members"
	storesPrefix  = "pd/api/v1/stores"
	// configPrefix           = "pd/api/v1/config"
	// clusterIDPrefix        = "pd/api/v1/cluster"
	// schedulersPrefix       = "pd/api/v1/schedulers"
	// pdLeaderPrefix         = "pd/api/v1/leader"
	// pdLeaderTransferPrefix = "pd/api/v1/leader/transfer"
)

// prophetClient is default implementation of prophetClient
type prophetClient struct {
	url        string
	endpoint   string
	httpClient *http.Client
}

// NewProphetClient returns a new ProphetClient
func NewProphetClient(url string, timeout time.Duration) ProphetClient {
	return &prophetClient{
		url:        url,
		endpoint:   strings.TrimPrefix(url, "http://"),
		httpClient: &http.Client{Timeout: timeout},
	}
}

// HealthInfo define Prophet's healthy info
type HealthInfo struct {
	Healths []MemberHealth
}

// MemberHealth define a prophet member's healthy info
type MemberHealth struct {
	Name       string
	MemberID   uint64
	ClientUrls []string
	Health     bool
}

// MembersInfo is Prophet members info returned from Prophet RESTful interface
//type Members map[string][]*ProphetMember
type ProphetMember struct {
	MemberId uint64
	Name     string
	PeerUrls []string
}
type MembersInfo struct {
	Members []*ProphetMember
	Leader  *ProphetMember
}

// StoresInfo is stores info returned from Prophet RESTful interface
type StoresInfo struct {
	Count int
	Code  int    `json:"code"`
	Err   string `json:"error"`
	// Stores []*pdapi.StoreInfo `json:"value"`
}

// StoreInfo is stores info returned from Prophet RESTful interface
type StoreInfo struct {
	Code int    `json:"code"`
	Err  string `json:"error"`
	// Store *pdapi.StoreInfo `json:"value"`
}

type schedulerInfo struct {
	Name    string `json:"name"`
	StoreID uint64 `json:"store_id"`
}

func (pc *prophetClient) GetHealth() (*HealthInfo, error) {
	//return getHealth(pc.endpoint)
	return nil, nil
}

func (pc *prophetClient) GetConfig() (time.Duration, error) {
	// need new api
	return MaxStoreDownTimeDuration, nil
}

func (pc *prophetClient) GetCluster() (string, error) {
	// need new api
	return "hyena-cluster-demo", nil
}

func (pc *prophetClient) AddMember(peerURL string) error {
	//return memberAdd(pc.endpoint,peerURL)
	return nil
}

func (pc *prophetClient) GetMembers() (*MembersInfo, error) {
	// members, err := memberList(pc.endpoint)
	// if err != nil {
	// 	return nil, err
	// }
	// return members, nil
	return nil, nil
}

func (pc *prophetClient) DeleteMemberByID(memberID uint64) error {
	// var exist bool
	// members, err := memberList(pc.endpoint)
	// if err != nil {
	// 	return err
	// }
	// for _, member := range members.Members {
	// 	if member.MemberId == memberID {
	// 		exist = true
	// 		break
	// 	}
	// }
	// if !exist {
	// 	return nil
	// }

	// return memberRemoveById(pc.endpoint, memberID)
	return nil
}

func (pc *prophetClient) DeleteMember(name string) error {
	// var exist bool
	// var memberID uint64
	// members, err := pc.GetMembers()
	// if err != nil {
	// 	return err
	// }
	// for _, member := range members.Members {
	// 	if member.Name == name {
	// 		exist = true
	// 		memberID = member.MemberId
	// 		break
	// 	}
	// }
	// if !exist {
	// 	return nil
	// }
	// return memberRemoveById(pc.endpoint, memberID)
	return nil
}

func (pc *prophetClient) GetStores() (*StoresInfo, error) {
	apiURL := fmt.Sprintf("%s/%s", pc.url, storesPrefix)
	body, err := pc.getBodyOK(apiURL)
	if err != nil {
		return nil, err
	}
	storesInfo := &StoresInfo{}
	err = json.Unmarshal(body, storesInfo)
	if err != nil {
		return nil, err
	}
	if storesInfo.Code != 0 {
		return nil, errors.New(storesInfo.Err)
	}
	return storesInfo, nil
}

func (pc *prophetClient) GetTombStoneStores() (*StoresInfo, error) {
	apiURL := fmt.Sprintf("%s/%s", pc.url, storesPrefix)
	body, err := pc.getBodyOK(apiURL)
	if err != nil {
		return nil, err
	}
	storesInfo := &StoresInfo{}
	err = json.Unmarshal(body, storesInfo)
	if err != nil {
		return nil, err
	}
	if storesInfo.Code != 0 {
		return nil, errors.New(storesInfo.Err)
	}

	tomStoneStores := &StoresInfo{}

	// for _, store := range storesInfo.Stores {
	// 	if store.Meta.State == TombStoneStoreState {
	// 		tomStoneStores.Stores = append(tomStoneStores.Stores, store)
	// 	}
	// }
	return tomStoneStores, nil
}

// func (pc *prophetClient) GetStore(storeID uint64) (*pdapi.StoreInfo, error) {
// 	apiURL := fmt.Sprintf("%s/%s/%d", pc.url, storesPrefix, storeID)
// 	body, err := pc.getBodyOK(apiURL)
// 	if err != nil {
// 		return nil, err
// 	}
// 	storeInfo := &StoreInfo{}
// 	err = json.Unmarshal(body, storeInfo)
// 	if err != nil {
// 		return nil, err
// 	}
// 	fmt.Println("storeInfo: ", storeInfo)
// 	if storeInfo.Code != 0 {
// 		return nil, errors.New(storeInfo.Err)
// 	}
// 	return storeInfo.Store, nil
// }

func (pc *prophetClient) DeleteStore(storeID uint64) error {

	// _, err := pc.GetStore(storeID)
	// if err != nil {
	// 	return err
	// }

	// apiURL := fmt.Sprintf("%s/%s/%d", pc.url, storesPrefix, storeID)
	// req, err := http.NewRequest("DELETE", apiURL, nil)
	// if err != nil {
	// 	return err
	// }
	// res, err := pc.httpClient.Do(req)
	// if err != nil {
	// 	return err
	// }
	// defer DeferClose(res.Body, &err)

	// // Remove an offline store should returns http.StatusOK
	// if res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNotFound {
	// 	return nil
	// }
	// body, err := ioutil.ReadAll(res.Body)
	// if err != nil {
	// 	return err
	// }

	// return fmt.Errorf("failed to delete store %d: %v", storeID, string(body))
	return nil

}

func (pc *prophetClient) SetStoreLabels(storeID uint64, labels map[string]string) (bool, error) {
	// to do
	return true, nil
}

func (pc *prophetClient) BeginEvictLeader(storeID uint64) error {
	// to do
	return nil
}

func (pc *prophetClient) EndEvictLeader(storeID uint64) error {
	// to do
	return nil
}

func (pc *prophetClient) GetEvictLeaderSchedulers() ([]string, error) {
	// to do
	return nil, nil
}

func (pc *prophetClient) GetProphetLeader() (*ProphetMember, error) {
	// to do
	return nil, nil
}

func (pc *prophetClient) TransferProphetLeader(memberName string) error {
	// to do
	return nil
}

func (pc *prophetClient) getBodyOK(apiURL string) ([]byte, error) {
	res, err := pc.httpClient.Get(apiURL)
	if err != nil {
		return nil, err
	}
	defer DeferClose(res.Body, &err)
	if res.StatusCode >= 400 {
		errMsg := fmt.Errorf(fmt.Sprintf("Error response %v", res.StatusCode))
		return nil, errMsg
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return body, err
}

// DeferClose captures the error returned from closing (if an error occurs).
// This is designed to be used in a defer statement.
func DeferClose(c io.Closer, err *error) {
	if cerr := c.Close(); cerr != nil && *err == nil {
		*err = cerr
	}
}

type FakeProphetControl struct {
	defaultProphetControl
}

func NewFakeProphetControl() *FakeProphetControl {
	return &FakeProphetControl{
		defaultProphetControl{prophetClients: map[string]ProphetClient{}},
	}
}

func (fpc *FakeProphetControl) SetProphetClient(hc *v1alpha1.HyenaCluster, prophetclient ProphetClient) {
	// fpc.defaultProphetControl.prophetClients[pdClientKey(hc.Namespace, hc.Name)] = prophetclient
}

type ActionType string

const (
	GetHealthActionType                ActionType = "GetHealth"
	GetConfigActionType                ActionType = "GetConfig"
	GetClusterActionType               ActionType = "GetCluster"
	GetMembersActionType               ActionType = "GetMembers"
	GetStoresActionType                ActionType = "GetStores"
	GetTombStoneStoresActionType       ActionType = "GetTombStoneStores"
	GetStoreActionType                 ActionType = "GetStore"
	DeleteStoreActionType              ActionType = "DeleteStore"
	DeleteMemberByIDActionType         ActionType = "DeleteMemberByID"
	DeleteMemberActionType             ActionType = "DeleteMember "
	SetStoreLabelsActionType           ActionType = "SetStoreLabels"
	BeginEvictLeaderActionType         ActionType = "BeginEvictLeader"
	EndEvictLeaderActionType           ActionType = "EndEvictLeader"
	GetEvictLeaderSchedulersActionType ActionType = "GetEvictLeaderSchedulers"
	GetProphetLeaderActionType         ActionType = "GetProphetLeader"
	TransferProphetLeaderActionType    ActionType = "TransferProphetLeader"
)

type NotFoundReaction struct {
	actionType ActionType
}

func (nfr *NotFoundReaction) Error() string {
	return fmt.Sprintf("not found %s reaction. Please add the reaction", nfr.actionType)
}

type Action struct {
	ID     uint64
	Name   string
	Labels map[string]string
}

type Reaction func(action *Action) (interface{}, error)

type FakeProphetClient struct {
	reactions map[ActionType]Reaction
}

func NewFakeProphetClient() *FakeProphetClient {
	return &FakeProphetClient{reactions: map[ActionType]Reaction{}}
}

func (pc *FakeProphetClient) AddReaction(actionType ActionType, reaction Reaction) {
	pc.reactions[actionType] = reaction
}

// fakeAPI is a small helper for fake API calls
func (pc *FakeProphetClient) fakeAPI(actionType ActionType, action *Action) (interface{}, error) {
	if reaction, ok := pc.reactions[actionType]; ok {
		result, err := reaction(action)
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, &NotFoundReaction{actionType}
}

func (pc *FakeProphetClient) GetHealth() (*HealthInfo, error) {
	action := &Action{}
	result, err := pc.fakeAPI(GetHealthActionType, action)
	if err != nil {
		return nil, err
	}
	return result.(*HealthInfo), nil
}

func (pc *FakeProphetClient) GetConfig() (time.Duration, error) {
	action := &Action{}
	_, err := pc.fakeAPI(GetConfigActionType, action)
	if err != nil {
		return 0, err
	}
	return MaxStoreDownTimeDuration, nil
}

func (pc *FakeProphetClient) GetCluster() (string, error) {
	action := &Action{}
	_, err := pc.fakeAPI(GetClusterActionType, action)
	if err != nil {
		return "", err
	}
	return "hyena-cluster-demo", nil
}

func (pc *FakeProphetClient) GetMembers() (*MembersInfo, error) {
	action := &Action{}
	result, err := pc.fakeAPI(GetMembersActionType, action)
	if err != nil {
		return nil, err
	}
	return result.(*MembersInfo), nil
}

func (pc *FakeProphetClient) GetStores() (*StoresInfo, error) {
	action := &Action{}
	result, err := pc.fakeAPI(GetStoresActionType, action)
	if err != nil {
		return nil, err
	}
	return result.(*StoresInfo), nil
}

func (pc *FakeProphetClient) GetTombStoneStores() (*StoresInfo, error) {
	action := &Action{}
	result, err := pc.fakeAPI(GetTombStoneStoresActionType, action)
	if err != nil {
		return nil, err
	}
	return result.(*StoresInfo), nil
}

// func (pc *FakeProphetClient) GetStore(id uint64) (*pdapi.StoreInfo, error) {
// 	action := &Action{
// 		ID: id,
// 	}
// 	result, err := pc.fakeAPI(GetStoreActionType, action)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return result.(*pdapi.StoreInfo), nil
// }

func (pc *FakeProphetClient) DeleteStore(id uint64) error {
	if reaction, ok := pc.reactions[DeleteStoreActionType]; ok {
		action := &Action{ID: id}
		_, err := reaction(action)
		return err
	}
	return nil
}

func (pc *FakeProphetClient) DeleteMemberByID(id uint64) error {
	if reaction, ok := pc.reactions[DeleteMemberByIDActionType]; ok {
		action := &Action{ID: id}
		_, err := reaction(action)
		return err
	}
	return nil
}

func (pc *FakeProphetClient) DeleteMember(name string) error {
	if reaction, ok := pc.reactions[DeleteMemberActionType]; ok {
		action := &Action{Name: name}
		_, err := reaction(action)
		return err
	}
	return nil
}

// SetStoreLabels sets Store labels
func (pc *FakeProphetClient) SetStoreLabels(storeID uint64, labels map[string]string) (bool, error) {
	if reaction, ok := pc.reactions[SetStoreLabelsActionType]; ok {
		action := &Action{ID: storeID, Labels: labels}
		result, err := reaction(action)
		return result.(bool), err
	}
	return true, nil
}

func (pc *FakeProphetClient) BeginEvictLeader(storeID uint64) error {
	if reaction, ok := pc.reactions[BeginEvictLeaderActionType]; ok {
		action := &Action{ID: storeID}
		_, err := reaction(action)
		return err
	}
	return nil
}

func (pc *FakeProphetClient) EndEvictLeader(storeID uint64) error {
	if reaction, ok := pc.reactions[EndEvictLeaderActionType]; ok {
		action := &Action{ID: storeID}
		_, err := reaction(action)
		return err
	}
	return nil
}

func (pc *FakeProphetClient) GetEvictLeaderSchedulers() ([]string, error) {
	if reaction, ok := pc.reactions[GetEvictLeaderSchedulersActionType]; ok {
		action := &Action{}
		result, err := reaction(action)
		return result.([]string), err
	}
	return nil, nil
}

func (pc *FakeProphetClient) GetProphetLeader() (*ProphetMember, error) {
	if reaction, ok := pc.reactions[GetProphetLeaderActionType]; ok {
		action := &Action{}
		result, err := reaction(action)
		return result.(*ProphetMember), err
	}
	return nil, nil
}

func (pc *FakeProphetClient) TransferProphetLeader(memberName string) error {
	if reaction, ok := pc.reactions[TransferProphetLeaderActionType]; ok {
		action := &Action{Name: memberName}
		_, err := reaction(action)
		return err
	}
	return nil
}
