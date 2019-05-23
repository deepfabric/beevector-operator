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
	"time"

	"github.com/golang/glog"
	"github.com/infinivision/hyena-operator/pkg/apis/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/client/clientset/versioned"
	informers "github.com/infinivision/hyena-operator/pkg/client/informers/externalversions"
	listers "github.com/infinivision/hyena-operator/pkg/client/listers/infinivision.com/v1alpha1"
	"github.com/infinivision/hyena-operator/pkg/controller"
	mm "github.com/infinivision/hyena-operator/pkg/manager/member"
	"github.com/infinivision/hyena-operator/pkg/manager/meta"
	perrors "github.com/pingcap/errors"
	apps "k8s.io/api/apps/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	eventv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

// controllerKind contains the schema.GroupVersionKind for this controller type.
var controllerKind = v1alpha1.SchemeGroupVersion.WithKind("HyenaCluster")

// Controller controls hyenaclusters.
type Controller struct {
	// kubernetes client interface
	kubeClient kubernetes.Interface
	// operator client interface
	cli versioned.Interface
	// control returns an interface capable of syncing a hyena cluster.
	// Abstracted out for testing.
	control ControlInterface
	// hcLister is able to list/get hyenaclusters from a shared informer's store
	hcLister listers.HyenaClusterLister
	// hcListerSynced returns true if the hyenacluster shared informer has synced at least once
	hcListerSynced cache.InformerSynced
	// setLister is able to list/get stateful sets from a shared informer's store
	setLister appslisters.StatefulSetLister
	// setListerSynced returns true if the statefulset shared informer has synced at least once
	setListerSynced cache.InformerSynced
	// hyenaclusters that need to be synced.
	queue workqueue.RateLimitingInterface
}

// NewController creates a hyenacluster controller.
func NewController(
	kubeCli kubernetes.Interface,
	cli versioned.Interface,
	informerFactory informers.SharedInformerFactory,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
) *Controller {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&eventv1.EventSinkImpl{
		Interface: eventv1.New(kubeCli.CoreV1().RESTClient()).Events("")})
	recorder := eventBroadcaster.NewRecorder(v1alpha1.Scheme, corev1.EventSource{Component: "hyenacluster"})

	hcInformer := informerFactory.Infinivision().V1alpha1().HyenaClusters()
	setInformer := kubeInformerFactory.Apps().V1beta1().StatefulSets()
	svcInformer := kubeInformerFactory.Core().V1().Services()
	pvcInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	pvInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	podInformer := kubeInformerFactory.Core().V1().Pods()
	nodeInformer := kubeInformerFactory.Core().V1().Nodes()

	hcControl := controller.NewRealHyenaClusterControl(cli, hcInformer.Lister(), recorder)
	prophetControl := controller.NewDefaultProphetControl()
	setControl := controller.NewRealStatefuSetControl(kubeCli, setInformer.Lister(), recorder)
	svcControl := controller.NewRealServiceControl(kubeCli, svcInformer.Lister(), recorder)
	pvControl := controller.NewRealPVControl(kubeCli, pvcInformer.Lister(), pvInformer.Lister(), recorder)
	pvcControl := controller.NewRealPVCControl(kubeCli, recorder, pvcInformer.Lister())
	podControl := controller.NewRealPodControl(kubeCli, prophetControl, podInformer.Lister(), recorder)
	storeScaler := mm.NewStoreScaler(prophetControl, pvcInformer.Lister(), pvcControl, podInformer.Lister())
	storeFailover := mm.NewStoreFailover(prophetControl)
	prophetUpgrader := mm.NewProphetUpgrader(prophetControl, podControl, podInformer.Lister())
	storeUpgrader := mm.NewStoreUpgrader(prophetControl, podControl, podInformer.Lister())

	hcc := &Controller{
		kubeClient: kubeCli,
		cli:        cli,
		control: NewDefaultHyenaClusterControl(
			hcControl,
			mm.NewProphetMemberManager(
				prophetControl,
				setControl,
				svcControl,
				setInformer.Lister(),
				svcInformer.Lister(),
				podInformer.Lister(),
				podControl,
				pvcInformer.Lister(),
				prophetUpgrader,
			),
			mm.NewStoreMemberManager(
				prophetControl,
				setControl,
				svcControl,
				setInformer.Lister(),
				svcInformer.Lister(),
				podInformer.Lister(),
				nodeInformer.Lister(),
				false,
				storeFailover,
				storeScaler,
				storeUpgrader,
			),
			meta.NewReclaimPolicyManager(
				pvcInformer.Lister(),
				pvInformer.Lister(),
				pvControl,
			),
			meta.NewMetaManager(
				pvcInformer.Lister(),
				pvcControl,
				pvInformer.Lister(),
				pvControl,
				podInformer.Lister(),
				podControl,
			),
			mm.NewOrphanPodsCleaner(
				podInformer.Lister(),
				podControl,
				pvcInformer.Lister(),
			),
			recorder,
		),
		queue: workqueue.NewNamedRateLimitingQueue(
			workqueue.DefaultControllerRateLimiter(),
			"hyenacluster",
		),
	}

	hcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: hcc.enqueueHyenaCluster,
		UpdateFunc: func(old, cur interface{}) {
			hcc.enqueueHyenaCluster(cur)
		},
		DeleteFunc: hcc.enqueueHyenaCluster,
	})
	hcc.hcLister = hcInformer.Lister()
	hcc.hcListerSynced = hcInformer.Informer().HasSynced

	setInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: hcc.addStatefulSet,
		UpdateFunc: func(old, cur interface{}) {
			hcc.updateStatefuSet(old, cur)
		},
		DeleteFunc: hcc.deleteStatefulSet,
	})
	hcc.setLister = setInformer.Lister()
	hcc.setListerSynced = setInformer.Informer().HasSynced

	return hcc
}

// Run runs the hyenacluster controller.
func (hcc *Controller) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer hcc.queue.ShutDown()

	glog.Info("Starting hyenacluster controller")
	defer glog.Info("Shutting down hyenacluster controller")

	if !cache.WaitForCacheSync(stopCh, hcc.hcListerSynced, hcc.setListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(hcc.worker, time.Second, stopCh)
	}

	<-stopCh
}

// worker runs a worker goroutine that invokes processNextWorkItem until the the controller's queue is closed
func (hcc *Controller) worker() {
	for hcc.processNextWorkItem() {
		// revive:disable:empty-block
		glog.V(2).Infof("process one item")
	}
	glog.V(2).Infof("worker over")
}

// processNextWorkItem dequeues items, processes them, and marks them done. It enforces that the syncHandler is never
// invoked concurrently with the same key.
func (hcc *Controller) processNextWorkItem() bool {
	key, quit := hcc.queue.Get()
	if quit {
		return false
	}
	defer hcc.queue.Done(key)
	glog.V(2).Infof("do sync")
	if err := hcc.sync(key.(string)); err != nil {
		if perrors.Find(err, controller.IsRequeueError) != nil {
			glog.Infof("HyenaCluster: %v, still need sync: %v, requeuing", key.(string), err)
		} else {
			utilruntime.HandleError(fmt.Errorf("HyenaCluster: %v, sync failed %v, requeuing", key.(string), err))
		}
		hcc.queue.AddRateLimited(key)
	} else {
		hcc.queue.Forget(key)
	}
	return true
}

// sync syncs the given hyenacluster.
func (hcc *Controller) sync(key string) error {
	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing HyenaCluster %q (%v)", key, time.Since(startTime))
	}()

	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	hc, err := hcc.hcLister.HyenaClusters(ns).Get(name)
	if errors.IsNotFound(err) {
		glog.Infof("HyenaCluster has been deleted %v", key)
		return nil
	}
	if err != nil {
		return err
	}

	return hcc.syncHyenaCluster(hc.DeepCopy())
}

func (hcc *Controller) syncHyenaCluster(hc *v1alpha1.HyenaCluster) error {
	return hcc.control.UpdateHyenaCluster(hc)
}

// enqueueHyenaCluster enqueues the given hyenacluster in the work queue.
func (hcc *Controller) enqueueHyenaCluster(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Cound't get key for object %+v: %v", obj, err))
		return
	}
	hcc.queue.Add(key)
}

// addStatefulSet adds the hyenacluster for the statefulset to the sync queue
func (hcc *Controller) addStatefulSet(obj interface{}) {
	set := obj.(*apps.StatefulSet)
	ns := set.GetNamespace()
	setName := set.GetName()

	if set.DeletionTimestamp != nil {
		// on a restart of the controller manager, it's possible a new statefulset shows up in a state that
		// is already pending deletion. Prevent the statefulset from being a creation observation.
		hcc.deleteStatefulSet(set)
		return
	}

	// If it has a ControllerRef, that's all that matters.
	hc := hcc.resolveHyenaClusterFromSet(ns, set)
	if hc == nil {
		return
	}
	glog.V(4).Infof("StatefuSet %s/%s created, HyenaCluster: %s/%s", ns, setName, ns, hc.Name)
	hcc.enqueueHyenaCluster(hc)
}

// updateStatefuSet adds the hyenacluster for the current and old statefulsets to the sync queue.
func (hcc *Controller) updateStatefuSet(old, cur interface{}) {
	curSet := cur.(*apps.StatefulSet)
	oldSet := old.(*apps.StatefulSet)
	ns := curSet.GetNamespace()
	setName := curSet.GetName()
	if curSet.ResourceVersion == oldSet.ResourceVersion {
		// Periodic resync will send update events for all known statefulsets.
		// Two different versions of the same statefulset will always have different RVs.
		return
	}

	// If it has a ControllerRef, that's all that matters.
	hc := hcc.resolveHyenaClusterFromSet(ns, curSet)
	if hc == nil {
		return
	}
	glog.V(4).Infof("StatefulSet %s/%s updated, %+v -> %+v.", ns, setName, oldSet.Spec, curSet.Spec)
	hcc.enqueueHyenaCluster(hc)
}

// deleteStatefulSet enqueues the hyenacluster for the statefulset accounting for deletion tombstones.
func (hcc *Controller) deleteStatefulSet(obj interface{}) {
	set, ok := obj.(*apps.StatefulSet)
	ns := set.GetNamespace()
	setName := set.GetName()

	// When a delete is dropped, the relist will notice a statefuset in the store not
	// in the list, leading to the insertion of a tombstone object which contains
	// the deleted key/value.
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone %+v", obj))
			return
		}
		set, ok = tombstone.Obj.(*apps.StatefulSet)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a statefuset %+v", obj))
			return
		}
	}

	// If it has a HyenaCluster, that's all that matters.
	hc := hcc.resolveHyenaClusterFromSet(ns, set)
	if hc == nil {
		return
	}
	glog.V(4).Infof("StatefulSet %s/%s deleted through %v.", ns, setName, utilruntime.GetCaller())
	hcc.enqueueHyenaCluster(hc)
}

// resolveHyenaClusterFromSet returns the HyenaCluster by a StatefulSet,
// or nil if the StatefulSet could not be resolved to a matching HyenaCluster
// of the correct Kind.
func (hcc *Controller) resolveHyenaClusterFromSet(namespace string, set *apps.StatefulSet) *v1alpha1.HyenaCluster {
	controllerRef := metav1.GetControllerOf(set)
	if controllerRef == nil {
		return nil
	}

	// We can't look up by UID, so look up by Name and then verify UID.
	// Don't even try to look up by Name if it's the wrong Kind.
	if controllerRef.Kind != controllerKind.Kind {
		return nil
	}
	hc, err := hcc.hcLister.HyenaClusters(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if hc.UID != controllerRef.UID {
		// The controller we found with this Name is not the same one that the
		// ControllerRef points to.
		return nil
	}
	return hc
}
