package collector

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"k8s.io/kubernetes/cmd/kubeaudit/db"
)

/*
参考kubectl的代码，监听所有资源的变更。如果有资源不支持(报错the server could not find the requested resource)，则忽略。
*/

type K8sCollector struct {
	f cmdutil.Factory

	// resource info
	resourceInfo map[string]bool
}

func NewK8sCollector(f cmdutil.Factory) *K8sCollector {
	res := &K8sCollector{}
	res.f = f
	res.resourceInfo = make(map[string]bool)

	return res
}

func (t *K8sCollector) Run() {
	for {
		t.WatchResourceInfo()
		time.Sleep(time.Minute * 5)
	}
}

func (t *K8sCollector) WatchResourceInfo() {
	disClient, err := t.f.ToDiscoveryClient()
	if err != nil {
		klog.Fatalf("get dis client failed %v. ", err)
	}
	resourceList, err := disClient.ServerPreferredResources()
	if err != nil {
		klog.Fatalf("get resource list failed %v. ", err)
	}
	for _, res := range resourceList {
		//klog.Infof("kind: %s, apiversion: %s, groupVersion: %s ",
		//	res.Kind, res.APIVersion, res.GroupVersion)
		for _, api := range res.APIResources {
			klog.V(2).Infoln(api.String())
			klog.V(2).Infof("name: %s, kind: %s, gv: %s. ", api.Name, api.Kind, res.GroupVersion)
			if t.resourceInfo[api.Name] {
				continue
			}
			t.resourceInfo[api.Name] = true
			go WatchResources(t.f, api.Name)
		}
	}

}

func WatchResources(f cmdutil.Factory, resName string) {
	var buf [64]byte
	runtime.Stack(buf[:], false)
	klog.Infoln("watch res: ", resName, string(buf[:]))
	r := f.NewBuilder().
		Unstructured().
		NamespaceParam("").AllNamespaces(true).
		RequestChunksOf(0).
		ResourceTypeOrNameArgs(true, resName).
		ContinueOnError().
		Latest().
		Flatten().
		Do()
	if r.Err() != nil {
		klog.Fatalf("new builder for %s failed %v. ", resName, r.Err())
	}

	w, err := r.Watch("0")
	if err != nil {
		if strings.Contains(err.Error(), "the server could not find the requested resource ") ||
			strings.Contains(err.Error(), "the server does not allow this method on the requested resource") {
			klog.Warningf("res %s not supported watch ,skip. ", resName)
			return
		}
		klog.Fatalf("start watch for %s faield %v. ", resName, err)
	}

	resourceHistory := make(map[types.UID]*unstructured.Unstructured, 0)

	for {
		select {
		case ev := <-w.ResultChan():
			if ev.Object == nil {
				continue
			}
			obj := ev.Object.(*unstructured.Unstructured)
			uid := obj.GetUID()
			klog.V(1).Infof("get event: %#v ", obj.GetName())
			err = KeepObject(obj, resourceHistory[uid], string(ev.Type))
			if err != nil {
				var buf [64]byte
				runtime.Stack(buf[:], false)
				klog.Errorf("keep object failed %v, buf %s. ", err, string(buf[:]))
			}
			resourceHistory[uid] = obj
		}
	}

}

func KeepObject(curObj, oldObj *unstructured.Unstructured, event string) error {
	var err error

	tx := db.DB
	//defer tx.Commit() // always commit

	mt := db.NewMetaData(curObj)
	ai := db.NewAuditInfo(curObj, oldObj, event)
	ldb := tx.Find(&db.MetaData{}, "uuid = ? ", mt.Uuid)
	if ldb.Error != nil {
		return fmt.Errorf("get meta data failed %v. ", ldb.Error)
	}
	if ldb.RowsAffected == 0 {
		err = tx.Create(&mt).Error
		if err != nil {
			return fmt.Errorf("create meta data failed %v. ", err)
		}
	}
	ldb = tx.Find(&db.AuditInfo{}, "uuid = ? and res_version = ? ", ai.Uuid, ai.ResVersion)
	if ldb.Error != nil {
		return fmt.Errorf("get audit data failed %v. ", ldb.Error)
	}
	if ldb.RowsAffected == 0 {
		if event == "" {
			ai.Event = string(watch.Added)
		}
		err = tx.Create(&ai).Error
		if err != nil {
			return fmt.Errorf("create audit info failed %v. ", err)
		}
	} else if event != "" {
		//klog.Warningf("get same res_version. %#v ", ai.Uuid, ai.ResVersion)
	}

	return nil
}
