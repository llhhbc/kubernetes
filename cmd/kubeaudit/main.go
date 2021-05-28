package main

import (
	"flag"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/klog"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
)

var (
	//kubeconfig = flag.String(clientcmd.RecommendedConfigPathFlag, os.Getenv(clientcmd.RecommendedConfigPathEnvVar), "Path to kubeconfig containing embedded authinfo.")
	jaegerServer = flag.String("jaegerServer", "http://myjaeger-collector.observability:14268/api/traces", "jaeger server addr. ")
)

func main()  {
	flag.Parse()

	InitDb()

	//kubeConfig, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	//if err != nil {
	//	log.Fatalf("init config failed %v. ", err)
	//}

	kubeConfigFlags := genericclioptions.NewConfigFlags(true).WithDeprecatedPasswordFlag()
	matchVersionKubeConfigFlags := cmdutil.NewMatchVersionFlags(kubeConfigFlags)

	f := cmdutil.NewFactory(matchVersionKubeConfigFlags)

	disClient, err := f.ToDiscoveryClient()
	if err != nil {
		klog.Fatalf("get dis client failed %v. ", err)
	}
	resourceList, err  := disClient.ServerPreferredResources()
	if err != nil {
		klog.Fatalf("get resource list failed %v. ", err)
	}
	for _, res := range resourceList {
		//klog.Infof("kind: %s, apiversion: %s, groupVersion: %s ",
		//	res.Kind, res.APIVersion, res.GroupVersion)
		for _, api := range res.APIResources {
			if api.Kind == "ComponentStatus" {
				continue
			}
			if strings.Contains("authorization.k8s.io/v1,authentication.k8s.io/v1", res.GroupVersion) {
				continue
			}

			klog.V(2).Infoln(api.String())
			klog.V(2).Infof("name: %s, kind: %s, gv: %s. ", api.Name, api.Kind, res.GroupVersion)
			WatchResources(f, api.Name)
		}
	}
	select {
	}
}

func WatchResources(f cmdutil.Factory, resName string)  {
	klog.Infoln("watch res: ", resName)
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

	infos, err := r.Infos()
	if err != nil {
		if strings.Contains(err.Error(), "the server could not find the requested resource"){
			klog.Warningf("get infao failed %v. skip ", err)
			return
		}
		klog.Fatalf("get info failed %v. ", err)
	}

	klog.V(2).Infoln("get info: ", infos)

	for _, info := range infos {
		err = KeepObject(info.Object.(*unstructured.Unstructured), "")
		if err != nil {
			klog.Fatalf("init info failed %v. ", err)
		}
		klog.Infof("watch %s, %s, %s. ", info.Name, info.Namespace, info.Object.(*unstructured.Unstructured).GetKind())
		go WatchInfo(info)
	}
}

func KeepObject(obj *unstructured.Unstructured, event string) error {
	var err error

	tx := DB.Begin()
	defer tx.Commit() // always commit

	mt := NewMetaData(obj)
	ai := NewAuditInfo(obj, event)
	ldb := tx.Find(&MetaData{}, "uuid = ? ", mt.Uuid)
	if ldb.Error != nil {
		return fmt.Errorf("get meta data failed %v. ", ldb.Error)
	}
	if ldb.RowsAffected == 0 {
		err = tx.Create(&mt).Error
		if err != nil {
			return fmt.Errorf("create meta data failed %v. ", err)
		}
	}
	ldb = tx.Find(&AuditInfo{}, "uuid = ? and res_version = ? ", ai.Uuid, ai.ResVersion)
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
		klog.Warningf("get same res_version. %#v ", ai.ToString())
	}

	return nil
}

func WatchInfo(info *resource.Info)  {
	w, err := info.Watch(info.ResourceVersion)
	if err != nil {
		klog.Fatalf("watch res failed %v. ", err)
	}
	for {
		select {
		case ev := <- w.ResultChan():
			if ev.Object == nil {
				continue
			}
			klog.Infof("get event: %#v ", ev.Object.(*unstructured.Unstructured).GetName())
			err = KeepObject(ev.Object.(*unstructured.Unstructured), string(ev.Type))
			if err != nil {
				klog.Errorf("keep object failed %v. ", err)
			}
		}
	}
}