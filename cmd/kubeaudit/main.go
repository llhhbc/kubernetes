package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/klog"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"k8s.io/kubernetes/cmd/kubeaudit/collector"
	"k8s.io/kubernetes/cmd/kubeaudit/db"
	"k8s.io/kubernetes/cmd/kubeaudit/tracer"
)

var (
	jaegerServer = pflag.String("jaegerServer", os.Getenv("JAEGER_SERVER"), "jaeger server addr. ")
	apiserver    = pflag.String("apiserver", "", "apiserver addr")
	addr         = pflag.String("addr", ":7080", "listen addr")
)

func main() {
	log := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	klog.InitFlags(log)
	pflag.CommandLine.AddGoFlagSet(log)
	pflag.Parse()

	db.InitDb()

	kubeConfigFlags := genericclioptions.NewConfigFlags(true).WithDeprecatedPasswordFlag()
	kubeConfigFlags.APIServer = apiserver
	matchVersionKubeConfigFlags := cmdutil.NewMatchVersionFlags(kubeConfigFlags)

	f := cmdutil.NewFactory(matchVersionKubeConfigFlags)

	c := collector.NewK8sCollector(f)

	go c.Run()

	go RunJaegerByGroup()

	go http.ListenAndServe(*addr, nil) // for pprof

	select {}
}

func RunJaegerByGroup() {
	groupFlag := make(map[string]bool, 0)

	for {
		var apiVersion []string
		err := db.DB.Distinct("api_version").Model(&db.MetaData{}).Scan(&apiVersion).Error
		if err != nil {
			klog.Fatalf("get api_version info failed %v. ", err)
		}
		for _, av := range apiVersion {
			if groupFlag[av] {
				continue
			}
			go tracer.RunJaegerAudit(*jaegerServer, "", av)
			groupFlag[av] = true
		}
		time.Sleep(time.Minute)
	}
}
