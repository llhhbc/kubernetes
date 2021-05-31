package main

import (
	"flag"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"k8s.io/kubernetes/cmd/kubeaudit/collector"
	"k8s.io/kubernetes/cmd/kubeaudit/db"
)

var (
	jaegerServer = flag.String("jaegerServer", "http://myjaeger-collector.observability:14268/api/traces", "jaeger server addr. ")
)

func main()  {
	flag.Parse()

	db.InitDb()

	kubeConfigFlags := genericclioptions.NewConfigFlags(true).WithDeprecatedPasswordFlag()
	matchVersionKubeConfigFlags := cmdutil.NewMatchVersionFlags(kubeConfigFlags)

	f := cmdutil.NewFactory(matchVersionKubeConfigFlags)

	c := collector.NewK8sCollector(f)

	go c.Run()

	select {
	}
}
