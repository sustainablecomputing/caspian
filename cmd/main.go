package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sustainablecomputing/caspian/core"
	"github.com/sustainablecomputing/caspian/monitoring"
	"github.com/sustainablecomputing/caspian/scheduler"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

func main() {

	periodLength := time.Duration(core.DefaultRevisitTime)
	var kube_contxt string
	var optimizer string
	var period_length string
	flag.StringVar(&kube_contxt, "kube-context", "k3d-hub", "The Kubernetes context.")
	flag.StringVar(&optimizer, "optimizer", "sustainable", "Optimizer.")
	flag.StringVar(&period_length, "period_length", fmt.Sprint(periodLength), "Frequency.")

	flag.Parse()
	conf, err := config.GetConfigWithContext(kube_contxt)
	if err != nil {
		fmt.Println(err, "Unable to get kubeconfig")
		os.Exit(1)
	}

	S := scheduler.NewScheduler(conf)
	M := monitoring.NewMonitor(conf)

	for {
		M.UpdateClusterInfo()
		S.Schedule(optimizer)
		time.Sleep(periodLength)
	}

}
