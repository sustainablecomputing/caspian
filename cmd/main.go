package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
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
	int_length, _ := strconv.Atoi(period_length)
	S := scheduler.NewScheduler(conf, int_length)
	M := monitoring.NewMonitor(conf)
	periodLength = time.Duration(int_length * 1000000000)

	for {
		M.UpdateClusterInfo()
		S.Schedule(optimizer)
		time.Sleep(periodLength)
	}

}
