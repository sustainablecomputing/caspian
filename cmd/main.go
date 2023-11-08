package main

import (
	"os"
	"time"

	"github.com/sustainablecomputing/caspian/core"
	"github.com/sustainablecomputing/caspian/dispatcher"
	"github.com/sustainablecomputing/caspian/monitoring"
)

func main() {

	period_length := time.Duration(core.DefaultRevisitTime)
	dirname, _ := os.UserHomeDir()
	kube_config := dirname + "/.kube/config"
	hub_contxt := "kind-hub"
	D := dispatcher.NewDispatcher(kube_config, hub_contxt)
	M := monitoring.NewMonitor(kube_config, hub_contxt)

	for {
		M.UpdateClusterInfo()
		D.Run()
		time.Sleep(period_length)
	}

}
