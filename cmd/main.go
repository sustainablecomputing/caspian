package main

import (
	"os"
	"time"

	"github.com/sustainablecomputing/caspian/dispatcher"
	"github.com/sustainablecomputing/caspian/monitoring"
	//"github.ibm.com/tbahreini/caspian-mcad/dispatcher"
)

func main() {
	exam_slots := 2
	period_length := time.Duration(10)
	dirname, _ := os.UserHomeDir()
	kube_config := dirname + "/.kube/config"
	hub_contxt := "kind-hub"
	D := dispatcher.NewDispatcher(kube_config, hub_contxt)
	M := monitoring.NewMonitor(kube_config, hub_contxt)

	for t := 1; t < exam_slots; t++ {
		M.UpdateClusterInfo()
		D.Run()
		time.Sleep(period_length)
	}
	//firstDate := time.Date(2022, 4, 13, 3, 0, 0, 0, time.UTC)

}
