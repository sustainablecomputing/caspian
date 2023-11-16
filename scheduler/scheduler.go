// Note: the example only works with the code within the same release/branch.
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/lanl/clp"
	core "github.com/sustainablecomputing/caspian/core"
	mcadv1beta1 "github.com/tayebehbahreini/mcad/api/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/scheme"
)

type Scheduler struct {
	N            int // number of  jobs (AWs) (to be scheduled/rescheduled)
	M            int // number of available clusters
	T            int // length of time horizon (number of timeslots)
	PeriodLength int
	Jobs         []core.Job     // list of jobs
	Clusters     []core.Cluster // spoke clusters
	crdClient    *rest.RESTClient
}

var AWResource = schema.GroupVersionResource{Group: mcadv1beta1.GroupVersion.Group,
	Version: mcadv1beta1.GroupVersion.Version, Resource: "appwrappers"}

// NewDispatcher : create a new dispather instance and
// configure the clients with kube_config and hub-context
func NewScheduler(config *rest.Config) *Scheduler {
	s := &Scheduler{
		N:            0,
		M:            0,
		T:            core.DefaultT,
		PeriodLength: core.DefaultSlotLength,
		Jobs:         []core.Job{},
		Clusters:     []core.Cluster{},
		crdClient:    &rest.RESTClient{},
	}
	//config, err := buildConfigWithContextFromFlags(hub_contxt, kube_config)
	crdConfig := *config
	crdConfig.ContentConfig.GroupVersion = &schema.GroupVersion{Group: mcadv1beta1.GroupVersion.Group,
		Version: mcadv1beta1.GroupVersion.Version}
	crdConfig.APIPath = "/apis"
	crdConfig.NegotiatedSerializer = serializer.NewCodecFactory(scheme.Scheme)
	crdConfig.UserAgent = rest.DefaultKubernetesUserAgent()
	var err error
	s.crdClient, err = rest.UnversionedRESTClientFor(&crdConfig)
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}
	return s
}

// retrive all Appwrappers in hub: running+ non-running AWs.
// Calculate requested resources  of each AW.
// Save all AWs with their characteristics in Jobs array
func (s *Scheduler) GetAppWrappers() {
	result := mcadv1beta1.AppWrapperList{}
	s.Jobs = []core.Job{}
	err := s.crdClient.
		Get().
		Resource("appwrappers").
		Do(context.Background()).
		Into(&result)
	if err != nil {
		panic(err)
	}
	s.N = 0
	fmt.Println("\nList of Appwrappers ")
	fmt.Println("Name\t CPU\t RemainTime  \t Deadline \t \t \t Status ")
	for _, aw := range result.Items {

		// Aggregated request by AppWrapper

		if aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Queued") ||
			aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Running") ||
			aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Dispatching") ||
			aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Requeuing") { //aw.Spec.Sustainable &&
			TimeDispatched := aw.Spec.DispatcherStatus.TimeDispatched //+
			if aw.Spec.DispatcherStatus.Phase != mcadv1beta1.AppWrapperPhase("Queued") {
				TimeDispatched += int64(time.Since(aw.Spec.DispatcherStatus.LastDispatchingTime.Time).Seconds())
			}

			TimeDispatched = aw.Spec.DispatcherStatus.TimeDispatched / int64(s.PeriodLength)

			remainTime := aw.Spec.Sustainable.RunTime - TimeDispatched
			//if
			if remainTime < 0 {
				fmt.Println("\n Deleting Appwrapper ", aw.Name)
				s.DeleteAppWrapper(aw.Name)
			} else {

				awRequest := aggregateRequests(&aw)
				s.N = s.N + 1
				newJob := core.Job{
					Name:       aw.Name,
					CPU:        awRequest["cpu"],
					RemainTime: remainTime,
					Deadline:   int64(math.Ceil(float64(aw.Spec.Sustainable.Deadline.Sub(metav1.Now().Time).Seconds()) / float64(s.PeriodLength))),
				}

				s.Jobs = append(s.Jobs, newJob)
				fmt.Println(aw.Name, "\t", newJob.CPU, "\t", newJob.RemainTime, "\t\t", aw.Spec.Sustainable.Deadline, aw.Spec.DispatcherStatus.Phase)

			}
		}

	}

}

// get all clusterinfos in hub
func (s *Scheduler) GetClustersInfo() {
	result := mcadv1beta1.ClusterInfoList{}
	err := s.crdClient.
		Get().
		Resource("clusterinfo").
		Do(context.Background()).
		Into(&result)
	if err != nil {
		panic(err)
	}
	j := 0
	fmt.Println("\nList of Spoke Clusters ")
	fmt.Println("Name \t Available CPU \t GeoLocation  ")
	for _, cluster := range result.Items {
		newCluster := core.Cluster{
			Name:   cluster.Name,
			CPU:    float64(cluster.Status.Capacity.Cpu().Value()),
			Carbon: make([]float64, s.T),
		}
		for t := 0; t < s.T; t++ {
			newCluster.Carbon[t], _ = strconv.ParseFloat(cluster.Spec.Carbon[t], 64)
		}
		s.Clusters = append(s.Clusters, newCluster)
		fmt.Println(newCluster.Name, "\t", newCluster.CPU, "\t\t", cluster.Spec.Geolocation)

		j = j + 1
	}
	s.M = len(result.Items)

}

// find an AW
func (s *Scheduler) IsValidAppWrapper(Name string) error {
	err := s.crdClient.Get().Resource("appwrappers").
		Namespace(apiv1.NamespaceDefault).Name(Name).Do(context.Background()).Error()

	return err
}

// delete an AW
func (s *Scheduler) DeleteAppWrapper(Name string) error {
	err := s.crdClient.Delete().Resource("appwrappers").
		Namespace(apiv1.NamespaceDefault).Name(Name).Do(context.Background()).Error()
	return err
}

// add sustainability gate from the appwrapper
func (s *Scheduler) PutHoldOnAppWrapper(Name string) error {
	err := s.IsValidAppWrapper(Name)

	if err == nil {
		var a [1]string
		a[0] = "sustainable"
		patch := []interface{}{
			map[string]interface{}{
				"op":    "replace",
				"path":  "/spec/dispatchingGates",
				"value": a,
			},
		}

		payload, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		err = s.crdClient.Patch(types.JSONPatchType).Resource("appwrappers").Name(Name).
			Namespace(apiv1.NamespaceDefault).Body(payload).Do(context.Background()).Error()
		if err == nil {
			fmt.Println(Name, "\t Suspend ")
		}

		return err

	} else {
		fmt.Println(Name, "is invalid Appwrapper")
		return err
	}
}

// delete sustainability gate from the appwrapper and set the targetCluster
func (s *Scheduler) ReleasHoldOnAppWrapper(Name string, TargetCluster string) error {
	err := s.IsValidAppWrapper(Name)

	if err == nil {
		var a []string
		patch := []interface{}{
			map[string]interface{}{
				"op":    "replace",
				"path":  "/spec/dispatchingGates",
				"value": a,
			},
			map[string]interface{}{
				"op":    "add",
				"path":  "/spec/schedulingSpec/clusterScheduling/policyResult/targetCluster/name",
				"value": TargetCluster,
			},
		}
		payload, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		err = s.crdClient.Patch(types.JSONPatchType).Resource("appwrappers").Name(Name).
			Namespace(apiv1.NamespaceDefault).Body(payload).Do(context.Background()).Error()
		if err == nil {
			fmt.Println(Name, "\t \tScheduled on", TargetCluster)
		}

		return err

	} else {
		fmt.Println(Name, "is invalid Appwrapper")
		return err
	}

}

func (s *Scheduler) Schedule() {

	s.GetAppWrappers()
	s.GetClustersInfo()
	fmt.Println("\nDecision made by the optimizer:")
	if s.N > 0 {

		Targets := s.APX()

		fmt.Println("Job Name\t  \tDecision ")
		for i := 0; i < s.N; i++ {

			if Targets[i] < 0 {
				s.PutHoldOnAppWrapper(s.Jobs[i].Name)

			} else {
				s.ReleasHoldOnAppWrapper(s.Jobs[i].Name, s.Clusters[Targets[i]].Name)
			}
		}
	}
}

func (s *Scheduler) APX() []int {

	N := s.N
	M := s.M
	T := s.T

	// Set up the problem.
	Targets := make([]int, N)
	rb := []clp.Bounds{}
	mat := clp.NewPackedMatrix()
	obj := make([]float64, N*M*T)

	//set coeff of x in each row (constraint) and in objective function
	//Index is the index of constraint, Value is the coefficient of variable
	for i := 0; i < N; i++ {
		for j := 0; j < M; j++ {
			for t := 0; t < T; t++ {
				tmp := []clp.Nonzero{}
				tmp = append(tmp, clp.Nonzero{Index: i, Value: 1.0})                       // placement constraint
				tmp = append(tmp, clp.Nonzero{Index: N + M*T + i*M*T + j*T + t, Value: 1}) //0<=x_ij^t<=1 constraint

				for tt := t; tt < T; tt++ { // capacity constraint
					tmp = append(tmp, clp.Nonzero{Index: N + j*T + tt, Value: float64(s.Jobs[i].CPU)})
					if tt > t+int(s.Jobs[i].RemainTime) {
						break
					}
				}
				mat.AppendColumn(tmp)
				coeff := math.Log(1000 - float64(s.Jobs[i].Deadline-s.Jobs[i].RemainTime))
				obj[i*M*T+j*T+t] = 0
				for tt := t; tt < T; tt++ {
					obj[i*T*M+j*T+t] += s.Jobs[i].CPU / (coeff * s.Clusters[j].Carbon[tt]) ///
					if tt < t+int(s.Jobs[i].RemainTime) {
						break
					}
				}
			}
		}
	}
	for i := 0; i < N; i++ {
		rb = append(rb, clp.Bounds{Lower: 0, Upper: 1})
	}
	for j := 0; j < M; j++ {
		for t := 0; t < T; t++ {
			rb = append(rb, clp.Bounds{Lower: 0, Upper: float64(s.Clusters[j].CPU)}) //capacity limit
		}
	}

	for i := 0; i < N; i++ {
		for j := 0; j < M; j++ {
			for t := 0; t < int(s.T); t++ {
				rb = append(rb, clp.Bounds{Lower: 0, Upper: 1})

			}

		}
	}

	simp := clp.NewSimplex()
	simp.LoadProblem(mat, nil, obj, rb, nil)
	simp.SetOptimizationDirection(clp.Maximize)

	// Solve the optimization problem.
	simp.Primal(clp.NoValuesPass, clp.NoStartFinishOptions)
	//val := simp.ObjectiveValue()
	soln := simp.PrimalColumnSolution()
	f := .5
	for i := 0; i < N; i++ {
		Targets[i] = -1
		for j := 0; j < M; j++ {
			for t := 0; t < T; t++ {
				if soln[i*M*T+j*T+t] > f {
					Targets[i] = j
				}
			}
		}
	}
	return Targets
}

// Aggregated request by AppWrapper
func aggregateRequests(appWrapper *mcadv1beta1.AppWrapper) core.Weights2 {
	request := core.Weights2{}
	for _, r := range appWrapper.Spec.Resources.GenericItems {
		for _, cpr := range r.CustomPodResources {
			request.AddProd2(cpr.Replicas, core.NewWeights2(cpr.Requests))
		}
	}
	return request
}
