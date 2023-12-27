// Note: the example only works with the code within the same release/branch.
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/lanl/clp"
	core "github.com/sustainablecomputing/caspian/core"
	mcadv1beta1 "github.com/tayebehbahreini/mcad/api/v1beta1"
	apiv1 "k8s.io/api/core/v1"
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

type SchedulingDecision struct {
	i    int     //job index
	j    int     //cluster index
	t    int     //time slot
	xbar float64 //allocation by lp
}

type Objective struct { //
	j   int
	t   int
	val float64
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
	fmt.Println("Name\t CPU\t GPU\t RemainTime  \t Deadline \t \t \t Status ")
	for _, aw := range result.Items {

		// Aggregated request by AppWrapper

		if aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Queued") ||
			aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Running") ||
			aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Dispatching") ||
			aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Requeuing") ||
			aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Failed") { //aw.Spec.Sustainable &&
			TimeDispatched := aw.Spec.DispatcherStatus.TimeDispatched //+
			if aw.Spec.DispatcherStatus.Phase != mcadv1beta1.AppWrapperPhase("Queued") ||
				aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Failed") {
				TimeDispatched += int64(time.Since(aw.Spec.DispatcherStatus.LastDispatchingTime.Time).Seconds())
			}
			TimeDispatched += 20 //add a bit more
			//TimeDispatched = aw.Spec.DispatcherStatus.TimeDispatched

			remainTime := int64(math.Ceil(float64(aw.Spec.Sustainable.RunTime-TimeDispatched) / float64(s.PeriodLength)))
			if aw.Spec.Sustainable.RunTime == 0 { //runtime is not specified by user
				remainTime = int64(math.Ceil(float64(core.DefaultRunTime-int(TimeDispatched)) / float64(s.PeriodLength)))

			}
			deadline := int64(s.T)
			if !aw.Spec.Sustainable.Deadline.IsZero() { //deadline is not specified by user

				deadline = 0 - int64(math.Ceil(float64(time.Now().Sub(aw.Spec.Sustainable.Deadline.Time).Seconds()))/float64(s.PeriodLength))

			}
			//if

			awRequest := aggregateRequests(&aw)

			newJob := core.Job{
				Name:       aw.Name,
				Namespace:  aw.Namespace,
				CPU:        awRequest["cpu"],
				GPU:        awRequest["nvidia.com/gpu"],
				RemainTime: remainTime,
				Deadline:   deadline,
			}
			if remainTime <= 0 {
				//fmt.Println("\n Suspendind Appwrapper ", aw.Name)
				//s.DeleteAppWrapper(aw.Name)
				s.PutHoldOnAppWrapper(newJob)
			} else {
				s.N = s.N + 1
				s.Jobs = append(s.Jobs, newJob)
				fmt.Println(aw.Name, "\t", newJob.CPU, "\t", newJob.GPU, "\t", newJob.RemainTime, "\t\t", deadline, "\t", aw.Spec.DispatcherStatus.Phase)
			}

		}

	}

}

// get all clusterinfos in hub
func (s *Scheduler) GetClustersInfo() {
	s.Clusters = []core.Cluster{}
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
	fmt.Println("Name \t Available CPU \t Available GPU \t GeoLocation  ")
	for _, cluster := range result.Items {
		slope := 1.0 // strconv.ParseFloat(cluster.Spec.PowerSlope, 64)
		weight := core.NewWeights2(cluster.Status.Capacity)
		newCluster := core.Cluster{
			Name:        cluster.Name,
			GeoLocation: cluster.Spec.Geolocation,
			Carbon:      make([]float64, s.T),
			CPU:         weight["cpu"],
			GPU:         weight["nvidia.com/gpu"],
			PowerSlope:  slope,
		}
		for t := 0; t < s.T; t++ {
			newCluster.Carbon[t], _ = strconv.ParseFloat(cluster.Spec.Carbon[t], 64)
		}
		s.Clusters = append(s.Clusters, newCluster)
		fmt.Println(newCluster.Name, "\t", newCluster.CPU, "\t\t", newCluster.GPU, "\t\t", newCluster.GeoLocation)

		j = j + 1
	}
	s.M = len(result.Items)

}

// find an AW
func (s *Scheduler) IsValidAppWrapper(Job core.Job) error {
	err := s.crdClient.Get().Resource("appwrappers").
		Namespace(Job.Namespace).Name(Job.Name).Do(context.Background()).Error()

	return err
}

// delete an AW
func (s *Scheduler) DeleteAppWrapper(Name string) error {
	err := s.crdClient.Delete().Resource("appwrappers").
		Namespace(apiv1.NamespaceDefault).Name(Name).Do(context.Background()).Error()
	return err
}

// add sustainability gate from the appwrapper
func (s *Scheduler) PutHoldOnAppWrapper(Job core.Job) error {
	err := s.IsValidAppWrapper(Job)

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
		err = s.crdClient.Patch(types.JSONPatchType).Resource("appwrappers").Name(Job.Name).
			Namespace(Job.Namespace).Body(payload).Do(context.Background()).Error()
		/*if err == nil {
			fmt.Println(Job.Name, "\t Suspend ")
		}*/

		return err

	} else {
		fmt.Println(Job.Name, "is invalid Appwrapper")
		return err
	}
}

// delete sustainability gate from the appwrapper and set the targetCluster
func (s *Scheduler) RemoveGateFromAW(Job core.Job, TargetCluster string) error {
	err := s.IsValidAppWrapper(Job)

	if err == nil {
		var a []string
		patch := []interface{}{
			map[string]interface{}{
				"op":    "replace",
				"path":  "/spec/dispatchingGates",
				"value": a,
			}, map[string]interface{}{
				"op":    "replace",
				"path":  "/spec/schedulingSpec/clusterScheduling/policyResult/targetCluster/name",
				"value": TargetCluster,
			},
		}
		payload, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		err = s.crdClient.Patch(types.JSONPatchType).Resource("appwrappers").Name(Job.Name).
			Namespace(Job.Namespace).Body(payload).Do(context.Background()).Error()
		if err == nil {
			fmt.Println(Job.Name, "\t \tScheduled on", TargetCluster)
		} else {
			panic(err) //return err
		}

		return err

	} else {
		fmt.Println(Job.Name, "is invalid Appwrapper")
		return err
	}

}

func (s *Scheduler) Schedule(optimizer string) {
	sustainable := false
	if optimizer == "sustainable" {
		sustainable = true
	}
	s.GetAppWrappers()
	s.GetClustersInfo()
	fmt.Println("\nDecision made by the optimizer:")
	if s.N > 0 && s.M > 0 {

		Targets := s.Optimize(sustainable)

		for i := 0; i < s.N; i++ {

			if Targets[i] < 0 {
				s.PutHoldOnAppWrapper(s.Jobs[i])

			} else {
				s.RemoveGateFromAW(s.Jobs[i], s.Clusters[Targets[i]].Name)
			}
		}
	}
}
func (s *Scheduler) Optimize(sustainable bool) []int {

	N := s.N
	M := s.M
	T := s.T
	omega1 := 0.0
	omega2 := 1.0
	theta1 := 1.0
	theta2 := 1.0
	obj1 := make([]float64, N*M*T)
	obj2 := make([]float64, N*M*T)
	obj := make([]float64, N*M*T)
	Available_GPU := make([]float64, M*T)
	Available_CPU := make([]float64, M*T)
	Targets := make([]int, N)
	Objectives := make([][]Objective, N)
	for i := 0; i < N; i++ {
		for j := 0; j < M; j++ {
			for t := 0; t < T; t++ {
				lateness := float64(t) + float64(s.Jobs[i].RemainTime-s.Jobs[i].Deadline)
				if lateness < 0 {
					lateness = 0
				}

				obj1[i*M*T+j*T+t] = 0

				for tt := t; tt < min(s.T, t+int(s.Jobs[i].RemainTime)); tt++ {
					obj1[i*T*M+j*T+t] += (s.Jobs[i].CPU + s.Jobs[i].GPU) / (s.Clusters[j].Carbon[tt] * s.Clusters[j].PowerSlope)
				}
				obj2[i*M*T+j*T+t] = float64(1.0 / (float64(t) + float64(s.Jobs[i].RemainTime) + lateness))
				fmt.Println(obj1[i*T*M+j*T+t], obj2[i*T*M+j*T+t], "fff")
			}
		}
	}

	if sustainable {
		omega1 = .5
		omega2 = .5
		_, theta1 = s.LPSolve(obj1)
		_, theta2 = s.LPSolve(obj2)
		fmt.Println(theta1, theta2, "lll")
	}
	for i := 0; i < N; i++ {
		cnt := 0
		Objectives[i] = make([]Objective, M*T)
		for j := 0; j < M; j++ {
			for t := 0; t < T; t++ {
				obj[i*M*T+j*T+t] = omega1*obj1[i*M*T+j*T+t]/theta1 + omega2*obj2[i*M*T+j*T+t]/theta2
				fmt.Println(omega1*obj1[i*M*T+j*T+t]/theta1, omega2*obj2[i*M*T+j*T+t]/theta2, "kkk")
				Objectives[i][cnt].j = j
				Objectives[i][cnt].t = t
				Objectives[i][cnt].val = obj[i*T*M+j*T+t]
				Available_GPU[j*T+t] = s.Clusters[j].GPU
				Available_CPU[j*T+t] = s.Clusters[j].CPU
				cnt++
			}
		}
		sort.Slice(Objectives[i], func(h, k int) bool {
			return Objectives[i][h].val > Objectives[i][k].val
		})
	}

	SchedulingDecisions, _ := s.LPSolve(obj)

	sort.Slice(SchedulingDecisions, func(i, j int) bool {
		return SchedulingDecisions[i].xbar > SchedulingDecisions[j].xbar
	})

	for ii := 0; ii < N; ii++ {

		i := SchedulingDecisions[ii].i
		j := SchedulingDecisions[ii].j
		t := SchedulingDecisions[ii].t
		Targets[i] = -1
		found := false
		if SchedulingDecisions[ii].xbar >= 1 {

			found = true
		} else {
			for cnt := 0; cnt < M*T; cnt++ {
				j = Objectives[i][cnt].j
				t = Objectives[i][cnt].t

				found = true
				for tt := t; tt < t+int(s.Jobs[i].RemainTime); tt++ {
					if Available_GPU[j*T+tt] < s.Jobs[i].GPU || Available_CPU[j*T+tt] < s.Jobs[i].CPU {
						//	fmt.Println(i, j, t, "llll")
						found = false
						break
					}
				}
				if found {
					//fmt.Println(i, j, t, "kkkk")
					break

				}
			}
		}

		if found {
			if t == 0 {
				Targets[i] = j
			}

			for tt := t; tt < t+int(s.Jobs[i].RemainTime); tt++ {
				Available_GPU[j*T+tt] -= s.Jobs[i].GPU
				Available_CPU[j*T+tt] -= s.Jobs[i].CPU

			}
		}
	}
	return Targets

}

func (s *Scheduler) LPSolve(obj []float64) ([]SchedulingDecision, float64) {

	N := s.N
	M := s.M
	T := s.T
	SchedulingDecisions := make([]SchedulingDecision, N)
	OPT := 0.0
	// Set up the problem.

	rb := []clp.Bounds{}
	mat := clp.NewPackedMatrix()
	//obj := make([]float64, N*M*T)
	//set coeff of x in each row (constraint) and in objective function
	//Index is the index of constraint, Value is the coefficient of variable
	for i := 0; i < N; i++ {
		for j := 0; j < M; j++ {
			for t := 0; t < T; t++ {
				tmp := []clp.Nonzero{}
				tmp = append(tmp, clp.Nonzero{Index: i, Value: 1.0})                         // placement constraint
				tmp = append(tmp, clp.Nonzero{Index: N + 2*M*T + i*M*T + j*T + t, Value: 1}) //0<=x_ij^t<=1 constraint;2 is for two capacity constarint:cpu and gpu

				for tt := t; tt < T; tt++ { // capacity constraint: cpu and gpu
					if s.Jobs[i].CPU > 0 {
						tmp = append(tmp, clp.Nonzero{Index: N + j*T + tt, Value: float64(s.Jobs[i].CPU)})

					}
					if s.Jobs[i].GPU > 0 {
						tmp = append(tmp, clp.Nonzero{Index: N + M*T + j*T + tt, Value: float64(s.Jobs[i].GPU)})
					}
					if tt > t+int(s.Jobs[i].RemainTime) {
						break
					}
				}
				mat.AppendColumn(tmp)

			}
		}
	}
	for i := 0; i < N; i++ {
		rb = append(rb, clp.Bounds{Lower: 0, Upper: 1})
	}
	for j := 0; j < M; j++ {
		for t := 0; t < T; t++ {
			rb = append(rb, clp.Bounds{Lower: 0, Upper: float64(s.Clusters[j].CPU)}) //cpu capacity limit
		}
	}
	for j := 0; j < M; j++ {
		for t := 0; t < T; t++ {
			rb = append(rb, clp.Bounds{Lower: 0, Upper: float64(s.Clusters[j].GPU)}) //gpu capacity limit
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
	OPT = simp.ObjectiveValue()
	soln := simp.PrimalColumnSolution()
	for i := 0; i < N; i++ {
		SchedulingDecisions[i].i = i
		SchedulingDecisions[i].j = -1
		SchedulingDecisions[i].xbar = 0
		for j := 0; j < M; j++ {
			for t := 0; t < T; t++ {
				if soln[i*M*T+j*T+t] > 0 {
					SchedulingDecisions[i].t = t
					SchedulingDecisions[i].j = j
					SchedulingDecisions[i].xbar = soln[i*M*T+j*T+t]
				}
			}
		}
	}
	return SchedulingDecisions, OPT
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
