// Note: the example only works with the code within the same release/branch.
package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/lanl/clp"
	mcadv1beta1 "github.com/tayebehbahreini/mcad/api/v1beta1"
	core "github.ibm.com/tbahreini/caspian-mcad/core"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubectl/pkg/scheme"
)

type Dispatcher struct {
	N             int // number of  jobs (AWs) (to be scheduled/rescheduled)
	M             int // number of available clusters
	T             int // length of time horizon (number of timeslots)
	PeriodLength  int
	Jobs          []core.Job     // list of jobs
	Clusters      []core.Cluster // spoke clusters
	crdClient     *rest.RESTClient
	dynamicClient dynamic.Interface
}

var AWResource = schema.GroupVersionResource{Group: mcadv1beta1.GroupVersion.Group,
	Version: mcadv1beta1.GroupVersion.Version, Resource: "appwrappers"}

// NewDispatcher : create a new dispather instance and
// configure the clients with kube_config and hub-context
func NewDispatcher(kube_config string, hub_contxt string) *Dispatcher {
	d := &Dispatcher{
		N:             0,
		M:             0,
		T:             core.DefaultTValue,
		PeriodLength:  core.DefaultLengthValue,
		Jobs:          []core.Job{},
		Clusters:      []core.Cluster{},
		crdClient:     &rest.RESTClient{},
		dynamicClient: nil,
	}
	config, err := buildConfigWithContextFromFlags(hub_contxt, kube_config)
	crdConfig := *config
	crdConfig.ContentConfig.GroupVersion = &schema.GroupVersion{Group: mcadv1beta1.GroupVersion.Group,
		Version: mcadv1beta1.GroupVersion.Version}
	crdConfig.APIPath = "/apis"
	crdConfig.NegotiatedSerializer = serializer.NewCodecFactory(scheme.Scheme)
	crdConfig.UserAgent = rest.DefaultKubernetesUserAgent()
	d.crdClient, err = rest.UnversionedRESTClientFor(&crdConfig)
	if err != nil {
		panic(err)
	}

	d.dynamicClient, err = dynamic.NewForConfig(&crdConfig)
	if err != nil {
		panic(err)
	}
	return d
}

// retrive all Appwrappers in hub: running+ non-running AWs.
// Calculate requested resources  of each AW.
// Save all AWs with their characteristics in Jobs array
func (d *Dispatcher) GetAppWrappers() {
	result := mcadv1beta1.AppWrapperList{}
	d.Jobs = []core.Job{}
	err := d.crdClient.
		Get().
		Resource("appwrappers").
		Do(context.Background()).
		Into(&result)
	if err != nil {
		panic(err)
	}
	d.N = 0
	fmt.Println("\n Name\t CPU\t RemainTime  \t Deadline  ")
	for _, aw := range result.Items {

		// Aggregated request by AppWrapper

		if true || aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Queued") ||
			aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Running") ||
			aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Dispatching") ||
			aw.Spec.DispatcherStatus.Phase == mcadv1beta1.AppWrapperPhase("Requeuing") { //aw.Spec.Sustainable &&
			remainTime := aw.Spec.Sustainable.RunTime - aw.Spec.DispatcherStatus.TimeDispatched/int64(d.PeriodLength)
			if remainTime < 0 {
				//d.DeleteAppWrapper(aw.Name)
			} else {
				awRequest := aggregateRequests(&aw)
				d.N = d.N + 1
				newJob := core.Job{
					Name:       aw.Name,
					CPU:        awRequest["cpu"],
					RemainTime: remainTime, // - C.int(aw.Spec.DispatcherStatus.DispatchedNanos/20000000000),
					Deadline:   int64(math.Ceil(float64(aw.Spec.Sustainable.Deadline.Sub(metav1.Now().Time).Seconds()) / float64(d.PeriodLength))),
				}
				fmt.Print(newJob.CPU)

				d.Jobs = append(d.Jobs, newJob)
				fmt.Println(aw.Name, "\t", newJob.CPU, "\t", newJob.RemainTime, "\t\t", newJob.Deadline)

			}
		}

	}

}

// get all clusterinfos in hub
func (d *Dispatcher) GetClustersInfo() {
	result := mcadv1beta1.ClusterInfoList{}
	err := d.crdClient.
		Get().
		Resource("clusterinfo").
		Do(context.Background()).
		Into(&result)
	if err != nil {
		panic(err)
	}
	j := 0
	for _, cluster := range result.Items {
		newCluster := core.Cluster{
			Name:   cluster.Name,
			CPU:    7000, //cluster.Status.Capacity.Cpu().Value(),
			Carbon: make([]float64, 24),
		}
		for t := 0; t < 24; t++ {
			newCluster.Carbon[t], _ = strconv.ParseFloat(cluster.Spec.Carbon[t], 64)
		}
		d.Clusters = append(d.Clusters, newCluster)
		j = j + 1
	}
	d.M = len(result.Items)
	fmt.Println(d.Clusters[0].CPU)
}

// retrive an AW
func (d *Dispatcher) GetAppWrapper(name string) (unstructured.Unstructured, error) {

	result, err := d.dynamicClient.Resource(AWResource).Namespace(apiv1.NamespaceDefault).Get(context.Background(),
		name, metav1.GetOptions{})
	if err != nil {
		return unstructured.Unstructured{}, err
	}
	return *result, err
}

// delete an AW
func (d *Dispatcher) DeleteAppWrapper(name string) error {

	err := d.dynamicClient.Resource(AWResource).Namespace(apiv1.NamespaceDefault).Delete(context.Background(),
		name, metav1.DeleteOptions{})
	if err != nil {
		panic(err)
	}

	return err
}

// add sustainability gate from the appwrapper
func (d *Dispatcher) PutHoldOnAppWrapper(Name string) error {
	_, err := d.GetAppWrapper(Name)

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
			panic(err)
		}
		AWClient := d.dynamicClient.Resource(AWResource).Namespace(apiv1.NamespaceDefault)

		_, err = AWClient.Patch(context.Background(), Name, types.JSONPatchType, payload, metav1.PatchOptions{})
		if err != nil {
			panic(err)

		}
	}

	return nil
}

// delete sustainability gate from the appwrapper and set the targetCluster
func (d *Dispatcher) ReleasHoldOnAppWrapper(Name string, TargetCluster string) error {
	_, err := d.GetAppWrapper(Name)

	if err == nil {
		var a []string

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
		AWClient := d.dynamicClient.Resource(AWResource).Namespace(apiv1.NamespaceDefault)

		_, err = AWClient.Patch(context.Background(), Name, types.JSONPatchType, payload, metav1.PatchOptions{})
		if err != nil {
			return err
		}

		patch = []interface{}{
			map[string]interface{}{
				"op":    "replace",
				"path":  "/spec/schedulingSpec/clusterScheduling/policyResult/targetCluster/name",
				"value": TargetCluster,
			},
		}

		payload, err = json.Marshal(patch)
		if err != nil {
			return err
		}

		_, err = AWClient.Patch(context.Background(), Name, types.JSONPatchType, payload, metav1.PatchOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Dispatcher) Run() {

	d.GetAppWrappers()
	d.GetClustersInfo()

	if d.N > 0 {

		Targets := d.APX()

		fmt.Println("Name\t Status ")
		for i := 0; i < d.N; i++ {
			print(Targets[i], " ")

			if Targets[i] < 0 {
				d.PutHoldOnAppWrapper(d.Jobs[i].Name)
				fmt.Println(d.Jobs[i].Name, "\t On Hold ")
			} else {
				d.ReleasHoldOnAppWrapper(d.Jobs[i].Name, d.Clusters[Targets[i]].Name)
				fmt.Println(d.Jobs[i].Name, "\t No Hold  -->", d.Clusters[Targets[i]].Name)
			}

		}
	}

}

func (d *Dispatcher) APX() []int {

	N := d.N
	M := d.M
	T := d.T

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
					tmp = append(tmp, clp.Nonzero{Index: N + j*T + tt, Value: float64(d.Jobs[i].CPU)})
					if tt > t+int(d.Jobs[i].RemainTime) {
						break
					}
				}
				mat.AppendColumn(tmp)
				/*	mat.AppendColumn([]clp.Nonzero{
						{Index: i, Value: 1.0},                              // placement constraint
						{Index: N + j*T + t, Value: float64(d.Jobs[i].CPU)}, // capacity constraint
						{Index: N + M*T + i*M*T + j*T + t, Value: 1},        //0<=x_ij^t<=1 constraint
					})
				*/
				coeff := math.Log(1000 - float64(d.Jobs[i].Deadline-d.Jobs[i].RemainTime))
				obj[i*M*T+j*T+t] = 0
				for tt := t; tt < T; tt++ {
					obj[i*T*M+j*T+t] += d.Jobs[i].CPU / (coeff * d.Clusters[j].Carbon[tt]) ///
					if tt < t+int(d.Jobs[i].RemainTime) {
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
			rb = append(rb, clp.Bounds{Lower: 0, Upper: float64(d.Clusters[j].CPU)}) //capacity limit
		}
	}

	for i := 0; i < N; i++ {
		for j := 0; j < M; j++ {
			for t := 0; t < int(d.T); t++ {

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

func buildConfigWithContextFromFlags(context string, kubeconfigPath string) (*rest.Config, error) {
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath},
		&clientcmd.ConfigOverrides{
			CurrentContext: context,
		}).ClientConfig()
}

