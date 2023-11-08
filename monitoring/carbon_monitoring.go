package monitoring

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"

	mcadv1beta1 "github.com/tayebehbahreini/mcad/api/v1beta1"
	core "github.ibm.com/tbahreini/caspian-mcad/core"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubectl/pkg/scheme"
)

var ClusterInfoResource = schema.GroupVersionResource{Group: mcadv1beta1.GroupVersion.Group,
	Version: mcadv1beta1.GroupVersion.Version, Resource: "clusterinfo"}

// A monitor instace contains a list of Clusters, a rest client, and a dynamic client.
type Monitor struct {
	Spokes        []core.Cluster
	dynamicClient dynamic.Interface
	crdClient     *rest.RESTClient
}

// NewMonior creates a Monitor instance and configures its clients.
func NewMonitor(kube_config string, hub_contxt string) *Monitor {
	m := &Monitor{
		Spokes:        []core.Cluster{},
		dynamicClient: nil,
		crdClient:     &rest.RESTClient{},
	}
	config, _ := buildConfigWithContextFromFlags(hub_contxt, kube_config)
	crdConfig := *config
	crdConfig.ContentConfig.GroupVersion = &schema.GroupVersion{Group: mcadv1beta1.GroupVersion.Group, Version: mcadv1beta1.GroupVersion.Version}
	crdConfig.APIPath = "/apis"
	crdConfig.NegotiatedSerializer = serializer.NewCodecFactory(scheme.Scheme)
	crdConfig.UserAgent = rest.DefaultKubernetesUserAgent()
	var err error
	m.crdClient, err = rest.UnversionedRESTClientFor(&crdConfig)
	if err != nil {
		panic(err)
	}
	m.dynamicClient, err = dynamic.NewForConfig(&crdConfig)
	if err != nil {
		panic(err)
	}

	return m
}

// create the list of spoke Clusters, each spoke is corresponding to one clusterinfo object in hub
func (m *Monitor) GetClusters() error {

	result := mcadv1beta1.ClusterInfoList{}
	err := m.crdClient.
		Get().
		Resource("clusterinfo").
		Do(context.Background()).
		Into(&result)

	for _, cluster := range result.Items {
		newCluster := core.Cluster{
			Name:        cluster.Name,
			GeoLocation: cluster.Spec.Geolocation,
		}
		m.Spokes = append(m.Spokes, newCluster)

	}
	return err
}

// get the names and geolocation of each spoke,
// update the carbon intensity of its corresponding cluster Info object in the hub.
func (m Monitor) UpdateClusterInfo() error {
	err := m.GetClusters()
	if err != nil {
		panic(err)
	}
	M := int(len(m.Spokes))

	for i := 0; i < M; i++ {
		err = m.UpdateCarbon(m.Spokes[i])
	}
	return err
}

// Retrive carbon intensity and update the carbon field of clusterinfo object
func (m Monitor) UpdateCarbon(spoke core.Cluster) error {

	I, _ := m.GetCarbonFromFile("./monitoring/" + spoke.GeoLocation + ".csv")
	patch := []interface{}{
		map[string]interface{}{
			"op":    "replace",
			"path":  "/spec/carbon",
			"value": I,
		},
	}
	payload, err := json.Marshal(patch)
	if err != nil {
		panic(err)
	}
	ClusterInfoClient := m.dynamicClient.Resource(ClusterInfoResource).Namespace(apiv1.NamespaceDefault)
	_, err = ClusterInfoClient.Patch(context.Background(), spoke.Name, types.JSONPatchType, payload, metav1.PatchOptions{})
	return err

}

// Read carbon from file
func (m Monitor) GetCarbonFromFile(FilePath string) ([]string, error) {
	I := make([]string, 24)
	file, err := os.Open(FilePath)
	if err != nil {
		fmt.Print("Carbon Data is not available")
	}
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow variable number of fields
	data, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	t := 0
	for _, row := range data {
		I[t] = row[0]
		t = t + 1
		if t > 23 {
			break
		}
	}
	return I, err

}

func buildConfigWithContextFromFlags(context string, kubeconfigPath string) (*rest.Config, error) {
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath},
		&clientcmd.ConfigOverrides{
			CurrentContext: context,
		}).ClientConfig()
}
