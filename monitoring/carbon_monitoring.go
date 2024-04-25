package monitoring

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"io/ioutil"
	"net/http"

	core "github.com/sustainablecomputing/caspian/core"
	mcadv1beta1 "github.com/tayebehbahreini/mcad/api/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/scheme"
)

var ClusterInfoResource = schema.GroupVersionResource{Group: mcadv1beta1.GroupVersion.Group,
	Version: mcadv1beta1.GroupVersion.Version, Resource: "clusterinfo"}

// A monitor instace contains a list of Clusters, and rest client
type Monitor struct {
	Spokes    []core.Cluster
	crdClient *rest.RESTClient
	T         int
}

// NewMonior creates a Monitor instance and configures its clients.
func NewMonitor(config *rest.Config,) *Monitor {
	m := &Monitor{
		Spokes:    []core.Cluster{},
		crdClient: &rest.RESTClient{},
		T:         core.DefaultT,
	}

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
	I, DataIsFetched := m.GetForecastedCarbonIntensity(spoke.GeoLocation)
	if !DataIsFetched {
		fmt.Print("Error to fetch data. Using history of Carbon....")
		I, DataIsFetched = m.GetCarbonFromFile("./monitoring/data/" + spoke.GeoLocation + ".csv")
	}
	if !DataIsFetched {
		for t := 0; t < m.T; t++ {
			I[t] = strconv.Itoa(core.DefaultCarbonIntensity)
		}
	}
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

	err = m.crdClient.Patch(types.JSONPatchType).Resource("clusterinfo").Name(spoke.Name).
		Namespace(apiv1.NamespaceDefault).Body(payload).Do(context.Background()).Error()

	return err

}

// Read carbon from file
func (m Monitor) GetCarbonFromFile(FilePath string) ([]string, bool) {
	I := make([]string, m.T)
	DataIsFetched := false
	file, err := os.Open(FilePath)
	if err != nil {
		fmt.Print("Carbon Data is not available")
	} else {

		reader := csv.NewReader(file)
		reader.FieldsPerRecord = -1 // Allow variable number of fields
		data, err := reader.ReadAll()
		if err == nil {

			t := 0
			for _, row := range data {
				I[t] = row[1]
				t = t + 1
				if t > m.T {
					break
				}
			}
			DataIsFetched = true
		}
	}
	return I, DataIsFetched

}

func (m Monitor) GetForecastedCarbonIntensity(zone string) ([]string, bool) {
	I := make([]string, m.T)
	DataIsFetched := false
	url := "https://api-access.electricitymaps.com/2w97h07rvxvuaa1g/carbon-intensity/forecast?zone=" + zone

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("auth-token", "qzfZVeA6xB0x198zJcvQ7ZyzaAz9Slhe")
	res, _ := http.DefaultClient.Do(req)

	if res != nil {
		defer res.Body.Close()
		body, _ := ioutil.ReadAll(res.Body)
		response := string(body)
		resBytes := []byte(response)                    // Converting the string "res" into byte array
		var jsonRes map[string][]map[string]interface{} // declaring a map for key names as string and values as interface
		err := json.Unmarshal(resBytes, &jsonRes)       // Unmarshalling
		if err == nil {
			a := jsonRes["forecast"]
			if a != nil {
				DataIsFetched = true
				for t := 0; t < m.T; t++ {
					I[t] = fmt.Sprintf("%v", a[t]["carbonIntensity"])
				}
			}
		}

	}
	if !DataIsFetched {
		url := "https://api-access.electricitymaps.com/free-tier/carbon-intensity/latest?zone=" + zone

		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Add("auth-token", "qzfZVeA6xB0x198zJcvQ7ZyzaAz9Slhe")

		res, _ := http.DefaultClient.Do(req)
		if res != nil {
			defer res.Body.Close()

			body, _ := ioutil.ReadAll(res.Body)

			resBytes := []byte(body)                  // Converting the string "res" into byte array
			var jsonRes map[string]interface{}        // declaring a map for key names as string and values as interface
			err := json.Unmarshal(resBytes, &jsonRes) // Unmarshalling
			if err != nil {
				a := jsonRes["carbonIntensity"]
				if a != nil {
					DataIsFetched = true
					for t := 0; t < m.T; t++ {
						I[t] = fmt.Sprintf("%v", a)
					}
				}
			}

		}

	}

	return I, DataIsFetched
}
