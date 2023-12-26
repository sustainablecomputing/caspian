package core

import (
	"gopkg.in/inf.v0"
	v1 "k8s.io/api/core/v1"
)

// Each Cluster instance represents a clusterinfo object in hub.
type Cluster struct {
	Name        string
	GeoLocation string
	Carbon      []float64
	CPU         float64
	GPU         float64
	PowerSlope  float64
}

type Job struct {
	Name            string
	Namespace       string
	Deadline        int64
	CPU             float64
	GPU             float64
	RunTime         int64
	RemainTime      int64
	PreferedCluster string
}

type Weights map[v1.ResourceName]*inf.Dec

// Converts a ResourceList to Weights
func NewWeights(r v1.ResourceList) Weights {
	w := Weights{}
	for k, v := range r {
		w[k] = v.AsDec() // should be lossless
	}
	return w
}

// / Add coefficient * weights to receiver
func (w Weights) AddProd(coefficient int32, r Weights) {
	for k, v := range r {
		if w[k] == nil {
			w[k] = &inf.Dec{} // fresh zero
		}
		tmp := inf.NewDec(int64(coefficient), 0)
		tmp.Mul(tmp, v)
		w[k].Add(w[k], tmp)
	}
}

type Weights2 map[v1.ResourceName]float64

// Converts a ResourceList to Weights
func NewWeights2(r v1.ResourceList) Weights2 {
	w := Weights2{}
	for k, v := range r {
		w[k] = float64(v.Value()) // should be lossless
	}
	return w
}

// / Add coefficient * weights to receiver
func (w Weights2) AddProd2(coefficient int32, r Weights2) {
	for k, v := range r {

		tmp := float64(coefficient) * v
		w[k] = w[k] + tmp
	}
}
