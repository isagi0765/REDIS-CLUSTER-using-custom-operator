/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RedisClusterSpec defines the desired state of RedisCluster
type RedisClusterSpec struct {
	// nodes is the number of master/shard nodes in the cluster.
	// +kubebuilder:validation:Minimum=3
	// +kubebuilder:default=3
	Nodes int32 `json:"nodes"`

	// replicasPerNode is the number of replicas per master shard.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	ReplicasPerNode int32 `json:"replicasPerNode"`

	// image is the Redis/Valkey container image to run.
	// +kubebuilder:default="redis:7.2-alpine"
	Image string `json:"image,omitempty"`

	// resources sets CPU/memory requests and limits for each node.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// storageSize is the size of each node's persistent volume, e.g. "1Gi".
	// +kubebuilder:default="1Gi"
	StorageSize string `json:"storageSize,omitempty"`

	// storageClassName is the StorageClass used for each node's PVC.
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`
}

// RedisClusterStatus defines the observed state of RedisCluster.
type RedisClusterStatus struct {
	// phase is a short human-readable summary of the cluster's current state.
	// +optional
	Phase string `json:"phase,omitempty"`

	// readyMasters is how many master nodes are currently healthy.
	// +optional
	ReadyMasters int32 `json:"readyMasters,omitempty"`

	// readyReplicas is how many replica nodes are currently healthy.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the RedisCluster resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Nodes",type=integer,JSONPath=`.spec.nodes`
// +kubebuilder:printcolumn:name="Ready Masters",type=integer,JSONPath=`.status.readyMasters`
// +kubebuilder:printcolumn:name="Ready Replicas",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// RedisCluster is the Schema for the redisclusters API
type RedisCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of RedisCluster
	// +required
	Spec RedisClusterSpec `json:"spec"`

	// status defines the observed state of RedisCluster
	// +optional
	Status RedisClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RedisClusterList contains a list of RedisCluster
type RedisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []RedisCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &RedisCluster{}, &RedisClusterList{})
		return nil
	})
}
