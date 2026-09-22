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

package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1 "yourorg.io/redis-cluster-operator/api/v1"
)

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cache.yourorg.io,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.yourorg.io,resources=redisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.yourorg.io,resources=redisclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;delete

func labelsFor(name string) map[string]string {
	return map[string]string{
		"app":                                 "redis-cluster",
		"redis-cluster.cache.yourorg.io/name": name,
	}
}

//go:fix inline
func nodeInclusionPolicyPtr(p corev1.NodeInclusionPolicy) *corev1.NodeInclusionPolicy {
	return new(p)
}

// desiredHeadlessService builds the governing headless Service every
// StatefulSet requires for stable pod network identity.
func desiredHeadlessService(rc *cachev1.RedisCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rc.Name + "-headless",
			Namespace: rc.Namespace,
			Labels:    labelsFor(rc.Name),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  labelsFor(rc.Name),
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: 6379, TargetPort: intstr.FromInt32(6379)},
				{Name: "gossip", Port: 16379, TargetPort: intstr.FromInt32(16379)},
			},
		},
	}
}

// desiredStatefulSet builds the StatefulSet running all nodes as uniform
// pods -- no static leader/follower identity. Role assignment happens
// later, in our own Go code, once we can see actual pod placement --
// deliberately NOT encoded as a Kubernetes scheduling constraint, which is
// exactly what caused every deadlock we hit with the pre-built operator.
func desiredStatefulSet(rc *cachev1.RedisCluster) (*appsv1.StatefulSet, error) {
	totalReplicas := rc.Spec.Nodes * (1 + rc.Spec.ReplicasPerNode)

	storageQty, err := resource.ParseQuantity(rc.Spec.StorageSize)
	if err != nil {
		return nil, fmt.Errorf("invalid storageSize %q: %w", rc.Spec.StorageSize, err)
	}

	labels := labelsFor(rc.Name)

	pvcTemplate := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: storageQty},
			},
		},
	}
	if rc.Spec.StorageClassName != "" {
		pvcTemplate.Spec.StorageClassName = &rc.Spec.StorageClassName
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rc.Name,
			Namespace: rc.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: rc.Name + "-headless",
			Replicas:    &totalReplicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					// Plain, single spread rule -- no competing per-pod
					// exclusion, so HARD is safe here (unlike the
					// pre-built operator's webhook + balance-rule
					// combination, which deadlocked repeatedly because
					// two DIFFERENT hard rules fought over the same
					// pods). 6 pods / 3 nodes divides evenly, so this
					// will always be satisfiable.
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
						{
							MaxSkew:           1,
							TopologyKey:       "kubernetes.io/hostname",
							WhenUnsatisfiable: corev1.DoNotSchedule,
							LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
							// Without this, a tainted/unschedulable node
							// (e.g. the control-plane) still counts as a
							// valid "empty" domain in the skew math,
							// dragging the effective minimum to 0 and
							// blocking any pod past the first N-1 nodes.
							// Confirmed hands-on: the 4th pod got stuck
							// Pending with "didn't match pod topology
							// spread constraints" on all 3 real worker
							// nodes, even though 2-per-node should have
							// been well within maxSkew:1.
							NodeTaintsPolicy: nodeInclusionPolicyPtr(corev1.NodeInclusionPolicyHonor),
						},
					},
					Containers: []corev1.Container{
						{
							Name:            "redis",
							Image:           rc.Spec.Image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{
									Name: "POD_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
									},
								},
							},
							Command: []string{
								"redis-server",
								"--port", "6379",
								"--cluster-enabled", "yes",
								"--cluster-config-file", "/data/nodes.conf",
								"--cluster-node-timeout", "5000",
								"--appendonly", "yes",
								"--protected-mode", "no",
								"--bind", "0.0.0.0",
								// baked in from day one -- this exact
								// omission caused the stale-gossip
								// "?:6379" bug we hit twice with the
								// hand-built cluster.
								"--cluster-announce-ip", "$(POD_IP)",
							},
							Ports: []corev1.ContainerPort{
								{Name: "redis", ContainerPort: 6379},
								{Name: "gossip", ContainerPort: 16379},
							},
							Resources: rc.Spec.Resources,
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/data"},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{pvcTemplate},
		},
	}, nil
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var redisCluster cachev1.RedisCluster
	if err := r.Get(ctx, req.NamespacedName, &redisCluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("reconciling RedisCluster",
		"name", redisCluster.Name,
		"nodes", redisCluster.Spec.Nodes,
		"replicasPerNode", redisCluster.Spec.ReplicasPerNode,
	)

	// 1. Headless Service -- create if missing. Not updating existing ones
	// yet; that comes later once this basic create path is proven.
	svc := desiredHeadlessService(&redisCluster)
	if err := ctrl.SetControllerReference(&redisCluster, svc, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	var existingSvc corev1.Service
	if err := r.Get(ctx, client.ObjectKeyFromObject(svc), &existingSvc); apierrors.IsNotFound(err) {
		log.Info("creating headless service", "name", svc.Name)
		if err := r.Create(ctx, svc); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating headless service: %w", err)
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}

	// 2. StatefulSet -- create if missing.
	sts, err := desiredStatefulSet(&redisCluster)
	if err != nil {
		// A bad spec (e.g. unparseable storageSize) isn't something
		// retrying will fix -- surface it in status instead of
		// requeuing forever.
		redisCluster.Status.Phase = "Failed"
		_ = r.Status().Update(ctx, &redisCluster)
		return ctrl.Result{}, err
	}
	if err := ctrl.SetControllerReference(&redisCluster, sts, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	var existingSts appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(sts), &existingSts); apierrors.IsNotFound(err) {
		log.Info("creating statefulset", "name", sts.Name, "replicas", *sts.Spec.Replicas)
		if err := r.Create(ctx, sts); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating statefulset: %w", err)
		}
		existingSts = *sts
	} else if err != nil {
		return ctrl.Result{}, err
	}

	// 3. Nothing to do at the Redis level until every pod is actually
	// running -- MEET against a pod that has no IP yet just fails.
	if existingSts.Status.ReadyReplicas != *sts.Spec.Replicas {
		log.Info("waiting for all pods to be ready",
			"ready", existingSts.Status.ReadyReplicas, "desired", *sts.Spec.Replicas)
		return ctrl.Result{}, r.setPhase(ctx, &redisCluster, "Provisioning", 0, 0)
	}

	// 4. Gather the real pod -> Kubernetes node mapping. This is the input
	// that makes safe pairing possible, and it can only come from the
	// Kubernetes API -- Redis has no idea what a node is.
	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(redisCluster.Namespace),
		client.MatchingLabels(labelsFor(redisCluster.Name)),
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing pods: %w", err)
	}

	nodes := make([]nodeInfo, 0, len(podList.Items))
	for _, pod := range podList.Items {
		if pod.Status.PodIP == "" || pod.Spec.NodeName == "" {
			// Still settling; come back shortly rather than acting on a
			// half-known topology.
			log.Info("pod not fully scheduled yet, requeueing", "pod", pod.Name)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		nodes = append(nodes, nodeInfo{
			PodName: pod.Name,
			K8sNode: pod.Spec.NodeName,
			IP:      pod.Status.PodIP,
		})
	}
	// Deterministic order so repeated reconciles behave identically.
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].PodName < nodes[j].PodName })

	// 5. Bootstrap the Redis cluster if it isn't formed yet. Guarded so
	// this is idempotent -- an already-healthy cluster is never touched.
	if !isBootstrapped(ctx, nodes) {
		log.Info("bootstrapping redis cluster", "pods", len(nodes), "masters", redisCluster.Spec.Nodes)
		if err := bootstrapCluster(ctx, nodes, int(redisCluster.Spec.Nodes)); err != nil {
			log.Error(err, "bootstrap failed, will retry")
			_ = r.setPhase(ctx, &redisCluster, "Bootstrapping", 0, 0)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		log.Info("bootstrap complete")
		// Give gossip a moment to settle before reporting roles.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 6. Cluster is formed. Populate real Redis role/pairing data into
	// `nodes` -- this was the actual bug: rebalanceMasters() was
	// previously called on the bare pod-list version of `nodes` (only
	// PodName/K8sNode/IP set), so every IsMaster was false by zero-value
	// default and the imbalance check silently never triggered, no
	// matter how unbalanced the real cluster was.
	byIP := map[string]*nodeInfo{}
	for i := range nodes {
		byIP[nodes[i].IP] = &nodes[i]
	}
	nodesKnown := false
	for _, n := range nodes {
		if err := parseClusterNodes(ctx, n.IP, byIP); err == nil {
			nodesKnown = true
			break
		}
	}
	if !nodesKnown {
		log.Info("could not read cluster state from any pod, will retry")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Before reporting status, check whether a past failover left two
	// masters on the same K8s node -- planRoles() only ran once, at
	// bootstrap, so this is the only thing that catches and corrects
	// drift after a real failure. Confirmed necessary: our own failover
	// test produced exactly this imbalance.
	if acted, rbErr := rebalanceMasters(ctx, nodes); rbErr != nil {
		log.Error(rbErr, "master rebalance check failed")
		// Don't fail the whole reconcile over this -- still report
		// current (imperfect) status below, and try again next cycle.
	} else if acted {
		log.Info("issued CLUSTER FAILOVER to correct a same-node master imbalance")
		// Give the failover a moment to complete before re-observing.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 7. Report observed roles (from the same data we already fetched
	// above -- no need to re-query Redis a second time).
	var mastersReady, replicasReady int32
	for _, n := range nodes {
		if n.NodeID == "" {
			continue
		}
		if n.IsMaster {
			mastersReady++
		} else {
			replicasReady++
		}
	}
	log.Info("cluster healthy", "masters", mastersReady, "replicas", replicasReady)
	if err := r.setPhase(ctx, &redisCluster, "Ready", mastersReady, replicasReady); err != nil {
		return ctrl.Result{}, err
	}
	// Requeue periodically even when nothing changed at the Kubernetes
	// level -- master/replica role changes inside Redis are invisible to
	// K8s watches entirely, so without this the rebalance check above
	// would only ever fire once, right after a pod-recreation event, and
	// never again even if imbalance persisted.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// setPhase writes status only when something actually changed, so we don't
// generate a self-triggering write loop on every reconcile.
func (r *RedisClusterReconciler) setPhase(
	ctx context.Context, rc *cachev1.RedisCluster, phase string, masters, replicas int32,
) error {
	if rc.Status.Phase == phase &&
		rc.Status.ReadyMasters == masters &&
		rc.Status.ReadyReplicas == replicas {
		return nil
	}
	rc.Status.Phase = phase
	rc.Status.ReadyMasters = masters
	rc.Status.ReadyReplicas = replicas
	return r.Status().Update(ctx, rc)
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1.RedisCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Named("rediscluster").
		Complete(r)
}
