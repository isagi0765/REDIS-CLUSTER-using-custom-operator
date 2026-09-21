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
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	redisPort  = 6379
	totalSlots = 16384
)

// nodeInfo is one Redis node as the operator understands it: which pod it
// is, which Kubernetes node it sits on, and (once the cluster knows about
// it) its Redis cluster identity and role.
//
// The K8sNode field is the whole point of this struct -- it is what lets us
// compute a pairing where no replica shares a node with its own master.
// Redis itself has no concept of Kubernetes nodes, so this mapping can only
// come from the operator.
type nodeInfo struct {
	PodName  string
	K8sNode  string
	IP       string
	NodeID   string // Redis's own 40-char node id
	IsMaster bool
	MasterID string // set when this node is a replica
	HasSlots bool
}

func redisClient(ip string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", ip, redisPort),
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		// Explicit short timeouts on purpose: a dead pod should fail fast
		// rather than hang the whole reconcile. The hand-built Go client
		// earlier in this project sat for 25s on a killed master because
		// it relied on the OS default dial timeout.
	})
}

// clusterInfoField pulls a single field out of CLUSTER INFO's flat
// "key:value" text output.
func clusterInfoField(ctx context.Context, ip, field string) (string, error) {
	c := redisClient(ip)
	defer c.Close()

	out, err := c.ClusterInfo(ctx).Result()
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, field+":") {
			return strings.TrimPrefix(line, field+":"), nil
		}
	}
	return "", fmt.Errorf("field %q not found in CLUSTER INFO", field)
}

// parseClusterNodes reads CLUSTER NODES from one pod and fills in Redis
// identity/role for every node we already know about by IP.
//
// CLUSTER NODES line format:
//
//	<id> <ip:port@cport[,hostname]> <flags> <master-id> ... <slots...>
func parseClusterNodes(ctx context.Context, ip string, byIP map[string]*nodeInfo) error {
	c := redisClient(ip)
	defer c.Close()

	raw, err := c.ClusterNodes(ctx).Result()
	if err != nil {
		return err
	}

	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		addr := strings.Split(fields[1], "@")[0]
		nodeIP := strings.Split(addr, ":")[0]

		n, ok := byIP[nodeIP]
		if !ok {
			// A node the cluster knows about that is not one of our
			// current pods -- i.e. a leftover entry from a deleted pod.
			// The Python reconciler had to learn about these the hard
			// way ("ghost nodes" that stay flagged fail forever and
			// block every other health check). Purging them belongs in
			// a later step; for now we just don't let them corrupt our
			// view of the real pods.
			continue
		}

		n.NodeID = fields[0]
		n.IsMaster = strings.Contains(fields[2], "master")
		if !n.IsMaster {
			n.MasterID = fields[3]
		}
		n.HasSlots = len(fields) >= 9 && fields[8] != ""
	}
	return nil
}

// isBootstrapped reports whether the Redis cluster is already formed, so
// reconcile stays idempotent and never re-bootstraps a live cluster.
func isBootstrapped(ctx context.Context, nodes []nodeInfo) bool {
	for _, n := range nodes {
		state, err := clusterInfoField(ctx, n.IP, "cluster_state")
		if err != nil {
			continue
		}
		assigned, err := clusterInfoField(ctx, n.IP, "cluster_slots_assigned")
		if err != nil {
			continue
		}
		if state == "ok" && assigned == fmt.Sprint(totalSlots) {
			return true
		}
	}
	return false
}

// slotRanges splits the 16384 slots across numMasters, giving any remainder
// to the last master (so 3 masters get 0-5460, 5461-10922, 10923-16383).
func slotRanges(numMasters int) [][2]int {
	per := totalSlots / numMasters
	ranges := make([][2]int, 0, numMasters)
	start := 0
	for i := 0; i < numMasters; i++ {
		end := start + per - 1
		if i == numMasters-1 {
			end = totalSlots - 1
		}
		ranges = append(ranges, [2]int{start, end})
		start = end + 1
	}
	return ranges
}

// planRoles decides which pods become masters and which master each replica
// backs, such that no replica ever shares a Kubernetes node with its own
// master.
//
// This is the piece that no amount of Kubernetes scheduling configuration
// could reliably deliver earlier in this project: a hard scheduling rule
// deadlocked (the scheduler places pods one at a time with no backtracking,
// so two valid-looking early choices can leave the last pod with zero legal
// options), and a soft rule is advisory, so it silently produced unsafe
// layouts with no "violation" any descheduler could even detect.
//
// Doing it here instead sidesteps all of that: by this point every pod
// already exists and we can see exactly where each one landed, so we choose
// the pairing with full information rather than asking the scheduler to
// guess its way into one.
//
// Strategy: group pods by Kubernetes node, take one master per node, then
// rotate -- the replica(s) sitting on node i back the master on the next
// node round-robin. Because each master is on a distinct node, a rotation
// by one guarantees master and replica never coincide.
func planRoles(nodes []nodeInfo, numMasters int) (masters []*nodeInfo, replicaOf map[string]string, err error) {
	byK8sNode := map[string][]*nodeInfo{}
	for i := range nodes {
		n := &nodes[i]
		byK8sNode[n.K8sNode] = append(byK8sNode[n.K8sNode], n)
	}

	k8sNodeNames := make([]string, 0, len(byK8sNode))
	for name := range byK8sNode {
		k8sNodeNames = append(k8sNodeNames, name)
	}
	// Sort for determinism -- the same input placement should always
	// produce the same plan, so repeated reconciles don't churn roles.
	sort.Strings(k8sNodeNames)

	if len(k8sNodeNames) < numMasters {
		return nil, nil, fmt.Errorf(
			"need at least %d Kubernetes nodes to place one master each, but pods are spread over only %d (%v)",
			numMasters, len(k8sNodeNames), k8sNodeNames)
	}

	// One master per Kubernetes node, for the first numMasters nodes.
	masterNodeIdx := map[string]int{} // k8s node -> index in k8sNodeNames
	for i := 0; i < numMasters; i++ {
		name := k8sNodeNames[i]
		pods := byK8sNode[name]
		sort.Slice(pods, func(a, b int) bool { return pods[a].PodName < pods[b].PodName })
		masters = append(masters, pods[0])
		masterNodeIdx[name] = i
	}

	masterIsChosen := map[string]bool{}
	for _, m := range masters {
		masterIsChosen[m.PodName] = true
	}

	// Everything else is a replica, assigned by rotating one node over.
	replicaOf = map[string]string{} // replica pod name -> master pod name
	for _, k8sNode := range k8sNodeNames {
		idx, isMasterNode := masterNodeIdx[k8sNode]
		for _, pod := range byK8sNode[k8sNode] {
			if masterIsChosen[pod.PodName] {
				continue
			}
			var target *nodeInfo
			if isMasterNode {
				// rotate one node over, so never our own node's master
				target = masters[(idx+1)%numMasters]
			} else {
				// This node hosts no master at all (more k8s nodes than
				// masters), so any master is safe; spread them round-robin
				// by pod name for determinism.
				target = masters[len(replicaOf)%numMasters]
			}
			replicaOf[pod.PodName] = target.PodName
		}
	}

	return masters, replicaOf, nil
}

// bootstrapCluster forms a brand-new cluster: introduce every node to every
// other (MEET), assign slots to the chosen masters, then attach replicas.
//
// This is the Go equivalent of `redis-cli --cluster create`, which is not a
// Redis command at all but a client-side helper that performs this same
// sequence. Doing it ourselves is what lets us choose the master/replica
// pairing deliberately instead of accepting redis-cli's IP-based guess --
// that guess is exactly what produced same-node master/replica pairs
// earlier in this project.
func bootstrapCluster(ctx context.Context, nodes []nodeInfo, numMasters int) error {
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes to bootstrap")
	}

	// 1. MEET: introduce all nodes from the first one. Gossip spreads
	// knowledge from there, so a single hub is enough.
	hub := redisClient(nodes[0].IP)
	defer hub.Close()
	for _, n := range nodes[1:] {
		if err := hub.ClusterMeet(ctx, n.IP, fmt.Sprint(redisPort)).Err(); err != nil {
			return fmt.Errorf("CLUSTER MEET %s: %w", n.IP, err)
		}
	}

	// 2. Wait until every node knows the full set, otherwise REPLICATE
	// below can fail with "unknown node".
	if err := waitKnownNodes(ctx, nodes, len(nodes)); err != nil {
		return err
	}

	// 3. Decide roles from real pod placement.
	masters, replicaOf, err := planRoles(nodes, numMasters)
	if err != nil {
		return err
	}

	// 4. Assign slots to masters.
	ranges := slotRanges(numMasters)
	for i, m := range masters {
		c := redisClient(m.IP)
		err := c.ClusterAddSlotsRange(ctx, ranges[i][0], ranges[i][1]).Err()
		c.Close()
		if err != nil {
			return fmt.Errorf("CLUSTER ADDSLOTSRANGE on %s (%d-%d): %w",
				m.PodName, ranges[i][0], ranges[i][1], err)
		}
	}

	// 5. Learn the node ids now that the cluster is formed, so replicas can
	// be pointed at their master by id.
	byIP := map[string]*nodeInfo{}
	for i := range nodes {
		byIP[nodes[i].IP] = &nodes[i]
	}
	if err := parseClusterNodes(ctx, nodes[0].IP, byIP); err != nil {
		return fmt.Errorf("reading CLUSTER NODES after slot assignment: %w", err)
	}

	byPodName := map[string]*nodeInfo{}
	for i := range nodes {
		byPodName[nodes[i].PodName] = &nodes[i]
	}

	// 6. Attach each replica to its planned master.
	for replicaPod, masterPod := range replicaOf {
		replica := byPodName[replicaPod]
		master := byPodName[masterPod]
		if replica == nil || master == nil {
			return fmt.Errorf("internal: role plan references unknown pod %q/%q", replicaPod, masterPod)
		}
		if master.NodeID == "" {
			return fmt.Errorf("master %s has no known Redis node id yet", masterPod)
		}
		c := redisClient(replica.IP)
		err := c.ClusterReplicate(ctx, master.NodeID).Err()
		c.Close()
		if err != nil {
			return fmt.Errorf("CLUSTER REPLICATE %s -> %s: %w", replicaPod, masterPod, err)
		}
	}

	return nil
}

// waitKnownNodes blocks until every node's gossip view contains the
// expected number of nodes, or the context/deadline runs out.
func waitKnownNodes(ctx context.Context, nodes []nodeInfo, expected int) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		allKnow := true
		for _, n := range nodes {
			known, err := clusterInfoField(ctx, n.IP, "cluster_known_nodes")
			if err != nil || known != fmt.Sprint(expected) {
				allKnow = false
				break
			}
		}
		if allKnow {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for all %d nodes to know each other", expected)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// observeRoles counts healthy masters and replicas for status reporting.
func observeRoles(ctx context.Context, nodes []nodeInfo) (mastersReady, replicasReady int32) {
	byIP := map[string]*nodeInfo{}
	for i := range nodes {
		byIP[nodes[i].IP] = &nodes[i]
	}
	for _, n := range nodes {
		if err := parseClusterNodes(ctx, n.IP, byIP); err == nil {
			break
		}
	}
	for i := range nodes {
		if nodes[i].NodeID == "" {
			continue
		}
		if nodes[i].IsMaster {
			mastersReady++
		} else {
			replicasReady++
		}
	}
	return mastersReady, replicasReady
}

// rebalanceMasters is the ongoing counterpart to planRoles(): a real
// failover can promote a replica whose pod happens to already share a
// Kubernetes node with another master, leaving that node hosting 2 masters
// and some other node hosting none -- exactly the property this whole
// custom operator exists to prevent. planRoles() only runs once, at
// initial bootstrap, so without this check that property can be silently
// violated by any future failover and never get corrected.
//
// Confirmed hands-on: our own failover test produced exactly this --
// worker3 ended up hosting both the original master for slots 10922-16383
// AND the newly promoted master for slots 0-5460, because both pods
// happened to already live there before the kill.
//
// Fix mirrors reconcile-cluster.py's REBALANCE_MASTER case: find a
// master on an overloaded node whose replica sits on an under-loaded
// node, and CLUSTER FAILOVER that replica to move the master role there.
// One correction per call, same "re-check before acting again" discipline
// as everywhere else in this project.
func rebalanceMasters(ctx context.Context, nodes []nodeInfo) (acted bool, err error) {
	byK8sNode := map[string][]*nodeInfo{}
	for i := range nodes {
		byK8sNode[nodes[i].K8sNode] = append(byK8sNode[nodes[i].K8sNode], &nodes[i])
	}

	masterCount := map[string]int{}
	for k8sNode, pods := range byK8sNode {
		for _, p := range pods {
			if p.IsMaster && p.HasSlots {
				masterCount[k8sNode]++
			}
		}
	}

	var overloaded, underloaded []string
	for k8sNode := range byK8sNode {
		switch {
		case masterCount[k8sNode] > 1:
			overloaded = append(overloaded, k8sNode)
		case masterCount[k8sNode] == 0:
			underloaded = append(underloaded, k8sNode)
		}
	}
	if len(overloaded) == 0 || len(underloaded) == 0 {
		return false, nil // balanced, or no safe move exists this cycle
	}
	sort.Strings(overloaded) // deterministic which one we fix first
	sort.Strings(underloaded)

	for _, overNode := range overloaded {
		for _, m := range byK8sNode[overNode] {
			if !m.IsMaster || !m.HasSlots {
				continue
			}
			for i := range nodes {
				replica := &nodes[i]
				if replica.MasterID != m.NodeID {
					continue
				}
				for _, u := range underloaded {
					if replica.K8sNode != u {
						continue
					}
					c := redisClient(replica.IP)
					ferr := c.ClusterFailover(ctx).Err()
					c.Close()
					if ferr != nil {
						return false, fmt.Errorf("CLUSTER FAILOVER on %s (%s): %w",
							replica.PodName, replica.IP, ferr)
					}
					return true, nil
				}
			}
		}
	}
	// An overloaded and an underloaded node both exist, but no single-hop
	// replica move between them was safe (e.g. the replica backing the
	// overloaded node's extra master lives on another already-loaded
	// node). Mirrors reconcile-cluster.py's UNRESOLVABLE_MASTER_IMBALANCE
	// case -- report it rather than guess.
	return false, fmt.Errorf(
		"master imbalance detected (overloaded=%v, underloaded=%v) but no safe single-step fix found this cycle",
		overloaded, underloaded)
}
