import json
import subprocess
import sys

NAMESPACE = "default"

def run_cmd(cmd, check=True):
    try:
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)
        return res.stdout.strip(), res.returncode
    except subprocess.CalledProcessError as e:
        return e.stderr.strip(), e.returncode

# 1. Fetch Pods from K8s
pods_json_raw, rc = run_cmd(f"kubectl get pods -n {NAMESPACE} -l app=redis-cluster -o json")
if rc != 0:
    print(f"Error fetching pods: {pods_json_raw}")
    sys.exit(1)
pods_data = json.loads(pods_json_raw)

ip_to_pod = {}
pod_to_node = {}
ready_pods = []

for item in pods_data.get("items", []):
    pod_name = item["metadata"]["name"]
    pod_ip = item["status"].get("podIP", "")
    node_name = item["spec"].get("nodeName", "")
    phase = item["status"].get("phase", "")
    conditions = item["status"].get("conditions", [])
    is_ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)

    if pod_ip:
        ip_to_pod[pod_ip] = pod_name
        pod_to_node[pod_name] = node_name
    if phase == "Running" and is_ready:
        ready_pods.append(pod_name)

if not ready_pods:
    print("No Ready pods found — cluster may be mid-restart. Try again shortly.")
    sys.exit(1)

def slot_count(line_parts):
    """Sum up slot ranges like '0-5460' or single slots on a master line."""
    total = 0
    for tok in line_parts[8:]:
        if tok.startswith("["):  # importing/migrating markers, ignore
            continue
        if "-" in tok:
            lo, hi = tok.split("-")
            total += int(hi) - int(lo) + 1
        elif tok.isdigit():
            total += 1
    return total

def is_complete(raw):
    """A pod's gossip view is only trustworthy once it agrees all 16384
    slots are assigned and it isn't still showing '?' for any peer address
    (both signs its gossip table hasn't caught up yet after a restart)."""
    if "?" in raw:
        return False
    total = 0
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        if "master" in parts[2]:
            total += slot_count(parts)
    return total == 16384

# 2. Fetch Redis Cluster Nodes — try each Ready pod and keep the first
#    response whose gossip view looks COMPLETE (all 16384 slots assigned,
#    no unresolved '?' peer addresses). A pod can be K8s-Ready (process up,
#    port open) well before its internal gossip table has caught up after
#    a restart, so "responded successfully" alone isn't enough — we also
#    fall back to the best (highest slot-count) response seen if none are
#    fully complete.
cluster_raw = None
queried_pod = None
best_raw, best_pod, best_score = None, None, -1

for candidate in ready_pods:
    out, rc = run_cmd(
        f"kubectl exec -n {NAMESPACE} {candidate} -- redis-cli cluster nodes",
        check=False,
    )
    if rc != 0 or not out:
        continue
    if is_complete(out):
        cluster_raw, queried_pod = out, candidate
        break
    score = sum(
        slot_count(p.split())
        for p in out.splitlines()
        if len(p.split()) >= 3 and "master" in p.split()[2]
    )
    if score > best_score:
        best_raw, best_pod, best_score = out, candidate, score

if cluster_raw is None:
    if best_raw is None:
        print("Could not get 'cluster nodes' output from any Ready pod.")
        sys.exit(1)
    print(f"⚠️  No pod had a fully caught-up gossip view yet — using best available "
          f"({best_pod}, {best_score}/16384 slots seen). Results below may be partial; "
          f"re-run in a few seconds.\n")
    cluster_raw, queried_pod = best_raw, best_pod
else:
    print(f"(queried cluster state via: {queried_pod})\n")

id_to_pod = {}
id_to_role = {}
id_to_target = {}
id_to_slots = {}

for line in cluster_raw.splitlines():
    parts = line.split()
    if len(parts) < 3:
        continue

    node_id = parts[0]
    ip_port = parts[1].split("@")[0]
    ip = ip_port.split(":")[0]
    role_flag = parts[2]
    master_id = parts[3]
    slots = " ".join(parts[8:]) if len(parts) >= 9 else "-"

    pod_name = ip_to_pod.get(ip, ip)
    id_to_pod[node_id] = pod_name

    if "master" in role_flag:
        id_to_role[node_id] = "MASTER"
        id_to_slots[node_id] = slots
    else:
        id_to_role[node_id] = "REPLICA"
        id_to_target[node_id] = master_id

# 3. Print Clean Table
print("=" * 90)
print(f"{'REDIS CLUSTER TOPOLOGY MAP':^90}")
print("=" * 90)
print(f"{'POD NAME':<18} {'K8S NODE':<24} {'ROLE':<10} {'SLOTS OWNED':<20} {'BACKS UP (TARGET POD)':<20}")
print("-" * 90)

for pod in sorted(pod_to_node.keys()):
    node = pod_to_node[pod]

    node_id = next((k for k, v in id_to_pod.items() if v == pod), None)
    role = id_to_role.get(node_id, "UNKNOWN")
    slots = id_to_slots.get(node_id, "-")

    target_pod = "-"
    if role == "REPLICA":
        target_id = id_to_target.get(node_id, "")
        target_pod = id_to_pod.get(target_id, "Unknown Master")

    print(f"{pod:<18} {node:<24} {role:<10} {slots:<20} {target_pod:<20}")

# 4. Topology Validation
print("\n" + "=" * 90)
print(f"{'ALTERNATE MASTER-REPLICA CHECK':^90}")
print("=" * 90)

node_to_pods = {}
for pod, node in pod_to_node.items():
    node_to_pods.setdefault(node, []).append(pod)

valid = True
for node in sorted(node_to_pods.keys()):
    pods = node_to_pods[node]
    print(f"\n📍 K8s Node: {node}")
    print(f"   Pods on Node: {', '.join(pods)}")

    roles = []
    for p in pods:
        n_id = next((k for k, v in id_to_pod.items() if v == p), None)
        roles.append((p, id_to_role.get(n_id, "UNKNOWN"), n_id))

    role_types = [r[1] for r in roles]

    if sorted(role_types) == ["MASTER", "REPLICA"]:
        print("   ✅ Role Layout: Balanced (1 Master + 1 Replica)")
    else:
        print(f"   ⚠️ Role Layout: Unbalanced ({' + '.join(role_types)})")
        valid = False

    for p, r, n_id in roles:
        if r == "REPLICA":
            target_id = id_to_target.get(n_id, "")
            target_pod = id_to_pod.get(target_id, "")
            target_node = pod_to_node.get(target_pod, "")

            if target_node == node:
                print(f"   ❌ FAIL: Replica {p} backs up {target_pod} on the SAME node ({node})!")
                valid = False
            else:
                print(f"   ✅ Safe Backup: Replica {p} backs up {target_pod} on DIFFERENT node ({target_node})")

print("\n" + "-" * 90)
if valid:
    print("🎉 RESULT: 100% HEALTHY! Alternate Master-Replica Rule is fully satisfied across all nodes.")
else:
    print("⚠️ RESULT: TOPOLOGY ALERT! Review warnings above.")
print("-" * 90)
