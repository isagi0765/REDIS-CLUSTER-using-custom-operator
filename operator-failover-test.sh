#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="default"
LABEL_SELECTOR="app=redis-cluster"
POLL_SECONDS=30

echo "=================================================================="
echo " CUSTOM OPERATOR FAILOVER TEST"
echo "=================================================================="

READY_PODS=($(kubectl get pods -n "${NAMESPACE}" -l "${LABEL_SELECTOR}" \
  --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}'))

if [ "${#READY_PODS[@]}" -eq 0 ]; then
  echo "No running pods found matching -l ${LABEL_SELECTOR} in ${NAMESPACE}."
  exit 1
fi

ENTRY_POD="${READY_PODS[0]}"
echo "==> Querying cluster state via ${ENTRY_POD}..."
BEFORE=$(kubectl exec -n "${NAMESPACE}" "${ENTRY_POD}" -- redis-cli cluster nodes)
echo "${BEFORE}"
echo ""

# Roles here are dynamic -- any pod could currently be a master or a
# replica, unlike a naming convention like leader-N/follower-N. So we
# either auto-pick the first master we find, or (if the user named a
# specific pod) verify it's actually a master right now before proceeding.
TARGET_ARG="${1:-}"

declare -A IP_TO_POD
for p in "${READY_PODS[@]}"; do
  ip=$(kubectl get pod -n "${NAMESPACE}" "$p" -o jsonpath='{.status.podIP}')
  IP_TO_POD["$ip"]="$p"
done

pick_master_line() {
  echo "${BEFORE}" | awk '$3 ~ /master/ && NF >= 9 {print}'
}

if [ -n "${TARGET_ARG}" ]; then
  TARGET_IP=$(kubectl get pod -n "${NAMESPACE}" "${TARGET_ARG}" -o jsonpath='{.status.podIP}' 2>/dev/null || true)
  TARGET_LINE=$(echo "${BEFORE}" | grep "${TARGET_IP}:6379" || true)
  if [ -z "${TARGET_LINE}" ] || ! echo "${TARGET_LINE}" | grep -q master; then
    echo "'${TARGET_ARG}' is not currently a master. Current masters:"
    pick_master_line | while read -r line; do
      ip=$(echo "$line" | awk '{print $2}' | cut -d: -f1 | cut -d@ -f1)
      echo "  ${IP_TO_POD[$ip]:-$ip}  (slots $(echo "$line" | awk '{print $9}'))"
    done
    exit 1
  fi
else
  TARGET_LINE=$(pick_master_line | head -1)
  TARGET_IP=$(echo "${TARGET_LINE}" | awk '{print $2}' | cut -d: -f1 | cut -d@ -f1)
  TARGET_ARG="${IP_TO_POD[$TARGET_IP]}"
fi

TARGET_ID=$(echo "${TARGET_LINE}" | awk '{print $1}')
TARGET_SLOTS=$(echo "${TARGET_LINE}" | awk '{print $9}')
SLOT_START=$(echo "${TARGET_SLOTS}" | cut -d- -f1)
TARGET_NODE=$(kubectl get pod -n "${NAMESPACE}" "${TARGET_ARG}" -o jsonpath='{.spec.nodeName}')

SURVIVOR=""
for p in "${READY_PODS[@]}"; do
  if [ "$p" != "${TARGET_ARG}" ]; then
    SURVIVOR="$p"
    break
  fi
done

echo "==> Target: ${TARGET_ARG} is master ${TARGET_ID}, owns slots ${TARGET_SLOTS}"
echo "==> Its K8s node is ${TARGET_NODE} -- cordoning it so it can't instantly reschedule"
echo "    back before the cluster's failover window elapses."
kubectl cordon "${TARGET_NODE}"

echo "==> Killing pod ${TARGET_ARG}..."
KILL_TIME=$(date +%s)
kubectl delete pod -n "${NAMESPACE}" "${TARGET_ARG}" --wait=false

echo "==> Polling ${SURVIVOR} for up to ${POLL_SECONDS}s, watching for a DIFFERENT node id"
echo "    to take over slot ${SLOT_START}..."
echo ""

PROMOTED=false
for i in $(seq 1 "${POLL_SECONDS}"); do
  sleep 1
  NOW=$(kubectl exec -n "${NAMESPACE}" "${SURVIVOR}" -- redis-cli cluster nodes 2>/dev/null || true)
  CID=$(echo "${NOW}" | awk -v slot="${SLOT_START}" '
    $3 ~ /master/ {
      for (i=9; i<=NF; i++) { split($i, a, "-"); if (a[1] == slot) { print $1; exit } }
    }')
  echo "--- t+${i}s: current owner of slot ${SLOT_START} = ${CID:-none yet} ---"
  if [ -n "${CID}" ] && [ "${CID}" != "${TARGET_ID}" ]; then
    ELAPSED=$(( $(date +%s) - KILL_TIME ))
    echo ""
    echo "REAL PROMOTION DETECTED after ~${ELAPSED}s"
    echo "  Old master: ${TARGET_ID} (${TARGET_ARG})"
    echo "  New master: ${CID}"
    PROMOTED=true
    break
  fi
done

kubectl uncordon "${TARGET_NODE}"

if [ "${PROMOTED}" = false ]; then
  echo ""
  echo "No promotion detected within ${POLL_SECONDS}s."
  kubectl exec -n "${NAMESPACE}" "${SURVIVOR}" -- redis-cli cluster nodes
  exit 1
fi

echo ""
echo "==> Waiting for ${TARGET_ARG} to reschedule and rejoin..."
kubectl wait --for=condition=Ready "pod/${TARGET_ARG}" -n "${NAMESPACE}" --timeout=120s || true

sleep 3
echo ""
echo "==> Final cluster state:"
kubectl exec -n "${NAMESPACE}" "${SURVIVOR}" -- redis-cli cluster nodes
echo ""
echo "==> Checking whether ${TARGET_ARG} rejoined cleanly or came back orphaned..."
SELF_VIEW=$(kubectl exec -n "${NAMESPACE}" "${TARGET_ARG}" -- redis-cli cluster nodes 2>/dev/null || echo "")
SELF_KNOWN=$(echo "${SELF_VIEW}" | wc -l)
if [ "${SELF_KNOWN}" -le 1 ]; then
  echo "  WARNING: ${TARGET_ARG} appears isolated (only knows about itself)."
  echo "  This would be an orphaned-node scenario -- not yet auto-healed by the Go operator."
else
  echo "  ${TARGET_ARG} knows about ${SELF_KNOWN} nodes -- looks properly rejoined."
fi
