#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repository_root"

make build

data_root=./data/demo
secret_file=$data_root/local.secret
client_state=$data_root/client.state
cluster_log=$(mktemp "${TMPDIR:-/tmp}/crane-demo-cluster.XXXXXX")

umask 077
rm -rf "$data_root"
mkdir -p "$data_root"
chmod 700 "$data_root"
head -c 32 /dev/urandom > "$secret_file"
trap 'kill "$cluster_pid" 2>/dev/null || true; wait "$cluster_pid" 2>/dev/null || true; pkill -f "$data_root/configs/" 2>/dev/null || true' EXIT HUP INT TERM

./bin/crane-cluster -nodes 3 -base-port 9000 -data-root "$data_root" -secret-file "$secret_file" -node-binary ./bin/crane-node > "$cluster_log" 2>&1 &
cluster_pid=$!

ready=0
attempt=1
while [ "$attempt" -le 60 ]; do
  if ./bin/crane jobs -config "$data_root/configs/node-1.json" -attempts 1 -timeout 1s >/dev/null 2>&1; then
    ready=1
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "crane demo: cluster did not become ready; log:" >&2
  cat "$cluster_log" >&2
  exit 1
fi

submit_line=$(./bin/crane submit -config "$data_root/configs/node-1.json" -state "$client_state" -topology examples/topologies/word-count.json)
job_id=$(printf '%s\n' "$submit_line" | sed -n 's/.*"job_id":"\([0-9a-f]\{32\}\)".*/\1/p')
if [ -z "$job_id" ]; then
  echo "crane demo: submit returned no job id: $submit_line" >&2
  exit 1
fi

state=""
deadline=$(( $(date +%s) + 120 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  state=$(./bin/crane status -config "$data_root/configs/node-1.json" -job "$job_id" | sed -n 's/.*"state":"\([a-z]*\)".*/\1/p')
  case "$state" in
    succeeded|failed|canceled) break ;;
  esac
  sleep 1
done

./bin/crane status -config "$data_root/configs/node-1.json" -job "$job_id"
if [ "$state" != "succeeded" ]; then
  echo "crane demo: job $job_id ended in state '$state'" >&2
  exit 1
fi
./bin/crane results -config "$data_root/configs/node-1.json" -job "$job_id" -count-by word -top 10
echo "crane demo: example job $job_id succeeded"
