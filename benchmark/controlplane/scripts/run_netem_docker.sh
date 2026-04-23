#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
CONTROLPLANE_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
BENCH_DIR=$(cd -- "$CONTROLPLANE_DIR/.." && pwd)
REPO_ROOT=$(cd -- "$BENCH_DIR/.." && pwd)

DELAY=${CONTROL_PLANE_NETEM_DELAY:-1ms}
JITTER=${CONTROL_PLANE_NETEM_JITTER:-0ms}
LOSS=${CONTROL_PLANE_NETEM_LOSS:-0%}
IMAGE=${CONTROL_PLANE_NETEM_IMAGE:-golang:1.26-bookworm}

suffix="_netem_${DELAY}"
if [[ "$JITTER" != "0ms" ]]; then
	suffix="${suffix}_j${JITTER}"
fi
if [[ "$LOSS" != "0%" ]]; then
	suffix="${suffix}_l${LOSS}"
fi

docker run --rm \
	--cap-add NET_ADMIN \
	-v "$REPO_ROOT:/workspace" \
	-w /workspace \
	-e CONTROL_PLANE_BENCHTIME="${CONTROL_PLANE_BENCHTIME:-500ms}" \
	-e CONTROL_PLANE_INPROC_COUNT="${CONTROL_PLANE_INPROC_COUNT:-5}" \
	-e CONTROL_PLANE_PROCESS_COUNT="${CONTROL_PLANE_PROCESS_COUNT:-5}" \
	-e CONTROL_PLANE_RECOVERY_COUNT="${CONTROL_PLANE_RECOVERY_COUNT:-5}" \
	-e CONTROL_PLANE_RESULT_SUFFIX="$suffix" \
	"$IMAGE" \
	bash -lc "
		set -euo pipefail
		export PATH=/usr/local/go/bin:\$PATH
		export DEBIAN_FRONTEND=noninteractive
		apt-get update >/dev/null
		apt-get install -y iproute2 >/dev/null
		tc qdisc add dev lo root netem delay $DELAY $JITTER loss $LOSS
		trap 'tc qdisc del dev lo root 2>/dev/null || true' EXIT
		./benchmark/succession/scripts/run_eval.sh
	"
