#!/usr/bin/env bash
# Build & run a Kubo (IPFS) node backed by the walrusds datastore plugin.
# Idempotent: safe to re-run; will rebuild the image and restart the node.
#
# Usage:
#   ./setup.sh                        # uses Walrus testnet defaults
#   PUB_URL=https://my-pub ./setup.sh # override publisher
#
# Env overrides:
#   IMAGE        Docker image tag           (default: kubo-walrus)
#   VOLUME       Docker volume name         (default: ipfsdata)
#   NAME         Container name             (default: kubo-walrus-node)
#   AGG_URL      Walrus aggregator URL      (default: testnet)
#   PUB_URL      Walrus publisher URL       (default: testnet)
#   INDEX_PATH   On-disk LevelDB index dir  (default: /data/ipfs/walrus-index)
#   EPOCHS       Walrus storage epochs      (default: 1)
#   API_PORT     Host port for IPFS API     (default: 5001)
#   GW_PORT      Host port for IPFS Gateway (default: 8080)
#   SWARM_PORT   Host port for libp2p swarm (default: 4001)

set -euo pipefail

IMAGE=${IMAGE:-kubo-walrus}
VOLUME=${VOLUME:-ipfsdata}
NAME=${NAME:-kubo-walrus-node}

AGG_URL=${AGG_URL:-https://aggregator.walrus-testnet.walrus.space}
PUB_URL=${PUB_URL:-https://publisher.walrus-testnet.walrus.space}
INDEX_PATH=${INDEX_PATH:-/data/ipfs/walrus-index}
EPOCHS=${EPOCHS:-1}
# Batch upload concurrency. Public testnet publisher rate-limits aggressively;
# small values (1-4) avoid HTTP 429s. Bump higher for self-hosted/paid publishers.
WORKERS=${WORKERS:-4}

API_PORT=${API_PORT:-5001}
GW_PORT=${GW_PORT:-8080}
SWARM_PORT=${SWARM_PORT:-4001}

cd "$(dirname "$0")"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required but not found in PATH." >&2
  exit 1
fi

echo ">> Stopping any previous container ($NAME) ..."
docker rm -f "$NAME" >/dev/null 2>&1 || true

echo ">> Building image: $IMAGE"
docker build -t "$IMAGE" -f Dockerfile.kubo-walrus .

echo ">> Initializing IPFS repo in volume: $VOLUME (skipped if already initialized)"
docker run --rm -v "$VOLUME:/data/ipfs" "$IMAGE" init >/dev/null 2>&1 || true

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

cat >"$TMP/spec.json" <<JSON
{
  "type": "mount",
  "mounts": [
    {
      "mountpoint": "/blocks",
      "type": "measure",
      "prefix": "walrus.datastore",
      "child": {
        "type": "walrusds",
        "aggregatorURL": "$AGG_URL",
        "publisherURL": "$PUB_URL",
        "indexPath": "$INDEX_PATH",
        "epochs": $EPOCHS,
        "workers": $WORKERS
      }
    },
    {
      "mountpoint": "/",
      "type": "measure",
      "prefix": "leveldb.datastore",
      "child": {
        "type": "levelds",
        "path": "datastore",
        "compression": "none"
      }
    }
  ]
}
JSON

# DiskSpec form: no measure wrapper, no "type" for walrusds, no "epochs".
# Must match what the plugin's DiskSpec() returns or Kubo refuses to start.
cat >"$TMP/datastore_spec" <<JSON
{"mounts":[{"aggregatorURL":"$AGG_URL","indexPath":"$INDEX_PATH","mountpoint":"/blocks","publisherURL":"$PUB_URL"},{"mountpoint":"/","path":"datastore","type":"levelds"}],"type":"mount"}
JSON

echo ">> Applying Datastore.Spec (walrusds)"
docker run --rm --entrypoint sh \
  -v "$VOLUME:/data/ipfs" \
  -v "$TMP/spec.json:/spec.json:ro" \
  "$IMAGE" -lc 'ipfs config Datastore.Spec --json "$(cat /spec.json)"'

echo ">> Writing matching datastore_spec"
docker run --rm --entrypoint sh \
  -v "$VOLUME:/data/ipfs" \
  -v "$TMP/datastore_spec:/datastore_spec:ro" \
  "$IMAGE" -lc 'cp /datastore_spec /data/ipfs/datastore_spec'

echo ">> Verifying configuration"
docker run --rm -v "$VOLUME:/data/ipfs" "$IMAGE" config Datastore.Spec >/dev/null

echo ">> Starting daemon: $NAME"
docker run --name "$NAME" -d \
  -p "$SWARM_PORT:4001" \
  -p "$API_PORT:5001" \
  -p "$GW_PORT:8080" \
  -v "$VOLUME:/data/ipfs" \
  "$IMAGE" >/dev/null

echo ">> Waiting for daemon to become ready ..."
for _ in $(seq 1 30); do
  if docker logs "$NAME" 2>&1 | grep -q "Daemon is ready"; then
    break
  fi
  sleep 1
done

if ! docker logs "$NAME" 2>&1 | grep -q "Daemon is ready"; then
  echo "ERROR: daemon did not become ready in time. Last logs:" >&2
  docker logs --tail 40 "$NAME" >&2 || true
  exit 1
fi

echo ">> Smoke test: ipfs add + ipfs cat"
CID=$(docker exec "$NAME" sh -lc 'echo "hello walrus from setup.sh" | ipfs add -Q')
GOT=$(docker exec "$NAME" ipfs cat "$CID")
echo "   CID:   $CID"
echo "   value: $GOT"

if [ "$GOT" != "hello walrus from setup.sh" ]; then
  echo "ERROR: round-trip mismatch." >&2
  exit 1
fi

cat <<EOF

DONE.

Container : $NAME
Image     : $IMAGE
Volume    : $VOLUME
API       : http://127.0.0.1:$API_PORT
Gateway   : http://127.0.0.1:$GW_PORT/ipfs/$CID
WebUI     : http://127.0.0.1:$API_PORT/webui

Useful:
  docker logs -f $NAME
  docker exec -it $NAME ipfs id
  docker exec -it $NAME ipfs add /etc/hostname
  docker stop $NAME && docker start -ai $NAME
EOF
