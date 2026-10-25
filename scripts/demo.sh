#!/usr/bin/env bash
# Master + 3 worker nodes + load, then kill a node and add a replacement.
# Everything is built into a temp dir and removed on exit.
set -u
cd "$(dirname "$0")/.."

ext=""
[ "${OS:-}" = "Windows_NT" ] && ext=".exe"

tmp=$(mktemp -d)
pids=()
cleanup() {
	kill -9 "${pids[@]}" 2>/dev/null
	rm -rf "$tmp"
}
trap cleanup EXIT

go build -o "$tmp/engine$ext" ./cmd/engine || exit 1
go build -o "$tmp/loadgen$ext" ./cmd/loadgen || exit 1

"$tmp/engine$ext" -role master -expect 3 -ckdir "$tmp/ck" -ckevery 1s 2>"$tmp/master.log" &
pids+=($!)
sleep 2

workers=()
for i in 1 2 3; do
	"$tmp/engine$ext" -role worker 2>/dev/null &
	workers+=($!)
	pids+=($!)
done
sleep 3

"$tmp/loadgen$ext" -d 16s -c 2 >"$tmp/load.out" 2>&1 &
load=$!
sleep 5

echo "== killing worker node 2"
kill -9 "${workers[1]}"
sleep 3

echo "== starting a replacement node"
"$tmp/engine$ext" -role worker 2>/dev/null &
pids+=($!)

wait "$load"

echo
echo "== master log"
grep -E "failed|restored|waiting|listening" "$tmp/master.log"
echo
echo "== load generator"
cat "$tmp/load.out"
