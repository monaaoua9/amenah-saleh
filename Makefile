test:
	go test -count=1 ./...

bench:
	go test -run xxx -bench . -benchmem ./internal/...

run:
	go run ./cmd/engine

cluster:
	go run ./cmd/engine -role master -expect 2

node:
	go run ./cmd/engine -role worker

demo:
	bash scripts/demo.sh

stress:
	go run ./cmd/loadgen -d 10s -c 4

proto:
	protoc -I. --go_out=. --go_opt=module=github.com/monaaoua9/amenah-saleh --go-grpc_out=. --go-grpc_opt=module=github.com/monaaoua9/amenah-saleh proto/ingest.proto proto/cluster.proto