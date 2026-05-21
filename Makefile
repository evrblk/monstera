.PHONY: build generate-proto

PROTOC := protoc --proto_path=. --go_out=. --go_opt=paths=source_relative
PROTOC_GRPC_FLAGS := --go-grpc_out=. --go-grpc_opt=paths=source_relative
PROTOC_VTPROTO_FLAGS := --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size --go-vtproto_opt=paths=source_relative

build: generate-proto
	go vet ./...
	go fmt ./...
	go build ./...

generate-proto:
	@echo "Generating proto files..."
	$(PROTOC) $(PROTOC_VTPROTO_FLAGS) ./x/*.proto
	$(PROTOC) $(PROTOC_VTPROTO_FLAGS) ./cluster/*.proto
	$(PROTOC) $(PROTOC_VTPROTO_FLAGS) $(PROTOC_GRPC_FLAGS) ./transport/grpc/monsterapb/*.proto
	$(PROTOC) $(PROTOC_VTPROTO_FLAGS) ./internal/raft/raftpb/*.proto
	$(PROTOC) $(PROTOC_VTPROTO_FLAGS) ./internal/replication/replicationpb/*.proto
