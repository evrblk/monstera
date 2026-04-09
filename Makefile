.PHONY: build generate-proto

build: generate-proto
	go vet ./...
	go fmt ./...
	go build ./...

generate-proto:
	@echo "Generating proto files..."
	protoc --proto_path=. \
		--go_out=. --go_opt=paths=source_relative \
		--go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size --go-vtproto_opt=paths=source_relative \
		./x/*.proto

	protoc --proto_path=. \
		--go_out=. --go_opt=paths=source_relative \
		--go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size --go-vtproto_opt=paths=source_relative \
		./cluster/*.proto

	protoc --proto_path=. \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		--go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size --go-vtproto_opt=paths=source_relative \
		./transport/grpc/monsterapb/*.proto

	protoc --proto_path=. \
		--go_out=. --go_opt=paths=source_relative \
		--go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size --go-vtproto_opt=paths=source_relative \
		./internal/raft/raftpb/*.proto
