.PHONY: build generate-proto

build: generate-proto
	go vet ./...
	go fmt ./...
	go build ./...

generate-proto:
	@echo "Generating proto files..."
	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative ./x/*.proto
	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative ./*.proto
