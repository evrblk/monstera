protoc --proto_path=. --go_out=. --go-grpc_out=. --go-grpc_opt=paths=source_relative --go_opt=paths=source_relative ./*.proto
protoc --proto_path=. --go_out=. --go_opt=paths=source_relative ./x/*.proto
