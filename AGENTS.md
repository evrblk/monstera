# Monstera Development Guide

## Build & Test Commands

```bash
make build                    # fully build Monstera
make generate                 # generate all protobufs
go test -v --race ./...       # run all tests with Go directly
make lint                     # run linter, statick check, go vet
```

## Code Style Guidelines

- Follow standard Go formatting (gofmt/goimports)
- Import order: standard lib, external packages (including other `evrblk/*` repositories), then `evrblk/monstera` packages
- Error handling: Always check errors with `if err != nil { return ... }`
- Document all exported functions, types, and variables
- Use table-driven tests when appropriate
- Use `testify/require` for test assertions
- In tests use `EqualValues` when comparing integers instead of `Equal` with a typecast
