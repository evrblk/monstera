# Monstera Development Guide

## Architecture

Read [`ARCHITECTURE.md`](./ARCHITECTURE.md) first — it is a dense orientation to the codebase
(request path, key files, invariants, concurrency map, codegen model, and open issues) written
for effective work on deep features without re-researching the codebase each session. See
`notes/` for review findings and `docs/` for user-facing documentation.

## Build & Test Commands

```bash
make generate                 # generate all protobufs
make build                    # fully build Monstera (including generation)
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
