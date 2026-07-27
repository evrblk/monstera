# Monstera Development Guide

## Development stage: no backward compatibility

Monstera is being actively developed and has no wide adoption yet. We do NOT care about
backward compatibility or breaking interface changes at the moment — do not weaken a design to
preserve existing interfaces, wire formats, serialized state, or on-disk layouts. The goal is
the best possible design and implementation now; compatibility guarantees come later, once
there is wide adoption. When a breaking change is the better design, make it (and update all
callers, including sibling repos like `grackle`), rather than adding compatibility shims,
optional escape hatches, or versioned/legacy paths. In protobufs specifically: do not add
`reserved` statements for removed fields and do not preserve old field numbers — renumber
fields freely to keep the schema clean.

## Architecture

Read [`ARCHITECTURE.md`](./ARCHITECTURE.md) first — it is a dense orientation to the codebase
(request path, key files, invariants, concurrency map, codegen model, and open issues) written
for effective work on deep features without re-researching the codebase each session. See 
`docs/` for user-facing documentation.

Keep `ARCHITECTURE.md` updated when any significant change or feature is done.

## Build & Test Commands

```bash
make generate                 # generate all protobufs
make build                    # fully build Monstera (including generation)
go test -v --race ./...       # run all tests with Go directly
make lint                     # run linter, static check, go vet
```

## Code Style Guidelines

- Follow standard Go formatting (gofmt/goimports)
- Import order: standard lib, external packages (including other `evrblk/*` repositories), then `evrblk/monstera` packages
- Error handling: Always check errors with `if err != nil { return ... }`
- Document all exported functions, types, and variables
- Use table-driven tests when appropriate
- Use `testify/require` for test assertions
- In tests use `EqualValues` when comparing integers instead of `Equal` with a typecast
- Do not mention in comments things from the past (what was replaced) and do not mention design docs and implementation phases
