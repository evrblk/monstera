# Monstera

[![Go](https://github.com/evrblk/monstera/actions/workflows/go.yml/badge.svg)](https://github.com/evrblk/monstera/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/evrblk/monstera)](https://goreportcard.com/report/github.com/evrblk/monstera)

![Monstera leaf](monstera.jpg)

Monstera is a framework that allows you to write stateful application logic in pure Go with all data in memory or on 
disk without worrying about scalability and availability. Monstera takes care of replication, sharding, snapshotting, 
and rebalancing.

Monstera is a half-technical and half-mental framework. The tech part of it has a pretty small surface. It leaves a 
lot up to you to implement. But sticking to the framework principles will ensure:

* Applications are insanely fast and efficient
* Cluster is horizontally scalable
* Business logic is easily testable
* Local development is enjoyable

Data and compute are brought together into a single process. You are free to use any in-memory data structure or 
embedded database. By eliminating network calls and limited query languages for communication with external databases, 
you are able to solve problems which are known to be hard in distributed systems with just a few lines of your 
favorite programming language as you would do it on a whiteboard.

Go to [official documentation](https://everblack.dev/docs/monstera) to learn more.

## Example

It might be easier to understand how it works from examples rather than from the documentation.

* [evrblk/grackle](https://github.com/evrblk/grackle) is a complete production-ready application built with Monstera.
* [evrblk/monstera-example](https://github.com/evrblk/monstera-example) repository has toy examples of applications 
  built with Monstera.

## Installing

Use go get to install the latest version of the library.

```
go get -u github.com/evrblk/monstera@latest
```

There are also few CLI tools (such as codegen), so it also should be added as a tool:

```
go get -tool github.com/evrblk/monstera/cmd/monstera@latest
```

## Status

Monstera is being actively developed. There will be no version tagging before `v0.1`, just development in `master` 
branch. After it is tagged `v0.1` it will be more or less stable and follow semver.

## License

Monstera is released under the [MIT License](https://opensource.org/licenses/MIT).
