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

[evrblk/monstera-example](https://github.com/evrblk/monstera-example) repository is an example of an application built
entirely on Monstera. It might be easier to understand how it works from that example rather than from the documentation.

## Installing

Use go get to install the latest version of the library.

```
go get -u github.com/evrblk/monstera@latest
```

## License

Monstera is released under the [MIT License](https://opensource.org/licenses/MIT).
