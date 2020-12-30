# beemport
A minimal version for the latest [beelog](https://github.com/Lz-Gustavo/beelog) log compaction technique. Instead of offering different structures and log-reduce algorithms, *beemport* implements only the most efficient **concTable** structure, discarding all unnecessary structures and abstractions piorly provided.

This repository is intended to work as a simple import version to couple *beelog* compaction into different comercial kvstores for benchmarking purposes. As so, the repository also stores different structure representations (mostly in protocol buffers) for WAL commands. For [etcd](https://github.com/etcd-io/etcd), *beemport* currently supports the latest 3.4.14, so make sure to download etcd dependency as:

```
go get go.etcd.io/etcd@release-3.4
```

As mentioned in [issue #12068](https://github.com/etcd-io/etcd/issues/12068), once 3.5 is released go mod automatically import should work just fine.
