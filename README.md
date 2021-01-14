fetch: limited etcd support for k8s
===================================

Etcd V3 is the underlying protocol used by Kubernetes to store
resources. Etcd V3 presents a protocol which allows querying ranges
of keys, deleting them, doing *CAS* (compare and set) operations,
as well as watching keys.

Etcd shares a number of properties with similar systems which
aim to provide cluster coordination facilities. It is optimized for:

- Small key cardinalities
- Strong consistency guarantees

Like its close cousins Zookeeper and Consul, Etcd relies on an
internal 3-phase commit approach for all operations and must hold its
entire dataset in memory at any point in time.

### Structure of the etcd protocol

Etcd V3 is exposed as a set of [GRPC](https://grpc.io) services:

- The *KV* service deals with access to keys and values
- The *Lease* service provides key lease management facilities
- The *Watch* service provides streaming updates for watched keys
- The *Cluster* service provides facilities to deal with other etcd servers
- The *Auth* service provides authorization and authentication management

### Relationship between Kubernetes and Etcd

Kubernetes uses only a subset of the functionality present in Etcd for
its operations. For instance, the *Cluster* and *Auth* service do not
need to be exposed at all for Kubernetes. The *Lease* service only
needs a dummy answer to a single endpoint to be viable.

As far as the *KV* service is concerned, the `put`, `deleteRange`,
and `compact` signatures are either unnecessary or return dummy
responses.

The necessary signatures to implement in the service are thus
`txn`, with only a few different shapes of transactions and `range`.

### Threading model

While the FDB API is built around `CompletableFuture` and is inherently
async. This daemon takes a resolute synchronous approach, allocating one
thread per request. Most low level functions to interact with the key space
deref the returned future.

### Shape of the fetch daemon

Fetch tries to abstract what is necessary for a storage layer to expose
etcd functionality. It is thus split into a *GRPC* layer, a *Storage Engine*
protocol, and a reference implementation of the protocol which targets
FoundationDB.

![](http://img.sos-ch-dk-2.exo.io/fetch/architecture.png)

#### The *GRPC* layer

The role of this layer is to correctly classify incoming requests
and prepare them for the storage engine. Additionally, it provides
facilities for preparing input and output payloads.

#### The *Storage Engine* protocol

The storage engine exposes the necessary signatures for the GRPC layer
to correctly expose the necessary subset of functionality needed by
Kubernetes.

For the *KV* service functionality, the following signatures need to
be implemented:

- `create-if-absent`
- `update-at-revision`
- `count-keys`
- `range-keys`
- `get-at-revision`
- `get-latest`
- `delete-key`

For the *Watch* service, the following need to be implemented:

- `create-watch-instance`
- `delete-watch-instance`
- `register-key-watch`
- `cancel-key-watch`
- `register-watch-listener`

#### FoundationDB reference implementation

The reference implementation assumes a FoundationDB [directory](https://apple.github.io/foundationdb/developer-guide.html#directories)
per etcd instance. Within each of these directories, several [subspaces](https://apple.github.io/foundationdb/developer-guide.html#subspaces)
are installed:

- `metadata`: A subspace to store metadata about this cluster
- `keys`: A subspace to store keys and values
- `revision`: A subspace to store the current write revision
- `instance`: A subspace to store watch instances
- `watches`: A subspace to store watches
- `events`: A subspace to store watch events.

For more details on the FoundationDB implementation, please refer to: [docs/foundationdb](docs/foundationdb.md)

### Inspiration and Documentation

### Kubernetes `etcd3` storage engine

The Kubernetes `etcd3` storage engine:
https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3
holds all interaction between Kubernetes and etcd.

Some areas need more attention than others:

- [The compactor comments](https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go#L73)
  notably explain the design of the special `compactRevKey` key.
- [The lease manager comments](https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3/lease_manager.go)
  explain how leases are used in Kubernetes.
- Throughout this package, all interactions with the store's internal `client`
  member must be analyzed, since they represent all actual interactions with
  etcd.

### FoundationDB documentation and Javadoc

To understand the decisions behind the design of the FoundationDB
schema, it is worthwhile going through the FoundationDB [developer
guide](https://apple.github.io/foundationdb/developer-guide.html)

Navigation of the FoundationDB documentation being a bit lacking, it is
also often useful to explore the contents of the documentation's
[sitemap](https://apple.github.io/foundationdb/contents.html).

Last, to understand the interaction with Clojure, the
[javadoc](https://apple.github.io/foundationdb/javadoc/index.html) is also a
great source.

### Kine

Kine is an etcd server backed by SQL databases. Fetch's design is similar
in some ways and they share a number of common compromises.

https://github.com/k3s-io/kine/

### Changelog

*no release yet*

### Architecture Decision Records

- [ADR006: GRPC interceptor to perform TLS authorization](docs/adrs/adr006.md)
- [ADR005: Operations run through an interceptor chain](docs/adrs/adr005.md)
- [ADR004: Implementing the kubernetes subset only](docs/adrs/adr004.md)
- [ADR003: Treating all keys as strings](docs/adrs/adr003.md)
- [ADR002: External artifact for GRPC definitions](docs/adrs/adr002.md)
- [ADR001: Synchronous threading model](docs/adrs/adr001.md)
- [ADR000: Building Fetch in Clojure](docs/adrs/adr000.md)
