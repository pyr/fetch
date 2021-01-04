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
