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

### Relevant subset of Etcd



Etcd is a consistent storage protocol which is used as the
storage part of a number of systems, including Kubernetes.
