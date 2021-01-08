## FoundationDB reference implementation

The reference implementation assumes a FoundationDB [directory](https://apple.github.io/foundationdb/developer-guide.html#directories)
per etcd instance. Within each of these directories, several [subspaces](https://apple.github.io/foundationdb/developer-guide.html#subspaces)
are installed:

- `metadata`: A subspace to store metadata about this cluster
- `keys`: A subspace to store keys and values
- `revision`: A subspace to store the current write revision
- `instance`: A subspace to store watch instances
- `watches`: A subspace to store watches
- `events`: A subspace to store watch events.

### The keys subspace

This subspace uses the following key and value structure:

- key: `{key []byte, revision int}`
- value: `{lease-id int, create-revision int, value []byte}`

### The revision subspace

This subspace is used to store a single int value, the current revision. It is
used to atomically increase and retrieve the current global revision.

### The metadata subspace

*currently unused*

### The watch instance subspace

A subspace to store currently started watch sessions. Structure:

- key: `{instance int}`
- value: `nil`

### The watches subspace

This subspace is used to store the currently watched prefixes to allow matching
them when key operations occur. It uses the following structure:

- key: `{prefix string}`
- value: `{instance int, id int, revision int}`

### The watch event subspace

This subspace is used to store events pertaining to a specific watch instance.
Events are stored per instance

### Operation breakdown

A per operation breakdown of what is done is available here: [fdb-operations.md]
