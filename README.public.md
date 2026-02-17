# @rljson/db

A high-level TypeScript database abstraction for content-addressed, immutable RLJSON data. Provides intuitive querying with native support for hierarchical structures, version history, and pluggable storage backends.

## Features

- **Content-Addressed:** All data identified by SHA-based hashes
- **Immutable:** Changes create new versions automatically
- **Hierarchical:** First-class tree structures with smart expansion
- **Type-Safe:** Full TypeScript support
- **Time-Travel:** Query historical versions
- **Storage-Agnostic:** Memory, file, or network backends

## Installation

```bash
npm install @rljson/db @rljson/io @rljson/rljson
```

**Requirements:** Node.js >= 22.14.0

## Quick Start

```typescript
import { Db } from '@rljson/db';
import { IoMem } from '@rljson/io';
import { Route, createComponentsTableCfg } from '@rljson/rljson';

// Initialize database with in-memory storage
const io = new IoMem();
await io.init();
const db = new Db(io);

// Create a table
const tableCfg = createComponentsTableCfg('users', [
  { key: 'name', type: 'string' },
  { key: 'email', type: 'string' }
]);
await db.core.createTableWithInsertHistory(tableCfg);

// Import data
await db.core.import({
  users: {
    _type: 'components',
    _data: [
      { name: 'Alice', email: 'alice@example.com', _hash: '...' }
    ]
  }
});

// Query data
const route = Route.fromFlat('users');
const result = await db.get(route, {});
console.log(result.rljson.users._data);
```

## Core Concepts

### Content-Addressed Data

All data in @rljson/db is identified by its **content hash** (SHA-256 based). This means:

- Identical data always has the same hash
- Changes to data produce a new hash
- Perfect caching: same hash = same data
- Deduplication: store each unique value once

```typescript
import { hsh } from '@rljson/hash';

const data = { name: 'Alice', age: 30 };
const hash = hsh(data)._hash;
// 'abc123...' - deterministic hash based on content
```

### Immutability

Data is **never modified in place**. All mutations create new versions:

- INSERT creates new data with new hash
- Old versions remain accessible
- Time-travel queries via insert history
- No update conflicts

### Routes

Routes define paths through related data. They use a flat string syntax:

```
/tableName                          # Root table
/tableName@hash                     # Specific row by hash
/tableName@timeId                   # Historic version
/tableName/childTable               # Relationship traversal
/tableName@hash/childTable@hash2    # Nested navigation
```

Example:

```typescript
// Simple route
Route.fromFlat('users')

// With hash reference
Route.fromFlat('users@abc123')

// Nested relationship
Route.fromFlat('projects/tasks')

// Complex navigation
Route.fromFlat('projects@hash1/tasks@hash2')
```

## Data Types

### 1. Components

Flat tables with arbitrary columns. Similar to traditional database tables.

```typescript
type Component = {
  [column: string]: JsonValue;
  _hash: string;
};
```

**Use Cases:**

- User records
- Configuration data
- Event logs
- Any flat, record-based data

**Example:**

```typescript
const component = {
  name: 'Alice',
  email: 'alice@example.com',
  role: 'admin',
  _hash: '...'
};
```

### 2. Trees

Hierarchical structures where each node has:

- `id`: Node identifier
- `children`: Array of child hash references
- `meta`: Node metadata
- `isParent`: Boolean flag for parent nodes
- `_hash`: Content hash

```typescript
type Tree = {
  id: string;
  children: string[];  // Hash references
  meta: Json;
  isParent: boolean;
  _hash: string;
};
```

**Use Cases:**

- File systems
- Organizational hierarchies
- Menu structures
- DOM-like structures

**Important:** Tree queries behave differently based on context:

- **WHERE clause** (`{ _hash: ... }`): Returns single node only (prevents infinite recursion)
- **Route navigation**: Expands children recursively

**Example:**

```typescript
const tree = {
  id: 'root',
  children: ['childHash1', 'childHash2'],
  meta: { name: 'Root Node' },
  isParent: true,
  _hash: '...'
};

// Single node query (efficient)
await db.get(route, { _hash: tree._hash });

// Expanded navigation (recursive)
await db.get(Route.fromFlat(`tree@${tree._hash}`));
```

### 3. Cakes

Multi-dimensional data cubes with slicing capabilities.

```typescript
type Cake = {
  sliceIdsTable: string;
  sliceIdsRow: string;
  layers: Record<string, string>;  // layerTable -> layerRef
  _hash: string;
};
```

**Use Cases:**

- Multi-environment configurations
- A/B testing variants
- Feature flags
- Dimensional data analysis

**Example:**

```typescript
const cake = {
  sliceIdsTable: 'environments',
  sliceIdsRow: 'production',
  layers: {
    configLayer: 'layerHash1',
    settingsLayer: 'layerHash2'
  },
  _hash: '...'
};
```

### 4. Layers

Composable data layers with inheritance and dimension filtering.

```typescript
type Layer = {
  base?: string;                  // Base layer hash
  sliceIdsTable: string;
  sliceIdsTableRow: string;
  componentsTable: string;
  _hash: string;
};
```

**Use Cases:**

- Configuration inheritance
- Environment-specific settings
- Progressive overrides
- Template-based data

**Example:**

```typescript
const layer = {
  base: 'baseLayerHash',
  sliceIdsTable: 'environments',
  sliceIdsTableRow: 'staging',
  componentsTable: 'settings',
  _hash: '...'
};
```

### 5. SliceIds

Dimension identifiers for cakes and layers. Enable multi-dimensional data organization.

```typescript
type SliceId = string;  // e.g., 'production', 'staging', 'featureA'
```

## Querying Data

### Basic Queries

```typescript
// Get all records from a table
const route = Route.fromFlat('users');
const result = await db.get(route, {});
console.log(result.rljson.users._data);  // All users

// Query by hash
const result = await db.get(route, { _hash: 'specific-hash' });
console.log(result.rljson.users._data[0]);  // Single user

// Query by field values
const result = await db.get(route, { role: 'admin' });
console.log(result.rljson.users._data);  // All admin users
```

### Nested Queries

```typescript
// Query with relationships
const route = Route.fromFlat('projects/tasks');
const result = await db.get(route, {
  projects: { status: 'active' },
  tasks: { priority: 'high' }
});

// Result contains both projects and their related tasks
console.log(result.rljson.projects);
console.log(result.rljson.tasks);
```

### Tree Queries

**Critical:** Tree behavior depends on query context.

```typescript
// ✅ WHERE clause - Returns ONLY requested node
// Use this for querying specific nodes without expansion
const nodeRoute = Route.fromFlat('fileSystem');
const nodeResult = await db.get(nodeRoute, { _hash: nodeHash });
// Returns: { fileSystem: { _data: [singleNode] } }
// No children expanded - prevents heap crashes on large trees

// ✅ Route navigation - Expands children recursively
// Use this for traversing tree structures
const treeRoute = Route.fromFlat(`fileSystem@${rootHash}`);
const treeResult = await db.get(treeRoute);
// Returns: Complete tree with all children expanded
// Access via treeResult.tree for hierarchical view
```

### Result Container

All queries return a `Container` object with three views of the data:

```typescript
type Container = {
  rljson: Rljson;      // Table data indexed by table name
  tree: Json;          // Hierarchical representation
  cell: Cell[];        // Path-value pairs for modifications
};

type Cell = {
  route: Route;
  value: JsonValue;
  row: JsonValue;
  path: Array<Array<string | number>>;
};
```

**Example:**

```typescript
const { rljson, tree, cell } = await db.get(route, where);

// RLJSON view - Table-oriented
console.log(rljson.users._data);

// Tree view - Hierarchical
console.log(tree.users[0].name);

// Cell view - Path-based for mutations
console.log(cell[0].path);  // ['users', 0, 'name']
console.log(cell[0].value); // 'Alice'
```

## Inserting Data

### Using isolate() and inject()

The recommended pattern for data modifications:

```typescript
import { isolate, inject } from '@rljson/db';

// 1. Query existing data
const route = Route.fromFlat('users/profile/settings');
const { tree, cell } = await db.get(route, {});

// 2. Isolate specific path
const path = cell[0].path;
const isolated = isolate(tree, path);
// Returns only the data at the specified path

// 3. Modify isolated data
isolated.profile.settings.theme = 'dark';
isolated.profile.settings.notifications = true;

// 4. Inject changes back
inject(isolated, path, {
  theme: 'dark',
  notifications: true
});

// 5. Insert into database
const results = await db.insert(route, isolated);
console.log(results);  // InsertHistoryRow[]
```

### Direct Import

For bulk data loading:

```typescript
await db.core.import({
  users: {
    _type: 'components',
    _data: [
      { name: 'Alice', email: 'alice@example.com', _hash: 'hash1' },
      { name: 'Bob', email: 'bob@example.com', _hash: 'hash2' }
    ]
  }
});
```

### Tree INSERT

**Important:** When inserting tree data that has already been isolated, use the fixed behavior:

```typescript
// The treeFromObject function now has skipRootCreation parameter
// This is handled internally by db.insert() - no action needed

const treeData = isolate(existingTree, path);
// Modify treeData...

// INSERT automatically uses skipRootCreation=true internally
await db.insert(route, treeData);
```

**Why this matters:** The `treeFromObject` function creates an explicit root node. When inserting already-isolated subtrees, this created a double-root structure causing navigation failures. The fix in v0.0.12+ handles this automatically.

## Version History

Every insert creates a history entry with timestamps and references:

```typescript
// Get insert history for a table
const history = await db.getInsertHistory('users', {
  sorted: true,
  ascending: false  // Most recent first
});

console.log(history.usersInsertHistory._data);
// [
//   {
//     timeId: 'ABC123:20260126T150000Z',
//     usersRef: 'hash-of-inserted-data',
//     route: '/users',
//     previous: ['previous-timeId'],
//     ...
//   }
// ]
```

### Time-Travel Queries

Query historical versions using timeIds:

```typescript
// Query specific version by timeId
const route = Route.fromFlat(`users@ABC123:20260126T150000Z`);
const historicData = await db.get(route);
console.log(historicData.rljson.users._data);  // Data as it was at that time

// Get all timeIds for a specific hash
const timeIds = await db.getTimeIdsForRef('users', 'hash1');
console.log(timeIds);  // ['timeId1', 'timeId2', ...]

// Get data hash for a specific timeId
const ref = await db.getRefOfTimeId('users', 'ABC123:20260126T150000Z');
console.log(ref);  // 'hash1'
```

## Advanced Features

### Join System

The Join system provides SQL-like operations on query results:

```typescript
import { Join, ColumnSelection, RowFilter } from '@rljson/db';

// Create a join from query results
const { rljson, tree, cell } = await db.get(route, where);
const join = new Join({ rljson, tree, cell });

// Apply column selection
const selection = new ColumnSelection(
  route,
  [
    { key: 'name', type: 'include' },
    { key: 'email', type: 'include' }
  ]
);
join.select(selection);

// Apply row filtering
const filter = new RowFilter(
  route,
  {
    age: { gt: 25 }  // Greater than 25
  }
);
join.filter(filter);

// Apply sorting
const sort = new RowSort(
  route,
  [{ key: 'name', direction: 'asc' }]
);
join.sort(sort);

// Get transformed results
const resultRows = await join.rows();
console.log(resultRows);
```

### Multi-Edit Operations

Transactional editing with rollback support:

```typescript
import { MultiEditManager } from '@rljson/db';

const manager = new MultiEditManager('configCake', db);
await manager.init();

// Perform multiple edits as a transaction
await manager.multiEdit(async (head) => {
  // Edit 1: Update column selection
  await head.edit({
    type: 'columnSelection',
    params: { columns: ['name', 'email'] }
  });

  // Edit 2: Apply filter
  await head.edit({
    type: 'rowFilter',
    params: { age: { gt: 18 } }
  });

  return head;
});

// Publish changes (commits transaction)
const published = await manager.publishHead();
console.log(published);
```

### Real-Time Notifications

Register callbacks for data changes:

```typescript
// Register callback
db.notify.registerCallback('myCallback', async (insertHistoryRow) => {
  console.log('Data changed:', insertHistoryRow);
  // Perform side effects (cache invalidation, UI updates, etc.)
});

// Perform operations - callback fires automatically
await db.insert(route, data);

// Unregister when done
db.notify.unregisterCallback('myCallback');

// Or unregister all callbacks for a route
db.notify.unregisterAll(route);
```

### Caching

The database automatically caches query results based on query signatures:

```typescript
// Check cache size
console.log(`Cache size: ${db.cache.size}`);

// Clear cache manually
db.clearCache();

// Caching is automatic when:
// - Route contains hash references (@hash)
// - Filters are applied
// - SliceIds are specified
```

## Best Practices

### 1. Content-Addressed Design

All data is immutable - never modify in place:

```typescript
// ❌ WRONG - Modifying data directly
const { rljson } = await db.get(route, {});
rljson.users._data[0].name = 'Changed';  // This won't persist!

// ✅ CORRECT - Use isolate/inject pattern
const { tree, cell } = await db.get(route, {});
const isolated = isolate(tree, cell[0].path);
isolated.name = 'Changed';
await db.insert(route, isolated);  // Creates new version
```

### 2. Tree Query Optimization

When querying trees by hash, use WHERE clauses for single nodes:

```typescript
// ✅ Efficient - Returns single node only
await db.get(Route.fromFlat('tree'), { _hash: nodeHash });

// ❌ Avoid for large trees - Expands ALL children recursively
// Only use for navigation when you need the full tree
await db.get(Route.fromFlat(`tree@${nodeHash}`));
```

This is **critical** for large trees to prevent:

- Heap exhaustion
- Infinite recursion
- Performance degradation

### 3. Batch Operations

Import data in batches for better performance:

```typescript
const batchSize = 100;
for (let i = 0; i < data.length; i += batchSize) {
  const batch = data.slice(i, i + batchSize);
  await db.core.import({
    users: { _type: 'components', _data: batch }
  });
}
```

### 4. Always Use Insert History

Create tables with insert history to enable time-travel:

```typescript
// ✅ CORRECT - Enables version tracking
await db.core.createTableWithInsertHistory(tableCfg);

// ❌ AVOID - No version history
await db.core.createTable(tableCfg);
```

### 5. Type Safety

Use TypeScript types from `@rljson/rljson` for type-safe operations:

```typescript
import type { Component, Tree, Cake, Layer } from '@rljson/rljson';

// Type-safe component
const user: Component = {
  name: 'Alice',
  email: 'alice@example.com',
  _hash: '...'
};

// Type-safe tree
const tree: Tree = {
  id: 'root',
  children: [],
  meta: { name: 'Root' },
  isParent: false,
  _hash: '...'
};
```

### 6. Error Handling

Always wrap database operations in try-catch:

```typescript
try {
  const result = await db.get(route, where);
  // Process result...
} catch (error) {
  if (error.message.includes('Maximum recursion depth')) {
    console.error('Infinite recursion detected in route');
  } else if (error.message.includes('not valid')) {
    console.error('Invalid route or data structure');
  } else {
    console.error('Unexpected error:', error);
  }
}
```

## Troubleshooting

### Tree INSERT Failures

**Symptoms:**

- Tree INSERT completes without errors
- GET queries return empty results or only root node
- Cell length is 1 instead of expected count

**Solution:** This was fixed in v0.0.12+. Upgrade to latest version:

```bash
pnpm update @rljson/db@latest
```

See [README.trouble.md](./README.trouble.md) for more troubleshooting guides.

### Infinite Recursion Errors

**Problem:** Tree queries causing stack overflow or heap exhaustion.

**Solution:** Use WHERE clause for single-node queries:

```typescript
// ✅ This
await db.get(route, { _hash: nodeHash });

// ❌ Not this (for large trees)
await db.get(Route.fromFlat(`tree@${nodeHash}`));
```

### Cache Not Clearing

**Problem:** Stale data persists after modifications.

**Solution:** Register notify callbacks to clear cache automatically:

```typescript
db.notify.register(route, async () => {
  db.clearCache();
});
```

## API Reference

### Core Methods

#### `db.get(route, where, filter?, sliceIds?, options?)`

Query data from the database.

**Parameters:**

- `route: Route` - The route to query
- `where: string | Json` - Filter criteria
- `filter?: ControllerChildProperty[]` - Optional child filters
- `sliceIds?: SliceId[]` - Optional dimension filters
- `options?: GetOptions` - Query options (`skipRljson`, `skipTree`, `skipCell`)

**Returns:** `Promise<ContainerWithControllers>`

#### `db.insert(route, data, origin?, refs?)`

Insert new data into the database.

**Parameters:**

- `route: Route` - Destination route
- `data: Json` - Data to insert
- `origin?: Ref` - Optional origin reference
- `refs?: ControllerRefs` - Optional controller references

**Returns:** `Promise<InsertHistoryRow[]>`

#### `db.getInsertHistory(table, options?)`

Get version history for a table.

**Parameters:**

- `table: string` - Table name
- `options?: { sorted?: boolean, ascending?: boolean }` - Sort options

**Returns:** `Promise<InsertHistoryTable>`

#### `db.core.import(rljson)`

Import RLJSON data into the database.

**Parameters:**

- `rljson: Rljson` - Data to import

**Returns:** `Promise<void>`

#### `db.core.dump()`

Export all database data.

**Returns:** `Promise<Rljson>`

#### `db.core.createTableWithInsertHistory(cfg)`

Create a table with automatic version tracking.

**Parameters:**

- `cfg: TableCfg` - Table configuration

**Returns:** `Promise<void>`

### Utility Functions

#### `isolate(tree, path, preservedKeys?)`

Extract data at a specific path.

**Parameters:**

- `tree: any` - Source tree
- `path: (string | number)[]` - Path to isolate
- `preservedKeys?: string[]` - Keys to preserve (e.g., metadata)

**Returns:** Isolated data subtree

#### `inject(tree, path, value)`

Insert value at a specific path.

**Parameters:**

- `tree: any` - Target tree
- `path: (string | number)[]` - Destination path
- `value: any` - Value to inject

**Returns:** Modified tree

#### `treeFromObject(obj, skipRootCreation?)`

Convert object to tree structure.

**Parameters:**

- `obj: Json` - Object to convert
- `skipRootCreation?: boolean` - Skip automatic root creation (default: false)

**Returns:** `Tree[]`

#### `makeUnique(array)`

Remove duplicates by hash.

**Parameters:**

- `array: any[]` - Array with potential duplicates

**Returns:** `any[]` - Unique elements

#### `mergeTrees(trees)`

Combine multiple tree structures.

**Parameters:**

- `trees: Tree[][]` - Arrays of trees to merge

**Returns:** `Tree[]`

## Storage Backends

@rljson/db works with any `@rljson/io` implementation:

### IoMem (In-Memory)

Perfect for development, testing, and temporary data:

```typescript
import { IoMem } from '@rljson/io';

const io = new IoMem();
await io.init();
const db = new Db(io);

// Data stored in memory - cleared on process exit
```

### IoFile (File System)

Persistent storage using the file system:

```typescript
import { IoFile } from '@rljson/io';

const io = new IoFile('/path/to/data');
await io.init();
const db = new Db(io);

// Data persisted to disk
```

### IoMulti (Multiple Backends)

Redundant storage across multiple backends with priority-based cascade:

```typescript
import { IoMulti, IoMem, IoFile } from '@rljson/io';

const io1 = new IoMem();
const io2 = new IoFile('/path/to/backup');
const io3 = new IoFile('/path/to/archive');

await io1.init();
await io2.init();
await io3.init();

const multi = new IoMulti([io1, io2, io3]);
const db = new Db(multi);

// Writes to all backends, reads from first available
// Priority: io1 > io2 > io3
```

## Connector (sync protocol)

The `Connector` bridges a local `Db` with a remote server via socket events. It enriches outgoing refs with protocol metadata and processes incoming refs with dedup, origin filtering, and gap detection.

### Creating a Connector

```typescript
import { Connector } from '@rljson/db';
import { Route } from '@rljson/rljson';
import type { SyncConfig } from '@rljson/rljson';

const route = Route.fromFlat('/sharedTree');

// Minimal — no enrichment
const connector = new Connector(db, route, socket);

// With sync config — enables enriched payloads
const syncConfig: SyncConfig = {
  causalOrdering: true,
  requireAck: true,
  ackTimeoutMs: 5_000,
  includeClientIdentity: true,
  maxDedupSetSize: 10_000,
};
const connector = new Connector(db, route, socket, { syncConfig });
```

### Sending refs

```typescript
// Fire-and-forget
connector.send(ref);

// Wait for server ACK (requires requireAck: true)
const ack = await connector.sendWithAck(ref);
// ack: { r, ok, receivedBy, totalClients }
```

### Receiving refs

```typescript
// Safe callback with dedup, origin filtering, gap detection
connector.listen(async (ref) => {
  console.log('New ref:', ref);
});
```

### Predecessors

When `causalOrdering` is enabled, the Connector automatically populates the `p` (predecessors) field from the `InsertHistoryRow.previous` array whenever the local Db notifies about a new insert. No manual call to `setPredecessors()` is needed for standard database-driven sends.

```typescript
// Manual override (advanced) — sets predecessors for the NEXT send only
connector.setPredecessors(['1700000000000:AbCd']);
```

### Bounded dedup

The Connector tracks recently sent and received refs to prevent duplicates. The dedup sets use **two-generation eviction**: when the current set reaches `maxDedupSetSize` (default 10 000), it rotates to previous and a new current set starts. Lookups check both generations. This caps memory usage at ≈ 2 × `maxDedupSetSize` entries.

### Bootstrap handling

The Connector automatically listens for `${route}:bootstrap` events from the server. When a client joins after data has already been sent, the server pushes the latest ref via this event. The Connector feeds it into `_processIncoming()`, so dedup, gap detection, and `listen()` callbacks all work identically to regular multicast refs.

No additional setup is needed — the bootstrap handler is registered in `_init()` and cleaned up in `tearDown()`.

### Cleanup

```typescript
connector.tearDown();
// Removes all socket listeners and clears internal state
```

## Examples

See [src/example.ts](src/example.ts) for a complete working example demonstrating:

- Table creation
- Data import/export
- Queries and filters
- Tree operations
- Version history
- Multi-edit operations

Run the example:

```bash
pnpm run build
node dist/example.js
```

## Related Packages

- `@rljson/rljson` - Core RLJSON data structures and types
- `@rljson/io` - Storage backend implementations
- `@rljson/hash` - Content-addressing utilities
- `@rljson/json` - JSON manipulation helpers
- `@rljson/validate` - Data validation utilities

## License

MIT

## Documentation

- [README.md](./README.md) - Main documentation index
- [README.public.md](./README.public.md) - This file (public API)
- [README.contributors.md](./README.contributors.md) - Developer guide
- [README.architecture.md](./README.architecture.md) - Technical architecture
- [README.trouble.md](./README.trouble.md) - Troubleshooting guide
- [README.blog.md](./README.blog.md) - Blog posts and updates
