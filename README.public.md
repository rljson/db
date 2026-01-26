# @rljson/db

A high-level TypeScript interface for reading and writing structured, content-addressed RLJSON data. The package provides a powerful database-like abstraction with support for hierarchical data models, version history, and complex data relationships.

## Installation

```bash
npm install @rljson/db @rljson/io @rljson/rljson
```

## Quick Start

```typescript
import { Db } from '@rljson/db';
import { IoMem } from '@rljson/io';
import { Route, createTreesTableCfg, treeFromObject } from '@rljson/rljson';

// Initialize database with in-memory storage
const io = new IoMem();
await io.init();
const db = new Db(io);

// Create a table
const treeCfg = createTreesTableCfg('myTree');
await db.core.createTableWithInsertHistory(treeCfg);

// Import data
const tree = treeFromObject({ x: 1, y: 2, z: { a: 3 } });
await db.core.import({
  myTree: { _type: 'trees', _data: tree }
});

// Query data
const route = Route.fromFlat('myTree');
const result = await db.get(route, {});
console.log(result.rljson);
```

## Core Concepts

### RLJSON Data Model

RLJSON (Relational JSON) extends JSON with:
- **Content-addressed hashing**: Every object has a unique `_hash` based on its content
- **Typed tables**: Data is organized into strongly-typed tables
- **Relationships**: Objects reference each other via content hashes
- **Version history**: Built-in support for tracking changes over time

### Table Types

@rljson/db supports five primary table types:

#### 1. **Components** (`components`)
Basic structured data with fields and values. Similar to database records.

```typescript
const component = {
  brand: 'Tesla',
  model: 'Model 3',
  year: 2024,
  _type: 'components',
  _hash: '...'
};
```

#### 2. **Trees** (`trees`)
Hierarchical data structures with parent-child relationships. Trees use content-addressed hashing where children are referenced by their hash values.

```typescript
// Create tree from object
const treeData = {
  root: {
    child1: { value: 1 },
    child2: { value: 2 }
  }
};
const trees = treeFromObject(treeData);

// Each node has: id, children (hash array), meta, isParent
const rootNode = trees[trees.length - 1]; // Last element is root
```

#### 3. **Cakes** (`cakes`)
Multi-dimensional data cubes with slicing capabilities. Useful for analytics and aggregated data.

```typescript
const cake = {
  base: 'salesData',
  sliceIdsTable: 'regions',
  sliceIdsRow: 'north',
  componentsTable: 'metrics',
  _type: 'cakes',
  _hash: '...'
};
```

#### 4. **Layers** (`layers`)
Stacked data with inheritance and composition. Layers can override or extend base layer data.

```typescript
const layer = {
  base: 'baseConfigHash',
  sliceIdsTable: 'environments',
  sliceIdsTableRow: 'production',
  componentsTable: 'settings',
  _type: 'layers',
  _hash: '...'
};
```

#### 5. **SliceIds** (`sliceIds`)
Dimension identifiers for cakes and layers, enabling multi-dimensional data organization.

## Working with Data

### Setting Up Tables

```typescript
import {
  createComponentsTableCfg,
  createTreesTableCfg,
  createCakesTableCfg,
  createLayersTableCfg
} from '@rljson/rljson';

// Create component table
const componentCfg = createComponentsTableCfg('users', [
  { key: 'name', type: 'string' },
  { key: 'email', type: 'string' },
  { key: 'age', type: 'number' }
]);
await db.core.createTableWithInsertHistory(componentCfg);

// Create tree table
const treeCfg = createTreesTableCfg('fileSystem');
await db.core.createTableWithInsertHistory(treeCfg);
```

### Importing Data

```typescript
// Import structured data
await db.core.import({
  users: {
    _type: 'components',
    _data: [
      { name: 'Alice', email: 'alice@example.com', age: 30, _hash: '...' },
      { name: 'Bob', email: 'bob@example.com', age: 25, _hash: '...' }
    ]
  }
});

// Import tree data
const fileTree = treeFromObject({
  src: {
    components: { 'Button.tsx': {} },
    utils: { 'helpers.ts': {} }
  }
});
await db.core.import({
  fileSystem: { _type: 'trees', _data: fileTree }
});
```

### Querying Data

#### Basic Queries

```typescript
// Get all records from a table
const route = Route.fromFlat('users');
const result = await db.get(route, {});
console.log(result.rljson.users._data); // All users

// Query by hash
const route = Route.fromFlat('users');
const result = await db.get(route, { _hash: 'specific-hash' });
console.log(result.rljson.users._data); // Single user
```

#### Tree Queries

```typescript
// Query single tree node (WHERE clause)
const route = Route.fromFlat('fileSystem');
const result = await db.get(route, { _hash: nodeHash });
// Returns ONLY the requested node without expanding children

// Navigate tree with route
const route = Route.fromFlat(`fileSystem@${rootHash}`);
const result = await db.get(route);
// Returns root node with children expanded
```

**Important:** Tree queries behave differently based on context:
- **WHERE clause** (`{ _hash: ... }`): Returns single node only
- **Route navigation**: Expands children recursively

This distinction prevents infinite recursion and heap crashes when querying large trees.

#### Filtering and Relationships

```typescript
// Query with nested relationships
const route = Route.fromFlat('projects/tasks');
const result = await db.get(route, {
  projects: { status: 'active' },
  tasks: { priority: 'high' }
});

// The result contains both projects and their related tasks
console.log(result.rljson.projects);
console.log(result.rljson.tasks);
```

### Inserting Data

```typescript
import { isolate, inject } from '@rljson/db';

// Isolate specific data from a structure
const route = Route.fromFlat('users/profile/settings');
const { tree, cell } = await db.get(route, {});
const path = cell[0].path;
const isolated = isolate(tree, path);

// Modify the isolated data
isolated.profile.settings.theme = 'dark';

// Inject changes back
inject(isolated, path, { theme: 'dark' });

// Insert into database
const results = await db.insert(route, isolated);
console.log(results); // InsertHistoryRow[]
```

### Version History

Every insert creates a history entry with timestamps and references:

```typescript
// Get insert history for a table
const history = await db.getInsertHistory('users', {
  sorted: true,
  ascending: false
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

// Query specific version by timeId
const route = Route.fromFlat(`users@ABC123:20260126T150000Z`);
const historicData = await db.get(route);
```

## Advanced Features

### Joins and Transformations

```typescript
import { Join } from '@rljson/db';
import { ColumnSelection } from '@rljson/db';
import { RowFilter } from '@rljson/db';

// Create a join from query results
const { rljson, tree, cell } = await db.get(route, where);
const join = new Join({ rljson, tree, cell });

// Apply column selection
const selection = new ColumnSelection(...);
join.select(selection);

// Apply filtering
const filter = new RowFilter(...);
join.filter(filter);

// Get transformed results
const resultRows = await join.rows();
```

### Multi-Edit Operations

```typescript
import { MultiEditManager } from '@rljson/db';

const manager = new MultiEditManager(db);

// Perform multiple edits as a transaction
await manager.multiEdit(async (head) => {
  await head.edit(editAction1);
  await head.edit(editAction2);
  return head;
});

// Publish changes
const published = await manager.publishHead();
```

### Real-time Notifications

```typescript
// Register callback for data changes
db.notify.registerCallback('myCallback', async (data) => {
  console.log('Data changed:', data);
});

// Unregister when done
db.notify.unregisterCallback('myCallback');
```

### Caching

```typescript
// The database automatically caches query results
// Get cache statistics
const cache = db.cache;
console.log(`Cache size: ${cache.size}`);

// Clear cache if needed
db.clearCache();
```

## Data Export and Backup

```typescript
// Dump entire database
const dump = await db.core.dump();
console.log(JSON.stringify(dump, null, 2));

// Dump specific table
const tableDump = await db.core.dumpTable('users');
console.log(JSON.stringify(tableDump, null, 2));

// Get all tables
const tables = await db.core.tables();
console.log(Object.keys(tables));
```

## Best Practices

### 1. Content-Addressed Design
All data is immutable and identified by content hash. Never modify data in place - always create new versions.

### 2. Tree Query Optimization
When querying trees by hash, use WHERE clauses to fetch single nodes:
```typescript
// ✅ Efficient - single node
await db.get(route, { _hash: nodeHash });

// ❌ Avoid for large trees - expands all children
await db.get(Route.fromFlat(`tree@${nodeHash}`));
```

### 3. Batch Operations
Import data in batches for better performance:
```typescript
const batchSize = 100;
for (let i = 0; i < data.length; i += batchSize) {
  const batch = data.slice(i, i + batchSize);
  await db.core.import({ users: { _type: 'components', _data: batch } });
}
```

### 4. Use Insert History
Always create tables with insert history to enable time-travel queries:
```typescript
await db.core.createTableWithInsertHistory(tableCfg);
```

### 5. Type Safety
Use TypeScript types from `@rljson/rljson` for type-safe operations:
```typescript
import type { Component, Tree, Cake, Layer } from '@rljson/rljson';
```

## Storage Backends

@rljson/db works with any `@rljson/io` implementation:

```typescript
import { IoMem } from '@rljson/io'; // In-memory
import { IoFile } from '@rljson/io'; // File system
import { IoMulti } from '@rljson/io'; // Multiple backends

// In-memory (development)
const memDb = new Db(new IoMem());

// File-based (production)
const fileDb = new Db(new IoFile('/path/to/data'));

// Multi-backend (redundancy)
const multiDb = new Db(new IoMulti([io1, io2, io3]));
```

## Error Handling

```typescript
try {
  const result = await db.get(route, where);
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

## Examples

See [src/example.ts](src/example.ts) for a complete working example.

## API Reference

### Core Methods

- `db.get(route, where, filter?, sliceIds?, options?)` - Query data
- `db.insert(route, data, origin?, refs?)` - Insert new data
- `db.getInsertHistory(table, options?)` - Get version history
- `db.core.import(rljson)` - Import RLJSON data
- `db.core.dump()` - Export all data
- `db.core.createTableWithInsertHistory(cfg)` - Create table

### Utility Functions

- `isolate(tree, path)` - Extract data at path
- `inject(tree, path, value)` - Insert value at path
- `treeFromObject(obj)` - Convert object to tree structure
- `makeUnique(array)` - Remove duplicates by hash
- `mergeTrees(trees)` - Combine tree structures

## License

MIT

## Related Packages

- `@rljson/rljson` - Core RLJSON data structures and types
- `@rljson/io` - Storage backend implementations
- `@rljson/hash` - Content-addressing utilities
- `@rljson/json` - JSON manipulation helpers
