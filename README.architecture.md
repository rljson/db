# @rljson/db Architecture

This document provides a deep dive into the architecture, design patterns, and implementation details of @rljson/db.

## Table of Contents

- [Overview](#overview)
- [Architecture Layers](#architecture-layers)
- [Core Components](#core-components)
- [Data Flow](#data-flow)
- [Controller Pattern](#controller-pattern)
- [Route Resolution](#route-resolution)
- [Caching Strategy](#caching-strategy)
- [Tree Processing](#tree-processing)
- [Join System](#join-system)
- [Multi-Edit System](#multi-edit-system)
- [Design Decisions](#design-decisions)

## Overview

@rljson/db is a TypeScript-based database abstraction layer for content-addressed, hierarchical RLJSON data. It provides a high-level query interface on top of pluggable storage backends (`@rljson/io`).

### Key Characteristics

- **Content-Addressed**: All data identified by SHA-based content hashes
- **Immutable**: Data is never modified in place; all changes create new versions
- **Hierarchical**: Native support for tree structures and nested relationships
- **Type-Safe**: Full TypeScript support with strong typing
- **Version-Tracked**: Built-in insert history for time-travel queries
- **Storage-Agnostic**: Works with any `Io` implementation (memory, file, network)

## Architecture Layers

```
┌─────────────────────────────────────────────┐
│              Application Layer              │
│         (User Queries & Mutations)          │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│               Db Layer                      │
│  • Route Resolution                         │
│  • Query Planning                           │
│  • Caching                                  │
│  • Notification                             │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│           Controller Layer                  │
│  • BaseController                           │
│  • TreeController (path-based expansion)    │
│  • CakeController                           │
│  • LayerController                          │
│  • ComponentController                      │
│  • SliceIdController                        │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│              Core Layer                     │
│  • Table Management                         │
│  • Data Import/Export                       │
│  • Validation                               │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│               Io Layer                      │
│  • IoMem (in-memory)                        │
│  • IoFile (file system)                     │
│  • IoMulti (redundant storage)              │
└─────────────────────────────────────────────┘
```

## Core Components

### 1. Db Class

The main entry point providing high-level query and mutation operations.

**Location**: `src/db.ts`

**Key Responsibilities**:

- Route parsing and resolution
- Query coordination across controllers
- Caching query results
- Insert operation orchestration
- Notification broadcasting

**Key Methods**:

```typescript
class Db {
  async get(route, where, filter?, sliceIds?, options?): Promise<ContainerWithControllers>
  async insert(route, data, origin?, refs?): Promise<InsertHistoryRow[]>
  async getInsertHistory(table, options?): Promise<InsertHistoryTable>
  // ... internal methods
  async _get(route, where, controllers, filter, sliceIds, routeAccumulator, options)
  async _getReferenceOfRouteSegment(segment): Promise<string>
  async _resolveSliceIds(table, row): Promise<SliceId[]>
}
```

**Data Structures**:

```typescript
type Container = {
  rljson: Rljson;      // Table data indexed by table name
  tree: Json;          // Hierarchical representation
  cell: Cell[];        // Path-value pairs for modifications
};

type Cell = {
  route: Route;        // Full route to this cell
  value: JsonValue;    // Current value
  row: JsonValue;      // Parent row data
  path: Array<Array<string | number>>;  // Path segments
};
```

### 2. Core Class

Low-level data management layer wrapping Io operations.

**Location**: `src/core.ts`

**Key Responsibilities**:

- Table creation and schema management
- Data import with validation
- Data export (dump operations)
- Reading/writing through Io layer

**Key Methods**:

```typescript
class Core {
  async createTableWithInsertHistory(tableCfg): Promise<void>
  async createTable(tableCfg): Promise<void>
  async import(data): Promise<void>
  async dump(): Promise<Rljson>
  async dumpTable(table): Promise<Rljson>
  async tables(): Promise<Record<string, TableCfg>>
  async readRows(params): Promise<Rljson>
  async write(params): Promise<void>
}
```

### 3. Controller Pattern

Controllers abstract table-specific operations and provide a uniform interface.

**Base Controller** (`src/controller/base-controller.ts`):

```typescript
abstract class Controller<N, R, T> {
  abstract init(): Promise<void>
  abstract get(where, filter?, path?): Promise<Rljson>
  abstract insert(command, value, origin?, refs?): Promise<InsertHistoryRow[]>
  abstract table(): Promise<T>
  abstract tableCfg(): TableCfg
  abstract getChildRefs(hash): Promise<Ref[]>
}
```

**Specialized Controllers**:

#### TreeController

Handles hierarchical tree structures with conditional children expansion.

**Critical Fix**: The `path` parameter controls whether children are expanded:

- `path === undefined`: WHERE clause query → Returns only requested node
- `path !== undefined`: Route navigation → Expands children recursively

```typescript
class TreeController extends Controller {
  async get(where, filter?, path?): Promise<Rljson> {
    // ... fetch tree node(s)

    const shouldExpandChildren = path !== undefined;

    if (!shouldExpandChildren) {
      // Return only requested node (prevents heap crash)
      return { [tableKey]: { _data: [tree], _type: 'trees' } };
    }

    // Expand children recursively
    for (const childRef of tree.children ?? []) {
      const child = await this.get(childRef, undefined, treeRoute.deeper().flat);
      children.push(...child[tableKey]._data);
    }

    return { [tableKey]: { _data: [...children, tree], _type: 'trees' } };
  }
}
```

This design prevents infinite recursion when querying trees by hash while preserving navigation capabilities.

#### CakeController

Handles multi-dimensional cube data with slicing.

```typescript
class CakeController extends Controller {
  // Resolves sliceIds and components
  // Handles dimension-based queries
}
```

#### LayerController

Manages layered data with inheritance.

```typescript
class LayerController extends Controller {
  // Resolves base layers
  // Handles layer composition
}
```

## Data Flow

### Query Flow (db.get)

```
1. Parse Route
   Route.fromFlat('/users/projects/tasks')
   ↓
2. Isolate Property Keys
   Extract hash references from route segments
   ↓
3. Index Controllers
   Create/cache controllers for each table in route
   ↓
4. Execute _get (Recursive)
   For each route segment:
   ├─ Fetch node data (via controller)
   ├─ Apply filters
   ├─ Resolve children (if not leaf)
   ├─ Recurse to deeper route
   └─ Merge results
   ↓
5. Build Container
   rljson: { users: {...}, projects: {...}, tasks: {...} }
   tree: nested structure
   cell: path-value pairs
   ↓
6. Cache Result
   Store in _cache map by route+where hash
   ↓
7. Return Container + Controllers
```

### Insert Flow (db.insert)

```
1. Validate Route
   ↓
2. Index Controllers
   ↓
3. Execute _insert (Recursive)
   For each route segment:
   ├─ Get target controller
   ├─ Extract data for this level
   ├─ Call controller.insert()
   ├─ Get InsertHistoryRow
   ├─ Recurse to children
   └─ Collect history rows
   ↓
4. Notify Callbacks
   Broadcast changes to registered listeners
   ↓
5. Return InsertHistoryRows
```

## Route Resolution

Routes define paths through related data. The route system supports:

### Route Syntax

```
/tableName                          # Root table
/tableName@hash                     # Specific row by hash
/tableName@timeId                   # Historic version
/tableName/childTable               # Relationship traversal
/tableName@hash/childTable@hash2    # Nested navigation
```

### Route Segments

```typescript
type RouteSegment = {
  tableKey: string;           // Table name
  ref?: string;               // Hash or timeId
  propertyKey?: string;       // For tree navigation
  sliceIds?: SliceId[];       // Dimension filters
};
```

### Route Resolution Process

1. **Parse**: Split flat route string into segments
2. **Validate**: Check table existence and relationships
3. **Resolve References**:
   - Look up hash references in insert history
   - Convert timeIds to current hashes
   - Handle default refs (latest version)
4. **Navigate**: Traverse from root to leaf segment

### Example Resolution

Route: `/users@hash123/projects`

```
Segment 1: users@hash123
├─ Table: users
├─ Ref: hash123
└─ Query: { _hash: 'hash123' }

Segment 2: projects
├─ Table: projects
├─ Parent: users (hash123)
└─ Query: WHERE projects references hash123
```

## Caching Strategy

### Cache Key Generation

```typescript
const cacheKey = hsh({
  route: route.flat,
  where: where,
  filter: filter,
  sliceIds: sliceIds,
  routeAccumulator: routeAccumulator.flat,
  options: options
})._hash;
```

### Cache Conditions

Caching is enabled when:

- Route contains hash references (`@hash`)
- Filters are applied
- SliceIds are specified

Caching is disabled for:

- Empty WHERE clauses
- Routes without references
- Time-based queries (to ensure freshness)

### Cache Invalidation

- Manual: `db.clearCache()`
- Automatic: On insert operations (via notify callbacks)
- Size-based: LRU eviction (if implemented)

## Tree Processing

### Tree Structure

```typescript
type Tree = {
  id: string;              // Node identifier
  children: string[];      // Hash references to child nodes
  meta: Json;             // Node data
  isParent: boolean;      // Has children flag
  _hash: string;          // Content hash
};
```

### Tree Expansion Algorithm

**Problem**: Trees use content-addressed children (hash references). Naively expanding all children causes:

- Infinite recursion on circular refs
- Heap exhaustion on large trees
- O(n²) complexity on deep trees

**Solution**: Conditional expansion based on query context

### Tree INSERT and Root Node Creation

**Problem**: The `treeFromObject` function (from `@rljson/rljson`) automatically creates an explicit root node with `id='root'`. During INSERT operations, if the tree object already represents an isolated subtree (e.g., from `isolate()`), this creates a double-root structure:
- Auto-root (id='root') → User-root (id='root') → actual data nodes

This causes navigation issues because `TreeController` stops at the first node matching `id='root'`.

**Solution**: The `treeFromObject` call in `db.ts` (line 1365) uses a `skipRootCreation` parameter:

```typescript
const trees = treeFromObject(treeObject, true); // true = skip automatic root creation
```

This prevents the extra root wrapper when inserting tree data, allowing the subtree to be inserted with its existing structure intact.

```typescript
async get(where, filter?, path?): Promise<Rljson> {
  // Fetch matching tree nodes
  const trees = await this._core.readRows({...});

  // Single node check
  if (trees.length === 1) {
    const tree = trees[0];

    // KEY DECISION: Expand children only if navigating
    const shouldExpandChildren = path !== undefined;

    if (!shouldExpandChildren) {
      // WHERE clause query - return only this node
      return { [tableKey]: { _data: [tree], _type: 'trees' } };
    }

    // Route navigation - expand children
    const children = [];
    for (const childHash of tree.children ?? []) {
      const child = await this.get(
        childHash,
        undefined,
        treeRoute.deeper().flat  // Pass path to trigger expansion
      );
      children.push(...child[tableKey]._data);
    }

    return { [tableKey]: { _data: [...children, tree], _type: 'trees' } };
  }

  // Multiple nodes or no nodes
  return { [tableKey]: { _data: trees, _type: 'trees' } };
}
```

### Tree Memoization

The `buildTreeFromTrees` method uses memoization to avoid reprocessing nodes:

```typescript
async buildTreeFromTrees(trees: Tree[]): Promise<Json> {
  const memo = new Map<string, Json>();

  const buildNode = async (hash: string): Promise<Json> => {
    if (memo.has(hash)) return memo.get(hash)!;

    const tree = trees.find(t => t._hash === hash);
    if (!tree) throw new Error(`Tree node ${hash} not found`);

    const node = { ...tree.meta };

    if (tree.children && tree.children.length > 0) {
      for (const childHash of tree.children) {
        const childTree = trees.find(t => t._hash === childHash);
        if (childTree) {
          node[childTree.id] = await buildNode(childHash);
        }
      }
    }

    memo.set(hash, node);
    return node;
  };

  // Build from root (last element)
  return buildNode(trees[trees.length - 1]._hash);
}
```

### Safety Mechanisms

1. **Recursion Depth Limit**: 100 levels max
2. **Path-based Expansion**: Only expand when navigating
3. **Memoization**: Prevent redundant processing
4. **Early Returns**: Short-circuit on leaf nodes

## Join System

The Join system provides SQL-like operations on query results.

**Location**: `src/join/join.ts`

### Join Operations

```typescript
class Join {
  // Selection (columns)
  select(columnSelection: ColumnSelection): void

  // Filtering (rows)
  filter(rowFilter: RowFilter): void

  // Sorting
  sort(rowSort: RowSort): void

  // Value mutation
  setValue(setValue: SetValue): void

  // Result access
  async rows(): Promise<JoinRows>
  async columns(): Promise<JoinColumn[]>
}
```

### Join Processing Pipeline

```
Container (rljson, tree, cell)
    ↓
Initialize Join
    ↓
┌─────────────┐
│  Selection  │ → Filter columns
└──────┬──────┘
       ↓
┌─────────────┐
│   Filter    │ → Filter rows
└──────┬──────┘
       ↓
┌─────────────┐
│    Sort     │ → Order rows
└──────┬──────┘
       ↓
┌─────────────┐
│  SetValue   │ → Modify values
└──────┬──────┘
       ↓
Result (JoinRows)
```

### Column Selection

```typescript
class ColumnSelection {
  constructor(
    route: Route,
    columns: ColumnInfo[],
    selectionType: 'include' | 'exclude' = 'include'
  )

  process(rows: JoinRows): JoinRows
}
```

### Row Filtering

```typescript
class RowFilter {
  constructor(
    route: Route,
    filters: Record<string, FilterProcessor>
  )

  process(rows: JoinRows): JoinRows
}
```

Supported filter types:

- String filters (equals, contains, startsWith, endsWith, regex)
- Number filters (equals, gt, gte, lt, lte, range)
- Boolean filters (equals, notEquals)
- Column filters (compare columns)

## Multi-Edit System

The MultiEditManager provides transactional editing capabilities.

**Location**: `src/edit/multi-edit-manager.ts`

### Architecture

```
MultiEditManager
├─ Tracks head MultiEditProcessor
├─ Registers as Db notify callback
├─ Manages edit history
└─ Publishes completed edits

MultiEditProcessor
├─ Executes individual EditActions
├─ Maintains intermediate state
├─ Builds result Join
└─ Supports rollback
```

### Edit Actions

```typescript
type EditAction = {
  type: 'columnSelection' | 'rowFilter' | 'rowSort' | 'setValue';
  params: {...};
};
```

### Transaction Flow

```
1. Create MultiEditManager
   ↓
2. Start multiEdit()
   ├─ Get or create head processor
   ├─ Execute edit callback
   └─ Return updated head
   ↓
3. Apply EditActions
   head.edit(action1)
   head.edit(action2)
   ↓
4. Publish Head
   manager.publishHead()
   ├─ Get result Join
   ├─ Insert into Db
   └─ Notify observers
```

## Design Decisions

### 1. Content-Addressed Immutability

**Decision**: All data is immutable and identified by content hash.

**Rationale**:

- Eliminates update conflicts
- Enables perfect caching
- Supports version history naturally
- Allows secure data sharing

**Trade-offs**:

- Higher storage requirements
- More complex mutation patterns
- Requires hash computation overhead

### 2. Controller Abstraction

**Decision**: Each table type has a specialized controller.

**Rationale**:

- Encapsulates type-specific logic
- Enables polymorphic operations
- Simplifies testing and maintenance
- Supports future table types

**Trade-offs**:

- More complex class hierarchy
- Requires careful interface design

### 3. Recursive Route Resolution

**Decision**: Routes are resolved recursively, segment by segment.

**Rationale**:

- Handles arbitrary nesting depth
- Natural fit for hierarchical data
- Enables lazy loading
- Supports circular references

**Trade-offs**:

- Risk of stack overflow (mitigated by depth limit)
- Complex debugging
- Performance overhead

### 4. Path-Based Tree Expansion

**Decision**: TreeController uses `path` parameter to control expansion.

**Rationale**:

- Prevents heap crashes on large trees
- Preserves navigation functionality
- Simple, clear semantics
- Minimal API changes

**Trade-offs**:

- Subtle behavior difference between query types
- Requires understanding of path parameter
- Documentation burden

### 5. Caching by Query Signature

**Decision**: Cache results by hash of route+where+filter+options.

**Rationale**:

- Optimal cache hit rate
- Automatic cache key generation
- No manual cache management
- Supports complex queries

**Trade-offs**:

- Memory growth on diverse queries
- No automatic invalidation
- Hash computation overhead

### 6. Join System Design

**Decision**: Separate Join class for data transformation.

**Rationale**:

- Separation of concerns
- Composable operations
- Testable in isolation
- Reusable across contexts

**Trade-offs**:

- Additional abstraction layer
- More object creation
- Steeper learning curve

## Performance Considerations

### Query Optimization

1. **Batch SliceId Resolution**: Resolve all sliceIds in parallel
2. **Controller Memoization**: Cache controller instances
3. **Result Caching**: Cache query results by signature
4. **Lazy Loading**: Only fetch data as needed
5. **Parallel Fetches**: Fetch independent data concurrently

### Memory Management

1. **Streaming**: Use streaming for large data exports
2. **Pagination**: Support paginated queries (future)
3. **Cache Eviction**: Implement LRU cache policy (future)
4. **Weak References**: Use WeakMap for temporary data (future)

### Recursion Safety

1. **Depth Limiting**: Max 100 recursion levels
2. **Path Checking**: Detect navigation context
3. **Memoization**: Avoid redundant processing
4. **Early Exits**: Short-circuit when possible

## Testing Strategy

### Unit Tests

- Controller operations (get, insert, table)
- Route parsing and validation
- Filter processing
- Cache behavior

### Integration Tests

- Full query flows (db.get)
- Insert operations (db.insert)
- Version history
- Multi-edit transactions

### Performance Tests

- Large tree queries
- Deep recursion
- Cache hit rates
- Memory usage

### Critical Test Cases

```typescript
describe('Tree WHERE clause fix', () => {
  it('should return ONLY ONE NODE when querying by _hash');
  it('should prevent heap crash with large trees');
  it('should expand children when path is provided');
});
```

## Future Enhancements

### Planned Features

1. **Query Optimization**
   - Query planner
   - Index support
   - Parallel execution

2. **Advanced Caching**
   - LRU eviction
   - Cache warming
   - Distributed caching

3. **Pagination**
   - Cursor-based pagination
   - Offset-limit pagination
   - Stream processing

4. **Transactions**
   - ACID guarantees
   - Rollback support
   - Conflict resolution

5. **Replication**
   - Master-slave replication
   - Multi-master replication
   - Conflict-free replicated data types (CRDTs)

## Debugging Tips

### Enable Query Logging

```typescript
// Add to Db._get for debugging
console.log('Route:', route.flat);
console.log('Where:', JSON.stringify(where));
console.log('Depth:', depth);
```

### Trace Tree Expansion

```typescript
// Add to TreeController.get
console.log(`Tree expansion: ${tree.id}, path=${path}`);
console.log(`Should expand: ${path !== undefined}`);
```

### Cache Analysis

```typescript
console.log('Cache size:', db.cache.size);
console.log('Cache keys:', Array.from(db.cache.keys()));
```

### Route Inspection

```typescript
console.log('Route segments:', route.segments);
console.log('Property key:', route.propertyKey);
console.log('Is valid:', route.isValid);
```

## Contributing

When contributing to @rljson/db:

1. **Understand the layers**: Know which layer owns which responsibility
2. **Preserve immutability**: Never modify data in place
3. **Test tree operations**: Always test with large/deep trees
4. **Document behavior**: Explain non-obvious design choices
5. **Benchmark changes**: Profile performance-critical paths
6. **Update tests**: Add tests for new features and bug fixes

## References

- [RLJSON Specification](https://github.com/rljson/rljson)
- [Content-Addressed Storage](https://en.wikipedia.org/wiki/Content-addressable_storage)
- [Merkle Trees](https://en.wikipedia.org/wiki/Merkle_tree)
- [Immutable Data Structures](https://en.wikipedia.org/wiki/Persistent_data_structure)
