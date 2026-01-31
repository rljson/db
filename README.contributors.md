# @rljson/db - Contributors Guide

Welcome, contributor! This guide helps you get started with development, testing, and contributing to @rljson/db.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Code Quality](#code-quality)
- [Contributing Guidelines](#contributing-guidelines)
- [Release Process](#release-process)
- [Useful Commands](#useful-commands)
- [Architecture Overview](#architecture-overview)
- [Debugging](#debugging)

## Getting Started

### Prerequisites

- **Node.js:** >= 22.14.0
- **pnpm:** 10.6.3+ (package manager)
- **Git:** For version control

### Initial Setup

```bash
# Clone the repository
git clone https://github.com/rljson/db.git
cd db

# Install dependencies
pnpm install

# Run tests to verify setup
pnpm test
```

See also: [doc/workflows/prepare.md](doc/workflows/prepare.md)

## Development Workflow

### 1. Create a Feature Branch

```bash
# Create and switch to a new branch
git checkout -b feature/my-feature

# Or use the script
node scripts/create-branch.js feature/my-feature
```

### 2. Make Changes

Edit source files in `src/`:
- `src/core.ts` - Low-level Core class
- `src/db.ts` - High-level Db class
- `src/controller/` - Controller implementations
- `src/join/` - Join system
- `src/edit/` - Multi-edit system
- `src/notify.ts` - Notification system
- `src/tools/` - Utility functions

### 3. Write Tests

Add tests in `test/`:
- `test/db.spec.ts` - Db class tests (74 tests)
- `test/core.spec.ts` - Core class tests
- `test/controller.spec.ts` - Controller tests (45 tests)
- `test/join/` - Join system tests
- `test/edit/` - Edit system tests

**Test Pattern:**
```typescript
import { describe, it, expect } from 'vitest';
import { Db } from '../src/db.ts';
import { IoMem } from '@rljson/io';

describe('MyFeature', () => {
  let db: Db;

  beforeEach(async () => {
    const io = new IoMem();
    await io.init();
    db = new Db(io);
  });

  it('should do something', async () => {
    // Arrange
    const input = { ... };

    // Act
    const result = await db.someMethod(input);

    // Assert
    expect(result).toEqual(expected);
  });
});
```

### 4. Run Tests

```bash
# Run all tests with coverage
pnpm test

# Run tests in watch mode (development)
pnpm run test -- --watch

# Run specific test file
pnpm run test -- db.spec.ts

# Run with specific pattern
pnpm run test -- --grep "tree INSERT"
```

### 5. Lint and Format

```bash
# Run linter
pnpm run lint

# TypeScript type checking is automatic during build
pnpm run build
```

### 6. Update Documentation

If your changes affect the public API or behavior:
- Update [README.public.md](./README.public.md)
- Update [README.architecture.md](./README.architecture.md) for design changes
- Add troubleshooting entries to [README.trouble.md](./README.trouble.md) if needed
- Update [CHANGELOG.md](./CHANGELOG.md)

### 7. Commit and Push

```bash
# Add changes
git add .

# Commit with descriptive message
git commit -m "feat: add tree conditional expansion

- Add path parameter to TreeController.get()
- Prevent infinite recursion on large trees
- Update tests and documentation"

# Push to remote
git push origin feature/my-feature
```

### 8. Create Pull Request

Push your branch and create a pull request on GitHub.

See also: [doc/workflows/develop.md](doc/workflows/develop.md)

## Project Structure

```
rljson-db/
├── src/                        # Source code
│   ├── core.ts                 # Core class (149 lines)
│   ├── db.ts                   # Db class (1822 lines)
│   ├── index.ts                # Public exports
│   ├── notify.ts               # Notification system (103 lines)
│   ├── connector/              # Connector implementations
│   │   └── connector.ts
│   ├── controller/             # Controller implementations
│   │   ├── base-controller.ts          # Abstract base (159 lines)
│   │   ├── component-controller.ts     # Components (504 lines)
│   │   ├── tree-controller.ts          # Trees (394 lines)
│   │   ├── cake-controller.ts          # Cakes (203 lines)
│   │   ├── layer-controller.ts         # Layers
│   │   ├── slice-id-controller.ts      # SliceIds
│   │   └── controller.ts               # Factory and types
│   ├── edit/                   # Multi-edit system
│   │   ├── edit-action.ts
│   │   ├── edit.ts
│   │   ├── multi-edit-manager.ts       # Manager (262 lines)
│   │   └── multi-edit-processor.ts
│   ├── join/                   # Join system
│   │   ├── join.ts                     # Main Join class (589 lines)
│   │   ├── filter/                     # Row filtering
│   │   ├── selection/                  # Column selection
│   │   ├── set-value/                  # Value mutation
│   │   └── sort/                       # Row sorting
│   └── tools/                  # Utility functions
│       ├── inject.ts                   # Path-based injection
│       ├── isolate.ts                  # Path-based isolation
│       ├── make-unique.ts              # Array deduplication
│       └── merge-trees.ts              # Tree merging
├── test/                       # Test suite
│   ├── test-setup.ts           # Vitest configuration
│   ├── db.spec.ts              # Db tests (74 tests)
│   ├── core.spec.ts            # Core tests
│   ├── controller.spec.ts      # Controller tests (45 tests)
│   ├── io-multi-tree-cascade.spec.ts  # IoMulti tests (7 tests)
│   ├── mass-data.spec.ts       # Performance tests
│   ├── connector/              # Connector tests
│   ├── edit/                   # Edit system tests
│   ├── join/                   # Join system tests
│   ├── goldens/                # Golden file tests
│   └── setup/                  # Test setup utilities
├── doc/                        # Documentation
│   ├── install/                # Installation guides
│   └── workflows/              # Development workflows
│       ├── prepare.md          # Environment setup
│       ├── develop.md          # Development guide
│       ├── debug-with-vscode.md  # Debugging
│       ├── tools.md            # Available tools
│       └── ...
├── scripts/                    # Build and automation scripts
│   ├── publish-to-npm.js       # NPM publishing
│   ├── add-version-tag.js      # Git tagging
│   ├── run-in-all-repos.js     # Monorepo utilities
│   └── ...
├── coverage/                   # Test coverage reports
├── package.json                # Dependencies and scripts
├── tsconfig.json               # TypeScript configuration
├── vite.config.mts             # Vite build configuration
├── vitest.config.mts           # Vitest test configuration
├── eslint.config.js            # ESLint configuration
└── README*.md                  # Documentation files
```

## Testing

### Test Coverage

We maintain **100% code coverage**. All new code must include tests.

Current stats (361 tests):
- **db.spec.ts:** 74 tests (Db class operations)
- **controller.spec.ts:** 45 tests (All controllers)
- **io-multi-tree-cascade.spec.ts:** 7 tests (IoMulti integration)
- **core.spec.ts:** Core functionality
- **join tests:** Join system operations
- **edit tests:** Multi-edit operations

### Running Tests

```bash
# Run all tests with coverage
pnpm test

# Run in watch mode (auto-reruns on changes)
pnpm run test -- --watch

# Run specific file
pnpm run test -- controller.spec.ts

# Run with pattern matching
pnpm run test -- --grep "tree INSERT"

# Update golden files (test fixtures)
pnpm run updateGoldens
```

### Test Structure

Tests use Vitest with the following patterns:

```typescript
describe('Feature', () => {
  // Setup
  let db: Db;

  beforeEach(async () => {
    const io = new IoMem();
    await io.init();
    db = new Db(io);
  });

  // Tests
  it('should handle basic case', async () => {
    // Test implementation
  });

  it('should handle edge case', async () => {
    // Test implementation
  });
});
```

### Golden Files

Tests use golden files in `test/goldens/` for complex expected outputs:

```typescript
import { readGolden, writeGolden } from './setup/golden.ts';

const result = await db.get(route, where);
const golden = await readGolden('path/to/golden.json');
expect(result).toEqual(golden);

// Update goldens (when intentional changes made)
if (process.env.UPDATE_GOLDENS) {
  await writeGolden('path/to/golden.json', result);
}
```

## Code Quality

### TypeScript

All code is written in TypeScript with strict type checking:

```typescript
// Always specify return types
async function myFunction(param: string): Promise<Rljson> {
  // ...
}

// Use type imports for types only
import type { Component, Tree } from '@rljson/rljson';

// Avoid 'any' - use specific types
const data: Rljson = await db.core.dump();  // Good
const data: any = await db.core.dump();      // Bad
```

### ESLint

We use ESLint with TypeScript support:

```bash
# Run linter
pnpm run lint

# Configuration in eslint.config.js
```

### Code Style

- Use **async/await** (not callbacks or raw promises)
- Add **JSDoc comments** for public APIs
- Keep functions **focused and small**
- Use **descriptive variable names**
- Prefer **const** over let
- Use **optional chaining** (`?.`) and **nullish coalescing** (`??`)

Example:
```typescript
/**
 * Fetches data from the database by route
 * @param route - The route to query
 * @param where - Optional filter criteria
 * @returns Container with query results
 */
async get(route: Route, where: Json): Promise<Container> {
  // Implementation
}
```

## Contributing Guidelines

### Branch Naming

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation only
- `refactor/description` - Code refactoring
- `test/description` - Test improvements

### Commit Messages

Follow conventional commits:

```
type(scope): short description

Longer description if needed.

- Bullet points for details
- Multiple lines OK

Fixes #123
```

**Types:**
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation
- `test:` - Tests
- `refactor:` - Code restructuring
- `perf:` - Performance improvement
- `chore:` - Maintenance

### Pull Requests

1. Create PR with clear title and description
2. Link related issues
3. Ensure all tests pass
4. Request review from maintainers
5. Address review feedback
6. Squash commits if requested

## Release Process

Releases are managed by maintainers. The process:

1. Update version in `package.json`
2. Update `CHANGELOG.md`
3. Create git tag: `node scripts/add-version-tag.js`
4. Publish to npm: `node scripts/publish-to-npm.js`

Version scheme: `MAJOR.MINOR.PATCH` (semver)

## Useful Commands

```bash
# Development
pnpm install              # Install dependencies
pnpm test                 # Run tests with coverage
pnpm run build            # Build for production
pnpm run lint             # Run linter

# Testing
pnpm run test -- --watch              # Watch mode
pnpm run test -- db.spec.ts           # Specific file
pnpm run test -- --grep "pattern"     # Pattern match
pnpm run updateGoldens                # Update golden files

# Scripts
node scripts/create-branch.js feature/name    # Create branch
node scripts/is-clean-repo.js                 # Check git status
node scripts/run-in-all-repos.js "command"    # Run in all repos
```

## Architecture Overview

### Layer Diagram

```
Application
    ↓
Db (High-level API)
    ↓
Controllers (Type-specific logic)
    ↓
Core (Data management)
    ↓
Io (Storage backends)
```

### Key Classes

- **Db:** Main entry point, route resolution, caching, notifications
- **Core:** Table management, data import/export, validation
- **Controller:** Abstract interface for table types
  - TreeController - Hierarchical structures
  - ComponentController - Flat tables
  - CakeController - Multi-dimensional data
  - LayerController - Layered configurations
- **Join:** SQL-like operations (select, filter, sort)
- **MultiEditManager:** Transactional editing

### Design Patterns

- **Controller Pattern:** Polymorphic table operations
- **Content-Addressing:** Data identified by hash
- **Immutability:** All mutations create new versions
- **Recursion with Safety:** Depth limits and memoization
- **Caching:** Query result memoization

See [README.architecture.md](./README.architecture.md) for deep dive.

## Debugging

### VS Code Debugging

See [doc/workflows/debug-with-vscode.md](doc/workflows/debug-with-vscode.md)

Launch configuration example:
```json
{
  "type": "node",
  "request": "launch",
  "name": "Debug Tests",
  "runtimeExecutable": "pnpm",
  "runtimeArgs": ["test", "--", "--run"],
  "console": "integratedTerminal"
}
```

### Debug Logging

Add strategic console.log statements (remove before commit):

```typescript
// Trace route resolution
console.log('Route:', route.flat);
console.log('Where:', JSON.stringify(where));

// Trace tree expansion
console.log(`Tree: ${tree.id}, path=${path}, expand=${path !== undefined}`);

// Trace cache behavior
console.log('Cache hit:', this._cache.has(cacheKey));
```

### Test Debugging

Run specific failing test:
```bash
pnpm run test -- --grep "exact test name"
```

Use `it.only` to isolate tests:
```typescript
it.only('should test specific thing', async () => {
  // Only this test runs
});
```

### Common Issues

**Issue:** Tests fail with "Maximum recursion depth"
**Solution:** Check TreeController path parameter handling

**Issue:** Cache not clearing after insert
**Solution:** Verify notify callbacks are registered

**Issue:** Golden file mismatch
**Solution:** Run `pnpm run updateGoldens` if changes are intentional

## Additional Resources

- [README.architecture.md](./README.architecture.md) - Technical architecture deep dive
- [README.trouble.md](./README.trouble.md) - Troubleshooting guide
- [doc/workflows/](./doc/workflows/) - Detailed workflow guides
- [GitHub Issues](https://github.com/rljson/db/issues) - Bug reports and feature requests

## Getting Help

- **Issues:** Report bugs via [GitHub Issues](https://github.com/rljson/db/issues)
- **Discussions:** Ask questions in GitHub Discussions
- **Code Review:** Request maintainer review on PRs

## License

MIT - See [LICENSE](./LICENSE)

Thank you for contributing to @rljson/db!
