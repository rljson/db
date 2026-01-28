# Changelog

## [Unreleased]

### Fixed

- **Tree INSERT Double-Root Issue**: Fixed bug where `treeFromObject` was creating an automatic root node wrapper even for already-isolated subtrees during INSERT operations. This caused a double-root structure (auto-root wrapping user-root, both with id='root') that prevented proper tree navigation. The fix adds a `skipRootCreation` parameter to `treeFromObject` call in `db.ts` line 1365, which is set to `true` to prevent the extra wrapper when inserting tree data.
  - Impact: Tree INSERT operations now work correctly without requiring `isolate()` calls
  - Tests: All 361 tests passing, including previously failing "insert on tree simple branch" and "insert new child on branch"

## [0.0.1]

Initial commit.
