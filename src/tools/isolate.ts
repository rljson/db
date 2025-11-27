// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

export const isolate = (tree: any, path: (string | number)[]): any => {
  // Handle empty path - return empty object/array based on tree type
  if (path.length === 0) {
    return Array.isArray(tree) ? [] : {};
  }

  // Handle null/undefined tree
  if (tree == null) {
    return null;
  }

  const [currentKey, ...remainingPath] = path;

  // Create new container based on tree type
  const result = Array.isArray(tree) ? [] : {};

  // Preserve properties with keys starting with "_"
  if (!Array.isArray(tree)) {
    for (const key in tree) {
      if (typeof key === 'string' && key.startsWith('_')) {
        (result as any)[key] = tree[key];
      }
    }
  }

  // Check if current key exists in tree
  if (!(currentKey in tree)) {
    return result;
  }

  const currentValue = tree[currentKey];

  // If this is the last key in path, include the full value
  if (remainingPath.length === 0) {
    if (Array.isArray(result)) {
      (result as any[])[currentKey as number] = currentValue;
    } else {
      (result as any)[currentKey] = currentValue;
    }
  } else {
    // Recursively isolate the remaining path
    const isolatedChild = isolate(currentValue, remainingPath);

    // Only include the key if the isolated child has content
    const hasContent = Array.isArray(isolatedChild)
      ? isolatedChild.length > 0
      : Object.keys(isolatedChild).length > 0;

    if (hasContent || isolatedChild === null) {
      if (Array.isArray(result)) {
        (result as any[])[currentKey as number] = isolatedChild;
      } else {
        (result as any)[currentKey] = isolatedChild;
      }
    }
  }

  return result;
};
