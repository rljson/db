// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';

export const mergeTrees = (
  trees: Json[],
  path: (string | number)[],
  preservedKeys: string[] = [],
): any => {
  // Handle empty trees array
  if (!trees || trees.length === 0) {
    return {};
  }

  // If path is empty, merge all trees at root level
  if (path.length === 0) {
    return trees.reduce((merged, tree) => {
      if (tree == null) return merged;

      // Initialize merged based on first non-null tree
      if (merged === undefined) {
        merged = Array.isArray(tree) ? [] : {};
      }

      if (Array.isArray(merged) && Array.isArray(tree)) {
        return [...merged, ...tree];
      } else if (!Array.isArray(merged) && !Array.isArray(tree)) {
        return { ...merged, ...tree };
      }

      return merged;
    }, undefined as any);
  }

  // Collect structure information along the path from all trees
  const pathStructure: any[] = new Array(path.length)
    .fill(null)
    .map(() => ({}));

  // Navigate to the path in each tree and collect all values + structure info
  const valuesAtPath: Json[] = [];

  for (const tree of trees) {
    if (tree == null) continue;

    let current = tree;
    let pathExists = true;

    // Navigate through the path and collect structure info
    for (let i = 0; i < path.length; i++) {
      const key = path[i];

      if (current == null || !(key in current)) {
        pathExists = false;
        break;
      }

      // Before moving to next level, collect any underscore properties and preservedKeys
      if (current && typeof current === 'object' && !Array.isArray(current)) {
        for (const prop in current) {
          if (prop.startsWith('_') || preservedKeys.includes(prop)) {
            pathStructure[i][prop] = (current as any)[prop];
          }
        }
      }

      current = (current as any)[key];
    }

    // If we successfully navigated the path, collect the value
    if (pathExists && current != null) {
      valuesAtPath.push(current);
    }
  }

  // If no values found at path, return empty object
  if (valuesAtPath.length === 0) {
    return {};
  }

  // Merge all collected values
  const mergedValue =
    valuesAtPath.reduce((merged, value) => {
      if (value == null) return merged;

      // Initialize merged based on first non-null value
      if (merged === undefined) {
        merged = Array.isArray(value) ? [] : {};
      }

      if (Array.isArray(merged) && Array.isArray(value)) {
        return [...merged, ...value];
      } else if (!Array.isArray(merged) && !Array.isArray(value)) {
        return { ...merged, ...value };
      }

      return merged;
    }, undefined as any) || {};

  // Build the result tree with the merged value at the specified path
  const result: any = {};

  // Add root level underscore properties and preservedKeys
  if (pathStructure[0]) {
    Object.assign(result, pathStructure[0]);
  }

  // Create the nested structure along the path
  let current = result;
  for (let i = 0; i < path.length - 1; i++) {
    const key = path[i];
    // Determine if next level should be array or object based on next key type
    const nextKey = path[i + 1];
    current[key] = typeof nextKey === 'number' ? [] : {};

    // Add underscore properties and preservedKeys for the next level
    if (
      pathStructure[i + 1] &&
      typeof current[key] === 'object' &&
      !Array.isArray(current[key])
    ) {
      Object.assign(current[key], pathStructure[i + 1]);
    }

    current = current[key];
  }

  // Set the merged value at the final path location
  if (path.length > 0) {
    current[path[path.length - 1]] = mergedValue;
  }

  return result;
};
