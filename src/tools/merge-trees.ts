// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';

/* v8 ignore next -- @preserve */
export const mergeTrees = (
  trees: {
    tree: Json;
    path: Array<string | number>;
  }[],
): Json => {
  // Handle empty trees array
  if (!trees || trees.length === 0) {
    return {};
  }

  // First, merge all tree structures to preserve all properties along all paths
  let result: Json = {};

  for (const { tree } of trees) {
    if (tree != null) {
      result = mergeStructures(result, tree);
    }
  }

  // Extract values at each specified path
  const pathValues: { path: Array<string | number>; value: Json }[] = [];

  for (const { tree, path } of trees) {
    if (tree == null) continue;

    let current: any = tree;
    let pathExists = true;

    // Navigate through the path
    for (const key of path) {
      if (current == null || !(key in current)) {
        pathExists = false;
        break;
      }
      current = current[key];
    }

    // If we successfully navigated the path, store the path and value
    if (pathExists && current != null) {
      pathValues.push({ path, value: current });
    }
  }

  // Group values by their paths
  const pathGroups = new Map<string, Json[]>();

  for (const { path, value } of pathValues) {
    const pathKey = JSON.stringify(path);
    if (!pathGroups.has(pathKey)) {
      pathGroups.set(pathKey, []);
    }
    pathGroups.get(pathKey)!.push(value);
  }

  // Merge values for each unique path and set them in the result
  for (const [pathKey, values] of pathGroups) {
    const path = JSON.parse(pathKey) as Array<string | number>;

    // Merge all values at this path
    let mergedValue: Json | undefined = undefined;
    for (const value of values) {
      if (value == null) continue;

      if (mergedValue === undefined) {
        mergedValue = value;
        continue;
      }

      if (Array.isArray(mergedValue) && Array.isArray(value)) {
        mergedValue = [...mergedValue, ...value] as unknown as Json;
      } else if (!Array.isArray(mergedValue) && !Array.isArray(value)) {
        mergedValue = {
          ...(mergedValue as any),
          ...(value as any),
        } as unknown as Json;
      }
      // If types mismatch, keep the existing mergedValue
    }

    // Set the merged value at the path in the result
    let current: any = result;
    for (let i = 0; i < path.length - 1; i++) {
      const key = path[i];
      if (current == null || !(key in current)) {
        // Path doesn't exist in result, create it
        current[key] = typeof path[i + 1] === 'number' ? [] : {};
      }
      current = current[key];
    }

    if (path.length > 0) {
      current[path[path.length - 1]] = mergedValue;
    }
  }

  return result;
};

// Helper function to recursively merge two tree structures
/* v8 ignore next -- @preserve */
function mergeStructures(target: Json, source: Json): Json {
  if (source == null) return target;
  if (target == null) return source;

  // If both are arrays, merge them
  if (Array.isArray(target) && Array.isArray(source)) {
    const result = [...target];
    for (let i = 0; i < source.length; i++) {
      if (result[i] === undefined) {
        result[i] = source[i];
      } else {
        result[i] = mergeStructures(result[i], source[i]);
      }
    }
    return result as unknown as Json;
  }

  // If both are objects, merge their properties
  if (
    typeof target === 'object' &&
    typeof source === 'object' &&
    !Array.isArray(target) &&
    !Array.isArray(source) &&
    target !== null &&
    source !== null
  ) {
    const result = { ...target } as any;

    for (const key in source) {
      if (key in result) {
        result[key] = mergeStructures(result[key], (source as any)[key]);
      } else {
        result[key] = (source as any)[key];
      }
    }

    return result;
  }

  // For primitive values or type mismatches, return source
  return source;
}
