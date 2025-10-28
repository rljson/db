// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { MultiEditResolved } from '../../src/edit/edit/multi-edit-resolved';
import { ViewCache } from '../../src/edit/view/view-cache';
import { ViewEditedMultiple } from '../../src/edit/view/view-edited-multiple';
import { ViewWithData } from '../../src/edit/view/view-with-data';

describe('ViewEditedMultipled', () => {
  describe('rows', () => {
    it('returns the original object, when no edits are applied', () => {
      const original = ViewWithData.example();
      const view = new ViewEditedMultiple(original, MultiEditResolved.empty);

      expect(view.rows).toBe(original.rows);
    });

    it('returns the original object with all edits applied', () => {
      const view = ViewEditedMultiple.example;

      for (const row of view.rows) {
        const [stringCol, intCol, floatCol, booleanCol, jsonObjectCol] = row;

        // 1. Set all items ending with "o" to true
        if (stringCol.endsWith('o')) {
          expect(intCol).toBe(1234);
          expect(floatCol).toBe(1.234);
        } else {
          expect(intCol).not.toBe(1234);
          expect(intCol).not.toBe(1234);
        }

        // 2. Set all items starting with "O"
        if (stringCol.startsWith('O')) {
          expect(booleanCol).toBe(true);
        } else {
          expect(booleanCol).toBe(row[0] === 'True' ? true : false);
        }

        // 3. Set all true items to done
        if (booleanCol) {
          expect(jsonObjectCol['done']).toBeTruthy();
        } else {
          expect(jsonObjectCol['done']).not.toBeTruthy();
        }
      }
    });
  });

  it('rowHashes()', () => {
    // expect(ViewEditedMultiple.example.rowHashes).toEqual([
    //   'WzfM6PMxBSVBqWN3ULh4Bp',
    //   'AfFcE7aUNTldp8msFa0Mgs',
    //   'aR2JEEqeDFbG8_Rhoc-FAF',
    //   '9gg--Ls3vgM1dH_8VMpg2Y',
    //   'WXf5gHQgr14k7pTZoUpA0D',
    //   'PZkBJ66TSwYEYsFGo6ZeP6',
    //   'qicVsOifObahZ3zpv-SnHA',
    //   'tAxKFppWwV0EGsMxDpp9b6',
    //   'ffl449gnBs8QAL7wd3svyC',
    //   'haw9RIq4I8h_NM3t2LItaS',
    //   'cF7jFamUJj9QaXyF7BxW-E',
    // ]);
  });

  describe('cache', () => {
    it('caches and restores the last edit by default', () => {
      // Prepare three edits a user is making
      const multiEdit2 = MultiEditResolved.example;
      const multiEdit1 = multiEdit2.previous!;
      const multiEdit0 = multiEdit1.previous!;

      // Create a cache
      const cache = new ViewCache(5);

      // A user makes a first edit. The edit is cached.
      const master = ViewWithData.example();
      const view0 = new ViewEditedMultiple(master, multiEdit0, cache);
      const view0FromCache = cache.get(master, multiEdit0);
      expect(view0FromCache?.rows).toBe(view0.rows);
      expect(view0.editCount).toBe(1);

      // A user makes a second edit.
      // The previous edit did not need to be applied again,
      // because it was taken from cache.
      const view1 = new ViewEditedMultiple(master, multiEdit1, cache);
      expect(view0.editCount).toBe(1);
      const view1FromCache = cache.get(master, multiEdit1);
      expect(view1FromCache?.rows).toBe(view1.rows);

      // A user makes a third edit.
      // The previous edit dids not need to be applied again,
      // because it was taken from cache.
      const view2 = new ViewEditedMultiple(master, multiEdit2, cache);
      expect(view0.editCount).toBe(1);
      const view2FromCache = cache.get(master, multiEdit2);
      expect(view2FromCache?.rows).toBe(view2.rows);

      // The result is the same as if all edits were applied at once
      expect(view2.rowHashes).toEqual(ViewEditedMultiple.example.rowHashes);
    });
  });

  it('columnTypes', () => {
    expect(ViewEditedMultiple.example.columnTypes).toEqual([
      'string',
      'number',
      'number',
      'boolean',
      'json',
      'jsonArray',
      'number',
    ]);
  });
});
