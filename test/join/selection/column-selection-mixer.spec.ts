// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { BehaviorSubject, firstValueFrom } from 'rxjs';
import { beforeEach, describe, expect, it } from 'vitest';

import { ColumnSelection } from '../../src/edit/selection/column-selection';
import { ColumnSelectionMixer } from '../../src/edit/selection/column-selection-mixer';

describe('ColumnSelectionMixer', () => {
  let csm: ColumnSelectionMixer;
  let viewColumnSelections: BehaviorSubject<ColumnSelection[]>;
  let multiEditColumnSelection: BehaviorSubject<ColumnSelection>;

  const masterColumnSelection = async () => {
    return (await firstValueFrom(csm.columnSelection$)).routes;
  };

  beforeEach(() => {
    csm = ColumnSelectionMixer.example;
    viewColumnSelections = csm.viewColumnSelections$ as BehaviorSubject<
      ColumnSelection[]
    >;

    multiEditColumnSelection =
      csm.multiEditColumnSelection$ as BehaviorSubject<ColumnSelection>;

    expect(viewColumnSelections).toBeDefined();
    expect(multiEditColumnSelection).toBeDefined();
  });

  describe('columnSelection', () => {
    describe('returns a new master column selection', () => {
      it('whenever the view or the multi edit column selections change', async () => {
        // The intial master column selection
        expect(await masterColumnSelection()).toEqual([
          'basicTypes/stringsRef/value',
          'basicTypes/numbersRef/intsRef/value',
          'basicTypes/numbersRef/floatsRef/value',
          'basicTypes/booleansRef/value',
          'complexTypes/jsonObjectsRef/value',
          'complexTypes/jsonArraysRef/value',
          'complexTypes/jsonValuesRef/value',
        ]);
      });
    });
  });
});
