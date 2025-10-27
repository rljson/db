// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { BehaviorSubject, firstValueFrom } from 'rxjs';
import { beforeAll, describe, expect, it } from 'vitest';

import { MultiEditMixer } from '../../src/edit/edit/multi-edit-mixer';
import { MultiEditResolved } from '../../src/edit/edit/multi-edit-resolved';
import { View } from '../../src/edit/view/view';
import { ViewWithData } from '../../src/edit/view/view-with-data';

describe('MultEditMixer', () => {
  let multiEditMixer: MultiEditMixer;
  let masterView: BehaviorSubject<View>;
  let multiEdit: BehaviorSubject<MultiEditResolved>;

  beforeAll(() => {
    multiEditMixer = MultiEditMixer.example;
    masterView = multiEditMixer.masterView$ as BehaviorSubject<View>;
    multiEdit = multiEditMixer.multiEdit$ as BehaviorSubject<MultiEditResolved>;

    expect(masterView).toBeDefined();
  });

  const editedView = (): Promise<View> => {
    return firstValueFrom(multiEditMixer.editedView$);
  };

  it('editedView', async () => {
    // At the beginning, the edited view is the same as the master view
    let t = await editedView();
    expect(t._hash).toEqual(ViewWithData.example()._hash);
    expect(t.rows.map(View.exampleSelect)).toEqual([
      ['Zero', 0, false, 'todo'],
      ['OneA', 1, false, 'todo'],
      ['Two', 2, false, 'todo'],
      ['OneB', 11, false, 'todo'],
      ['True', 12, true, 'todo'],
    ]);

    // Push the first edit. The edited view should change.
    multiEdit.next(MultiEditResolved.exampleStep0);
    t = await editedView();
    expect(t.rows.map(View.exampleSelect)).toEqual([
      ['Zero', 1234, false, 'todo'], // Edited
      ['OneA', 1, false, 'todo'],
      ['Two', 1234, false, 'todo'], // Edited
      ['OneB', 11, false, 'todo'],
      ['True', 12, true, 'todo'],
    ]);

    // Push the second edit. The edited view should change.
    multiEdit.next(MultiEditResolved.exampleStep0and1);
    t = await editedView();
    expect(t.rows.map(View.exampleSelect)).toEqual([
      ['Zero', 1234, false, 'todo'],
      ['OneA', 1, true, 'todo'], // Edited
      ['Two', 1234, false, 'todo'],
      ['OneB', 11, true, 'todo'],
      ['True', 12, true, 'todo'],
    ]);
  });
});
