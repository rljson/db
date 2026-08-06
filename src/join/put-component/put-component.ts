// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';
import { SliceId, TableKey } from '@rljson/rljson';

/**
 * Describes setting a whole content-addressed Component for a sliceId in
 * a Layer.
 *
 * Applying a PutComponent appends `component` to the layer's `add` map
 * under `sliceId` (`Layer.add[sliceId] = <ref of component>`), keeping
 * the write append-only and content-addressed: identical components
 * hash to the same ref, so re-applying the same PutComponent never
 * diverges. This lets a whole document (e.g. a Mongo upsert) be
 * expressed as ONE edit that carries the whole component object,
 * instead of one edit per changed field.
 */
export interface PutComponent extends Json {
  /**
   * The table key of the layer (collection) the component is set on.
   */
  layer: TableKey;

  /**
   * The slice id (document id) the component is assigned to.
   */
  sliceId: SliceId;

  /**
   * The whole content-addressed component object to set. Carries its own
   * `_hash` like any other Rljson row.
   */
  component: Json;
}

// .............................................................................
/**
 * An example PutComponent for test purposes.
 */
export const examplePutComponent = (): PutComponent => ({
  layer: 'carGeneralLayer',
  sliceId: 'VIN1',
  component: {
    brand: 'Rljson Motors',
    type: 'Prototype X',
    doors: 2,
    energyConsumption: 12.3,
    units: { energy: 'kWh/100km', _hash: '' },
    serviceIntervals: [10000, 20000],
    isElectric: true,
    meta: { pressText: 'A whole new component, put in one edit.', _hash: '' },
    _hash: '',
  },
  _hash: '',
});
