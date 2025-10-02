import { hsh } from '@rljson/hash';
import { Io } from '@rljson/io';
import { Json } from '@rljson/json';
import { Edit, EditProtocolRow, Layer, Rljson, timeId } from '@rljson/rljson';

import { Transform } from './transform.ts';

// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
export class TransformLayer implements Transform {
  constructor(
    private readonly _io: Io,
    private readonly _edit: Edit<Layer>,
    private readonly _tableKey: string,
  ) {}

  async run(): Promise<EditProtocolRow<any>> {
    //Value to add
    const layer = this._edit.value as Layer & { _hash?: string };
    const rlJson = { [this._tableKey]: { _data: [layer] } } as Rljson;

    //Write component to io
    await this._io.write({ data: rlJson });

    //Create EditProtocolRow
    const result = {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(layer as Json)._hash as string,

      //Data from edit
      route: this._edit.route,
      origin: this._edit.origin,
      previous: this._edit.previous,

      //Unique id/timestamp
      timeId: timeId(),
    } as EditProtocolRow<any>;

    return result;
  }
}
