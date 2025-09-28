import { Io } from '@rljson/io';
import { Json } from '@rljson/json';
import { Edit, EditProtocolRow } from '@rljson/rljson';

// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
class Transform<T extends Json> {
  constructor(private readonly _io: Io, private readonly edit: Edit<T>) {}
  async run(): Promise<EditProtocolRow<any>> {
    return {} as EditProtocolRow<any>;
  }
}
