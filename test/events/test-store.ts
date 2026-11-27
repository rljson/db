// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { Json } from '@rljson/json';
import { Rljson, TableCfg } from '@rljson/rljson';

// .............................................................................
const column = (key: string) => ({
  key,
  type: 'string' as const,
  titleLong: key,
  titleShort: key,
});

/** A components table, `cars` by default, with the columns brand and owner */
export const carsCfg = (key = 'cars'): TableCfg =>
  hip<any>({
    version: 0,
    key,
    type: 'components',
    isHead: false,
    isRoot: false,
    isShared: true,
    columns: [column('_hash'), column('brand'), column('owner')],
  }) as TableCfg;

/** Rljson with cars of the given owners */
export const carsData = (...owners: string[]): Rljson =>
  hip<any>({
    cars: {
      _type: 'components',
      _data: owners.map((owner, i) => ({ brand: `brand${i}`, owner })),
    },
  }) as Rljson;

/** The owners of the cars in an Rljson */
export const ownersOf = (data: Rljson): string[] =>
  (data.cars._data as Json[]).map((row) => row.owner as string).sort();

/** An IoMem with the table cars holding a car of alice and one of bob */
export const carsIo = async (): Promise<IoMem> => {
  const io = await IoMem.example();
  await io.createOrExtendTable({ tableCfg: carsCfg() });
  await io.write({ data: carsData('alice', 'bob') });
  return io;
};

/** Keeps the cars of alice and the rows of all other tables */
export const onlyAlice = (row: Json, table: string) =>
  table !== 'cars' || row.owner === 'alice';
