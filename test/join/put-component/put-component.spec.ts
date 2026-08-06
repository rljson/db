// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { Route } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { Container, Db } from '../../../src/db';
import { staticExample } from '../../../src/example-static/example-static';
import { Join, JoinColumn, JoinRows } from '../../../src/join/join';
import {
  examplePutComponent,
  PutComponent,
} from '../../../src/join/put-component/put-component';
import { ColumnSelection } from '../../../src/join/selection/column-selection';

describe('Join.putComponent()', () => {
  let db: Db;

  const cakeKey = 'carCake';
  const layerKey = 'carGeneralLayer';
  const componentsTable = 'carGeneral';

  // carCake._data[1] -> carGeneralLayer._data[1], which chains onto
  // carGeneralLayer._data[0] via `base` (see chainLayers() in
  // example-static.ts): layer[0] holds VIN1..VIN8, layer[1] adds
  // VIN9/VIN10 on top via base. Using this cake exercises both "own"
  // and "inherited" slices being preserved by putComponent.
  const cakeRef = staticExample().carCake._data[1]._hash as string;

  beforeEach(async () => {
    const io = new IoMem();
    await io.init();
    await io.isReady();

    db = new Db(io);

    for (const tableCfg of staticExample().tableCfgs._data) {
      await db.core.createTableWithInsertHistory(tableCfg);
    }

    await db.core.import(staticExample());
  });

  /**
   * Builds a minimal single-column Join carrying the target layer's
   * Container -- exactly what MultiEditProcessor seeds for the
   * first-ever putComponent edit of a chain (see multi-edit-processor.ts
   * Switch A).
   */
  const seedJoin = async (sliceId: string): Promise<Join> => {
    const layerRouteFlat = `${cakeKey}/${layerKey}`;
    const layerRoute = Route.fromFlat(layerRouteFlat);
    const container = await db.get(
      Route.fromFlat(`${cakeKey}@${cakeRef}/${layerKey}`),
      {},
    );
    const columnInfo = {
      key: layerKey,
      type: 'jsonValue' as const,
      alias: layerKey,
      route: layerRouteFlat,
      titleShort: layerKey,
      titleLong: layerKey,
    };
    const rows: JoinRows = {
      [sliceId]: [
        { route: layerRoute, value: container, inserts: null },
      ] as JoinColumn[],
    };
    return new Join(rows, new ColumnSelection([columnInfo]));
  };

  /**
   * Persists the pending sliceIds insert (as MultiEditProcessor does),
   * then runs the direct insert. Returns the written cake ref.
   */
  const publishJoin = async (join: Join): Promise<string> => {
    const pending = join.pendingSliceIdsInsert!;
    await db.core.import(
      {
        [pending.table]: { _type: 'sliceIds', _data: pending.rows },
      } as any,
      { validate: false },
    );
    const [{ route, tree }] = join.insert();
    const results = await db.insert(route, tree);
    return (results[0] as any)[`${cakeKey}Ref`] as string;
  };

  /**
   * Column selection over the layer's brand, used to assert the
   * sliceId-driven db.join (the reconstruction engine) sees a slice.
   */
  const brandSelection = new ColumnSelection([
    {
      key: 'brand',
      route: `${cakeKey}/${layerKey}/${componentsTable}/brand`,
      alias: 'brand',
      titleLong: '',
      titleShort: '',
      type: 'jsonValue' as const,
    },
  ]);

  it('sets add[sliceId], chains base, and extends both slice sets', async () => {
    const data = examplePutComponent();
    expect(data.layer).toBe(layerKey);
    expect(data.sliceId).toBe('VIN99'); // a brand-new document

    const join = (await seedJoin(data.sliceId)).putComponent(data);
    const inserts = join.insert();

    expect(inserts.length).toBe(1);

    const { route, tree } = inserts[0];
    expect(route.flat).toBe(`/${cakeKey}/${layerKey}/${componentsTable}`);

    const cakeRow = (tree as any)[cakeKey]._data[0];
    const layerRow = cakeRow.layers[layerKey]._data[0];

    // The whole component landed under add[sliceId], keyed by the
    // layer's componentsTable.
    expect(layerRow.add[data.sliceId][componentsTable]._data[0]).toEqual(
      data.component,
    );

    // Append-only: layer base points at the layer's CURRENT ref, other
    // layers of the cake are carried through untouched (still plain refs).
    expect(typeof layerRow.base).toBe('string');
    expect(layerRow.base.length).toBeGreaterThan(0);
    expect(typeof cakeRow.layers.carTechnicalLayer).toBe('string');
    expect(typeof cakeRow.layers.carColorLayer).toBe('string');

    // Both slice sets were re-pointed at the new (extended) sliceIds row.
    const pending = join.pendingSliceIdsInsert!;
    expect(pending.table).toBe('carSliceId');
    // cake.sliceIdsRow == layer.sliceIdsTableRow here, so one deduped row.
    expect(pending.rows.length).toBe(1);
    const newSliceIdsRef = pending.rows[0]._hash as string;
    expect(layerRow.sliceIdsTableRow).toBe(newSliceIdsRef);
    expect(cakeRow.sliceIdsRow).toBe(newSliceIdsRef);
    expect(pending.rows[0].add).toEqual([data.sliceId]);
  });

  it('INSERT: a brand-new sliceId is readable via db.get AND db.join', async () => {
    const data = examplePutComponent(); // sliceId VIN99 (new)

    const join = (await seedJoin(data.sliceId)).putComponent(data);
    const writtenCakeRef = await publishJoin(join);
    expect(writtenCakeRef).toBeDefined();

    // (a) sliceId-filtered db.get returns the new component.
    const newRead = await db.get(
      Route.fromFlat(
        `${cakeKey}@${writtenCakeRef}/${layerKey}/${componentsTable}/brand`,
      ),
      {},
      undefined,
      [data.sliceId],
    );
    expect(newRead.cell.length).toBe(1);
    expect(newRead.cell[0].value).toBe(data.component.brand);

    // (b) the sliceId-driven db.join (used to rebuild the join on
    // reconstruction) now includes VIN99 -- 10 existing + 1 new.
    const join2 = await db.join(brandSelection, cakeKey, writtenCakeRef);
    expect(join2.rowIndices).toContain(data.sliceId);
    expect(join2.rowIndices.length).toBe(11);
    const vin99Row = join2.row(data.sliceId);
    expect(vin99Row[0].value.cell[0].value).toBe(data.component.brand);

    // Existing slices (own + base-inherited) still resolve.
    for (const existing of ['VIN1', 'VIN9']) {
      const read = await db.get(
        Route.fromFlat(
          `${cakeKey}@${writtenCakeRef}/${layerKey}/${componentsTable}/brand`,
        ),
        {},
        undefined,
        [existing],
      );
      expect(read.cell.length).toBe(1);
    }

    // Other layers of the cake are untouched.
    const technicalRead = await db.get(
      Route.fromFlat(
        `${cakeKey}@${writtenCakeRef}/carTechnicalLayer/carTechnical/engine`,
      ),
      {},
      undefined,
      ['VIN1'],
    );
    expect(technicalRead.cell.length).toBe(1);

    // Dedup: re-publishing the identical put does not create a second
    // component row.
    const join3 = (await seedJoin(data.sliceId)).putComponent(data);
    await publishJoin(join3);
    const dump = await db.core.dumpTable(componentsTable);
    const matches = (dump[componentsTable]._data as any[]).filter(
      (c) => c.brand === data.component.brand && c.type === data.component.type,
    );
    expect(matches.length).toBe(1);
  });

  it('UPDATE: an existing sliceId is overwritten and stays readable, slice count unchanged', async () => {
    const data: PutComponent = {
      ...examplePutComponent(),
      sliceId: 'VIN1', // already present (via the layer's base chain)
      component: {
        ...examplePutComponent().component,
        brand: 'UpdatedBrand',
      },
    };

    const join = (await seedJoin(data.sliceId)).putComponent(data);
    const writtenCakeRef = await publishJoin(join);

    // VIN1 now reads the updated component.
    const read = await db.get(
      Route.fromFlat(
        `${cakeKey}@${writtenCakeRef}/${layerKey}/${componentsTable}/brand`,
      ),
      {},
      undefined,
      ['VIN1'],
    );
    expect(read.cell.length).toBe(1);
    expect(read.cell[0].value).toBe('UpdatedBrand');

    // Updating an existing doc does not grow the slice set (resolver
    // de-duplicates the redundant chain link).
    const join2 = await db.join(brandSelection, cakeKey, writtenCakeRef);
    expect(join2.rowIndices.length).toBe(10);
    expect(join2.row('VIN1')[0].value.cell[0].value).toBe('UpdatedBrand');
  });

  describe('throws', () => {
    it('when the Join has no column at all', () => {
      const join = Join.empty();
      expect(() => join.putComponent(examplePutComponent())).toThrow(
        `Join: Error while applying PutComponent: ` +
          `No column found referencing layer "carGeneralLayer". ` +
          `putComponent requires a Join that already touched the target ` +
          `layer.`,
      );
    });

    it('when no column references the target layer', () => {
      // Two columns that each fail a different half of the lookup's
      // guard: one route too short to even have a layer segment, one
      // route pointing at a DIFFERENT layer.
      const rows: JoinRows = {
        slice1: [
          {
            route: Route.fromFlat(cakeKey),
            value: {} as Container,
            inserts: null,
          },
          {
            route: Route.fromFlat(`${cakeKey}/carTechnicalLayer`),
            value: {} as Container,
            inserts: null,
          },
        ] as JoinColumn[],
      };
      const columnSelection = new ColumnSelection([
        {
          key: 'a',
          type: 'jsonValue' as const,
          alias: 'a',
          route: cakeKey,
          titleShort: 'a',
          titleLong: 'a',
        },
        {
          key: 'b',
          type: 'jsonValue' as const,
          alias: 'b',
          route: `${cakeKey}/carTechnicalLayer`,
          titleShort: 'b',
          titleLong: 'b',
        },
      ]);
      const join = new Join(rows, columnSelection);

      expect(() => join.putComponent(examplePutComponent())).toThrow(
        /No column found referencing layer "carGeneralLayer"/,
      );
    });
  });

  it('examplePutComponent() returns a well-formed PutComponent', () => {
    const example = examplePutComponent();
    expect(example.layer).toBe('carGeneralLayer');
    expect(example.sliceId).toBe('VIN99');
    expect(example.component).toBeDefined();
    expect(example.component.brand).toBeDefined();
    expect(example._hash).toBe('');
  });
});
