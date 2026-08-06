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

  it('sets the add entry so insert() returns exactly one {route, tree}', async () => {
    const data = examplePutComponent();
    expect(data.layer).toBe(layerKey);

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

    // Append-only: base points at the layer's CURRENT ref, other layers
    // of the cake are carried through untouched (still plain refs).
    expect(typeof layerRow.base).toBe('string');
    expect(layerRow.base.length).toBeGreaterThan(0);
    expect(typeof cakeRow.layers.carTechnicalLayer).toBe('string');
    expect(typeof cakeRow.layers.carColorLayer).toBe('string');
  });

  it('publishes through db.insert(): component deduped, add[sliceId] resolves, other slices preserved', async () => {
    const data = examplePutComponent();

    const join = (await seedJoin(data.sliceId)).putComponent(data);
    const [{ route, tree }] = join.insert();

    const results = await db.insert(route, tree);
    const writtenCakeRef = (results[0] as any)[`${cakeKey}Ref`] as string;
    expect(writtenCakeRef).toBeDefined();

    // New/overwritten slice reads back the just-put component.
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

    // A slice inherited through the layer's OWN base chain (VIN1 lives
    // in layer[0], layer[1]'s base) still resolves.
    const inheritedRead = await db.get(
      Route.fromFlat(
        `${cakeKey}@${writtenCakeRef}/${layerKey}/${componentsTable}/brand`,
      ),
      {},
      undefined,
      ['VIN1'],
    );
    expect(inheritedRead.cell.length).toBe(1);

    // A slice that was already directly on layer[1] (not via base) also
    // still resolves.
    const ownRead = await db.get(
      Route.fromFlat(
        `${cakeKey}@${writtenCakeRef}/${layerKey}/${componentsTable}/brand`,
      ),
      {},
      undefined,
      ['VIN9'],
    );
    expect(ownRead.cell.length).toBe(1);

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

    // Dedup: re-inserting the identical tree does not create a second
    // row in the components table.
    await db.insert(route, tree);
    const dump = await db.core.dumpTable(componentsTable);
    const matches = (dump[componentsTable]._data as any[]).filter(
      (c) => c.brand === data.component.brand && c.type === data.component.type,
    );
    expect(matches.length).toBe(1);
  });

  it('accepts a sliceId not previously present in the layer, in addition to existing ones', async () => {
    const data: PutComponent = {
      ...examplePutComponent(),
      sliceId: 'VIN9', // already on layer[1] directly (not via base)
    };

    const join = (await seedJoin(data.sliceId)).putComponent(data);
    const [{ route, tree }] = join.insert();
    const results = await db.insert(route, tree);
    const writtenCakeRef = (results[0] as any)[`${cakeKey}Ref`] as string;

    const read = await db.get(
      Route.fromFlat(
        `${cakeKey}@${writtenCakeRef}/${layerKey}/${componentsTable}/brand`,
      ),
      {},
      undefined,
      ['VIN10'], // sibling of VIN9 on layer[1], never touched
    );
    expect(read.cell.length).toBe(1);
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
    expect(example.sliceId).toBe('VIN1');
    expect(example.component).toBeDefined();
    expect(example.component.brand).toBeDefined();
    expect(example._hash).toBe('');
  });
});
