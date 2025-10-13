import { IoMem } from '@rljson/io';
import { exampleRljson, exampleTableCfgTable } from '@rljson/rljson';

import { Db } from './db.ts';

/**
 * The example function demonstrates how the package works
 * ⚠️ Run »pnpm updateGoldens« to update the golden files
 */
export const example = async () => {
  const l = console.log;

  l('Create an io instane');
  const io = await IoMem.example();
  await io.init();
  await io.isReady();

  l('Create an example instance');
  const db = new Db(io);

  l('Create the tables');
  const tableCfg = exampleTableCfgTable()._data[0];
  await db.core.createTable(tableCfg);

  l('Import Rljson');
  const rljson = exampleRljson();
  await db.core.import(rljson);

  l('Get the tables');
  const tables = await db.core.tables();
  l(Object.keys(tables).join(', '));

  l('Dump the database');
  const dump = await db.core.dump();
  l(JSON.stringify(dump, null, 2));

  l('Dump a single table');
  const tableDump = await db.core.dumpTable('table');
  l(JSON.stringify(tableDump, null, 2));
};
