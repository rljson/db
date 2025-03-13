import { IoMem } from '@rljson/io';
import { exampleRljson } from '@rljson/rljson';

import { Db } from './db.ts';

/**
 * The example function demonstrates how the package works
 * ⚠️ Run »pnpm updateGoldens« to update the golden files
 */
export const example = async () => {
  const l = console.log;

  l('# DB eample');

  l('## Create an io instane');
  const io = new IoMem();

  l('## Create an example instance');
  const db = new Db(io);

  l('## Create the tables');
  await db.core.createTable('table', 'properties');

  l('## Import Rljson');
  await db.core.import(exampleRljson());

  l('## Get the tables');
  const tables = await db.core.tables();
  l(tables.join(', '));

  l('## Dump the database');
  const dump = await db.core.dump();
  l(JSON.stringify(dump, null, 2));

  l('## Dump a single table');
  const tableDump = await db.core.dumpTable('table');
  l(JSON.stringify(tableDump, null, 2));
};
