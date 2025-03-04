import { Foo } from './foo.ts';


/**
 * The example function demonstrates how the package works
 */
export const example = () => {
  const print = console.log;
  const assert = console.assert;

  const db = new Foo();
  print(db.foo());
  assert(db.foo() === 'bar');
};

export class X {}
