import { Foo } from './foo.ts';

/**
 * The example function demonstrates how the package works
 */
export const example = () => {
  const print = console.log;
  const assert = console.assert;

  const validate = new Foo();
  print(validate.foo());
  assert(validate.foo() === 'bar');
};

export class X {}
