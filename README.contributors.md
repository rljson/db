# Contributors

## Important

⚠️ IMPORTANT: On `Windows, please checkout the repo on drive C`.
There is a bug in the Vscode vitest extension v 1.14.4, making debugging tests
not work. <https://github.com/vitest-dev/vscode/issues/548>
Please check from time to time, if the issue is fixed and remove this hint.

## Report issues

Visit <https://github.com/rljson/validate/issues>

Check if there is already an issue for your problem

If no, report the issue

## Check out

```bash
mkdir rljson
cd rljson
git clone https://github.com/rljson/validate.git
cd validate
```

## Install dependencies

```bash
npm install
```

## Run the tests

```bash
npm run test
```

## Build the package

```bash
npm run build
```

## Publish the package

Open `package.json`.

Increase `version`.

Compile typescript:

```bash
npm run build
```

Make publish dry-run

```bash
npm publish --dry-run
```

## Architecture

Reade [README.architecture.md](./README.architecture.md) to get an overview
of the package's architecture.

## Install Vscode extensions

Open this project in `vscode`.

Press `Cmd+Shift+P`.

Type `Extensions: Show Recommended Extensions` and press `Enter`.

The recommended extensions will be shown.

Make sure, all recommended extensions are shown.

## Uninstall all test extensions, e.g. Jest or Jasmine

Jest or Jasmine extensions conflict with the `Vitest` extension used for this
project.

Uninstall them, if you have installed them.

## Debug tests

In Vscode: At the `left side bar` click on the `Test tube` icon to open the `Test explorer`.

At the `top`, click on the `refresh` icon to show update the tests.

Open a test file (`*.spec.ts`)

Set a breakpoint.

Press `alt` and click on the play button left beside the test.

Execution should stop at the breakpoint.

## Update goldens

In various tests we are creating golden files, that are reference files that
are compared against the files created in the tests.

If change is detected, the fest fail.

An example is `test/goldens/README.md` which is compared against
`dist/README.md`.

If the changes are desired, update the golden files:

Open `update-goldens.ts`

Set `export const updateGoldens` to `true`

Run tests again.

Set `export const updateGoldens` to `false`.

Run tests again.
