/*
 * @license
 * Copyright (c) 2025 Rljson
 *
 * Use of this source code is governed by terms that can be
 * found in the LICENSE file in the root of this package.
 */

import { isCleanRepo } from './functions/is-clean-repo.js';

console.log(isCleanRepo() ? '✅ clean' : '❌ dirty');
