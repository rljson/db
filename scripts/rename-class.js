#!/bin/node

import fs from 'fs';
import path from 'path';

// .............................................................................
const fileNameRexp = /(\.ts|\.md|\.log|\.js|\.json)$/;
const excludedFiles = ['create-new-repo.md', 'rename-class.js'];

// .............................................................................

const isExcluded = (fileName) => {
  return excludedFiles.includes(fileName);
};

const shouldProcessFile = (file) =>
  fileNameRexp.test(file) && !isExcluded(file);

if (process.argv.length < 4) {
  console.error(`Usage: ${path.basename(process.argv[1])} <CLASS_A> <CLASS_B>`);
  process.exit(1);
}

const classA = process.argv[2];
const classB = process.argv[3];

const toSnakeCase = (str) => {
  return str.replace(/(?<=[a-z0-9])([A-Z])/g, '-$1').toLowerCase();
};

const toLowerCamelCase = (str) => {
  return str.replace(/([-_]\w)/g, (matches) => matches[1].toUpperCase());
};

const toUpperCamelCase = (str) => {
  return str.replace(/(?:^|[-_])(\w)/g, (_, c) => (c ? c.toUpperCase() : ''));
};

const classALower = toLowerCamelCase(classA);
const classBLower = toLowerCamelCase(classB);
const classAUpper = toUpperCamelCase(classA);
const classBUpper = toUpperCamelCase(classB);
const classASnake = toSnakeCase(classA);
const classBSnake = toSnakeCase(classB);

const replaceIncludesFirst = (directory) => {
  const files = fs.readdirSync(directory, { withFileTypes: true });

  for (const file of files) {
    const fullPath = path.join(directory, file.name);

    if (file.isDirectory()) {
      if (file.name.startsWith('.') || file.name === 'node_modules') continue;
      replaceIncludesFirst(fullPath);
    } else if (shouldProcessFile(file.name)) {
      let content = fs.readFileSync(fullPath, 'utf8');
      content = content.replace(
        new RegExp(`(\\/)${classASnake}(\\.ts)?([\\.\\/\\'\\\"\\\\])`, 'g'),
        `$1${classBSnake}$2$3`,
      );
      fs.writeFileSync(fullPath, content, 'utf8');
    }
  }
};

const replaceInFiles = (directory) => {
  const files = fs.readdirSync(directory, { withFileTypes: true });

  for (const file of files) {
    const fullPath = path.join(directory, file.name);

    if (file.isDirectory()) {
      if (file.name.startsWith('.') || file.name === 'node_modules') continue;
      replaceInFiles(fullPath);
    } else if (shouldProcessFile(file.name)) {
      let content = fs.readFileSync(fullPath, 'utf8');
      content = content
        .replace(new RegExp(classAUpper, 'g'), classBUpper)
        .replace(new RegExp(classALower, 'g'), classBLower)
        .replace(new RegExp(classASnake, 'g'), classBSnake);
      fs.writeFileSync(fullPath, content, 'utf8');
    }
  }
};

const renameFiles = (directory) => {
  const files = fs.readdirSync(directory, { withFileTypes: true });

  for (const file of files) {
    const fullPath = path.join(directory, file.name);

    if (file.isDirectory()) {
      if (file.name.startsWith('.') || file.name === 'node_modules') continue;
      renameFiles(fullPath);
    } else if (file.name.includes(classASnake)) {
      const newFileName = file.name.replace(classASnake, classBSnake);
      fs.renameSync(fullPath, path.join(directory, newFileName));
    }
  }
};

console.log('Replacing includes first...');
replaceIncludesFirst('.');

console.log('Renaming files...');
renameFiles('.');

console.log('Replacing occurrences in files...');
replaceInFiles('.');

console.log('Done.');
