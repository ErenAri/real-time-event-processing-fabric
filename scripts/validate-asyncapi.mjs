import process from 'node:process';
import path from 'node:path';

import { Parser, fromFile } from '@asyncapi/parser';

const asyncapiPath = path.resolve(process.cwd(), process.argv[2] ?? 'asyncapi.yaml');
const parser = new Parser();

const result = await fromFile(parser, asyncapiPath);
const diagnostics = result.diagnostics ?? [];

if (diagnostics.length > 0) {
  for (const diagnostic of diagnostics) {
    const severity = diagnostic.severity ?? 0;
    const location = diagnostic.range?.start
      ? `${diagnostic.range.start.line + 1}:${diagnostic.range.start.character + 1}`
      : 'unknown';
    const rule = diagnostic.code ? ` [${diagnostic.code}]` : '';
    console.error(`${severityToLabel(severity)}${rule} ${location} ${diagnostic.message}`);
  }
  process.exit(1);
}

console.log(`AsyncAPI contract is valid: ${path.relative(process.cwd(), asyncapiPath)}`);

function severityToLabel(severity) {
  switch (severity) {
    case 0:
      return 'error';
    case 1:
      return 'warn';
    case 2:
      return 'info';
    case 3:
      return 'hint';
    default:
      return 'diag';
  }
}
