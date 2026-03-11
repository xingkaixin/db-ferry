#!/usr/bin/env node

import { readFile, writeFile } from "node:fs/promises";

const version = process.argv[2];

if (!version) {
  console.error("usage: node scripts/set-npm-version.mjs <version>");
  process.exit(1);
}

const packageJsonFiles = [
  "package.json",
  "npm/db-ferry-darwin-arm64/package.json",
  "npm/db-ferry-darwin-x64/package.json",
  "npm/db-ferry-linux-arm64/package.json",
  "npm/db-ferry-linux-x64/package.json",
  "npm/db-ferry-win32-x64/package.json",
];

for (const file of packageJsonFiles) {
  const raw = await readFile(file, "utf8");
  const parsed = JSON.parse(raw);
  parsed.version = version;

  if (file === "package.json") {
    for (const name of Object.keys(parsed.optionalDependencies)) {
      parsed.optionalDependencies[name] = version;
    }
  }

  await writeFile(file, `${JSON.stringify(parsed, null, 2)}\n`);
}
