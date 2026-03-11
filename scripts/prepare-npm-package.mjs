#!/usr/bin/env node

import { chmod, copyFile, mkdir, rm } from "node:fs/promises";
import { join } from "node:path";

const packageDir = process.argv[2];
const sourceBinary = process.argv[3];

if (!packageDir || !sourceBinary) {
  console.error(
    "usage: node scripts/prepare-npm-package.mjs <package-dir> <source-binary>",
  );
  process.exit(1);
}

const isWindows = packageDir.includes("win32");
const targetBinary = join(
  packageDir,
  "bin",
  isWindows ? "db-ferry.exe" : "db-ferry",
);

await mkdir(join(packageDir, "bin"), { recursive: true });
await rm(targetBinary, { force: true });
await copyFile(sourceBinary, targetBinary);

if (!isWindows) {
  await chmod(targetBinary, 0o755);
}
