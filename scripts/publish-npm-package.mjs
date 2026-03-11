#!/usr/bin/env node

import { execFile, spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { promisify } from "node:util";
import { join } from "node:path";

const execFileAsync = promisify(execFile);
const packageDir = process.argv[2];

if (!packageDir) {
  console.error("usage: node scripts/publish-npm-package.mjs <package-dir>");
  process.exit(1);
}

const manifestPath = join(packageDir, "package.json");
const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
const spec = `${manifest.name}@${manifest.version}`;

try {
  await execFileAsync("npm", ["view", spec, "version"], { stdio: "ignore" });
  console.log(`[publish] skip existing package ${spec}`);
} catch {
  console.log(`[publish] publishing ${spec}`);
  const args = ["publish"];
  if (packageDir !== ".") {
    args.push(packageDir);
  }
  args.push("--access", "public");

  const child = spawn("npm", args, { stdio: "inherit" });
  const exitCode = await new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("exit", resolve);
  });

  if (exitCode !== 0) {
    process.exit(exitCode ?? 1);
  }
}
