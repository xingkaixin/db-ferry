#!/usr/bin/env node

const { spawn } = require("node:child_process");
const { existsSync } = require("node:fs");
const { dirname, join } = require("node:path");

const { getBinarySpec } = require("../lib/platform");

function fail(message) {
  console.error(message);
  process.exit(1);
}

function resolveBinary() {
  const spec = getBinarySpec(process.platform, process.arch);
  if (!spec.supported) {
    fail(spec.message);
  }

  let packageJsonPath;
  try {
    packageJsonPath = require.resolve(`${spec.packageName}/package.json`);
  } catch (error) {
    fail(
      `当前平台缺少 db-ferry 预编译二进制 (${process.platform}/${process.arch})。` +
        `请重新安装 package，或改用源码构建。原始错误: ${error.message}`,
    );
  }

  const binaryPath = join(dirname(packageJsonPath), spec.binaryRelativePath);
  if (!existsSync(binaryPath)) {
    fail(`未找到平台二进制: ${binaryPath}`);
  }

  return binaryPath;
}

const child = spawn(resolveBinary(), process.argv.slice(2), {
  stdio: "inherit",
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 1);
});

child.on("error", (error) => {
  fail(`启动 db-ferry 失败: ${error.message}`);
});
