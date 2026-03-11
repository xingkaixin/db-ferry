"use strict";

const PLATFORM_SPECS = {
  "darwin-arm64": {
    supported: true,
    packageName: "db-ferry-darwin-arm64",
    binaryRelativePath: "bin/db-ferry",
  },
  "darwin-x64": {
    supported: true,
    packageName: "db-ferry-darwin-x64",
    binaryRelativePath: "bin/db-ferry",
  },
  "linux-arm64": {
    supported: true,
    packageName: "db-ferry-linux-arm64",
    binaryRelativePath: "bin/db-ferry",
  },
  "linux-x64": {
    supported: true,
    packageName: "db-ferry-linux-x64",
    binaryRelativePath: "bin/db-ferry",
  },
  "win32-x64": {
    supported: true,
    packageName: "db-ferry-windows-x64",
    binaryRelativePath: "bin/db-ferry.exe",
  },
  "win32-arm64": {
    supported: false,
    message:
      "暂不提供 Windows arm64 的 db-ferry npm 二进制包。当前上游 DuckDB bindings 仅覆盖 windows-amd64。",
  },
};

function getBinarySpec(platform, arch) {
  return (
    PLATFORM_SPECS[`${platform}-${arch}`] || {
      supported: false,
      message: `暂不支持的平台: ${platform}/${arch}`,
    }
  );
}

module.exports = {
  getBinarySpec,
};
