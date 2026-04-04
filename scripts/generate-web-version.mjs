import { execFileSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "..");
const outputPath = path.join(repoRoot, "web", "version.json");

const tags = execFileSync("git", ["tag", "--list", "v*"], {
  cwd: repoRoot,
  encoding: "utf8",
})
  .split("\n")
  .map((tag) => tag.trim())
  .filter(Boolean);

const parsedTags = tags
  .map(parseTag)
  .filter((tag) => tag !== null)
  .sort(compareTags);

if (parsedTags.length === 0) {
  throw new Error("no valid v<major>.<minor>.<patch> tags found");
}

const latestTag = parsedTags[parsedTags.length - 1].raw;

writeFileSync(outputPath, `${JSON.stringify({ tag: latestTag }, null, 2)}\n`);
console.log(`[web-version] wrote ${path.relative(repoRoot, outputPath)} with ${latestTag}`);

function parseTag(raw) {
  const match = /^v(\d+)\.(\d+)\.(\d+)$/.exec(raw);
  if (!match) {
    return null;
  }

  return {
    raw,
    major: Number.parseInt(match[1], 10),
    minor: Number.parseInt(match[2], 10),
    patch: Number.parseInt(match[3], 10),
  };
}

function compareTags(a, b) {
  if (a.major !== b.major) {
    return a.major - b.major;
  }
  if (a.minor !== b.minor) {
    return a.minor - b.minor;
  }
  return a.patch - b.patch;
}
