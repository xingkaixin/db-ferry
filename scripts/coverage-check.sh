#!/usr/bin/env bash
set -euo pipefail

global_threshold="${GLOBAL_COVERAGE_THRESHOLD:-75}"
package_threshold="${PACKAGE_COVERAGE_THRESHOLD:-65}"
go_bin="${GO_BIN:-go}"

mkdir -p .cache/coverage
total_profile=".cache/coverage/total.out"
rm -f "$total_profile"

compare_lt() {
  awk -v lhs="$1" -v rhs="$2" 'BEGIN { exit !(lhs < rhs) }'
}

extract_total_pct() {
  local profile="$1"
  "$go_bin" tool cover -func="$profile" | awk '/^total:/{gsub("%","",$3); print $3}'
}

echo "[coverage] running all packages..."
"$go_bin" test -covermode=atomic -coverprofile="$total_profile" ./...
global_pct="$(extract_total_pct "$total_profile")"
echo "[coverage] global: ${global_pct}% (threshold: ${global_threshold}%)"
if compare_lt "$global_pct" "$global_threshold"; then
  echo "[coverage] global coverage below threshold"
  exit 1
fi

failed_packages=()
while IFS= read -r pkg; do
  pkg_name="${pkg##*/}"
  profile=".cache/coverage/${pkg//\//_}.out"
  rm -f "$profile"

  output="$("$go_bin" test -covermode=atomic -coverprofile="$profile" "$pkg" 2>&1)" || {
    echo "$output"
    echo "[coverage] package test failed: $pkg"
    exit 1
  }
  echo "$output"

  if [[ "$output" == *"[no test files]"* ]]; then
    failed_packages+=("$pkg (no test files)")
    continue
  fi

  if [[ ! -f "$profile" ]]; then
    failed_packages+=("$pkg (missing coverage profile)")
    continue
  fi

  pkg_pct="$(extract_total_pct "$profile")"
  echo "[coverage] package ${pkg}: ${pkg_pct}% (threshold: ${package_threshold}%)"
  if compare_lt "$pkg_pct" "$package_threshold"; then
    failed_packages+=("$pkg (${pkg_pct}%)")
  fi
done < <("$go_bin" list ./...)

if ((${#failed_packages[@]} > 0)); then
  echo "[coverage] package coverage check failed:"
  for item in "${failed_packages[@]}"; do
    echo "  - $item"
  done
  exit 1
fi

echo "[coverage] all checks passed."
