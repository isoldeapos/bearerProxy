#!/usr/bin/env bash
# build-release.sh - compile per-platform executables with bun, named exactly
# the way --self-update expects to find them on a GitHub release.
#
# Usage:
#   ./build-release.sh            # builds every platform into dist/
#   ./build-release.sh darwin-arm64 windows-x64   # just these
#
# Release checklist:
#   1. Bump VERSION in hermes-bearer-proxy.js AND version.json (keep in sync)
#   2. ./build-release.sh
#   3. Commit + push (so version.json on main reflects the new version)
#   4. Create a GitHub release with tag v<version> (e.g. v1.3.0) and upload
#      everything in dist/ as release assets
# Running exes then see the new version.json on startup and can install it
# with --self-update.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

TARGETS=("$@")
if [[ ${#TARGETS[@]} -eq 0 ]]; then
  TARGETS=(windows-x64 darwin-arm64 darwin-x64 linux-x64 linux-arm64)
fi

command -v bun >/dev/null || { echo "error: bun not installed (https://bun.sh)" >&2; exit 1; }

VER=$(node -e "console.log(require('./version.json').version)" 2>/dev/null || bun -e "console.log((await Bun.file('version.json').json()).version)")
mkdir -p dist

for target in "${TARGETS[@]}"; do
  ext=""
  [[ "$target" == windows-* ]] && ext=".exe"
  out="dist/hermes-bearer-proxy-${target}${ext}"
  echo "building $out (v$VER)"
  bun build --compile --target="bun-${target}" ./hermes-bearer-proxy.js --outfile "$out"
done

echo "done - upload the files in dist/ to the v$VER GitHub release"
