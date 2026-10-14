#!/usr/bin/env bash
# =============================================================================
# reset.sh - Clean up all build artifacts, temporary caches, and reset state
# =============================================================================

set -euo pipefail

# Navigate to repository root
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

echo "==> Cleaning mini_reactor workspace: ${REPO_ROOT}"

BUILD_DIR="${BUILD_DIR:-build}"

# 1. Remove build directory
if [[ -d "${BUILD_DIR}" ]]; then
  echo "  - Removing build directory: ${BUILD_DIR}/"
  rm -rf "${BUILD_DIR}"
fi

# 2. Remove orphaned compiler artifacts & object files
echo "  - Cleaning orphaned binaries, object files, and static libraries..."
find . -maxdepth 3 \( -name "*.o" -o -name "*.a" -o -name "*.dylib" -o -name "*.so" \) ! -path "./.git/*" -exec rm -f {} +

# 3. Remove macOS dSYM debug symbol bundles
find . -maxdepth 3 -name "*.dSYM" -type d ! -path "./.git/*" -exec rm -rf {} + 2>/dev/null || true

# 4. Remove compile_commands.json and editor caches
if [[ -f "compile_commands.json" ]]; then
  echo "  - Removing root compile_commands.json..."
  rm -f compile_commands.json
fi

if [[ -d ".cache" ]]; then
  echo "  - Removing .cache/ directory..."
  rm -rf .cache
fi

# 5. Remove OS and editor temporary files
find . -maxdepth 3 \( -name ".DS_Store" -o -name "*~" -o -name "*.swp" \) ! -path "./.git/*" -exec rm -f {} + 2>/dev/null || true

echo "==> Reset complete. Workspace is clean."

# Optional rebuild if requested via argument
if [[ "${1:-}" == "--build" || "${1:-}" == "-b" || "${1:-}" == "--rebuild" ]]; then
  echo "==> Rebuilding fresh from scratch..."
  cmake -B "${BUILD_DIR}" -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
  cmake --build "${BUILD_DIR}" -j
  if [[ -f "${BUILD_DIR}/compile_commands.json" ]]; then
    ln -sf "${BUILD_DIR}/compile_commands.json" compile_commands.json
  fi
  echo "==> Fresh build successful."
fi

if [[ "${1:-}" == "--test" || "${1:-}" == "-t" ]]; then
  echo "==> Running test suite..."
  cmake -B "${BUILD_DIR}"
  cmake --build "${BUILD_DIR}" -j
  ctest --test-dir "${BUILD_DIR}" --output-on-failure
fi
