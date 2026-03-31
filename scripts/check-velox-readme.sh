#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Updates the Velox compare link in README.md to match the submodule SHA.
# Run by pre-commit when either README.md or the velox submodule changes.
# If the link is already correct, does nothing. If it differs, updates the
# file in place and exits with 1 so pre-commit re-stages and retries.

set -euo pipefail

SUBMODULE_SHA=$(git ls-tree HEAD velox | awk '{print $3}')
README_SHA=$(grep -oE 'velox/compare/[0-9a-f]{40}' README.md | grep -oE '[0-9a-f]{40}' || true)

if [ -z "$README_SHA" ]; then
  echo "ERROR: No Velox compare link found in README.md"
  echo "Expected a link like: https://github.com/facebookincubator/velox/compare/<sha>...main"
  exit 1
fi

if [ "$SUBMODULE_SHA" != "$README_SHA" ]; then
  sed -i.bak "s|velox/compare/[0-9a-f]\{40\}|velox/compare/$SUBMODULE_SHA|" README.md
  rm -f README.md.bak
  echo "Updated Velox compare link in README.md: $README_SHA -> $SUBMODULE_SHA"
  exit 1
fi
