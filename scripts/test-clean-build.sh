#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
archive_root=$(mktemp -d "${TMPDIR:-/tmp}/crane-clean-build.XXXXXX")
trap 'rm -rf "$archive_root"' EXIT HUP INT TERM

git -C "$repository_root" archive --format=tar HEAD | tar -xf - -C "$archive_root"
GOTOOLCHAIN="${GOTOOLCHAIN:-go1.26.0}" make -C "$archive_root" build
test -x "$archive_root/bin/crane-node"
test -x "$archive_root/bin/crane-cluster"
