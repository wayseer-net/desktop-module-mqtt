#!/usr/bin/env bash
# Builds the module's program for GOOS/GOARCH and signs it into a package with `wayseer dev sign`,
# using the key and certificate in the files WAYSEER_DEV_KEY and WAYSEER_DEV_CERT name.
#
#	scripts/sign.sh <wayseer> <dist> <program>
set -euo pipefail
cd "$(dirname "$0")/.."

wayseer=$1 dist=$2 program=$3
goos=$(go env GOOS) goarch=$(go env GOARCH)
exe="$dist/$program-$goos-$goarch$(go env GOEXE)"
mkdir -p "$dist"
go build -trimpath -o "$exe" "./cmd/$program"
"$wayseer" dev sign --os "$goos" --arch "$goarch" "$exe" manifest.yaml "$dist"
