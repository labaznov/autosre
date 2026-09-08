#!/bin/sh
# Собирает тарбол бандла из готового бинаря (docs/adr/0028-bundle-and-systemd.md).
#
#   bundle/pack.sh <путь к бинарю> <версия> [каталог вывода]
#
# Версия обязана совпадать с версией в Cargo.toml: это проверяет CI, а здесь
# она нужна для имени. Пользуется только POSIX-утилитами, чтобы работать и в
# CI, и на ноутбуке.
set -eu

BINARY=${1:?путь к бинарю}
VERSION=${2:?версия}
OUT=${3:-dist}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
NAME="autosre-$VERSION-linux-x86_64"
STAGE=$(mktemp -d)

mkdir -p "$STAGE/$NAME" "$OUT"
cp "$BINARY" "$STAGE/$NAME/autosre"
chmod 0755 "$STAGE/$NAME/autosre"
cp "$ROOT/examples/autosre.toml" "$STAGE/$NAME/autosre.toml"
cp "$ROOT/bundle/autosre.env" "$STAGE/$NAME/autosre.env"
cp "$ROOT/bundle/autosre.service" "$STAGE/$NAME/autosre.service"
cp "$ROOT/bundle/install.sh" "$STAGE/$NAME/install.sh"
cp "$ROOT/bundle/README.md" "$STAGE/$NAME/README.md"
cp -R "$ROOT/examples/knowledge" "$STAGE/$NAME/knowledge"
find "$STAGE/$NAME/knowledge" -name .DS_Store -delete
mkdir -p "$STAGE/$NAME/knowledge/drafts" "$STAGE/$NAME/knowledge/reports/daily" \
    "$STAGE/$NAME/knowledge/reports/weekly" "$STAGE/$NAME/knowledge/reports/incidents"

tar -C "$STAGE" -czf "$OUT/$NAME.tar.gz" "$NAME"
(cd "$OUT" && { command -v sha256sum >/dev/null 2>&1 && sha256sum "$NAME.tar.gz" || shasum -a 256 "$NAME.tar.gz"; } > "$NAME.sha256")
rm -rf "$STAGE"
echo "$OUT/$NAME.tar.gz"
