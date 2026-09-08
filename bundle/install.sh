#!/bin/sh
# Установка и обновление агента из бандла (docs/adr/0028-bundle-and-systemd.md).
#
# Запускать из распакованного каталога бандла от root. Повторный запуск —
# это обновление: бинарь и юнит подменяются, настройки, секреты, база и
# репозиторий знаний остаются на месте. Прежний бинарь остаётся рядом как
# autosre.prev — откат это его возврат на место и перезапуск.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
BIN=/usr/local/bin/autosre
ETC=/etc/autosre
DATA=/opt/data/autosre
UNIT=/etc/systemd/system/autosre.service

say() { printf '%s\n' "$*"; }
die() { say "ошибка: $*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "нужен root: sudo ./install.sh"
[ -x "$HERE/autosre" ] || die "в каталоге нет бинаря autosre"
command -v systemctl >/dev/null 2>&1 || die "нет systemctl: бандл рассчитан на systemd"

# Пользователь без входа и без домашнего каталога: агенту нужен только
# каталог данных.
if ! id autosre >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /usr/sbin/nologin autosre 2>/dev/null \
        || useradd --system --no-create-home --shell /sbin/nologin autosre
    say "заведён пользователь autosre"
fi

install -d -m 0750 -o root -g autosre "$ETC"
install -d -m 0750 -o autosre -g autosre "$DATA"

# Бинарь: прежний остаётся для отката.
if [ -x "$BIN" ]; then
    cp -p "$BIN" "$BIN.prev"
fi
install -m 0755 -o root -g root "$HERE/autosre" "$BIN"

# Настройки и секреты — только если их ещё нет: правки оператора важнее образца.
if [ ! -f "$ETC/autosre.toml" ]; then
    install -m 0640 -o root -g autosre "$HERE/autosre.toml" "$ETC/autosre.toml"
    say "положен образец настроек: $ETC/autosre.toml"
fi
if [ ! -f "$ETC/autosre.env" ]; then
    install -m 0640 -o root -g autosre "$HERE/autosre.env" "$ETC/autosre.env"
    say "положен образец секретов: $ETC/autosre.env"
fi

# Стартовая база знаний — только на пустое место: на сервере это рабочая
# копия, и затирать её образцом нельзя.
if [ ! -d "$DATA/knowledge" ]; then
    cp -R "$HERE/knowledge" "$DATA/knowledge"
    mkdir -p "$DATA/knowledge/drafts" "$DATA/knowledge/reports"
    chown -R autosre:autosre "$DATA/knowledge"
    if command -v git >/dev/null 2>&1; then
        (cd "$DATA/knowledge" && [ -d .git ] || \
            su -s /bin/sh autosre -c "cd '$DATA/knowledge' && git init -q && git add -A && \
                git -c user.name=autosre -c user.email=autosre@localhost commit -qm 'стартовая база знаний'")
    fi
    say "разложена стартовая база знаний: $DATA/knowledge"
fi

install -m 0644 -o root -g root "$HERE/autosre.service" "$UNIT"
systemctl daemon-reload
systemctl enable autosre >/dev/null 2>&1 || true

if systemctl is-active --quiet autosre; then
    systemctl restart autosre
    say "агент обновлён и перезапущен: $("$BIN" --version)"
else
    say "агент установлен: $("$BIN" --version)"
    say ""
    say "дальше:"
    say "  1. впишите ключи в $ETC/autosre.env"
    say "  2. поправьте адреса и учётные записи в $ETC/autosre.toml"
    say "     хеш пароля: autosre hash 'пароль'"
    say "  3. проверьте: autosre check $ETC/autosre.toml"
    say "  4. запустите: systemctl start autosre"
    say "  5. смотрите: journalctl -u autosre -f"
fi
