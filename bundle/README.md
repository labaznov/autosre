# Установка autosre из бандла

Один статический бинарь под Linux x86-64, служба systemd, без Docker и без
сети. Подробности эксплуатации — `docs/OPERATIONS.md` в репозитории.

## Что нужно на сервере

- Linux x86-64 с systemd;
- доступ по сети до VictoriaLogs, VictoriaMetrics и модели за
  OpenAI-совместимым API;
- `git` — по желанию: с ним принятые черновики знаний попадают в коммиты.

## Установка

```sh
tar xzf autosre-<версия>-linux-x86_64.tar.gz
cd autosre-<версия>-linux-x86_64
sudo ./install.sh
```

Скрипт заводит пользователя `autosre`, кладёт бинарь в `/usr/local/bin`,
образцы настроек в `/etc/autosre`, стартовую базу знаний в
`/opt/data/autosre/knowledge` и юнит `autosre.service`.

Дальше руками:

1. Ключи — в `/etc/autosre/autosre.env`: ключ модели и ключ подписи сессий
   обязательны.
2. Адреса источников и модели, учётные записи — в `/etc/autosre/autosre.toml`.
   Хеш пароля считает сам агент: `autosre hash 'пароль'`.
3. Проверка до старта: `autosre check /etc/autosre/autosre.toml` — дойдёт до
   логов, метрик и модели и скажет, что не так.
4. `sudo systemctl start autosre`, журнал — `journalctl -u autosre -f`.

Веб-морда — `https://<сервер>:8096/`. Сертификат агент делает сам на первом
старте и кладёт в `/opt/data/autosre/tls/`; браузер предупредит, соединение
всё равно зашифровано. Свой сертификат и ключ в PEM можно положить по тем же
путям до первого старта или указать другие в разделе `[tls]` настроек.

## Обновление

Распаковать новый бандл и снова `sudo ./install.sh`: бинарь и юнит
подменяются, всё остальное остаётся. Прежний бинарь лежит рядом как
`/usr/local/bin/autosre.prev`.

Откат:

```sh
sudo mv /usr/local/bin/autosre.prev /usr/local/bin/autosre
sudo systemctl restart autosre
```

## Проверка целостности

```sh
sha256sum -c autosre-<версия>-linux-x86_64.sha256
```
