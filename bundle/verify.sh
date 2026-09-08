#!/bin/bash
# Полный прогон бандла под настоящим systemd: установка, check, служба,
# HTTPS, вход, живые данные, падение с перезапуском, копия, обновление,
# откат, остановка. Запускать в чистой виртуалке от root, рядом должен лежать
# тарбол /tmp/autosre-<версия>-linux-x86_64.tar.gz, а на хосте — лаборатория
# из testlab (виртуалка ходит в неё по host.lima.internal).
#
#   limactl create --name autosre --vm-type vz --rosetta template://debian-13
#   limactl cp dist/autosre-0.2.0-linux-x86_64.tar.gz autosre:/tmp/
#   limactl cp bundle/verify.sh autosre:/tmp/
#   limactl shell autosre -- sudo bash /tmp/verify.sh
#
# Виртуалка нужна ради systemd: контейнер с заглушкой systemctl проверяет
# скрипт установки, но не юнит, не перезапуск после паники и не SIGTERM.
set -u
VERSION=${1:-0.2.0}
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ✓ $*"; }
bad()  { FAIL=$((FAIL+1)); echo "  ✗ $*"; }
check(){ if eval "$2"; then ok "$1"; else bad "$1"; fi; }
HOST=host.lima.internal
SINCE=$(date +%s)
J="journalctl -u autosre --no-pager --since=@$SINCE"
# Чистый лист: прогон повторяемый, сертификат делается заново.
systemctl stop autosre 2>/dev/null; rm -f /usr/local/bin/autosre.prev; rm -rf /opt/data/autosre/tls

echo "== 1. установка"
cd /tmp && rm -rf "autosre-$VERSION-linux-x86_64" && tar xzf "/tmp/autosre-$VERSION-linux-x86_64.tar.gz" && cd "autosre-$VERSION-linux-x86_64"
./install.sh > /tmp/install-1.log 2>&1; check "install.sh отработал" "[ \$? -eq 0 ]"
check "пользователь autosre заведён" "id autosre >/dev/null 2>&1"
check "юнит включён" "systemctl is-enabled autosre >/dev/null 2>&1"
check "бинарь x86-64 запускается" "/usr/local/bin/autosre --version | grep -q \"autosre $VERSION\""

echo "== 2. настройки под лабораторию на хосте"
sed -i "s#http://192.0.2.12:9428#http://$HOST:9428#; s#http://192.0.2.11:8428#http://$HOST:8428#; s#http://192.0.2.10:4000#http://$HOST:4000#" /etc/autosre/autosre.toml
sed -i "s#^AUTOSRE_MODEL_KEY=.*#AUTOSRE_MODEL_KEY=sk-lab#; s#^AUTOSRE_SESSION_KEY=.*#AUTOSRE_SESSION_KEY=vm-session-key-9f3a#" /etc/autosre/autosre.env
sudo -u autosre /usr/local/bin/autosre check /etc/autosre/autosre.toml > /tmp/check.log 2>&1; RC=$?
cat /tmp/check.log | sed 's/^/     /'
check "check зелёный целиком" "[ $RC -eq 0 ]"

echo "== 3. служба"
systemctl start autosre; sleep 6
check "служба активна" "systemctl is-active --quiet autosre"
check "health по HTTPS отвечает 200" "curl -sk -o /dev/null -w '%{http_code}' https://127.0.0.1:8096/api/health | grep -q 200"
check "сертификат сделан в каталоге данных" "[ -f /opt/data/autosre/tls/cert.pem ] && [ -f /opt/data/autosre/tls/key.pem ]"
check "ключ закрыт от чужих" "[ \$(stat -c %a /opt/data/autosre/tls/key.pem) = 600 ]"
check "в журнале предупреждение о самоподписанном" "$J | grep -q 'самоподписанный'"
check "база в каталоге данных" "[ -f /opt/data/autosre/autosre.db ]"
check "по голому HTTP не отвечает" "! curl -s -m 3 -o /dev/null http://127.0.0.1:8096/api/health"
check "процесс под пользователем autosre" "[ \$(ps -o user= -p \$(systemctl show -p MainPID --value autosre)) = autosre ]"

echo "== 4. вход и кука"
COOKIE=$(curl -sk -o /dev/null -c - -d 'login=duty&password=autosre-lab' https://127.0.0.1:8096/login | grep autosre | awk '{print $7}')
check "вход дежурного удался" "[ -n \"$COOKIE\" ]"
check "кука помечена Secure" "curl -sk -i -d 'login=duty&password=autosre-lab' https://127.0.0.1:8096/login | grep -i '^set-cookie' | grep -q Secure"
check "лента открывается" "curl -sk -b autosre=$COOKIE -o /dev/null -w '%{http_code}' https://127.0.0.1:8096/ | grep -q 200"

echo "== 5. работа на живых данных лаборатории (ждём 4 минуты)"
sleep 240
check "бакеты снимаются: last_bucket свежий" "curl -sk https://127.0.0.1:8096/api/health | grep -q '\"status\":\"ok\"'"
curl -sk https://127.0.0.1:8096/metrics | grep -E '^autosre_(deviations|incidents|source_failures)_total' | sed 's/^/     /'
check "отказов источников нет" "curl -sk https://127.0.0.1:8096/metrics | grep -q '^autosre_source_failures_total 0'"

echo "== 6. падение и перезапуск"
PID=$(systemctl show -p MainPID --value autosre)
kill -ABRT "$PID"; sleep 9
check "systemd поднял службу после аварийного выхода" "systemctl is-active --quiet autosre"
check "счётчик перезапусков вырос" "[ \$(systemctl show -p NRestarts --value autosre) -ge 1 ]"
check "health снова 200" "curl -sk -o /dev/null -w '%{http_code}' https://127.0.0.1:8096/api/health | grep -q 200"
check "сертификат не пересоздан" "$J | grep -c 'самоподписанный' | grep -q '^1$'"

echo "== 7. копия базы"
sudo -u autosre /usr/local/bin/autosre backup /tmp/copy.db > /tmp/backup.log 2>&1
check "копия снята на живом агенте" "[ -s /tmp/copy.db ]"

echo "== 8. обновление и откат"
cd "/tmp/autosre-$VERSION-linux-x86_64" && ./install.sh > /tmp/install-2.log 2>&1
check "повторный install.sh перезапустил службу" "grep -q 'обновлён и перезапущен' /tmp/install-2.log"
check "прежний бинарь оставлен" "[ -x /usr/local/bin/autosre.prev ]"
check "настройки не затёрты" "grep -q $HOST /etc/autosre/autosre.toml"
check "секреты не затёрты" "grep -q sk-lab /etc/autosre/autosre.env"
sleep 5
check "служба активна после обновления" "systemctl is-active --quiet autosre"
mv /usr/local/bin/autosre.prev /usr/local/bin/autosre && systemctl restart autosre; sleep 5
check "откат: служба активна" "systemctl is-active --quiet autosre"
check "откат: health 200" "curl -sk -o /dev/null -w '%{http_code}' https://127.0.0.1:8096/api/health | grep -q 200"

echo "== 9. остановка"
systemctl stop autosre; sleep 1
check "остановилась по SIGTERM без таймаута" "$J | grep -q 'остановка по сигналу'"
check "статус после остановки чистый" "! systemctl is-failed --quiet autosre"

echo; echo "итого: прошло $PASS, не прошло $FAIL"
$J -p warning | tail -15 | sed 's/^/     /'
[ $FAIL -eq 0 ]
