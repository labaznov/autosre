---
name: errors-versus-load
title: Ошибки против нагрузки
horizon: 15m
when:
  signal: errors
  stream: '.*container="(orders-api|billing-api|litellm).*'
collect:
  - id: signatures
    vl: '_time:[{start}, {end}) {stream} (i(error*) OR i(timeout*)) | fields _msg'
    limit: 100
  - id: rps
    vm: 'sum(rate(http_requests_total{job="{service}"}[{horizon}]))'
  - id: latency
    vm: 'histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{job="{service}"}[{horizon}])))'
tools: [logs, metrics, knowledge]
---

Сопоставь рост ошибок с нагрузкой на сервис.

Ошибки выросли, запросы не выросли — проблема внутри сервиса или в том, от чего
он зависит. Смотри на латентность: если она тоже растёт, сервис ждёт кого-то
внизу.

Выросло и то и другое примерно одинаково — сервис захлебнулся нагрузкой, ищи
источник запросов выше по цепочке. Доля ошибок при этом важнее их числа.

Ошибки выросли, а запросов стало **меньше** — самый скверный случай: до сервиса
перестают доходить, а то, что доходит, падает. Ставь высокую важность.
