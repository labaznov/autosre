---
name: slow-degradation
title: Ползучая деградация ресурсов
horizon: 1h
when:
  signal: metric
  metric: '(process_resident_memory_bytes|node_filesystem_avail_bytes|go_goroutines)'
collect:
  - id: trend
    vm: '{metric}{instance=~"{service}.*"}'
  - id: sutki
    vm: 'avg_over_time({metric}{instance=~"{service}.*"}[24h])'
  - id: errors
    vl: '_time:[{start}, {end}) {stream} (i(error*) OR i(warn*)) | fields _msg'
    limit: 50
tools: [metrics, logs, knowledge]
---

Здесь важна не величина, а направление и скорость.

Прикинь, когда показатель дойдёт до предела при нынешней скорости: если счёт
идёт на часы — это уже инцидент, если на недели — заметка на будущее, а не
повод будить дежурного.

Отличай рост от пилы. Память, которая растёт и падает при перезапуске, — утечка.
Память, которая держится на новой полке после выката, — просто новый аппетит
сервиса, и это не поломка.

Свободное место на диске сравнивай не с процентами, а с оставшимся временем:
пять процентов на терабайте и пять процентов на десяти гигабайтах — разные
новости.
