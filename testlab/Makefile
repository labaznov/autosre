.DEFAULT_GOAL := help
COMPOSE := docker compose

help:
	@echo "up      поднять лабораторию: логи, метрики, генератор"
	@echo "agent   поднять агента поверх лаборатории"
	@echo "seed    засеять историю заново (генератор перезапускается)"
	@echo "watch   смотреть, что пишет генератор"
	@echo "errors  сколько ошибок по сервисам за последний час"
	@echo "buckets что агент успел снять"
	@echo "down    остановить"
	@echo "clean   остановить и стереть данные"

up:
	$(COMPOSE) up -d --build victorialogs victoriametrics generator
	@echo "логи   http://127.0.0.1:9428/select/vmui"
	@echo "метрики http://127.0.0.1:8428/vmui"

agent:
	$(COMPOSE) --profile agent up -d --build agent
	@echo "агент  http://127.0.0.1:8096/api/health"

seed:
	$(COMPOSE) restart generator

watch:
	$(COMPOSE) logs -f generator

errors:
	@curl -s 'http://127.0.0.1:9428/select/logsql/query' \
		--data-urlencode 'query=_time:1h (i(error*) OR i(exception*)) | stats by (service) count() as total | sort by (total desc)'

buckets:
	@$(COMPOSE) --profile agent exec agent sh -c \
		'wget -qO- http://127.0.0.1:8096/metrics | grep -E "sre_last_bucket|sre_source_failures"'

down:
	$(COMPOSE) --profile agent down

clean:
	$(COMPOSE) --profile agent down -v

.PHONY: help up agent seed watch errors buckets down clean
