.DEFAULT_GOAL := help
COMPOSE := docker compose

help:
	@echo "up      поднять лабораторию: логи, метрики, генератор, модель"
	@echo "agent   поднять агента поверх лаборатории"
	@echo "knowledge завести рабочую копию репозитория знаний"
	@echo "seed    засеять историю заново (генератор перезапускается)"
	@echo "watch   смотреть, что пишет генератор"
	@echo "errors  сколько ошибок по сервисам за последний час"
	@echo "buckets что агент успел снять"
	@echo "down    остановить"
	@echo "clean   остановить и стереть данные"

up:
	$(COMPOSE) up -d --build victorialogs victoriametrics generator model
	@echo "логи   http://127.0.0.1:9428/select/vmui"
	@echo "метрики http://127.0.0.1:8428/vmui"

agent: knowledge
	$(COMPOSE) --profile agent up -d --build agent
	@echo "агент  http://127.0.0.1:8096/api/health"

# Рабочая копия репозитория знаний: агент пишет в неё черновики и отчёты,
# а образец в examples остаётся нетронутым.
knowledge:
	@mkdir -p knowledge
	@cp -R ../examples/knowledge/. knowledge/
	@mkdir -p knowledge/drafts knowledge/reports
	@git -C knowledge rev-parse --git-dir >/dev/null 2>&1 || \
		(git -C knowledge init -q && git -C knowledge add -A && \
		 git -C knowledge -c user.name=лаборатория -c user.email=lab@localhost \
		 commit -qm "образец базы знаний")

seed:
	$(COMPOSE) restart generator

watch:
	$(COMPOSE) logs -f generator

errors:
	@curl -s 'http://127.0.0.1:9428/select/logsql/query' \
		--data-urlencode 'query=_time:1h (i(error*) OR i(exception*)) | stats by (service) count() as total | sort by (total desc)'

buckets:
	@$(COMPOSE) --profile agent exec agent sh -c \
		'wget -qO- http://127.0.0.1:8096/metrics | grep -E "autosre_last_bucket|autosre_source_failures"'

down:
	$(COMPOSE) --profile agent down

clean:
	$(COMPOSE) --profile agent down -v
	rm -rf knowledge

.PHONY: help up agent knowledge seed watch errors buckets down clean
