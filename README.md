# AGR-DATA-CRYPTO

Непрерывный сбор OHLCV 1m с Binance и Bybit (BTCUSDT / ETHUSDT / SOLUSDT)
на GitHub Actions. Два независимых workflow передают друг другу эстафету,
данные пишутся в Supabase Postgres, координация живёт в Upstash Redis.

Стоимость — 0: публичный репозиторий (Actions бесплатны), Supabase Free,
Upstash Free.

---

## 1. Что собирается

| Параметр | Значение |
| --- | --- |
| Биржи | `binance`, `bybit` (spot) |
| Символы | `BTCUSDT`, `ETHUSDT`, `SOLUSDT` |
| Интервал | `1m` (настраивается, но 1m — источник истины) |
| Свечи | **только закрытые** (`x:true` у Binance, `confirm:true` у Bybit) |
| Объём | 6 потоков × 1440 = **8 640 строк/сутки**, ≈260 тыс. строк/мес |

Незакрытые свечи отбрасываются на уровне парсера: строка, которая однажды
попала в таблицу, больше никогда не обновляется, поэтому частичная свеча
навсегда осталась бы частичной.

Старшие таймфреймы не собираются, а считаются на чтении из `1m`
(view `ohlcv_agg` — пример почасового роллапа).

`aggTrades`: модель и приоритеты описаны, но сбор **не реализован**.
`COLLECTOR_ENABLE_TRADES=true` игнорируется и пишет предупреждение в лог.

---

## 2. Архитектура

```
        ┌────────────────────────── GitHub Actions ──────────────────────────┐
        │                                                                    │
   cron 0 */4 * * *   collect-a (role=alpha)  ─── primary: binance            │
   cron 10 */4 * * *  collect-b (role=bravo)  ─── primary: bybit              │
        │                    │           │                                   │
        │                    │           └── REST backfill пропущенного       │
        │                    └────────────── WebSocket kline 1m               │
        │                                                                    │
   cron */15 * * *     watchdog ── hb:* ──┐                                  │
        └─────────────────────────────────┼──────────────────────────────────┘
                                          │
                          ┌───────────────┴───────────────┐
                          │ Upstash Redis                 │
                          │ leases + fences + cursors     │
                          │ heartbeats + alert dedupe     │
                          └───────────────┬───────────────┘
                                          │
                          ┌───────────────┴───────────────┐
                          │ Supabase Postgres (ohlcv)     │
                          │ PK = биржа+символ+ТФ+время    │
                          └───────────────────────────────┘
```

Роли зеркальны: `alpha` всегда хочет Binance, `bravo` — Bybit. Если пир
мёртв, роль забирает и его биржу (takeover).

---

## 3. Почему данные не теряются

Четыре независимых механизма, каждый закрывает свой сценарий:

1. **Только закрытые свечи.** Источник истины — 1m, строки иммутабельны.
2. **Идемпотентная запись.** PK `(exchange, symbol, interval, open_time)` +
   PostgREST `Prefer: resolution=ignore-duplicates` (= `ON CONFLICT DO NOTHING`).
   Повторная запись той же свечи — no-op, а не ошибка.
3. **Курсор двигается только после записи.** `Pipeline` сначала пишет батч в
   Supabase, и лишь потом публикует `cursor:*`. Падение между записью и
   обновлением курсора означает повторную выборку, а не потерю.
4. **Lease + fence на каждый поток.** Поток одновременно собирает ровно один
   процесс. `fence` монотонно растёт на поток, поэтому «зомби»-процесс,
   потерявший lease, не может сдвинуть курсор нового владельца.

Порядок старта принципиален: **сначала подписка на WebSocket, потом REST
backfill**. Пока REST добирает пропущенное, WS уже буферизует новые свечи,
поэтому стык не оставляет дыры.

### Передача эстафеты

| Время (UTC) | Событие |
| --- | --- |
| 00:00 | `collect-a` стартует, берёт Binance |
| 00:10 | `collect-b` стартует, берёт Bybit |
| 03:50 | `collect-a` достигает soft deadline (3ч50м) → drain → выход |
| 04:00 | `collect-a'` стартует и добирает Binance за 03:50–04:00 |
| 04:00 | `collect-b` достигает soft deadline → выход |
| 04:10 | `collect-b'` стартует и добирает Bybit за 04:00–04:10 |

Cron GitHub неточен (задержки 5–30 минут), и это не ломает схему: логика не
зависит от момента запуска. Опоздавший successor просто добирает больший
интервал через REST — история klines доступна за годы назад.

Takeover срабатывает, только если heartbeat пира исчез **и** роль работает
дольше `TAKEOVER_GRACE_SEC` (120 с). Отдельный `STARTUP_GRACE_SEC` (15 мин)
закрывает случай, когда пира ещё ни разу не видели живым: роли стартуют с
разницей 10 минут, и без этого A захватывал бы Bybit в начале каждого цикла.
Если пир был жив и пропал, takeover происходит через 2 минуты.

---

## 4. Установка

### 4.1 Supabase

1. Создать проект (Free).
2. SQL Editor → выполнить [`sql/schema.sql`](sql/schema.sql).
3. Project Settings → API → скопировать `URL` и **`service_role`** ключ.
   `anon` не подойдёт: RLS включён, политик нет.

### 4.2 Upstash Redis

1. Создать Redis базу (Free), регион — ближайший к GitHub-раннерам
   (`us-east-1`).
2. TLS обязателен: строка должна начинаться с `rediss://`.
3. Скопировать `UPSTASH_REDIS_REST_*` не нужно — используется обычный
   Redis-протокол, нужен `rediss://default:...@...:6379`.

### 4.3 Репозиторий и секреты

```bash
git init
git add .
git commit -m "initial collector"
git remote add origin git@github.com:<user>/agr-data-crypto.git
git push -u origin main
```

Репозиторий должен быть **публичным**, иначе минуты Actions кончатся за
неделю (2 job-hours × 24 = 48 job-hours/сутки).

Settings → Secrets and variables → Actions:

| Secret | Обязателен | Назначение |
| --- | --- | --- |
| `UPSTASH_REDIS_URL` | да | `rediss://...` |
| `SUPABASE_URL` | да | `https://xxx.supabase.co` |
| `SUPABASE_SERVICE_ROLE_KEY` | да | service_role ключ |
| `TELEGRAM_BOT_TOKEN` | нет | алерты |
| `TELEGRAM_CHAT_ID` | нет | алерты |

Settings → Actions → General → **Workflow permissions**:
выбрать **Read and write permissions**. Иначе `keepalive` не сможет сделать
коммит, а `watchdog` — вызвать `workflow_dispatch`.

### 4.4 Первый запуск

1. Actions → **backfill** → Run workflow (например, `binance`, `12` месяцев).
   Дождаться окончания.
2. Actions → **collect-a** → Run workflow.
3. Actions → **collect-b** → Run workflow.
4. Проверить Actions → **watchdog** → Run workflow: в логе должны быть
   `heartbeats: {'alpha': True, 'bravo': True}`.

Дальше всё идёт по cron. `ci` прогоняет линтер и тесты на каждый push/PR.

---

## 5. Переменные окружения

Все переменные имеют префикс `COLLECTOR_`. Полный список — в
[`.env.example`](.env.example); ниже значимые.

| Переменная | По умолчанию | Смысл |
| --- | --- | --- |
| `ROLE` | `alpha` | `alpha` = Binance primary, `bravo` = Bybit primary |
| `RUN_ID` | авто | входит в токен lease; в workflow = `run_id-run_attempt` |
| `INTERVAL` | `1m` | таймфрейм kline |
| `REDIS_URL` | — | пусто = degraded mode |
| `SUPABASE_URL` / `SUPABASE_KEY` | — | sink |
| `SOFT_DEADLINE_SEC` | `13800` | 3ч50м, затем drain и выход |
| `DRAIN_RESERVE_SEC` | `60` | запас на финальную запись |
| `LEASE_TTL_MS` | `180000` | TTL lease, должен быть ≫ heartbeat |
| `HEARTBEAT_TTL_MS` | `300000` | сколько пир считается живым после смерти |
| `HEARTBEAT_INTERVAL_SEC` | `60` | обновление hb + всех lease |
| `CURSOR_FLUSH_INTERVAL_SEC` | `120` | как часто курсоры уходят в Redis |
| `TAKEOVER_POLL_INTERVAL_SEC` | `120` | период reconcile |
| `TAKEOVER_GRACE_SEC` | `120` | запрет takeover сразу после старта |
| `STARTUP_GRACE_SEC` | `900` | сколько ждать первого heartbeat пира |
| `STANDBY_MAX_WAIT_SEC` | `600` | сколько ждать потоков, если ничего не досталось |
| `DEGRADED_MAX_SEC` | `900` | сколько работать без Redis, затем exit 1 |
| `FLUSH_ROWS` / `FLUSH_INTERVAL_SEC` | `200` / `5` | батчи в Supabase |
| `COLD_START_CANDLES` | `60` | сколько добрать при пустом курсоре |
| `REST_MIN_INTERVAL_SEC` | `0.3` | троттлинг REST |
| `ENABLE_TRADES` | `false` | пока не реализовано |

Интервалы подобраны под лимит Upstash Free (~10 тыс. команд/сутки):
2 роли × (60 heartbeat + 30 cursor + 30 peer-check) в час ≈ **5.8 тыс./сутки**,
плюс watchdog ≈ 0.4 тыс. — итого ≈6.2 тыс. Ускорять координацию бессмысленно:
при 1m гранулярности это ничего не даёт, а free tier заканчивается.

---

## 6. Ключи в Redis

| Ключ | TTL | Смысл |
| --- | --- | --- |
| `hb:{role}` | 300 с | heartbeat роли; отсутствие = роль мертва |
| `run:{role}` | 24 ч | hash: run_id, старт, hostname |
| `lease:{kind}:{exchange}:{symbol}` | 180 с | владение потоком, значение `role:run_id:fence` |
| `fence:{kind}:{exchange}:{symbol}` | — | монотонный счётчик поколений lease |
| `cursor:{kind}:{exchange}:{symbol}` | — | последняя **записанная** закрытая свеча (open_time, ms) |
| `cursor_fence:{...}` | — | fence владельца, последним двигавшего курсор |
| `alert:*` | 10–30 мин | дедупликация алертов |

Роль не блокирует другую роль целиком: блокировка существует только на
уровне потока.

---

## 7. Runbook

**Данные перестали идти.**
1. Actions → watchdog → Run workflow. В логе видно `heartbeats` и `stale cursors`.
2. `hb:alpha` / `hb:bravo` отсутствуют оба → watchdog сам вызовет оба workflow.
3. Один отсутствует → роль-пир заберёт потоки после истечения
   `HEARTBEAT_TTL_MS` + `TAKEOVER_GRACE_SEC` (≤7 минут).

**Redis недоступен.**
Коллектор не падает: он переходит в degraded mode, продолжает писать свою
биржу (lease синтетический, `fence=0`), но **не двигает курсоры** и через
`DEGRADED_MAX_SEC` выходит с кодом 1. Это осознанный компромисс: лучше
потерять 15 минут координации, чем данные. После восстановления Redis
следующий запуск доберёт всё через REST.

**В таблице дубликаты.**
Их не может быть по PK. Если запрос показывает повторы — скорее всего
запрос без `distinct` по `ohlcv_agg` или JOIN.

**Разрыв в данных.**
`python scripts/verify_continuity.py --window-minutes 260` покажет точные
границы. Закрывается повторным `backfill` за нужный период или ручным
запуском collector: он доберёт от курсора.

**Остановить сбор.**
Actions → выбрать workflow → Disable. Текущий run можно Cancel — процесс
получит SIGTERM, допишет буфер, опустит курсоры и освободит lease.
Жёсткое убийство тоже безопасно: lease истечёт, данные перезаберутся.

**Ротация ключей.**
Обновить секрет в GitHub → следующий запуск подхватит. Старые lease
истекут сами через 180 с.

---

## 8. Ограничения (честно)

- **Cron не гарантирует запуск.** GitHub может задержать job на 5–30 минут
  или пропустить его. Компенсация: backfill от курсора + watchdog каждые
  15 минут. Полной гарантии «процесс всегда жив» нет ни у кого на cron.
- **Scheduled workflows отключаются после 60 дней без активности
  репозитория.** `keepalive.yml` делает пустой коммит раз в месяц.
- **Supabase Free — 500 МБ.** При ≈44 МБ/мес (6 потоков) это ≈10–11 месяцев.
  Дальше: партиционирование + архив старых месяцев, либо платный план.
- **Upstash Free — 10 тыс. команд/сутки.** Текущая конфигурация ≈6.2 тыс.
  вместе с watchdog. При уменьшении интервалов координации лимит кончится.
- **「Ровно один раз」 — только на уровне хранилища.** Доставка в sink
  at-least-once; дедупликация обеспечивается PK. Это правильная гарантия:
  она не зависит от того, успел ли процесс записать курсор.
- **Bybit банит за бурст** (403/10006). Клиент держит паузу 0.3 с и
  экспоненциально отступает; не стоит уменьшать `REST_MIN_INTERVAL_SEC`.
- **Время только UTC.** Все `open_time` — миллисекунды epoch от биржи.
- **Нет funding rate, open interest, order book.** Это отдельные потоки,
  их в текущей схеме нет.

---

## 9. Структура

```
.github/workflows/
  ci.yml           линт + тесты
  collect-a.yml    role alpha, cron 0 */4 * * *
  collect-b.yml    role bravo, cron 10 */4 * * *
  watchdog.yml     каждые 15 минут, revive + алерты
  backfill.yml     ручной глухой бэкфилл
  keepalive.yml    пустой коммит раз в месяц
src/collector/
  config.py        настройки из env (dataclass, без pydantic)
  roles.py         кто какие потоки хочет и в каком порядке
  supervision.py   жизненный цикл run: acquire → WS → backfill → loop → drain
  pipeline.py      буфер, дедуп, курсоры после записи
  state/           Lua-скрипты, leases, cursors, facade
  exchanges/       REST + WS + парсеры Binance/Bybit
  sink/            Supabase (PostgREST) и in-memory
scripts/
  verify_continuity.py  проверка разрывов через PostgREST
  watchdog.py           revive через GitHub API
  bootstrap_history.py  месячные архивы Binance + REST-обход Bybit
  check_workflows.py    валидация YAML перед пушем
sql/schema.sql     таблица, view, RPC, RLS
tests/             36 тестов: парсеры, lease/fence, pipeline, takeover, supervisor
```

---

## 10. Локально

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe scripts\check_workflows.py

# smoke-тест без Redis и без Supabase (in-memory sink)
$env:PYTHONPATH="src"
$env:COLLECTOR_DRY_RUN="true"
$env:COLLECTOR_ROLE="alpha"
$env:COLLECTOR_SOFT_DEADLINE_SEC="120"
$env:COLLECTOR_DEGRADED_MAX_SEC="90"
.\.venv\Scripts\python.exe -m collector.main
```

В dry-run без Redis процесс по design'у завершится с кодом 1 через
`DEGRADED_MAX_SEC`, успев собрать и записать данные в память. В логе
`pipeline_written` показывает, сколько строк прошло.

Docker (для VPS / Oracle Always Free) — `docker/Dockerfile`; отличие только
в `COLLECTOR_SOFT_DEADLINE_SEC`.
