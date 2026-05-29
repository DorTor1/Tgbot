# Telegram-бот для 3x-ui

Бот регистрирует клиентов в [3x-ui](https://github.com/MHSanaei/3x-ui) **3.2+** (мастер-панель + ноды), хранит заявки в SQLite и выдаёт **одну ссылку мультиподписки**.

## Возможности

- Регистрация через `POST /panel/api/clients/add` (API Token / Bearer).
- **Мастер + ноды** — бот ходит только на мастер-панель; США и другие VPS подключаются как ноды в UI мастера, клиенты синхронизируются автоматически.
- **Одна ссылка подписки** — все локации в одном профиле (мультиподписка 3x-ui).
- Подтверждение заявки админом с выбором срока (7 / 30 / 90 / 180 / 365 дней).
- Напоминания за 3 дня, 1 день и при окончании подписки.
- Автоматическое чтение `subURI` / `subPath` с мастер-панели.

## Архитектура (рекомендуется)

```
Telegram-бот  →  API мастер-панели (DE)  →  синхронизация  →  нода (US)
                      ↓
              Subscription Server (мультиподписка)
```

В `.env` указывается **только мастер** (`PANEL_BASE_URL_1`, `PANEL_API_TOKEN_1`).  
`PANEL_INBOUND_IDS` — id inbound на мастере, к которым нужно привязать клиента (включая inbound с нод).

## Быстрый старт

```bash
cp .env.example .env
# BOT_TOKEN, PANEL_BASE_URL_1, PANEL_API_TOKEN_1, ADMIN_ID, PANEL_INBOUND_IDS

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

В панели: **Settings → Security → API Token**, **Settings → Subscription** (включить sub, проверить subURI), **Nodes** — добавить ноду США.

## Обновление на сервере (systemd)

```bash
sudo systemctl stop my_bot.service
cd /etc/vpn/Tgbot
git pull
source .venv/bin/activate
pip install -r requirements.txt
sudo systemctl start my_bot.service
journalctl -u my_bot.service -f
```

В логах после старта ищите `panel_api build=v3-only-2026-05-29`.

## Безопасность

- Не коммитьте `.env` и `*.db`.
- API Token и `BOT_TOKEN` при утечке — перевыпустить.

## Частые проблемы

- **Клиент создаётся, но в подписке один сервер** — проверьте `PANEL_INBOUND_IDS` (все нужные inbound на мастере) и мультиподписку в Settings → Subscription.
- **Продление не находит клиента** — после сброса панели старые записи в `bot.db` не совпадают с панелью; выдайте доступ заново.
- **404 при логине** — в `PANEL_BASE_URL` не должно быть `/panel/` на конце.
- **`TelegramConflictError`** — два процесса с одним `BOT_TOKEN` (часто встроенный бот 3x-ui).

Документация API: OpenAPI в панели 3x-ui 3.x, репозиторий [MHSanaei/3x-ui](https://github.com/MHSanaei/3x-ui).
