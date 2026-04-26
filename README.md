# Telegram-бот для 3x-ui

Бот регистрирует клиентов в панели [3x-ui](https://github.com/MHSanaei/3x-ui), хранит заявки в SQLite и выдаёт ссылки подписки.

## Возможности

- Регистрация во все inbound одной кнопкой.
- **Несколько VPS / панелей** — бот выдаёт по ссылке подписки на каждый сервер (один UUID/sub_id, два хоста).
- Подтверждение заявки админом с **выбором срока** (7 / 30 / 90 / 180 / 365 дней).
- Напоминания за 3 дня, 1 день и при окончании подписки.
- Автоматическое чтение параметров подписки (subURI / subPath) из самой панели — вручную в `.env` их задавать не нужно.
- Личный кабинет с FAQ и кнопкой «Написать админу».
- **Атомарная регистрация** на нескольких панелях: при сбое одной — клиент откатывается со всех уже успешных, чтобы не оставлять «полу-доступ».
- Защита от двойного клика админом (claim-then-create).

## Структура

- `main.py` — точка входа (запуск бота, фоновые задачи).
- `config.py` — `.env` и константы.
- `texts.py` — все строки UI.
- `formatting.py` — статус подписки, имена, даты.
- `keyboards.py` — все inline/reply клавиатуры.
- `services/` — бизнес-логика (`links`, `subscriptions`, `reminders`, `notifications`).
- `handlers/` — роутеры aiogram (`start`, `cabinet`, `access`, `renewal`, `agreement`, `admin`).
- `db.py` — SQLite (aiosqlite).
- `panel_api.py` — клиент 3x-ui API.

## Несколько серверов

Для каждого VPS задайте блок с суффиксом `_1`, `_2`, … в `.env`:

```
PANEL_NAME_1=Нидерланды
PANEL_BASE_URL_1=https://host1.example.com/prefix1
PANEL_LOGIN_1=admin
PANEL_PASSWORD_1=***

PANEL_NAME_2=Германия
PANEL_BASE_URL_2=https://host2.example.com/prefix2
PANEL_LOGIN_2=admin
PANEL_PASSWORD_2=***
```

Бот создаст клиента с одинаковым UUID/sub_id на обеих панелях и отправит пользователю две ссылки — по одной на каждый сервер. Если хоть одна панель недоступна при выдаче — вся операция откатится с ошибкой, чтобы не выдать «половину» доступа. Старые переменные без суффикса (`PANEL_BASE_URL` и т.д.) продолжают работать как один сервер (fallback).

## Быстрый старт

```bash
cp .env.example .env
# отредактируйте .env (как минимум BOT_TOKEN, PANEL_LOGIN, PANEL_PASSWORD, PANEL_BASE_URL, ADMIN_ID)

python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

## Обновление на сервере (systemd)

```bash
sudo systemctl stop my_bot.service

cd /etc/vpn/Tgbot
git pull                              # или scp/rsync: перезаписать файлы

source .venv/bin/activate
pip install -r requirements.txt       # если менялся requirements.txt

sudo systemctl start my_bot.service
sudo systemctl status my_bot.service
sudo journalctl -u my_bot.service -f  # посмотреть логи
```

## Безопасность и публикация на GitHub

- **Не коммитьте** `.env`, базы `*.db` / `*.sqlite3`, каталог `.venv/` — они в `.gitignore`.
- В репозитории не должно быть реальных URL панели, токена бота, паролей: только шаблоны в `.env.example`.
- Если `.env` или секреты когда-либо попали в git: `git rm --cached .env`, смените пароль панели и перевыпустите токен бота у [@BotFather](https://t.me/BotFather).
- Перед `git push` полезно проверить `git grep -iE 'token|password|AAG[A-Za-z0-9_-]{30,}'`.

## Частые проблемы

- **`TelegramConflictError`** — запущено два экземпляра с одним `BOT_TOKEN` (часто ещё висит встроенный бот внутри 3x-ui). Остановите лишний процесс.
- **404 при логине** — в `PANEL_BASE_URL` не должно быть `/panel/` на конце.
- **Ссылка подписки «не та»** — проверьте в панели раздел «Subscription»; бот автоматически подхватит оттуда `subURI`/`subPath`. Можно переопределить `SUBSCRIPTION_BASE_URL` и `SUBSCRIPTION_PATH` в `.env`.
