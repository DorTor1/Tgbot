# Telegram-бот для 3x-ui

Бот регистрирует клиентов в панели [3x-ui](https://github.com/MHSanaei/3x-ui), хранит заявки в SQLite и выдаёт ссылки подписки.

## Возможности

- Регистрация во все inbound одной кнопкой.
- Подтверждение заявки админом с **выбором срока** (7 / 30 / 90 / 180 / 365 дней).
- Напоминания за 3 дня, 1 день и при окончании подписки.
- Автоматическое чтение параметров подписки (subURI / subPath) из самой панели — вручную в `.env` их задавать не нужно.

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
