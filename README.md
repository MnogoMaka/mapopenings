# Разрытия & Камеры — Визуализатор

## Структура архива

Распакуй архив, должно получиться:

```
razritiya/
├── app.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── .env.example
└── result.json          ← файл с данными (привязка камер к разрытиям)
```

## Требования

- Docker Desktop: https://www.docker.com/products/docker-desktop/

## Первый запуск

```bash
# 1. Переименуй .env.example в .env и вставь OAuth-токен
cp .env.example .env

# Получить токен — открой в браузере:
# https://oauth.yandex.ru/authorize?response_type=token&client_id=1a6990aa636648e9b2ef855fa7bec2fb

# 2. Вставь токен в .env:
# OAUTH_TOKEN=y3_Aaaa...

# 3. Собери и запусти (первый раз ~2 минуты)
docker-compose up --build

# 4. Открой в браузере
# http://localhost:8050
```

## Работа с приложением

1. Настрой фильтры (даты, типы камер, слои)
2. Нажми **«🗺 Построить карту»**
3. Кликни на маркер камеры — фото загрузится прямо из облака

## Обновление result.json

Замени файл и перезапусти контейнер:
```bash
docker-compose restart
```

## Остановка

```bash
docker-compose down
```
