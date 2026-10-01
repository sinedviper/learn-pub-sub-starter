# learn-pub-sub-starter (Peril)

This is the starter code used in Boot.dev's [Learn Pub/Sub](https://learn.boot.dev/learn-pub-sub) course.

## Работа с RabbitMQ (`rabbit.sh`)

Скрипт `rabbit.sh` управляет Docker-контейнером `peril_rabbitmq` (образ `rabbitmq:3.13-management`). Для работы нужен установленный и запущенный Docker.

Если скрипт не исполняемый:

```bash
chmod +x rabbit.sh
```

### Команды

```bash
./rabbit.sh start   # запустить контейнер (создаст новый, если его ещё нет)
./rabbit.sh stop    # остановить контейнер
./rabbit.sh logs    # смотреть логи RabbitMQ в реальном времени (Ctrl+C — выход)
```

- При первом `start` контейнер создаётся через `docker run`; при последующих — просто запускается через `docker start`.
- Порты:
  - `5672` — AMQP (подключение приложения), строка подключения: `amqp://guest:guest@localhost:5672/`
  - `15672` — веб-интерфейс управления: <http://localhost:15672> (логин/пароль: `guest` / `guest`)

## Запуск нескольких серверов (`multiserver.sh`)

Запускает указанное количество экземпляров `./cmd/server` параллельно:

```bash
./multiserver.sh 3   # запустить 3 экземпляра сервера
```

Остановка всех экземпляров — `Ctrl+C`.

## Типичный порядок работы

```bash
./rabbit.sh start        # 1. поднять RabbitMQ
go run ./cmd/server      # 2. запустить сервер (в отдельном терминале)
go run ./cmd/client      # 3. запустить клиент (в другом терминале)
./rabbit.sh stop         # 4. остановить RabbitMQ по окончании работы
```
