# Mango Migrator UI

Webová aplikace pro one-button synchronizaci klientů z ISPAdmin REST API do Mango SOAP API.

## Co aplikace dělá

Po kliknutí na `Start` aplikace:

1. načte deaktivované klienty z ISPAdmin API přes `GET /clients?active=0`
2. načte aktivní klienty z ISPAdmin API přes `GET /clients?active=1`
3. načte aktivní uživatele z Mango
4. lokálně porovná data
5. do Mango zapíše jen změny:
   - deaktivuje klienty, kteří jsou v ISPAdminu odpojení
   - vytvoří klienty, kteří jsou v ISPAdminu aktivní a v Mango ještě nejsou
6. uloží JSON report do `data/reports`

## Konfigurace

Aplikace čte runtime konfiguraci z `cibs.env`.

### Mango SOAP

- `CIBS_ENV`
- `CIBS_BASE_URL_TEST`
- `CIBS_BASE_URL_PROD`
- `CIBS_VERIFY_TLS`
- `CIBS_USERNAME`
- `CIBS_PASSWORD`

### ISPAdmin REST API

- `ISPADMIN_API_BASE_URL`
  - např. `https://your-ispadmin.example/api/v1`
- `ISPADMIN_API_TOKEN`
- `ISPADMIN_VERIFY_TLS`
- `ISPADMIN_API_TIMEOUT_SEC`

## Lokální běh

```bash
docker compose up -d --build
```

UI poběží na `http://localhost:8099`.

## Backend endpointy

- `GET /`
- `GET /sync-logo.svg`
- `POST /api/start`
- `POST /api/stop?confirm=STOP`
- `POST /api/keepalive`
- `POST /api/disconnect?stop=true`
- `GET /api/status`
- `GET /api/events`
- `GET /api/reports`
- `GET /api/reports/{name}`
- `GET /api/reports/{name}/download`

## Poznámky

- běží vždy jen jeden job
- stop je safe-stop, ne hard kill
- import i deaktivace se párují do Mango přes `clientNumber` z ISPAdminu
- pokud klient v ISPAdmin API nemá `clientNumber`, záznam se přeskočí
- reporty jsou ukládány do `/app/data/reports`
