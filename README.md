# Mango Migrator UI

Webová aplikace pro import klientů z ISPAdminu do Mango přes Mango SOAP API.

Podporované režimy:

1. `CSV`
   - původní workflow se soubory `deaktivovani-klienti.csv` a `kontakty.csv`
   - zachovává deaktivace i import nových klientů
2. `ISPAdmin API`
   - načte aktivní i deaktivované klienty z ISPAdmin REST API
   - deaktivace řídí přes `GET /clients?active=0`
   - import nových klientů řídí přes `GET /clients?active=1`

## Co aplikace dělá

Po přípravě zdroje dat spustí background job, který:

1. přihlásí se do Mango SOAP API
2. načte export aktivních uživatelů z Mango
3. podle zvoleného zdroje:
   - `CSV`: provede deaktivace a potom import
   - `ISPAdmin API`: načte snapshot `active=0` a `active=1`, lokálně porovná s Mango a zapíše jen změny
4. uloží JSON report do `data/reports`

Připravený zdroj dat se po doběhu smaže z `data/uploads`.

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

## UI workflow

Aktuální frontend je zjednodušený na one-button API sync:

1. nastav `ISPADMIN_API_BASE_URL` a `ISPADMIN_API_TOKEN` v `cibs.env`
2. klikni na `Start`
3. aplikace sama:
   - načte deaktivované klienty z ISPAdmin API
   - načte aktivní klienty z ISPAdmin API
   - porovná je s Mango
   - zapíše jen změny

CSV podpora je zatím v backendu ponechaná jako fallback, ale frontend ji už nepoužívá.

## Backend endpointy

- `GET /`
- `GET /sync-logo.svg`
- `POST /api/upload`
- `POST /api/prepare-api-source`
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
- v API režimu se import i deaktivace párují do Mango přes `clientNumber` z ISPAdminu
- pokud klient v ISPAdmin API nemá `clientNumber`, záznam se přeskočí
