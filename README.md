# Mango Migrator UI

Webová aplikace pro bezpečný import klientů z ISPadmin CSV do Mango (SOAP API), bez nutnosti práce v CLI.

## Co aplikace dělá

Workflow je vždy 2-fázový:

1. **Deaktivace v Mango** podle `deaktivovani-klienti.csv`
   - párování přes `Klientské číslo` / `Uživatelské jméno` -> Mango `main_login`
   - cílová akce: `ws_user_edit(active=0)`
2. **Import nových klientů** podle `kontakty.csv`
   - nejdřív kontrola existence (`ws_users_list`)
   - neexistující klienti -> `ws_user_create`

Aplikace běží postupně (rate-limit API), aby zbytečně nepřetěžovala Mango API.

---

## Hlavní vlastnosti

- Upload 2 CSV souborů + validace povinných sloupců
- Live průběh přes SSE (stav, procenta, log)
- Start/Stop jobu (Stop s potvrzením přes text `STOP`)
- Safe cancel (zastavení na bezpečném bodě)
- Keepalive watchdog:
  - když klient/session zmizí, backend job automaticky stopne
- Report browser přímo v UI:
  - seznam reportů
  - view + download JSON
- Auto-cleanup `data/uploads` po doběhu jobu

---

## Požadavky na CSV

### `kontakty.csv`
Minimálně sloupce:
- `ID`
- `Klientské číslo`
- `Uživatelské jméno`
- `Klient`
- `Stav`

### `deaktivovani-klienti.csv`
Minimálně sloupce:
- `ID`
- `Klientské číslo`
- `Uživatelské jméno`

> Pozor: soubory se nesmí prohodit.

---

## Lokální běh (Docker Compose)

```bash
cd /home/mondy/.openclaw/workspace/apps/mango-migrator-ui
cp cibs.env.example cibs.env
# doplň CIBS_USERNAME / CIBS_PASSWORD (+ případně další hodnoty)

docker compose up -d --build
```

UI: `http://localhost:8099`

---

## Konfigurace

Aplikace čte Mango přístup z `cibs.env` (mount do kontejneru jako `/app/cibs.env`).

Použité proměnné:
- `CIBS_ENV` (`prod` / `test`)
- `CIBS_BASE_URL_TEST`
- `CIBS_BASE_URL_PROD`
- `CIBS_VERIFY_TLS`
- `CIBS_USERNAME`
- `CIBS_PASSWORD`

Volitelné runtime proměnné:
- `MANGO_SECRETS_ENV` (default `/app/cibs.env`)
- `MANGO_KEEPALIVE_TIMEOUT_SEC` (default `90`)

---

## Persistovaná data

Compose používá named volume `mango_migrator_data` mapované do `/app/data`:

- `/app/data/reports` -> reporty `run-*.json`
- `/app/data/uploads` -> dočasné uploady (po doběhu se čistí)

---

## API endpointy (interní UI backend)

- `GET /` - HTML UI
- `GET /sync-logo.svg` - branding logo
- `POST /api/upload` - upload + validace CSV
- `POST /api/start` - spuštění jobu
- `POST /api/stop?confirm=STOP` - bezpečné zastavení jobu
- `POST /api/keepalive` - heartbeat z UI
- `POST /api/disconnect?stop=true` - unload/refresh signal
- `GET /api/status` - aktuální stav
- `GET /api/events` - SSE stream stavu/logu
- `GET /api/reports` - seznam reportů
- `GET /api/reports/{name}` - načtení reportu
- `GET /api/reports/{name}/download` - download reportu

---

## Poznámky k implementaci (pro další zásahy)

### Backend
- Soubor: `app.py`
- Job běží v background threadu (`_run_job`)
- `JobState` drží runtime stav pro UI
- Cancel:
  - `cancel_requested=true`
  - kontrola přes `_raise_if_cancelled()` uvnitř hlavních smyček
- Keepalive watchdog:
  - samostatný daemon thread
  - při timeoutu nastaví `cancel_requested`
- Oprava rozbité diakritiky:
  - `_fix_mojibake()` při čtení SOAP tagů (`_get`)

### Frontend
- Soubor: `index.html` (single-page, vanilla JS)
- Live updates přes `EventSource('/api/events')`
- Start/Stop tlačítko sdílí jeden `runBtn`
- Stop modal je blokující potvrzení (`STOP`)
- Report list je renderovaný jako klikací položky (View/Download)

---

## Chování po doběhu

- V UI zůstane summary
- Last run = poslední **kompletně dokončený** běh (podle report files)
- Uploady se smažou (`data/uploads`)
- Report zůstane v `data/reports`

---

## Známé limity

- Běží vždy jen **1 job** najednou
- Pokud je backend uprostřed dlouhého SOAP callu, stop proběhne po návratu callu (safe stop, ne hard kill)
- Některé SOAP fault zprávy vrací server ve špatném encodingu (aplikace se to snaží opravovat)

---

## Doporučení pro GitHub

- Commitnout:
  - `app.py`, `index.html`, `Dockerfile`, `compose.yaml`, `cibs.env.example`, `README.md`, `sync-logo.svg`
- Necommitovat:
  - `cibs.env`
  - runtime data z `data/`

Doporučený `.gitignore`:

```gitignore
cibs.env
data/
__pycache__/
*.pyc
```
