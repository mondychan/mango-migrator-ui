#!/usr/bin/env python3
import html
import json
import os
import re
import shutil
import threading
import time
from datetime import UTC, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any, Dict

import requests
from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
REPORTS_DIR = DATA_DIR / "reports"
SECRETS_ENV = Path(os.environ.get("MANGO_SECRETS_ENV", "/app/cibs.env"))
PREPARED_SOURCE_FILE = UPLOAD_DIR / "source.json"
API_DEACT_FILE = UPLOAD_DIR / "ispadmin-inactive-clients.json"
API_IMPORT_FILE = UPLOAD_DIR / "ispadmin-active-clients.json"
SCHEDULE_FILE = DATA_DIR / "schedule.json"

for p in (UPLOAD_DIR, REPORTS_DIR):
    p.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Mango Migrator UI")


class JobState:
    def __init__(self):
        self.lock = threading.Lock()
        self.running = False
        self.current: Dict[str, Any] = {}

    def reset(self):
        self.current = {
            "started_at": None,
            "finished_at": None,
            "stage": "idle",
            "percent": 0,
            "log": [],
            "summary": None,
            "error": None,
            "report_path": None,
            "cancel_requested": False,
            "last_keepalive": None,
            "keepalive_required": False,
            "run_reason": None,
            "source_mode": None,
            "source_label": None,
        }


STATE = JobState()
STATE.reset()
KEEPALIVE_TIMEOUT_SEC = int(os.environ.get("MANGO_KEEPALIVE_TIMEOUT_SEC", "90"))
SCHEDULER_POLL_SEC = max(5, int(os.environ.get("MANGO_SCHEDULER_POLL_SEC", "15")))
SCHEDULE_LOCK = threading.Lock()


def _default_schedule() -> dict:
    return {
        "enabled": False,
        "time": "02:00",
        "interval_hours": 24,
        "next_run_at": None,
        "last_auto_started_at": None,
    }


def _parse_schedule_time(value: str) -> dt_time:
    try:
        return datetime.strptime(str(value).strip(), "%H:%M").time()
    except Exception:
        return dt_time(2, 0)


def _next_run_at(now: datetime, start_time: dt_time, interval_hours: int) -> datetime:
    candidate = datetime.combine(now.date(), start_time)
    if candidate > now:
        return candidate
    delta_hours = (now - candidate).total_seconds() / 3600.0
    steps = int(delta_hours // interval_hours) + 1
    return candidate + timedelta(hours=steps * interval_hours)


def _normalize_schedule(payload: dict | None) -> dict:
    current = _default_schedule()
    if isinstance(payload, dict):
        current.update(payload)

    enabled = current.get("enabled", False)
    if isinstance(enabled, str):
        enabled = enabled.strip().lower() in {"1", "true", "yes", "on"}

    schedule_time = str(current.get("time") or "02:00").strip()
    schedule_time = _parse_schedule_time(schedule_time).strftime("%H:%M")

    try:
        interval_hours = int(current.get("interval_hours", 24))
    except Exception:
        interval_hours = 24

    next_run_at = current.get("next_run_at")
    if next_run_at is not None:
        next_run_at = str(next_run_at).strip() or None

    last_auto_started_at = current.get("last_auto_started_at")
    if last_auto_started_at is not None:
        last_auto_started_at = str(last_auto_started_at).strip() or None

    return {
        "enabled": bool(enabled),
        "time": schedule_time,
        "interval_hours": max(1, min(interval_hours, 168)),
        "next_run_at": next_run_at,
        "last_auto_started_at": last_auto_started_at,
    }


def _load_schedule() -> dict:
    if not SCHEDULE_FILE.exists():
        return _default_schedule()
    try:
        return _normalize_schedule(json.loads(SCHEDULE_FILE.read_text(encoding="utf-8")))
    except Exception:
        return _default_schedule()


def _save_schedule(schedule: dict):
    normalized = _normalize_schedule(schedule)
    _write_json(SCHEDULE_FILE, normalized)


SCHEDULE_STATE = _load_schedule()


def _schedule_status() -> dict:
    with SCHEDULE_LOCK:
        return dict(SCHEDULE_STATE)


def _keepalive_watchdog():
    while True:
        time.sleep(3)
        with STATE.lock:
            running = STATE.running
            cancel_requested = bool(STATE.current.get("cancel_requested"))
            last_keepalive = STATE.current.get("last_keepalive")
            keepalive_required = bool(STATE.current.get("keepalive_required"))
        if not running or cancel_requested or not keepalive_required or not last_keepalive:
            continue
        if (time.time() - float(last_keepalive)) > KEEPALIVE_TIMEOUT_SEC:
            with STATE.lock:
                STATE.current["cancel_requested"] = True
            _append_log(f"Keepalive timeout (> {KEEPALIVE_TIMEOUT_SEC}s). Auto-stop requested.")


threading.Thread(target=_keepalive_watchdog, daemon=True).start()


def _scheduler_loop():
    next_run: datetime | None = None
    signature = None
    while True:
        schedule = _schedule_status()
        enabled = bool(schedule.get("enabled"))
        if not enabled:
            next_run = None
            signature = None
            with SCHEDULE_LOCK:
                if SCHEDULE_STATE.get("next_run_at") is not None:
                    SCHEDULE_STATE["next_run_at"] = None
                    _save_schedule(SCHEDULE_STATE)
            time.sleep(SCHEDULER_POLL_SEC)
            continue

        current_signature = (schedule.get("enabled"), schedule.get("time"), schedule.get("interval_hours"))
        if signature != current_signature or next_run is None:
            next_run = _next_run_at(datetime.now(), _parse_schedule_time(schedule.get("time", "02:00")), int(schedule.get("interval_hours", 24)))
            signature = current_signature
            with SCHEDULE_LOCK:
                SCHEDULE_STATE["next_run_at"] = next_run.isoformat()
                _save_schedule(SCHEDULE_STATE)

        if next_run and datetime.now() >= next_run:
            started = _start_job("schedule")
            if started:
                with SCHEDULE_LOCK:
                    SCHEDULE_STATE["last_auto_started_at"] = datetime.now(UTC).isoformat()
                    _save_schedule(SCHEDULE_STATE)
                interval = max(1, int(schedule.get("interval_hours", 24)))
                while next_run <= datetime.now():
                    next_run += timedelta(hours=interval)
                with SCHEDULE_LOCK:
                    SCHEDULE_STATE["next_run_at"] = next_run.isoformat()
                    _save_schedule(SCHEDULE_STATE)
            time.sleep(SCHEDULER_POLL_SEC)
            continue

        time.sleep(SCHEDULER_POLL_SEC)


def _append_log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with STATE.lock:
        STATE.current["log"].append(f"[{ts}] {msg}")
        STATE.current["log"] = STATE.current["log"][-1200:]


def _set_progress(stage: str, percent: int):
    with STATE.lock:
        STATE.current["stage"] = stage
        STATE.current["percent"] = max(0, min(100, percent))


def _cancel_requested() -> bool:
    with STATE.lock:
        return bool(STATE.current.get("cancel_requested"))


def _touch_keepalive():
    with STATE.lock:
        STATE.current["last_keepalive"] = time.time()


def _raise_if_cancelled():
    if _cancel_requested():
        raise RuntimeError("RUN_CANCELLED_BY_USER")


def _cleanup_uploads():
    for p in UPLOAD_DIR.glob("*"):
        try:
            if p.is_file() or p.is_symlink():
                p.unlink(missing_ok=True)
            elif p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
        except Exception:
            pass


def _load_env(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(f"Secrets file not found: {path}. Verify mount and cibs.env presence.")
    if path.is_dir():
        raise RuntimeError(f"Secrets path is a directory, not a file: {path}")
    env = {}
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def _soap_call(url: str, verify: bool, method: str, inner: str, timeout: int = 60):
    env = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="API_NAMESPACE">'
        f"<soapenv:Body><api:{method}>{inner}</api:{method}></soapenv:Body></soapenv:Envelope>"
    )
    response = requests.post(
        url,
        data=env.encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": ""},
        timeout=timeout,
        verify=verify,
    )
    return response.status_code, response.text


def _fix_mojibake(value: str) -> str:
    s = value or ""
    if any(ch in s for ch in ("Ä", "Ă", "Å", "Ĺ")):
        for enc in ("latin-1", "cp1250"):
            try:
                repaired = s.encode(enc).decode("utf-8")
                if repaired.count("�") <= s.count("�"):
                    s = repaired
                    break
            except Exception:
                pass
    return s


def _get(tag: str, text: str) -> str:
    match = re.search(fr"<{tag}[^>]*>(.*?)</{tag}>", text, re.S)
    return _fix_mojibake(match.group(1).strip()) if match else ""


def _esc(value: str) -> str:
    return html.escape((value or "").strip())


def _norm_phone(value: str) -> str:
    digits = "".join(ch for ch in (value or "") if ch.isdigit())
    if len(digits) == 9:
        digits = "420" + digits
    return digits


def _split_street_house(street: str, house: str):
    street = (street or "").strip()
    house = (house or "").strip()
    if house:
        return street, house
    match = re.match(r"^(.*?)[ ,]+(\d+[A-Za-z0-9/\-]*)$", street)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return street, ""


def _sanitize_email(value: str) -> str:
    mail = (value or "").strip().lower()
    if not mail or " " in mail or "@" not in mail:
        return ""
    local, _, domain = mail.partition("@")
    if "+" in local and domain in ("gmail.com", "googlemail.com"):
        local = local.split("+", 1)[0]
        mail = f"{local}@{domain}"
    if re.match(r"^[a-z0-9._%\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", mail):
        return mail
    return ""


def _first_email(client: dict) -> str:
    for key in ("email", "billing_email", "technical_email", "business_email"):
        raw = (client.get(key) or "").strip().lower()
        if not raw:
            continue
        parts = [part.strip() for part in re.split(r"[;,]+", raw) if part.strip()]
        for part in parts:
            mail = _sanitize_email(part)
            if mail:
                return mail
    return ""


def _phones(client: dict) -> str:
    seen = []
    for key in ("mobile", "phone", "fax"):
        phone = _norm_phone(client.get(key, ""))
        if phone and phone not in seen:
            seen.append(phone)
    return ",".join(seen)


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any):
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_prepared_source(spec: dict):
    _write_json(PREPARED_SOURCE_FILE, spec)


def _load_prepared_source() -> dict | None:
    if not PREPARED_SOURCE_FILE.exists():
        return None
    try:
        spec = _load_json(PREPARED_SOURCE_FILE)
    except Exception:
        return None

    if spec.get("mode") != "api":
        return None
    deact_path = Path(spec.get("deactivate_path", ""))
    import_path = Path(spec.get("import_path", ""))
    if not import_path.exists() or not deact_path.exists():
        return None
    return spec


def _source_status() -> dict | None:
    spec = _load_prepared_source()
    if not spec:
        return None
    return {
        "mode": spec.get("mode"),
        "label": spec.get("label"),
        "prepared_at": spec.get("prepared_at"),
        "summary": spec.get("summary") or {},
    }


def _ispadmin_config(env: dict) -> dict:
    base_url = (env.get("ISPADMIN_API_BASE_URL") or "").strip()
    token = (env.get("ISPADMIN_API_TOKEN") or "").strip()
    if not base_url:
        raise RuntimeError("ISPADMIN_API_BASE_URL is not configured.")
    if not token:
        raise RuntimeError("ISPADMIN_API_TOKEN is not configured.")
    return {
        "base_url": base_url.rstrip("/"),
        "token": token,
        "verify": (env.get("ISPADMIN_VERIFY_TLS", "true").lower() == "true"),
        "timeout": int(env.get("ISPADMIN_API_TIMEOUT_SEC", "120")),
    }


def _ispadmin_request(config: dict, path: str, params: dict | None = None):
    response = requests.get(
        config["base_url"] + path,
        params=params,
        headers={
            "Token": config["token"],
            "Accept": "application/json",
        },
        timeout=config["timeout"],
        verify=config["verify"],
    )
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if response.status_code >= 400:
        detail = ""
        if isinstance(payload, dict):
            detail = payload.get("error") or payload.get("message") or json.dumps(payload, ensure_ascii=False)
        elif payload is not None:
            detail = json.dumps(payload, ensure_ascii=False)
        else:
            detail = response.text[:400]
        raise RuntimeError(f"ISPAdmin API {path} failed ({response.status_code}): {detail}")
    return payload


def _ispadmin_status_map(config: dict) -> dict[int, str]:
    payload = _ispadmin_request(config, "/client-statuses")
    out = {}
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            try:
                key = int(item.get("id"))
            except Exception:
                continue
            out[key] = str(item.get("name") or "").strip()
    return out


def _normalize_ispadmin_client_rows(env: dict, active: int) -> list[dict]:
    config = _ispadmin_config(env)
    statuses = _ispadmin_status_map(config)
    payload = _ispadmin_request(config, "/clients", params={"active": active})
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected ISPAdmin response for /clients.")

    rows = []
    for _, item in sorted(payload.items(), key=lambda kv: str(kv[0])):
        if not isinstance(item, dict):
            continue
        address = item.get("contactAddress") or {}
        if not isinstance(address, dict):
            address = {}
        state_id = item.get("state")
        try:
            state_id_int = int(state_id)
        except Exception:
            state_id_int = None
        rows.append(
            {
                "source_id": str(item.get("id") or "").strip(),
                "login": str(item.get("clientNumber") or "").strip(),
                "name": str(item.get("name") or "").strip(),
                "status": statuses.get(state_id_int, str(state_id or "")).strip(),
                "ico": str(item.get("ic") or "").strip(),
                "dic": str(item.get("dic") or "").strip(),
                "street": str(address.get("street") or "").strip(),
                "house": str(address.get("buildingNumber") or "").strip(),
                "city": str(address.get("city") or "").strip(),
                "zip": str(address.get("zip") or "").strip(),
                "email": str(item.get("email") or "").strip(),
                "billing_email": str(item.get("billingEmail") or "").strip(),
                "technical_email": str(item.get("technicalEmail") or "").strip(),
                "business_email": str(item.get("businessEmail") or "").strip(),
                "mobile": str(item.get("mobile") or "").strip(),
                "phone": str(item.get("phone") or "").strip(),
                "fax": str(item.get("fax") or "").strip(),
            }
        )
    return rows


def _normalize_ispadmin_deactivation_rows(env: dict) -> list[dict]:
    rows = []
    for item in _normalize_ispadmin_client_rows(env, active=0):
        rows.append(
            {
                "source_id": item.get("source_id", ""),
                "login": (item.get("login") or "").strip(),
            }
        )
    return rows


def _normalize_ispadmin_import_rows(env: dict) -> list[dict]:
    return _normalize_ispadmin_client_rows(env, active=1)


def _prepare_api_source() -> dict:
    env = _load_env(SECRETS_ENV)
    deact_rows = _normalize_ispadmin_deactivation_rows(env)
    import_rows = _normalize_ispadmin_import_rows(env)
    _write_json(API_DEACT_FILE, deact_rows)
    _write_json(API_IMPORT_FILE, import_rows)
    spec = {
        "mode": "api",
        "label": "ISPAdmin API",
        "prepared_at": datetime.now(UTC).isoformat(),
        "deactivate_path": str(API_DEACT_FILE),
        "import_path": str(API_IMPORT_FILE),
        "summary": {
            "deactivate_count": len(deact_rows),
            "import_count": len(import_rows),
        },
    }
    _save_prepared_source(spec)
    return spec


def _load_source_rows(spec: dict) -> tuple[list[dict], list[dict]]:
    if spec.get("mode") != "api":
        raise RuntimeError(f"Unsupported source mode: {spec.get('mode')}")
    deact_path = Path(spec.get("deactivate_path", ""))
    import_path = Path(spec["import_path"])
    deact_rows = _load_json(deact_path) if deact_path.exists() else []
    import_rows = _load_json(import_path)
    if not isinstance(deact_rows, list) or not isinstance(import_rows, list):
        raise RuntimeError("Prepared ISPAdmin snapshot is invalid.")
    return deact_rows, import_rows


def _start_job(reason: str) -> bool:
    source_spec = {
        "mode": "api",
        "label": "ISPAdmin API",
        "prepared_at": datetime.now(UTC).isoformat(),
        "deactivate_path": None,
        "import_path": None,
        "summary": {},
    }
    keepalive_required = reason == "manual"

    with STATE.lock:
        if STATE.running:
            return False
        STATE.running = True
        STATE.reset()
        STATE.current["started_at"] = datetime.now(UTC).isoformat()
        STATE.current["stage"] = "starting"
        STATE.current["percent"] = 1
        STATE.current["last_keepalive"] = time.time() if keepalive_required else None
        STATE.current["keepalive_required"] = keepalive_required
        STATE.current["run_reason"] = reason
        STATE.current["source_mode"] = source_spec.get("mode")
        STATE.current["source_label"] = source_spec.get("label")

    thread = threading.Thread(target=_run_job, args=(source_spec, reason), daemon=True)
    thread.start()
    return True


def _run_job(source_spec: dict, run_reason: str):

    report = {
        "started_at": datetime.now(UTC).isoformat(),
        "run_reason": run_reason,
        "source_mode": source_spec.get("mode"),
        "source_label": source_spec.get("label"),
        "source_summary": source_spec.get("summary") or {},
        "deactivated": [],
        "deactivate_skipped": [],
        "deactivate_errors": [],
        "created": [],
        "import_skipped": [],
        "import_errors": [],
        "stopped_on_error": False,
    }

    session = None
    url = None
    verify = True

    try:
        _append_log(f"Job starting ({run_reason})")
        if source_spec.get("mode") == "api":
            _set_progress("source_prepare", 1)
            _append_log("Loading client snapshots from ISPAdmin API...")
            source_spec = _prepare_api_source()
            with STATE.lock:
                STATE.current["source_mode"] = source_spec.get("mode")
                STATE.current["source_label"] = source_spec.get("label")

        _set_progress("source_load", 1)
        _append_log(f"Preparing source rows ({source_spec.get('label')})")
        deact_rows, import_rows = _load_source_rows(source_spec)
        report["source_summary"] = {
            "deactivate_count": len(deact_rows),
            "import_count": len(import_rows),
        }

        env = _load_env(SECRETS_ENV)
        base = env["CIBS_BASE_URL_PROD"] if env.get("CIBS_ENV", "test") == "prod" else env["CIBS_BASE_URL_TEST"]
        url = base.rstrip("/") + "/ws_cibs.php"
        verify = env.get("CIBS_VERIFY_TLS", "true").lower() == "true"
        user, pwd = env["CIBS_USERNAME"], env["CIBS_PASSWORD"]
        ct = "252"

        _set_progress("login", 1)
        _append_log("Login to Mango SOAP")
        _raise_if_cancelled()
        _, login_resp = _soap_call(
            url,
            verify,
            "ws_session_login",
            f"<login><![CDATA[{user}]]></login><password><![CDATA[{pwd}]]></password>",
            120,
        )
        session = _get("session", login_resp)
        if not session:
            raise RuntimeError("Login failed")

        _soap_call(url, verify, "ws_session_set_ct", f"<session><![CDATA[{session}]]></session><ct>{ct}</ct>", 120)

        _set_progress("export_active", 3)
        _append_log("Loading active users export from Mango (this can take 1-3 minutes)...")
        _raise_if_cancelled()
        code, resp = _soap_call(
            url,
            verify,
            "ws_users_export",
            f"<session><![CDATA[{session}]]></session><services_state>ACTIVE</services_state>",
            300,
        )
        result = _get("result", resp)
        if code >= 400 or not result or (result.startswith("-") and result != ""):
            raise RuntimeError(f"ws_users_export failed http={code} result={result[:100]}")

        exported_xml = html.unescape(result)
        active_users = {}
        for match in re.finditer(r"<customer\s+([^>]+)/?>", exported_xml, re.S):
            attrs = dict(re.findall(r"(\w+)=\"(.*?)\"", match.group(1)))
            if (attrs.get("active_customer", "1").strip() or "1") != "1":
                continue
            login = (attrs.get("main_login") or "").strip()
            if not login:
                continue
            active_users[login] = {
                "id": attrs.get("user_id", "").strip(),
                "type": attrs.get("type", "S").strip() or "S",
                "first_name": attrs.get("first_name", "").strip() or "-",
                "last_name": attrs.get("last_name", "").strip() or "-",
                "company_name": attrs.get("company_name", "").strip(),
            }
        _append_log(f"Active Mango users loaded: {len(active_users)}")

        known_existing_logins = set(active_users.keys())
        total_steps = max(1, len(deact_rows) + len(import_rows))
        done = 0
        has_deactivation = bool(deact_rows)

        if has_deactivation:
            _set_progress("deactivation", 5)
            for index, row in enumerate(deact_rows, 1):
                _raise_if_cancelled()
                login = (row.get("login") or "").strip()
                source_id = row.get("source_id", "")
                if not login:
                    report["deactivate_skipped"].append({"reason": "no_login", "source_id": source_id})
                else:
                    user_info = active_users.get(login)
                    if not user_info:
                        report["deactivate_skipped"].append(
                            {"reason": "not_found_or_already_inactive", "main_login": login, "source_id": source_id}
                        )
                    else:
                        type_val = user_info["type"]
                        first_name = user_info["first_name"] if type_val == "S" else ""
                        surname = user_info["last_name"] if type_val == "S" else ""
                        company = user_info["company_name"] if type_val == "P" else ""
                        edit_xml = (
                            f"<user><id>{_esc(user_info['id'])}</id><active>0</active><type>{_esc(type_val)}</type>"
                            f"<firstname>{_esc(first_name)}</firstname><surname>{_esc(surname)}</surname><company>{_esc(company)}</company></user>"
                        )
                        code, response = _soap_call(
                            url,
                            verify,
                            "ws_user_edit",
                            f"<session><![CDATA[{session}]]></session>{edit_xml}",
                            180,
                        )
                        fault = _get("faultstring", response)
                        result = _get("result", response) or _get("return", response)
                        if fault or code >= 400 or (result.startswith("-") and result != ""):
                            report["deactivate_errors"].append(
                                {
                                    "main_login": login,
                                    "source_id": source_id,
                                    "mango_user_id": user_info["id"],
                                    "http": code,
                                    "fault": fault,
                                    "result": result,
                                }
                            )
                        else:
                            report["deactivated"].append(
                                {"main_login": login, "source_id": source_id, "mango_user_id": user_info["id"]}
                            )

                done += 1
                if index == 1 or index % 25 == 0:
                    _append_log(
                        "deact "
                        f"{index}/{len(deact_rows)} | deactivated={len(report['deactivated'])} "
                        f"skipped={len(report['deactivate_skipped'])} errors={len(report['deactivate_errors'])}"
                    )
                _set_progress("deactivation", int(5 + (done / total_steps) * 45))

        import_base = 50 if has_deactivation else 5
        import_span = 50 if has_deactivation else 95
        _set_progress("import", import_base)
        for index, row in enumerate(import_rows, 1):
            _raise_if_cancelled()
            login = (row.get("login") or "").strip()
            source_id = row.get("source_id", "")
            if not login:
                report["import_skipped"].append({"reason": "no_login", "source_id": source_id, "name": row.get("name", "")})
            elif login in known_existing_logins:
                report["import_skipped"].append({"reason": "already_exists", "main_login": login, "source_id": source_id})
            else:
                _, chk = _soap_call(
                    url,
                    verify,
                    "ws_users_list",
                    f"<session><![CDATA[{session}]]></session><search><login>{_esc(login)}</login></search>",
                )
                exists = bool(re.search(fr"<login[^>]*>{re.escape(login)}</login>", chk))
                if exists:
                    known_existing_logins.add(login)
                    report["import_skipped"].append({"reason": "already_exists", "main_login": login, "source_id": source_id})
                else:
                    name = (row.get("name") or "").strip()
                    ico = (row.get("ico") or "").strip()
                    dic = (row.get("dic") or "").strip()
                    is_company = bool(ico or dic)
                    type_val = "P" if is_company else "S"
                    if is_company:
                        company, first_name, surname = name, "", ""
                    else:
                        parts = name.split()
                        first_name = parts[0] if parts else "-"
                        surname = " ".join(parts[1:]) if len(parts) > 1 else "-"
                        company = ""

                    street, house = _split_street_house(row.get("street", ""), row.get("house", ""))
                    city = (row.get("city") or "").strip()
                    zip_code = (row.get("zip") or "").strip()
                    mail = _first_email(row)
                    phones = _phones(row)
                    status = (row.get("status") or "").strip().lower()
                    active = "1" if ("aktiv" in status or status == "") else "0"
                    varsym = login if login.isdigit() else ""

                    user_xml = (
                        "<user>"
                        f"<active>{active}</active><mail_info_flag>1</mail_info_flag><login>{_esc(login)}</login><type>{type_val}</type>"
                        f"<firstname>{_esc(first_name)}</firstname><surname>{_esc(surname)}</surname><company>{_esc(company)}</company>"
                        f"<address><street>{_esc(street)}</street><house_id>{_esc(house)}</house_id><city>{_esc(city)}</city><zip>{_esc(zip_code)}</zip></address>"
                        "<delivery_address_as_default>0</delivery_address_as_default>"
                        f"<delivery_address><street>{_esc(street)}</street><house_id>{_esc(house)}</house_id><city>{_esc(city)}</city><zip>{_esc(zip_code)}</zip></delivery_address>"
                        "<billing_address_as_default>0</billing_address_as_default><billing_address_as_delivery>1</billing_address_as_delivery>"
                        f"<billing_address><street>{_esc(street)}</street><house_id>{_esc(house)}</house_id><city>{_esc(city)}</city><zip>{_esc(zip_code)}</zip></billing_address>"
                        f"<phones>{_esc(phones)}</phones><mail>{_esc(mail)}</mail><agreement>{_esc(login)}</agreement><varsym>{_esc(varsym)}</varsym><ico>{_esc(ico)}</ico><dic>{_esc(dic)}</dic>"
                        "</user>"
                    )
                    code, response = _soap_call(
                        url,
                        verify,
                        "ws_user_create",
                        f"<session><![CDATA[{session}]]></session>{user_xml}",
                        200,
                    )
                    fault = _get("faultstring", response)
                    result = _get("result", response) or _get("return", response)
                    if fault or code >= 400 or (result.startswith("-") and result != ""):
                        report["import_errors"].append(
                            {
                                "main_login": login,
                                "source_id": source_id,
                                "http": code,
                                "fault": fault,
                                "result": result,
                                "mail_used": mail,
                            }
                        )
                    else:
                        known_existing_logins.add(login)
                        report["created"].append(
                            {"main_login": login, "source_id": source_id, "mango_user_id": result}
                        )

            done += 1
            if index == 1 or index % 25 == 0:
                _append_log(
                    "import "
                    f"{index}/{len(import_rows)} | created={len(report['created'])} "
                    f"skipped={len(report['import_skipped'])} errors={len(report['import_errors'])}"
                )
            _set_progress("import", int(import_base + (done / total_steps) * import_span))

        if session:
            _soap_call(url, verify, "ws_session_logout", f"<session><![CDATA[{session}]]></session>", 120)
            session = None

        report["finished_at"] = datetime.now(UTC).isoformat()
        report["deactivated_count"] = len(report["deactivated"])
        report["deactivate_skipped_count"] = len(report["deactivate_skipped"])
        report["deactivate_errors_count"] = len(report["deactivate_errors"])
        report["created_count"] = len(report["created"])
        report["import_skipped_count"] = len(report["import_skipped"])
        report["import_errors_count"] = len(report["import_errors"])

        report_path = REPORTS_DIR / f"run-{int(time.time())}.json"
        _write_json(report_path, report)

        with STATE.lock:
            STATE.current["summary"] = {
                "source_mode": source_spec.get("mode"),
                "deactivated_count": report["deactivated_count"],
                "deactivate_skipped_count": report["deactivate_skipped_count"],
                "deactivate_errors_count": report["deactivate_errors_count"],
                "created_count": report["created_count"],
                "import_skipped_count": report["import_skipped_count"],
                "import_errors_count": report["import_errors_count"],
            }
            STATE.current["report_path"] = str(report_path)
            STATE.current["finished_at"] = datetime.now(UTC).isoformat()
            STATE.current["percent"] = 100
            STATE.current["stage"] = "done"
        _append_log(
            "Summary: "
            f"deactivated={report['deactivated_count']} "
            f"deact_skipped={report['deactivate_skipped_count']} "
            f"deact_errors={report['deactivate_errors_count']} "
            f"created={report['created_count']} "
            f"import_skipped={report['import_skipped_count']} "
            f"import_errors={report['import_errors_count']}"
        )
        _append_log(f"Done. Report: {report_path}")

    except Exception as exc:
        cancelled = str(exc) == "RUN_CANCELLED_BY_USER"
        with STATE.lock:
            STATE.current["error"] = None if cancelled else str(exc)
            STATE.current["stage"] = "cancelled" if cancelled else "error"
            STATE.current["finished_at"] = datetime.now(UTC).isoformat()
        if cancelled:
            _append_log("Run cancelled by user.")
        else:
            _append_log(f"ERROR: {exc}")
    finally:
        if session and url:
            try:
                _soap_call(url, verify, "ws_session_logout", f"<session><![CDATA[{session}]]></session>", 120)
            except Exception:
                pass
        _cleanup_uploads()
        _append_log("Prepared source cleaned (data/uploads)")
        with STATE.lock:
            STATE.running = False
            STATE.current["cancel_requested"] = False


threading.Thread(target=_scheduler_loop, daemon=True).start()


@app.get("/", response_class=HTMLResponse)
def index():
    return (BASE_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/sync-logo.svg")
def sync_logo():
    return FileResponse(BASE_DIR / "sync-logo.svg", media_type="image/svg+xml")


@app.post("/api/start")
def start_job():
    if not _start_job("manual"):
        raise HTTPException(status_code=409, detail="Run already in progress")
    return {"ok": True, "source": _source_status(), "schedule": _schedule_status()}


@app.post("/api/schedule")
def update_schedule(payload: dict = Body(...)):
    updated = _normalize_schedule(payload)
    updated["last_auto_started_at"] = _schedule_status().get("last_auto_started_at")
    if updated["enabled"]:
        next_run = _next_run_at(datetime.now(), _parse_schedule_time(updated["time"]), updated["interval_hours"])
        updated["next_run_at"] = next_run.isoformat()
    else:
        updated["next_run_at"] = None

    with SCHEDULE_LOCK:
        SCHEDULE_STATE.update(updated)
        _save_schedule(SCHEDULE_STATE)
        current = dict(SCHEDULE_STATE)
    return {"ok": True, "schedule": current}


@app.post("/api/stop")
def stop_job(confirm: str = ""):
    if str(confirm).strip().upper() != "STOP":
        raise HTTPException(status_code=400, detail="Confirmation required: confirm=STOP")
    with STATE.lock:
        if not STATE.running:
            raise HTTPException(status_code=409, detail="No running job")
        STATE.current["cancel_requested"] = True
    _append_log("Stop requested by user. Waiting for safe stop point...")
    return {"ok": True}


@app.post("/api/keepalive")
def keepalive():
    with STATE.lock:
        running = STATE.running
    if running:
        _touch_keepalive()
    return {"ok": True, "running": running}


@app.post("/api/disconnect")
def disconnect(stop: bool = False):
    with STATE.lock:
        running = STATE.running
        keepalive_required = bool(STATE.current.get("keepalive_required"))
        if running:
            STATE.current["last_keepalive"] = time.time()
            if stop and keepalive_required:
                STATE.current["cancel_requested"] = True
    if running and stop and keepalive_required:
        _append_log("Client disconnected/unloaded. Auto-stop requested.")
    return {"ok": True, "running": running}


@app.get("/api/reports")
def reports_list(limit: int = 50):
    files = sorted(REPORTS_DIR.glob("run-*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    out = []
    for path in files[: max(1, min(limit, 200))]:
        stat = path.stat()
        out.append({"name": path.name, "path": str(path), "size": stat.st_size, "mtime": stat.st_mtime})
    return {"reports": out}


@app.get("/api/reports/{name}")
def report_get(name: str):
    if "/" in name or ".." in name or not name.endswith(".json"):
        raise HTTPException(status_code=400, detail="Invalid report name")
    path = REPORTS_DIR / name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Report not found")
    try:
        data = _load_json(path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Cannot parse report: {exc}")
    return {"name": name, "data": data}


@app.get("/api/reports/{name}/download")
def report_download(name: str):
    if "/" in name or ".." in name or not name.endswith(".json"):
        raise HTTPException(status_code=400, detail="Invalid report name")
    path = REPORTS_DIR / name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Report not found")
    return FileResponse(path, media_type="application/json", filename=name)


@app.get("/api/status")
def status():
    with STATE.lock:
        payload = {"running": STATE.running, **STATE.current}
    payload["prepared_source"] = _source_status()
    payload["schedule"] = _schedule_status()
    return JSONResponse(payload)


@app.get("/api/events")
def events():
    def gen():
        last_sig = None
        while True:
            with STATE.lock:
                payload = {"running": STATE.running, **STATE.current}
            payload["prepared_source"] = _source_status()
            payload["schedule"] = _schedule_status()
            raw = json.dumps(payload, ensure_ascii=False)
            sig = (
                payload.get("running"),
                payload.get("stage"),
                payload.get("percent"),
                len(payload.get("log", [])),
                payload.get("error"),
                payload.get("report_path"),
                payload.get("cancel_requested"),
                payload.get("run_reason"),
                payload.get("keepalive_required"),
                json.dumps(payload.get("prepared_source"), ensure_ascii=False, sort_keys=True),
                json.dumps(payload.get("schedule"), ensure_ascii=False, sort_keys=True),
            )
            if sig != last_sig:
                last_sig = sig
                yield f"data: {raw}\n\n"
            else:
                yield ": ping\n\n"
            time.sleep(1)

    return StreamingResponse(gen(), media_type="text/event-stream")
