import os
import time
import json
from typing import Any, Dict, List, Optional, Set
import requests

# =========================
# Config from ENV
# =========================
API_URL = os.getenv("API_URL", "https://my.exnessaffiliates.com/api/reports/clients/").strip()
JWT_TOKEN = os.getenv("EXNESS_JWT", "").strip()         # ต้องขึ้นต้นด้วย "JWT "
TG_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Google Sheets
GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GSHEET_NAME = os.getenv("GSHEET_NAME", "clients_snapshot").strip()
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()  # ทางเลือกถ้าไม่มีไฟล์

# 0 = run once (เหมาะกับ cron / Railway job), >0 = loop (เหมาะกับ Railway service)
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "0"))
# เก็บ snapshot ล่าสุดของ client_account เพื่อเทียบหาของใหม่
STATE_FILE = os.getenv("STATE_FILE", "state_clients.json").strip()

# ครั้งแรกที่ยังไม่มี state ถ้าตั้ง true จะไม่ส่งข้อความเริ่มงาน
FIRST_RUN_SILENT = os.getenv("FIRST_RUN_SILENT", "false").lower() in ("1", "true", "yes")

if not JWT_TOKEN or not TG_TOKEN or not TG_CHAT_ID:
    raise SystemExit("❌ Missing ENV: EXNESS_JWT, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID")

HEADERS = {
    "Accept": "application/json",
    "Authorization": JWT_TOKEN,  # format: "JWT <token>"
}

# =========================
# Telegram
# =========================
def send_tg(text: str):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    data = {"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        r = requests.post(url, data=data, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"⚠️ send_tg error: {e}")

# =========================
# API helpers
# =========================
def robust_get(url: str, params: Optional[Dict[str, Any]] = None, retries: int = 3, timeout: int = 30) -> requests.Response:
    for i in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 401:
                raise SystemExit("❌ Unauthorized (401) – ตรวจ EXNESS_JWT (ต้องมีคำว่า 'JWT ' นำหน้า)")
            time.sleep(1.2 * i)
        except requests.RequestException as e:
            if i == retries:
                raise
            time.sleep(1.2 * i)
    raise SystemExit("❌ GET failed after retries")

def normalize_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            return [x for x in payload["results"] if isinstance(x, dict)]
        if isinstance(payload.get("data"), list):
            return [x for x in payload["data"] if isinstance(x, dict)]
        return [payload]
    return []

def fetch_all_clients() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    next_url = API_URL
    params = None
    while next_url:
        resp = robust_get(next_url, params=params)
        try:
            payload = resp.json()
        except json.JSONDecodeError:
            raise SystemExit("❌ Response is not JSON")
        part = normalize_rows(payload)
        rows.extend(part)
        params = None
        if isinstance(payload, dict) and payload.get("next"):
            next_url = payload["next"]
        else:
            break
    return rows

# =========================
# State (for new detection)
# =========================
def extract_accounts(rows: List[Dict[str, Any]]) -> Set[str]:
    s: Set[str] = set()
    for r in rows:
        val = r.get("client_account")
        if val is not None:
            s.add(str(val).strip())
    return s

def load_state() -> Set[str]:
    if not os.path.exists(STATE_FILE):
        return set()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(map(str, data.get("client_accounts", [])))
    except Exception:
        return set()

def save_state(accounts: Set[str]):
    payload = {"client_accounts": sorted(list(accounts))}
    os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ save_state error: {e}")

# =========================
# Google Sheets
# =========================
def ensure_gspread_client():
    """รองรับทั้ง GOOGLE_APPLICATION_CREDENTIALS (ไฟล์) หรือ GOOGLE_SERVICE_ACCOUNT_JSON (สตริง)"""
    import gspread
    from google.oauth2.service_account import Credentials

    creds_path = GOOGLE_CREDS_PATH
    if not creds_path:
        if GOOGLE_CREDS_JSON:
            # เขียนคีย์ลงไฟล์ชั่วคราวในคอนเทนเนอร์
            creds_path = "/tmp/google_sa.json"
            try:
                os.makedirs("/tmp", exist_ok=True)
                with open(creds_path, "w", encoding="utf-8") as f:
                    f.write(GOOGLE_CREDS_JSON)
            except Exception as e:
                raise SystemExit(f"❌ Cannot write GOOGLE_SERVICE_ACCOUNT_JSON to /tmp: {e}")
        else:
            raise SystemExit("❌ Missing Google credentials: set either GOOGLE_APPLICATION_CREDENTIALS (file path) or GOOGLE_SERVICE_ACCOUNT_JSON (json string)")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc

def unique_columns(rows: List[Dict[str, Any]]) -> List[str]:
    cols = set()
    for r in rows:
        cols.update(r.keys())
    preferred = [
        "id", "partner_account", "partner_account_name", "client_uid",
        "client_account", "client_account_type", "country", "currency",
        "reg_date", "trade_finish", "volume_lots", "volume_usd",
        "reward", "reward_usd", "comment",
    ]
    rest = [c for c in sorted(cols) if c not in preferred]
    return [c for c in preferred if c in cols] + rest

def write_snapshot_to_gsheet(rows: List[Dict[str, Any]]):
    """เขียนสแน็ปชอตลงชีต: เคลียร์แล้วเขียนใหม่ทั้งหมด (ง่ายและชัด)"""
    if not GSHEET_ID:
        print("⚠️ GSHEET_ID not set; skip writing to Google Sheet.")
        return

    gc = ensure_gspread_client()
    sh = gc.open_by_key(GSHEET_ID)

    # หา/สร้าง worksheet
    try:
        ws = sh.worksheet(GSHEET_NAME)
        ws.clear()
    except Exception:
        ws = sh.add_worksheet(title=GSHEET_NAME, rows="100", cols="26")

    if not rows:
        ws.update("A1", [["No data"]])
        print("✅ Sheet updated (no rows).")
        return

    cols = unique_columns(rows)
    data = [cols]
    for r in rows:
        data.append([r.get(c, "") for c in cols])

    ws.update("A1", data)
    print(f"✅ Wrote {len(rows)} rows to sheet '{GSHEET_NAME}'.")

# =========================
# Core flow
# =========================
def check_export_and_notify():
    """
    ลำดับ: ดึง -> เขียน Google Sheet -> แจ้ง 'เฉพาะรายการใหม่' (เทียบ state เดิม) -> อัปเดต state
    """
    rows = fetch_all_clients()
    current = extract_accounts(rows)
    previous = load_state()
    total_now = len(current)

    # 1) เขียน Google Sheet สแน็ปชอตล่าสุด
    write_snapshot_to_gsheet(rows)

    # 2) แจ้งเตือน (เฉพาะ 'ใหม่')
    if not previous:
        save_state(current)
        if not FIRST_RUN_SILENT:
            send_tg(f"📊 เริ่มบันทึกข้อมูลลง Google Sheet และเฝ้าดูรายการใหม่\nจำนวน client_account ปัจจุบัน: {total_now}")
        return

    new_accounts = sorted(list(current - previous))
    if new_accounts:
        if len(new_accounts) <= 30:
            msg = "🆕 พบ client_account ใหม่:\n" + "\n".join(f"• {a}" for a in new_accounts)
        else:
            msg = f"🆕 พบ client_account ใหม่ {len(new_accounts)} รายการ"
        msg += f"\n\n📊 จำนวน client_account ปัจจุบัน: {total_now}"
        send_tg(msg)
    else:
        print("No new accounts.")

    # 3) อัปเดต state หลังแจ้งเสร็จ
    save_state(current)

def main():
    if POLL_SECONDS > 0:
        send_tg(f"⏱️ บอทเริ่มทำงาน (ตรวจทุก {POLL_SECONDS} วินาที)")
        while True:
            try:
                check_export_and_notify()
            except Exception as e:
                print(f"❌ loop error: {e}")
            time.sleep(POLL_SECONDS)
    else:
        check_export_and_notify()

if __name__ == "__main__":
    main()
