import os
import time
import json
from typing import Any, Dict, List, Optional, Set
import requests

# =========================
# Config from ENV
# =========================
API_URL = os.getenv("API_URL", "https://my.exnessaffiliates.com/api/reports/clients/").strip()
JWT_TOKEN = os.getenv("EXNESS_JWT", "").strip()  # ต้องขึ้นต้นด้วย "JWT "
TG_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# 0 = run once (เหมาะกับ cron / Railway job), >0 = loop (เหมาะกับ Railway service)
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "0"))
# เก็บ snapshot ล่าสุด (ใช้ volume /data เพื่อกันแจ้งซ้ำตอนรีสตาร์ต)
STATE_FILE = os.getenv("STATE_FILE", "state_clients.json").strip()

# ครั้งแรกที่ยังไม่มี state ถ้าตั้ง true จะไม่ส่งข้อความ "เริ่มเฝ้าดูแล้ว"
FIRST_RUN_SILENT = os.getenv("FIRST_RUN_SILENT", "false").lower() in ("1", "true", "yes")

if not JWT_TOKEN or not TG_TOKEN or not TG_CHAT_ID:
    raise SystemExit("❌ Missing ENV: EXNESS_JWT, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID")

HEADERS = {
    "Accept": "application/json",
    "Authorization": JWT_TOKEN,  # format: "JWT <token>"
}


# =========================
# Utilities
# =========================
def send_tg(text: str):
    """ส่งข้อความไป Telegram"""
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    data = {"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        r = requests.post(url, data=data, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"⚠️ send_tg error: {e}")


def robust_get(url: str, params: Optional[Dict[str, Any]] = None, retries: int = 3, timeout: int = 30) -> requests.Response:
    """GET พร้อม retry และ error handling พื้นฐาน"""
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
    """รองรับผลลัพธ์หลายรูปแบบ: list, {results:[]}, {data:[]}, หรือ object เดี่ยว"""
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
    """ดึงทุกหน้า ถ้ามี 'next' (สไตล์ Django REST Framework)"""
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


def extract_accounts(rows: List[Dict[str, Any]]) -> Set[str]:
    """ดึง set ของ client_account (แปลงเป็นสตริงและ trim)"""
    s: Set[str] = set()
    for r in rows:
        val = r.get("client_account")
        if val is not None:
            s.add(str(val).strip())
    return s


def load_state() -> Set[str]:
    """อ่าน snapshot ล่าสุดจากไฟล์"""
    if not os.path.exists(STATE_FILE):
        return set()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(map(str, data.get("client_accounts", [])))
    except Exception:
        return set()


def save_state(accounts: Set[str]):
    """บันทึก snapshot ปัจจุบันลงไฟล์"""
    payload = {"client_accounts": sorted(list(accounts))}
    os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ save_state error: {e}")


# =========================
# Core logic
# =========================
def check_and_notify():
    rows = fetch_all_clients()
    current = extract_accounts(rows)
    previous = load_state()

    total_now = len(current)  # จำนวนรวมปัจจุบัน

    if not previous:
        save_state(current)
        if not FIRST_RUN_SILENT:
            send_tg(f"📊 เริ่มเฝ้าดูรายชื่อ client_account\nจำนวนปัจจุบัน: {total_now} accounts")
        return

    new_accounts = sorted(list(current - previous))
    missing_accounts = sorted(list(previous - current))

    if not new_accounts and not missing_accounts:
        print("No changes.")
        return

    msgs = []
    if new_accounts:
        if len(new_accounts) <= 20:
            msgs.append("🆕 พบ client_account ใหม่:\n" + "\n".join(f"• {a}" for a in new_accounts))
        else:
            msgs.append(f"🆕 พบ client_account ใหม่ {len(new_accounts)} รายการ")

    if missing_accounts:
        if len(missing_accounts) <= 20:
            msgs.append("🗑️ รายการที่หายไปจากลิสต์:\n" + "\n".join(f"• {a}" for a in missing_accounts))
        else:
            msgs.append(f"🗑️ client_account ที่หายไป {len(missing_accounts)} รายการ")

    # สรุปจำนวนรวมปัจจุบัน
    msgs.append(f"📊 จำนวน client_account ปัจจุบัน: {total_now}")

    send_tg("\n\n".join(msgs))
    save_state(current)


def main():
    if POLL_SECONDS > 0:
        send_tg(f"⏱️ บอทเริ่มทำงาน (ตรวจทุก {POLL_SECONDS} วินาที)")
        while True:
            try:
                check_and_notify()
            except Exception as e:
                print(f"❌ loop error: {e}")
            time.sleep(POLL_SECONDS)
    else:
        # รันครั้งเดียว (เหมาะกับ cron/Job)
        check_and_notify()


if __name__ == "__main__":
    main()
