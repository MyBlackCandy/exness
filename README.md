# Client Accounts Watcher (Telegram Bot)

บอทสำหรับเฝ้าดูรายการ **client_account** จาก API (Exness affiliates endpoint) และ **แจ้งเตือนอัตโนมัติ** เมื่อ:
- มีบัญชี **ใหม่** โผล่เข้ามา
- มีบัญชี **หายไป** จากลิสต์
- พร้อมสรุป **จำนวนทั้งหมด ณ ขณะนั้น**

รองรับการรันแบบ:
- **ครั้งเดียว** (เหมาะกับ cron / Railway Job)
- **ทำงานต่อเนื่อง** ตรวจทุก N วินาที (เหมาะกับ Railway Service)

## โครงสร้างโปรเจกต์
repo/
├─ main.py
└─ requirements.txt

> ไม่ต้องมี Dockerfile/Procfile — Railway ใช้ Nixpacks ตรวจ Python อัตโนมัติ

---

## Environment Variables

ต้องตั้งค่าบน Railway ( หรือ `.env` ในเครื่องระหว่างทดสอบ ):

- `EXNESS_JWT` : โทเคน JWT สำหรับ API (ต้องมีคำว่า `JWT ` นำหน้า)
- `TELEGRAM_TOKEN` : โทเคนบอทจาก @BotFather
- `TELEGRAM_CHAT_ID` : ไอดีปลายทาง (คน/กลุ่ม/แชนแนล)
- `API_URL` : (เลือกได้) ค่าเริ่มต้น `https://my.exnessaffiliates.com/api/reports/clients/`
- `POLL_SECONDS` : `0` = รันครั้งเดียว, `>0` = ลูป เช่น `300` (ตรวจทุก 5 นาที)
- `STATE_FILE` : (แนะนำให้ตั้งเป็น `/data/state_clients.json`) เพื่อเก็บ snapshot ล่าสุด
- `FIRST_RUN_SILENT` : `true/false` — ถ้า `true` ครั้งแรกที่ยังไม่มี state จะไม่ส่งข้อความเริ่มงาน

---

## ดีพลอยด้วย **GitHub + Railway**

1. Push โค้ดขึ้น GitHub
2. ที่ Railway: **New Project → Deploy from GitHub → เลือก repo**
3. ไปที่ **Settings → Variables** ใส่ ENV ตามด้านบน
4. ไปที่ **Storage → Add Volume**
   - Name: `data`
   - Size: 1GB (พอ)
   - **Mount Path:** `/data`
5. **Start Command** (ถ้าจำเป็น): `python main.py`
6. Deploy

> ใช้ Volume เพื่อให้ไฟล์สถานะ (`STATE_FILE`) อยู่รอดหลังรีสตาร์ต/รีดีพลอย ป้องกันการแจ้งเตือนซ้ำโดยไม่จำเป็น

---

## โหมดการทำงาน

### โหมดลูป (Service)
ตั้ง `POLL_SECONDS=300` แล้ว Railway จะรันต่อเนื่อง ตรวจทุก 5 นาที

### โหมดครั้งเดียว (Job/Cron)
ตั้ง `POLL_SECONDS=0` แล้วสั่งรันด้วย

python main.py
หรือใช้ Railway Jobs / GitHub Actions (schedule) เรียกซ้ำตามเวลาที่ต้องการ

---

## ตัวอย่างข้อความแจ้งเตือน
🆕 พบ client_account ใหม่:
• 193651763
• 14653813

🗑️ รายการที่หายไปจากลิสต์:
• 109083692

📊 จำนวน client_account ปัจจุบัน: 25


---

## เคล็ดลับ

- ห้าม commit โทเคนจริงลง GitHub — เก็บใน Railway Variables เท่านั้น
- ถ้า API มี pagination แบบ `next` โค้ดรองรับแล้ว
- ถ้าไม่อยากให้บอทส่งข้อความตอนเริ่มครั้งแรก ให้ตั้ง `FIRST_RUN_SILENT=true`
- ถ้าจะย้าย state ไปเก็บ **Redis/SQLite** เพื่อรองรับหลายอินสแตนซ์/ชาร์ด บอกได้ เดี๋ยวผมจัดให้

## ทดสอบบนเครื่อง (Local Dev)

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# ตั้ง ENV ชั่วคราว (ตัวอย่าง)
export EXNESS_JWT="JWT xxx.yyy.zzz"
export TELEGRAM_TOKEN="123456:ABC-xyz"
export TELEGRAM_CHAT_ID="-1001234567890"
export STATE_FILE="./state_clients.json"
export POLL_SECONDS=0

python main.py
หากต้องการ log เพิ่ม/ฟิลเตอร์ข้อมูล/รูปแบบข้อความเฉพาะกิจ แจ้งได้ครับ

```

ถ้าต้องการให้ผมอัปโหลดไฟล์ทั้งชุดนี้ขึ้น **GitHub repo** ตัวอย่าง (พร้อม badge และ screenshot logs) หรือเพิ่ม **Dockerfile**/**GitHub Actions (schedule)** ให้ด้วย บอกชื่อ repo/ความถี่ที่ต้องการได้เลย เดี๋ยวจัดต่อให้ครับ 🚀

