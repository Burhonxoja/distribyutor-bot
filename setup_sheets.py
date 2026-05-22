#!/usr/bin/env python3
"""
Google Sheets avtomatik sozlash skripti.
Barcha kerakli varaqlarni va sarlavhalarni yaratadi.
Faqat bir marta ishlatiladi.

Ishlatish:
  python3 setup_sheets.py
"""

import os, json
from google.oauth2.service_account import Credentials
import gspread

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")

if not all([SPREADSHEET_ID, GOOGLE_CREDS_JSON]):
    print("❌ SPREADSHEET_ID va GOOGLE_CREDS_JSON environment variablelarni o'rnating!")
    exit(1)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS_JSON), scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)

SHEETS = {
    "Dokonlar": [
        "ID", "Nomi", "Adres", "MCHJ", "Tel1", "Tel2", "Sotuvchi",
        "Dist_ID", "Dist_Name", "Lat", "Lng", "Channel_Msg_ID", "Sana"
    ],
    "Mahsulotlar_Asosiy": [
        "ID", "Nomi", "Turi", "Birlik",
        "Zavod_Narxi", "Sotish_Narxi_Default", "Status", "Sana"
    ],
    "Mahsulotlar_Dist_Default": [
        "Dist_ID", "Mahsulot_ID", "Nomi", "Turi", "Sotish_Narxi", "Sana"
    ],
    "Mahsulotlar_Maxsus_Narx": [
        "Dist_ID", "Dokon_ID", "Dokon_Nomi",
        "Mahsulot_ID", "Mahsulot", "Turi", "Sotish_Narxi", "Sana"
    ],
    "Qabul": [
        "Sana", "Dist_ID", "Mahsulot", "Turi",
        "Miqdor", "Birlik", "Zavod_Narxi", "Jami", "Status", "Qabul_ID"
    ],
    "Ombor": [
        "Dist_ID", "Mahsulot", "Turi", "Miqdor", "Birlik"
    ],
    "Buyurtmalar": [
        "Sana", "Dist_ID", "Dokon", "Dokon_ID", "Mahsulot", "Turi",
        "Miqdor", "Birlik", "Narx", "Jami", "Status", "Zakaz_ID"
    ],
    "Topshirish": [
        "Sana", "Dist_ID", "Dokon", "Dokon_ID", "Zakaz_ID",
        "Mahsulot", "Turi", "Zakaz_Miqdor", "Topshirish_Miqdor",
        "Vozvrat_Miqdor", "Birlik", "Naqd", "Qarz", "Tarozi",
        "Eslatma_Kun", "Eslatma_Sana", "Qaymoq_Bor", "Izoh", "Status"
    ],
    "Tolov": [
        "Sana", "Dist_ID", "Dokon", "Dokon_ID",
        "Summa", "Turi", "Izoh", "Status"
    ],
}

DEFAULT_PRODUCTS = [
    ["mhs_01", "Tvorog",   "1kg",   "kg",   15000, 18000, "active"],
    ["mhs_02", "Tvorog",   "400gr", "dona",  6000,  7500, "active"],
    ["mhs_03", "Tvorog",   "200gr", "dona",  3000,  3800, "active"],
    ["mhs_04", "Suzma",    "1kg",   "kg",   13000, 16000, "active"],
    ["mhs_05", "Suzma",    "400gr", "dona",  5200,  6500, "active"],
    ["mhs_06", "Suzma",    "200gr", "dona",  2600,  3300, "active"],
    ["mhs_07", "Qaymoq",   "1kg",   "kg",   20000, 25000, "active"],
    ["mhs_08", "Qaymoq",   "0.4kg", "dona",  8000, 10000, "active"],
    ["mhs_09", "Qaymoq",   "0.2kg", "dona",  4000,  5000, "active"],
    ["mhs_10", "Qurt",     "-",     "kg",   50000, 60000, "active"],
    ["mhs_11", "Toshqurt", "kg",    "kg",   55000, 65000, "active"],
    ["mhs_12", "Toshqurt", "dona",  "dona",  5000,  6000, "active"],
    ["mhs_13", "Brinza",   "-",     "dona",  8000, 10000, "active"],
    ["mhs_14", "Yogurt",   "-",     "dona",  3000,  4000, "active"],
]

existing_titles = [ws.title for ws in sh.worksheets()]
print(f"📊 Mavjud varaqlar: {existing_titles}")

for sheet_name, headers in SHEETS.items():
    if sheet_name in existing_titles:
        print(f"  ✅ {sheet_name} — mavjud, o'tkazib yuborildi")
        continue

    ws = sh.add_worksheet(title=sheet_name, rows=2000, cols=len(headers) + 2)
    ws.append_row(headers)
    print(f"  ✅ {sheet_name} — yaratildi ({len(headers)} ustun)")

# Mahsulotlar_Asosiy ga default mahsulotlarni qo'shish
ws_prod = sh.worksheet("Mahsulotlar_Asosiy")
existing = ws_prod.get_all_records()
if not existing:
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    for p in DEFAULT_PRODUCTS:
        ws_prod.append_row(p + [today])
    print(f"\n✅ {len(DEFAULT_PRODUCTS)} ta mahsulot qo'shildi")
else:
    print(f"\n✅ Mahsulotlar_Asosiy da {len(existing)} ta mahsulot mavjud")

print("\n🎉 Sheets sozlash yakunlandi!")
print(f"🔗 Jadval: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
