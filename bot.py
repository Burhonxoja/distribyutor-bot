"""
Alba Milk Distribyutor Bot v4.0
- To'liq inline keyboard
- Barcha funksiyalar to'g'ri ishlaydi
"""
import os, logging, json, re, base64, random
from datetime import datetime, timedelta, time as dtime
import gspread
from google.oauth2.service_account import Credentials
import google.auth.transport.requests
from telegram import (Update, InlineKeyboardMarkup, InlineKeyboardButton,
                      ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove)
from telegram.ext import (Application, CommandHandler, MessageHandler,
                          CallbackQueryHandler, ConversationHandler,
                          filters, ContextTypes)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN         = os.environ.get("BOT_TOKEN", "")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON", "")
SPREADSHEET_ID    = os.environ.get("SPREADSHEET_ID", "")
ADMIN_IDS         = [int(x) for x in os.environ.get("ADMIN_IDS","0").split(",") if x.strip()]

# ── STATES ────────────────────────────────────────────────────────────────────
(
    ST_LANG,
    ST_REG_NAME, ST_REG_FNAME, ST_REG_PHONE, ST_REG_PASSPORT,
    ST_WAIT_APPROVE,
    ST_MAIN,
    ST_ZAVOD_QTY,
    ST_TOP_STORE, ST_TOP_PROD, ST_TOP_PHOTO, ST_TOP_PAY, ST_TOP_NAQD,
    ST_VOZ_PROD, ST_VOZ_QTY,
    ST_ZAKAZ_COMMENT,
    ST_DI_NAME, ST_DI_ADDR, ST_DI_MCHJ, ST_DI_TEL1, ST_DI_TEL2, ST_DI_EGA, ST_DI_PHOTO,
    ST_NARX_VAL, ST_NARX_COST,
    ST_NARX_DOKON_VAL, ST_NARX_DOKON_COST,
    ST_ZAKAZ_FROM_QTY,
    ST_ZAKAZ_EDIT_QTY,
    ST_DOKON_EDIT_VAL,
    ST_ADM_MAHSULOT_UZ, ST_ADM_MAHSULOT_RU, ST_ADM_MAHSULOT_UNIT,
    ST_ADM_NARX_VAL, ST_ADM_NARX_COST,
    ST_ADM_DOKON_NAME, ST_ADM_DOKON_ADDR, ST_ADM_DOKON_DIST,
    ST_ADM_DIST_NAME, ST_ADM_DIST_TG,
    ST_ADM_BROADCAST,
    ST_TOLOV_SUMMA,
) = range(42)


# ── MAHSULOTLAR ───────────────────────────────────────────────────────────────
DEFAULT_PRODUCTS = [
    {"id":1,"uz":"Tvorog","ru":"Tvorog","unit":"kg"},
    {"id":2,"uz":"Sut","ru":"Sut","unit":"litr"},
    {"id":3,"uz":"Qatiq","ru":"Qatiq","unit":"kg"},
    {"id":4,"uz":"Brinza","ru":"Brinza","unit":"kg"},
    {"id":5,"uz":"Qaymoq 0.4","ru":"Qaymoq 0.4","unit":"dona"},
    {"id":6,"uz":"Qaymoq 0.2","ru":"Qaymoq 0.2","unit":"dona"},
    {"id":7,"uz":"Suzma 0.5","ru":"Suzma 0.5","unit":"kg"},
    {"id":8,"uz":"Qurt","ru":"Qurt","unit":"dona"},
    {"id":9,"uz":"Tosh qurt","ru":"Tosh qurt","unit":"dona"},
]

BRINZA_KEYWORDS = ["brinza"]

def is_brinza(name):
    return any(k in str(name).lower() for k in BRINZA_KEYWORDS)

def get_products():
    try:
        recs = db_all("Mahsulotlar")
        if recs:
            return [{"id":int(r.get("ID",0)),"uz":r.get("Nomi_UZ",""),
                     "ru":r.get("Nomi_RU",""),"unit":r.get("Birlik","kg")}
                    for r in recs if str(r.get("Faol","1"))=="1"]
    except Exception: pass
    return DEFAULT_PRODUCTS

def fmt_qty(qty, unit, prod_name="", topshirish=False):
    """
    Miqdorni formatlash:
    Brinza: topshirish=True → kg, topshirish=False → dona
    Boshqalar: unit bo'yicha
    """
    def fmt_float(v, u):
        if v == int(v): return f"{int(v)} {u}"
        s = f"{v:.3f}".rstrip('0').rstrip('.')
        return f"{s} {u}"

    if is_brinza(prod_name):
        if topshirish:
            return fmt_float(float(qty), "kg")  # do'konga berishda kg
        else:
            return f"{int(round(qty))} dona"    # zavod, zakaz, omborda dona
    elif unit == "dona":
        return f"{int(round(qty))} dona"
    elif unit == "kg":   return fmt_float(float(qty), "kg")
    elif unit == "litr": return fmt_float(float(qty), "litr")
    else:
        v = float(qty)
        if v == int(v): return f"{int(v)} {unit}"
        s = f"{v:.3f}".rstrip('0').rstrip('.')
        return f"{s} {unit}"

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
SHEET_HEADERS = {
    "Foydalanuvchilar": ["TG_ID","Ism","Familiya","Telefon","Rol","Til","Passport","Status","Short_ID","Sana"],
    "Mahsulotlar":      ["ID","Nomi_UZ","Nomi_RU","Birlik","Faol","Sana"],
    "Dokonlar":         ["ID","Nomi","Adres","MCHJ","Tel1","Tel2","Ega_Ismi","Dist_ID","Dist_Ism","Sana"],
    "Narxlar":          ["Mahsulot_ID","Mahsulot","Narx","Tannarx","Dist_ID","Dokon_ID","Sana"],
    "Qabul":            ["Sana","Dist_ID","Dist_Ism","Mahsulot","Miqdor","Birlik","Narx","Jami","Status","Qabul_ID"],
    "Topshirish":       ["Sana","Dist_ID","Dokon","Dokon_ID","Mahsulot","Miqdor","Birlik","Narx","Jami","Pay_Type","Naqd","Qarz","Status","Top_ID"],
    "Tolov":            ["Sana","Dist_ID","Dokon","Dokon_ID","Summa","Status","Tolov_ID"],
    "Buyurtmalar":      ["Sana","Dokon_ID","Dokon","Dist_ID","Mahsulot","Miqdor","Status","Izoh","Zakaz_ID"],
    "Vozvrat":          ["Sana","Dist_ID","Dokon","Dokon_ID","Mahsulot","Miqdor","Birlik","Narx","Jami","Status","Voz_ID"],
    "Sozlamalar":       ["Kalit","Qiymat","Sana"],
}

def _get_creds():
    return json.loads(GOOGLE_CREDS_JSON) if GOOGLE_CREDS_JSON else {}

def _get_sheet():
    if not GOOGLE_CREDS_JSON: return None
    try:
        creds = Credentials.from_service_account_info(
            _get_creds(),
            scopes=["https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)
    except Exception as e:
        logger.error(f"Sheet error: {e}"); return None

def get_ws(name):
    wb = _get_sheet()
    if not wb: return None
    try: return wb.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        w = wb.add_worksheet(name, rows=3000, cols=25)
        if name in SHEET_HEADERS: w.append_row(SHEET_HEADERS[name])
        return w
    except Exception as e:
        logger.error(f"get_ws {name}: {e}"); return None

def db_append(tab, row):
    try:
        w = get_ws(tab)
        if w: w.append_row([str(x) for x in row])
    except Exception as e: logger.error(f"db_append {tab}: {e}")

def db_all(tab):
    try:
        w = get_ws(tab)
        return w.get_all_records() if w else []
    except Exception as e: logger.error(f"db_all {tab}: {e}"); return []

def db_update(tab, sc, sv, uc, uv):
    try:
        w = get_ws(tab)
        if not w: return False
        headers = w.row_values(1)
        if uc not in headers: return False
        for i, r in enumerate(w.get_all_records()):
            if str(r.get(sc,"")).strip() == str(sv).strip():
                w.update_cell(i+2, headers.index(uc)+1, str(uv)); return True
    except Exception as e: logger.error(f"db_update {tab}: {e}")
    return False

def db_delete(tab, sc, sv):
    try:
        w = get_ws(tab)
        if not w: return
        for i, r in enumerate(w.get_all_records()):
            if str(r.get(sc,"")).strip() == str(sv).strip():
                w.delete_rows(i+2); return
    except Exception as e: logger.error(f"db_delete {tab}: {e}")

def now_str():   return datetime.now().strftime("%Y-%m-%d %H:%M")
def today_str(): return datetime.now().strftime("%Y-%m-%d")

def make_sid():
    existing = {str(r.get("Short_ID","")) for r in db_all("Foydalanuvchilar")}
    while True:
        sid = str(random.randint(100000,999999))
        if sid not in existing: return sid

def make_id(p=""): return p+datetime.now().strftime("%m%d%H%M%S")+str(random.randint(10,99))

# ── FOYDALANUVCHI ─────────────────────────────────────────────────────────────
def get_user(uid):
    for r in db_all("Foydalanuvchilar"):
        if str(r.get("TG_ID","")).strip()==str(uid).strip(): return r
    return None

def get_user_by_sid(sid):
    for r in db_all("Foydalanuvchilar"):
        if str(r.get("Short_ID","")).strip()==str(sid).strip(): return r
    return None

def is_approved(uid):
    if int(uid) in ADMIN_IDS: return True
    u = get_user(uid)
    return u and str(u.get("Status","")).lower() in ["tasdiqlangan","1"]

def get_sid(uid):
    if int(uid) in ADMIN_IDS: return "ADMIN"
    u = get_user(uid)
    return u.get("Short_ID","?") if u else "?"

def la(ctx): return ctx.user_data.get("lang","uz")

# ── NARX ──────────────────────────────────────────────────────────────────────
def get_price(pid, dist_id=None, dokon_id=None):
    try:
        recs = db_all("Narxlar")
        # 1. Dokon maxsus narxi
        if dist_id and dokon_id:
            for r in recs:
                if (str(r.get("Mahsulot_ID",""))==str(pid) and
                    str(r.get("Dist_ID",""))==str(dist_id) and
                    str(r.get("Dokon_ID",""))==str(dokon_id)):
                    return float(r.get("Narx",0) or 0), float(r.get("Tannarx",0) or 0)
        # 2. Dist narxi
        if dist_id:
            for r in recs:
                if (str(r.get("Mahsulot_ID",""))==str(pid) and
                    str(r.get("Dist_ID",""))==str(dist_id) and
                    not str(r.get("Dokon_ID","")).strip()):
                    return float(r.get("Narx",0) or 0), float(r.get("Tannarx",0) or 0)
        # 3. Umumiy narx
        for r in recs:
            if (str(r.get("Mahsulot_ID",""))==str(pid) and
                not str(r.get("Dist_ID","")).strip() and
                not str(r.get("Dokon_ID","")).strip()):
                return float(r.get("Narx",0) or 0), float(r.get("Tannarx",0) or 0)
    except Exception as e: logger.error(f"get_price: {e}")
    return 0.0, 0.0

def set_price(pid, pname, price, cost, dist_id="", dokon_id=""):
    try:
        w = get_ws("Narxlar")
        if not w: return
        for i,r in enumerate(w.get_all_records()):
            if (str(r.get("Mahsulot_ID",""))==str(pid) and
                str(r.get("Dist_ID",""))==str(dist_id) and
                str(r.get("Dokon_ID",""))==str(dokon_id)):
                w.update(f"A{i+2}:G{i+2}",
                    [[str(pid),pname,str(price),str(cost),str(dist_id),str(dokon_id),now_str()]])
                return
        w.append_row([str(pid),pname,str(price),str(cost),str(dist_id),str(dokon_id),now_str()])
    except Exception as e: logger.error(f"set_price: {e}")

# ── DO'KON ────────────────────────────────────────────────────────────────────
def get_stores(dist_id=None):
    recs = db_all("Dokonlar")
    if dist_id: return [r for r in recs if str(r.get("Dist_ID","")).strip()==str(dist_id).strip()]
    return recs

def get_debt(dokon_id):
    try:
        tops = db_all("Topshirish")
        tolovs = db_all("Tolov")
        voz = db_all("Vozvrat")
        qarz = sum(float(r.get("Qarz",0) or 0) for r in tops
                   if str(r.get("Dokon_ID",""))==str(dokon_id) and r.get("Status","")=="tasdiqlangan")
        paid = sum(float(r.get("Summa",0) or 0) for r in tolovs
                   if str(r.get("Dokon_ID",""))==str(dokon_id) and r.get("Status","")=="tasdiqlangan")
        voz_s = sum(float(r.get("Jami",0) or 0) for r in voz
                    if str(r.get("Dokon_ID",""))==str(dokon_id) and r.get("Status","")=="tasdiqlangan")
        return max(0.0, qarz - paid - voz_s)
    except Exception: return 0.0

def get_ombor(dist_id):
    """
    Brinza: dona saqlanadi (zavod dona, topshirish kg -> dona ga o'tkazilmaydi)
    Boshqalar: birlik bo'yicha
    """
    uid = str(dist_id)
    st = {}
    # Tasdiqlangan va kutilmoqda ham (real vaqt uchun)
    for r in db_all("Qabul"):
        if str(r.get("Dist_ID",""))!=uid: continue
        if r.get("Status","") not in ("tasdiqlangan","kutilmoqda"): continue
        k=r.get("Mahsulot",""); st[k]=st.get(k,0)+float(r.get("Miqdor",0) or 0)
    for r in db_all("Topshirish"):
        if str(r.get("Dist_ID",""))!=uid: continue
        if r.get("Status","") != "tasdiqlangan": continue
        k=r.get("Mahsulot",""); qty=float(r.get("Miqdor",0) or 0)
        # Brinza topshirishda kg, ombordan dona ayirish
        # 1 dona ~ 1 kg deb hisoblaymiz (admin sozlashda)
        st[k]=st.get(k,0)-qty
    for r in db_all("Vozvrat"):
        if str(r.get("Dist_ID",""))!=uid: continue
        if r.get("Status","") != "tasdiqlangan": continue
        k=r.get("Mahsulot",""); st[k]=st.get(k,0)+float(r.get("Miqdor",0) or 0)
    return {k:v for k,v in st.items() if v>0.001}

def parse_num(text):
    t = str(text).strip().replace(" ","").replace(",",".")
    try: return float(t)
    except: return 0.0

def parse_weight(text):
    v = parse_num(text)
    return round(v/1000,3) if v>=100 else round(v,3)

def parse_money(text):
    t = str(text).strip().replace(" ","")
    if re.match(r'^\d+[.,]\d{3}$',t): t = re.sub(r'[.,]','',t)
    else: t = t.replace(",",".")
    try: return float(t)
    except: return 0.0

def clean_phone(text):
    return re.sub(r'[^\d+]','',str(text).strip())

# ── OCR ───────────────────────────────────────────────────────────────────────
async def vision_ocr(image_bytes):
    try:
        import httpx
        creds = Credentials.from_service_account_info(
            _get_creds(), scopes=["https://www.googleapis.com/auth/cloud-vision"])
        creds.refresh(google.auth.transport.requests.Request())
        b64 = base64.b64encode(image_bytes).decode()
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://vision.googleapis.com/v1/images:annotate",
                headers={"Authorization": f"Bearer {creds.token}"},
                json={"requests":[{"image":{"content":b64},"features":[{"type":"TEXT_DETECTION"}]}]})
            return resp.json()["responses"][0].get("fullTextAnnotation",{}).get("text","").strip()
    except Exception as e: logger.error(f"OCR: {e}"); return ""

def parse_scale(text):
    nums = re.findall(r'\d+', str(text))
    if not nums: return 0.0
    v = int(nums[0])
    return round(v/1000,3) if v>=100 else float(v)

# ── SOZLAMALAR ────────────────────────────────────────────────────────────────
def get_setting(key, default="1"):
    for r in db_all("Sozlamalar"):
        if r.get("Kalit","")==key: return str(r.get("Qiymat",default))
    return default

def set_setting(key, val):
    w = get_ws("Sozlamalar")
    if not w: return
    for i,r in enumerate(w.get_all_records()):
        if r.get("Kalit","")==key:
            w.update_cell(i+2,2,str(val)); w.update_cell(i+2,3,now_str()); return
    w.append_row([key,str(val),now_str()])


# ── MOTIVATSIYA ───────────────────────────────────────────────────────────────
MOTIVATSIYA = [
    "🔥 Barakalla! Tovar qabul qilindi. Endi sotuv vaqti — har bir do'kon sizni kutmoqda!",
    "💪 Zo'r! Omboringiz to'ldi, endi harakatga vaqt. Bugun qancha do'konni aylanasiz?",
    "🚀 Ajoyib! Tovar qo'lda — bu muvaffaqiyatning yarmi. Ikkinchi yarmi sotuv!",
    "⚡ Yangi tovar — yangi imkoniyat! Distribyutor sifatida siz bozorni egallab oling!",
    "🏆 Professional distribyutor shunday ishlaydi: tez qabul, tez yetkazib berish. Oldinga!",
    "📈 Har bir tovar yetkazib berish — daromadingizni oshiradi. Ko'proq harakating — ko'proq foyda!",
    "💡 Aqlli distribyutor: bugun qabul qildi — bugun yetkazdi. Tezlik = pul!",
    "🎯 Maqsad aniq: do'konlar sizi kutmoqda. Vaqtni yo'qotmang — yo'lga chiqing!",
    "🦁 Kuchli distribyutor omborni to'ldiradi, undan ham tezroq bo'shatadi!",
    "🌟 Har bir kg tovar — bu kelajakdagi foyda. Bugun tez harakat = ertaga ko'p daromad!",
    "🔑 Muvaffaqiyatning siri: doimiy harakat. Siz bugun ham shu yo'ldasiz. Zo'r!",
    "📦 Tovar qabul qilindi — bu boshlanishi! Asosiy ish oldinda: do'konlarga yetkazish!",
    "💰 Har bir yetkazib berish — bu sizning daromadingiz. Ko'proq harakat = ko'proq pul!",
    "🏅 Professional ish — tez va sifatli yetkazib berish. Siz bugun ham shu standartni saqladingiz!",
    "🎖️ Eng yaxshi distribyutor — eng tez distribyutor. Raqobatdan oldinda boring!",
    "⚙️ Sistem ishlayapti: tovar qabul — yetkazib berish — pul. Sizning biznesingiz aylanmoqda!",
    "🌍 Har kuni yangi do'konlar, yangi mijozlar, yangi imkoniyatlar. Siz ularni qo'lga kiring!",
    "🚀 Bugun qabul qilgan tovaringiz ertaga foyda bo'lsin — bugundan harakatga!",
    "💥 Energiya baland — natija yuqori! Do'konlarga chiqish vaqti keldi!",
    "🎯 Distribyutor muvaffaqiyatining formulasi: tez qabul + tez yetkazish = ko'p daromad!",
    "🏃 Vaqt — pul! Tovar omborga keldi, endi uni do'konlarga olib boring!",
    "🌱 Har bir yaxshi yetkazib berish — bu doimiy mijoz. Bugun yaxshi xizmat = ertaga sadoqatli do'kon!",
    "💎 Sifatli va o'z vaqtida yetkazish — bu sizning obro'ingiz. Obro' = ko'proq buyurtma!",
    "🔥 Omboringiz to'ldi! Bu quvonch emas — bu mas'uliyat. Do'konlar kutmoqda!",
    "📊 Har bir tovar yetkazib berish — bu statistikangizni yaxshilaydi. Oldinga harakating davom etsin!",
    "⭐ A'lo distribyutor: har doim o'z vaqtida, har doim sifatli. Siz shu standartni ushlab turasiz!",
    "🎪 Bozor sizniki — uni egallab oling! Bugun qabul qilgan tovaringizni tezda tarqating!",
    "💡 Bir kunda ko'proq do'kon = ko'proq sotish = ko'proq foyda. Formula oddiy!",
    "🌊 Tovar oqimi to'xtamassin! Qabul-yetkazish ritmini saqlang!",
    "🦅 Baland ko'tariling! Har bir tovar qabuli — yangi cho'qqiga qadam!",
    "🔑 Bugun qilingan mehnat — ertangi muvaffaqiyatning kaliti. Omboringiz to'ldi, ish boshlandi!",
    "⚡ Siz tezkor distribyutorsiz! Tovar qabul qilindi — endi tez harakatga!",
    "🎯 Aniq maqsad: bugun ombordan hamma tovarni do'konlarga yetkazish. Muvaffaqiyat sizniki!",
    "🏆 Champion distribyutor: tovar oladi va tezda tarqatadi. Siz champion!",
    "💰 Har bir soat — bu pul! Tovar omborga keldi, vaqt yo'qotmang!",
    "🌟 Omboringiz — bu sizning kapitalingiz. Uni aylantirishni to'xtamang!",
    "🔥 Kuchingiz bor, tezligingiz bor, tovaringiz bor — endi faqat harakat qoldi!",
    "📦 Tovar qabul = 50% ish bajarildi. Qolgan 50% — yetkazib berish! Oldinga!",
    "💪 Har bir kg yetkazilgan tovar — bu sizning mehnatka olingan moyan. Ko'proq harakat!",
    "🚀 Distribyutor raketa kabi bo'lishi kerak: tez va to'g'ri nishonga. Siz shundasiz!",
    "🌍 Katta distribyutorlar katta harakat qiladi. Siz bugun ham katta ish qildingiz!",
    "⚙️ Biznes mashinasi ishlayapti: siz uning eng muhim qismi — distribyutorsiz!",
    "🎖️ Har bir muvaffaqiyatli yetkazib berish — bu sizning reputatsiyangizni mustahkamlaydi!",
    "💡 Strategik fikrla: bugun ko'proq do'konga borsang, ertaga ko'proq zakaz olasan!",
    "🏅 Siz professional! Tovar qabul qilish — bu sizning ishi. Endi yetkazib berish ham!",
    "🔥 Omboringiz boy — do'konlarga xabar bering! Ular sizni kutmoqda!",
    "⭐ Yuqori samaradorlik: kam vaqtda ko'p do'konga yetkazish. Bu sizning kuchingiz!",
    "🌱 Har bir yaxshi xizmat — yangi imkoniyat. Bugun yaxshi ish = ertaga ko'proq zakaz!",
    "🎯 Distribyutor shiori: tovar olaman, tez tarqataman, ko'p pul topaman!",
    "💰 Bugun qabul qilgan tovaringiz — bu ertangi foyda. Tez harakat = ko'p foyda!",
]

def get_motiv(): return random.choice(MOTIVATSIYA)

# ── NAMOZ VAQTLARI (TOSHKENT) ─────────────────────────────────────────────────


# ── INLINE KEYBOARD YARATUVCHILAR ─────────────────────────────────────────────
def ik(*rows):
    """Inline keyboard yaratish: ik(["matn","cb"], ["matn2","cb2"])"""
    return InlineKeyboardMarkup([[InlineKeyboardButton(r[0], callback_data=r[1])] if len(r)==2
                                  else InlineKeyboardButton(r[0], callback_data=r[1])
                                  for r in rows])

def ikr(*rows):
    """Inline keyboard - har bir rows list bo'ladi"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(b[0], callback_data=b[1]) for b in row]
        for row in rows
    ])

def phone_kb(la_):
    btn = "📱 Telefon yuborish" if la_=="uz" else "📱 Отправить телефон"
    return ReplyKeyboardMarkup([[KeyboardButton(btn, request_contact=True)]], resize_keyboard=True)

def main_kb(uid, la_):
    """Asosiy menyu - inline"""
    is_admin = int(uid) in ADMIN_IDS
    rows = [
        [("📥 Zavoddan qabul","m:qabul"), ("📋 Buyurtmalar","m:buyurtma")],
        [("🚚 Mol topshirish","m:topshir"), ("💵 To'lov qabul","m:tolov")],
        [("📊 Kunlik natija","m:natija"), ("📦 Ombor","m:ombor")],
        [("🗺 Marshrut","m:marshrut"), ("📈 Hisobot","m:hisobot")],
        [("🏪 Do'konlarim","m:dokonlar"), ("💰 Narxlarim","m:narxlar")],
    ]
    if is_admin: rows.append([("⚙️ Admin panel","m:admin")])
    return ikr(*rows)

def prod_kb(prods, la_, prefix="prod", back_cb="m:main"):
    """Mahsulotlar inline keyboard"""
    rows = []
    for i in range(0, len(prods), 2):
        row = [(prods[i][la_], f"{prefix}:{prods[i]['id']}")]
        if i+1 < len(prods): row.append((prods[i+1][la_], f"{prefix}:{prods[i+1]['id']}"))
        rows.append(row)
    rows.append([("🔙 Orqaga" if la_=="uz" else "🔙 Назад", back_cb)])
    return ikr(*rows)

def store_kb(stores, prefix="store", back_cb="m:main"):
    """Do'konlar inline keyboard"""
    rows = [[( s.get("Nomi",""), f"{prefix}:{s.get('ID','')}")] for s in stores]
    rows.append([("🔙 Orqaga", back_cb)])
    return ikr(*rows)

def back_ik(cb="m:main", txt="🔙 Orqaga"):
    return ikr([(txt, cb)])

def yes_no_ik(yes_cb, no_cb, la_):
    return ikr(
        [("✅ Ha" if la_=="uz" else "✅ Да", yes_cb)],
        [("❌ Yo'q" if la_=="uz" else "❌ Нет", no_cb)]
    )


# ── YORDAMCHI FUNKSIYALAR ─────────────────────────────────────────────────────
async def send_main(bot_or_ctx, uid, la_, sid, edit=None):
    """Asosiy menyuni yuborish yoki tahrirlash"""
    text = f"📋 <b>Asosiy menyu</b>\n🔑 ID: <b>{sid}</b>"
    kb = main_kb(uid, la_)
    if edit:
        try:
            await edit.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot_or_ctx.send_message(uid, text, reply_markup=kb, parse_mode="HTML")
    else:
        await bot_or_ctx.send_message(uid, text, reply_markup=kb, parse_mode="HTML")

async def answer(q, text=""):
    try: await q.answer(text)
    except Exception: pass

async def edit_or_send(upd, ctx, text, kb=None, parse_mode="HTML"):
    """Callback bo'lsa edit, bo'lmasa yangi xabar"""
    if upd.callback_query:
        q = upd.callback_query
        await answer(q)
        try:
            await q.edit_message_text(text, reply_markup=kb, parse_mode=parse_mode)
        except Exception:
            await ctx.bot.send_message(upd.effective_user.id, text, reply_markup=kb, parse_mode=parse_mode)
    else:
        await upd.message.reply_text(text, reply_markup=kb, parse_mode=parse_mode)

# ── START & LANG ──────────────────────────────────────────────────────────────
async def start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = upd.effective_user.id
    user = get_user(uid)
    # Avval eski ReplyKeyboard ni o'chirish
    await upd.message.reply_text("...", reply_markup=ReplyKeyboardRemove())

    if uid in ADMIN_IDS:
        la_ = ctx.user_data.get("lang","uz")
        ctx.user_data["lang"] = la_
        sid = get_sid(uid)
        await upd.message.reply_text(
            f"📋 <b>Asosiy menyu</b>\n🔑 ID: <b>{sid}</b>",
            reply_markup=main_kb(uid, la_), parse_mode="HTML")
        return ST_MAIN

    if user:
        la_ = user.get("Til","uz"); ctx.user_data["lang"] = la_
        sid = user.get("Short_ID","?")
        status = str(user.get("Status","")).lower()
        if status in ["tasdiqlangan","1"]:
            await upd.message.reply_text(
                f"📋 <b>Asosiy menyu</b>\n🔑 ID: <b>{sid}</b>",
                reply_markup=main_kb(uid, la_), parse_mode="HTML")
            return ST_MAIN
        elif status in ["rad_etildi"]:
            await upd.message.reply_text(
                "❌ Hisobingiz rad etildi. Qayta ro'yxatdan o'ting:" if la_=="uz"
                else "❌ Аккаунт отклонён. Зарегистрируйтесь заново:")
            return ST_REG_NAME
        else:
            await upd.message.reply_text(
                "⏳ Hisobingiz tasdiqlanmagan. Admin tasdiqlashini kuting." if la_=="uz"
                else "⏳ Аккаунт не подтверждён. Ожидайте.",
                reply_markup=ikr([("📤 Qayta yuborish","resend")]))
            return ST_WAIT_APPROVE

    kb = ikr([("🇺🇿 O'zbek","lang:uz")],[("🇷🇺 Русский","lang:ru")])
    await upd.message.reply_text("Tilni tanlang / Выберите язык:", reply_markup=kb)
    return ST_LANG

async def lang_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q)
    la_ = q.data.split(":")[1]; ctx.user_data["lang"] = la_
    uid = upd.effective_user.id

    if uid in ADMIN_IDS:
        sid = get_sid(uid)
        await q.edit_message_text(f"📋 <b>Asosiy menyu</b>\n🔑 ID: <b>{sid}</b>",
            reply_markup=main_kb(uid,la_), parse_mode="HTML")
        return ST_MAIN

    user = get_user(uid)
    if user:
        la_ = user.get("Til",la_); ctx.user_data["lang"]=la_
        sid = user.get("Short_ID","?")
        status = str(user.get("Status","")).lower()
        if status in ["tasdiqlangan","1"]:
            await q.edit_message_text(f"📋 <b>Asosiy menyu</b>\n🔑 ID: <b>{sid}</b>",
                reply_markup=main_kb(uid,la_), parse_mode="HTML")
            return ST_MAIN
        elif status == "rad_etildi":
            await q.edit_message_text("❌ Rad etildi. Ism kiriting:")
            return ST_REG_NAME
        else:
            await q.edit_message_text("⏳ Tasdiqlanmagan.",
                reply_markup=ikr([("📤 Qayta yuborish","resend")]))
            return ST_WAIT_APPROVE

    await q.edit_message_text("Ismingizni kiriting:" if la_=="uz" else "Введите имя:")
    return ST_REG_NAME

# ── RO'YXAT ───────────────────────────────────────────────────────────────────
async def reg_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["rn"] = upd.message.text.strip()
    la_ = la(ctx)
    await upd.message.reply_text("Familiyangizni kiriting:" if la_=="uz" else "Введите фамилию:")
    return ST_REG_FNAME

async def reg_fname(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["rf"] = upd.message.text.strip()
    la_ = la(ctx)
    await upd.message.reply_text(
        "📱 Telefon raqamingizni yuboring:" if la_=="uz" else "📱 Отправьте номер телефона:",
        reply_markup=phone_kb(la_))
    return ST_REG_PHONE

async def reg_phone(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx)
    if upd.message.contact: phone = upd.message.contact.phone_number
    else:
        phone = clean_phone(upd.message.text)
        if len(phone.replace("+","")) < 7:
            await upd.message.reply_text("❌ Noto'g'ri raqam! Qaytadan:", reply_markup=phone_kb(la_))
            return ST_REG_PHONE
    ctx.user_data["rph"] = phone
    await upd.message.reply_text(
        "📷 Passport rasmini yuboring (yoki o'tkazib yuboring):" if la_=="uz"
        else "📷 Фото паспорта (или пропустите):",
        reply_markup=ikr([("⏭ O'tkazib yuborish" if la_=="uz" else "⏭ Пропустить","skip_passport")]),
        reply_markup_remove=ReplyKeyboardRemove())
    return ST_REG_PASSPORT

async def reg_passport(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = upd.effective_user.id; la_ = la(ctx)
    if upd.callback_query:
        await upd.callback_query.answer()
        passport = "otkazildi"
        photo_id = None
    else:
        passport = "rasm_bor" if upd.message.photo else (upd.message.text or "otkazildi")
        photo_id = upd.message.photo[-1].file_id if upd.message.photo else None

    name=ctx.user_data.get("rn",""); fname=ctx.user_data.get("rf","")
    phone=ctx.user_data.get("rph",""); full=f"{name} {fname}".strip()
    sid = make_sid()

    db_delete("Foydalanuvchilar","TG_ID",str(uid))
    db_append("Foydalanuvchilar",[str(uid),name,fname,phone,"distributor",la_,passport,"kutilmoqda",sid,now_str()])

    for adm in ADMIN_IDS:
        try:
            await ctx.bot.send_message(adm,
                f"👤 <b>YANGI DISTRIBYUTOR</b>\n"
                f"Ism: {full}\nTel: {phone}\nTG_ID: {uid}\nID: <b>{sid}</b>\n\n"
                f"✅ /approve_{uid}\n❌ /reject_{uid}", parse_mode="HTML")
            if photo_id: await ctx.bot.send_photo(adm, photo_id, caption=f"Passport: {full}")
        except Exception as e: logger.error(f"admin notify: {e}")

    await ctx.bot.send_message(uid,
        f"✅ <b>Ro'yxatdan o'tdingiz!</b>\n🔑 Sizning ID: <b>{sid}</b>\n\n"
        f"Bu IDni saqlang!\n\nAdmin tasdiqlashini kuting...",
        parse_mode="HTML",
        reply_markup=ikr([("📤 Qayta yuborish","resend")]))
    return ST_WAIT_APPROVE

async def wait_approve(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = upd.effective_user.id; la_ = la(ctx)
    if upd.callback_query:
        await upd.callback_query.answer()
        data = upd.callback_query.data
        if data == "resend":
            user = get_user(uid)
            if user:
                full=f"{user.get('Ism','')} {user.get('Familiya','')}".strip()
                sid=user.get("Short_ID","?"); phone=user.get("Telefon","")
                for adm in ADMIN_IDS:
                    try:
                        await ctx.bot.send_message(adm,
                            f"👤 <b>QAYTA YUBORILDI</b>\n{full}\nTel: {phone}\nID: {sid}\n"
                            f"✅ /approve_{uid}\n❌ /reject_{uid}", parse_mode="HTML")
                    except Exception: pass
            await upd.callback_query.edit_message_text(
                "✅ Adminga qayta yuborildi.", reply_markup=ikr([("📤 Yana yuborish","resend")]))
    if is_approved(uid):
        user=get_user(uid); sid=user.get("Short_ID","?") if user else "?"
        await ctx.bot.send_message(uid,
            f"📋 <b>Asosiy menyu</b>\n🔑 ID: <b>{sid}</b>",
            reply_markup=main_kb(uid,la_), parse_mode="HTML")
        return ST_MAIN
    return ST_WAIT_APPROVE

async def approve_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m = re.search(r'/approve_(\d+)', upd.message.text or "")
    if not m: return
    target = m.group(1)
    db_update("Foydalanuvchilar","TG_ID",target,"Status","tasdiqlangan")
    await upd.message.reply_text(f"✅ Tasdiqlandi: {target}")
    try:
        u=get_user(target); la_=u.get("Til","uz") if u else "uz"
        sid=u.get("Short_ID","?") if u else "?"
        await ctx.bot.send_message(int(target),
            f"✅ <b>Hisobingiz tasdiqlandi!</b>\n🔑 ID: <b>{sid}</b>\n\n/start bosing.",
            parse_mode="HTML")
    except Exception as e: logger.error(f"approve: {e}")

async def reject_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m = re.search(r'/reject_(\d+)', upd.message.text or "")
    if not m: return
    target = m.group(1)
    db_update("Foydalanuvchilar","TG_ID",target,"Status","rad_etildi")
    await upd.message.reply_text(f"❌ Rad etildi: {target}")
    try:
        u=get_user(target); la_=u.get("Til","uz") if u else "uz"
        await ctx.bot.send_message(int(target), "❌ Hisobingiz rad etildi." if la_=="uz" else "❌ Аккаунт отклонён.")
    except Exception as e: logger.error(f"reject: {e}")


# ── ASOSIY MENYU CALLBACK ─────────────────────────────────────────────────────
async def main_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q)
    uid = upd.effective_user.id; la_ = la(ctx)
    sid = get_sid(uid)
    data = q.data  # "m:qabul", "m:topshir" etc

    if not is_approved(uid):
        await q.edit_message_text("⏳ Tasdiqlanmagan.")
        return ST_WAIT_APPROVE

    if data == "m:main":
        await q.edit_message_text(f"📋 <b>Asosiy menyu</b>\n🔑 ID: <b>{sid}</b>",
            reply_markup=main_kb(uid,la_), parse_mode="HTML")
        return ST_MAIN

    if data == "m:qabul": return await zavod_start(upd, ctx)
    if data == "m:topshir": return await topshir_start(upd, ctx)
    if data == "m:buyurtma": return await buyurtma_show(upd, ctx)
    if data == "m:tolov": return await tolov_show(upd, ctx)
    if data == "m:natija": return await daily_show(upd, ctx)
    if data == "m:ombor": return await ombor_show(upd, ctx)
    if data == "m:marshrut": return await marshrut_show(upd, ctx)
    if data == "m:hisobot": return await hisobot_menu(upd, ctx)
    if data == "m:dokonlar": return await dokonlar_show(upd, ctx)
    if data == "m:narxlar": return await narxlar_start(upd, ctx)
    if data == "m:admin": return await admin_menu(upd, ctx)

    return ST_MAIN

# ── ZAVOD QABUL ───────────────────────────────────────────────────────────────
async def zavod_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); prods = get_products()
    await edit_or_send(upd, ctx,
        "📥 <b>Zavoddan qabul</b>\nMahsulotni tanlang:" if la_=="uz"
        else "📥 <b>Получить с завода</b>\nВыберите товар:",
        prod_kb(prods, la_, prefix="zav", back_cb="m:main"))
    return ST_MAIN

async def zavod_prod_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q)
    la_ = la(ctx); uid = upd.effective_user.id
    pid = int(q.data.split(":")[1])
    prods = get_products()
    p = next((x for x in prods if x["id"]==pid), None)
    if not p: return ST_MAIN
    ctx.user_data["zav_p"] = p
    price, _ = get_price(pid, dist_id=str(uid))
    if price == 0: price, _ = get_price(pid)
    ctx.user_data["zav_price"] = price
    brinza = is_brinza(p[la_])
    hint = "Miqdorni kiriting (dona, butun son):" if brinza else "Miqdorni kiriting (masalan: 5 yoki 5.5):"
    await q.edit_message_text(
        f"📥 <b>{p[la_]}</b>\n"
        f"💰 Narx: {price:,.0f} so'm/{p['unit']}\n"
        f"{'🧀 Brinza: dona bilan hisob!' if brinza else ''}\n\n{hint}",
        reply_markup=back_ik("m:qabul"), parse_mode="HTML")
    return ST_ZAVOD_QTY

async def zavod_qty(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = upd.effective_user.id
    p = ctx.user_data.get("zav_p",{}); price = ctx.user_data.get("zav_price",0)
    brinza = is_brinza(p.get(la_,""))
    if brinza:
        try: qty = int(float(upd.message.text.strip().replace(",",".")))
        except: qty = 0
        if qty <= 0:
            await upd.message.reply_text("❌ Butun son kiriting (dona):"); return ST_ZAVOD_QTY
    else:
        qty = parse_weight(upd.message.text)
        if qty <= 0:
            await upd.message.reply_text("❌ Noto'g'ri miqdor:"); return ST_ZAVOD_QTY
    jami = qty * price
    qid = make_id("Q")
    u = get_user(uid)
    dn = f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else str(uid)
    sid = u.get("Short_ID","?") if u else "ADMIN"
    unit_save = p.get("unit","")
    db_append("Qabul",[now_str(),str(uid),dn,p[la_],qty,unit_save,price,jami,"kutilmoqda",qid])
    qty_str = fmt_qty(qty, unit_save, p[la_], topshirish=False)
    for adm in ADMIN_IDS:
        try:
            await ctx.bot.send_message(adm,
                f"⏳ <b>ZAVOD SO'ROVI</b>\n"
                f"Dist: {dn} (ID:{sid})\n"
                f"{p[la_]}: {qty_str}\n"
                f"Narx: {price:,.0f}\nJami: {jami:,.0f}\nRef: {qid}\n\n"
                f"✅ /zok_{qid}\n❌ /zrad_{qid}", parse_mode="HTML")
        except Exception as e: logger.error(f"zavod admin: {e}")
    await upd.message.reply_text(
        f"⏳ So'rov yuborildi.\n{p[la_]}: {qty_str}" if la_=="uz"
        else f"⏳ Запрос отправлен.\n{p[la_]}: {qty_str}",
        reply_markup=main_kb(uid,la_))
    return ST_MAIN



async def zok_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m = re.search(r'/zok_(\w+)', upd.message.text or "")
    if not m: return
    qid = m.group(1)
    db_update("Qabul","Qabul_ID",qid,"Status","tasdiqlangan")
    await upd.message.reply_text(f"✅ Tasdiqlandi: {qid}")
    for r in db_all("Qabul"):
        if r.get("Qabul_ID","")==qid:
            try:
                did = str(r.get("Dist_ID",""))
                u = get_user(did); la_ = u.get("Til","uz") if u else "uz"
                await ctx.bot.send_message(int(did),
                    "✅ <b>Zavod so'rovi tasdiqlandi!</b>\nOmboringiz yangilandi." if la_=="uz"
                    else "✅ <b>Запрос подтверждён!</b>\nСклад обновлён.", parse_mode="HTML")
                await ctx.bot.send_message(int(did), f"\n{get_motiv()}")
            except Exception: pass
            break

async def zrad_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m = re.search(r'/zrad_(\w+)', upd.message.text or "")
    if not m: return
    qid = m.group(1)
    db_update("Qabul","Qabul_ID",qid,"Status","rad_etildi")
    await upd.message.reply_text(f"❌ Rad etildi: {qid}")
    for r in db_all("Qabul"):
        if r.get("Qabul_ID","")==qid:
            try:
                did = str(r.get("Dist_ID",""))
                u = get_user(did); la_ = u.get("Til","uz") if u else "uz"
                await ctx.bot.send_message(int(did),
                    "❌ Zavod so'rovi rad etildi." if la_=="uz" else "❌ Запрос отклонён.")
            except Exception: pass
            break

# ── TOPSHIRISH ────────────────────────────────────────────────────────────────
async def topshir_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id)
    stores = get_stores(dist_id=uid)
    if not stores:
        await edit_or_send(upd,ctx,
            "⚠️ Do'konlar yo'q. Avval do'kon qo'shing." if la_=="uz"
            else "⚠️ Магазинов нет. Сначала добавьте магазин.",
            back_ik("m:main"))
        return ST_MAIN
    ombor = get_ombor(uid)
    if not ombor:
        await edit_or_send(upd,ctx,
            "❌ Omboringiz bo'sh!\nAvval zavoddan tovar qabul qiling." if la_=="uz"
            else "❌ Склад пуст!\nСначала получите товар с завода.",
            back_ik("m:main"))
        return ST_MAIN
    ctx.user_data["stores"] = stores
    await edit_or_send(upd,ctx,
        "🚚 <b>Mol topshirish</b>\nDo'konni tanlang:" if la_=="uz"
        else "🚚 <b>Передача товара</b>\nВыберите магазин:",
        store_kb(stores, prefix="top_s", back_cb="m:main"))
    return ST_TOP_STORE

async def top_store_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q)
    la_ = la(ctx); uid = str(upd.effective_user.id)
    store_id = q.data.split(":")[1]
    stores = ctx.user_data.get("stores", get_stores(dist_id=uid))
    store = next((s for s in stores if str(s.get("ID",""))==store_id), None)
    if not store: return ST_MAIN
    ctx.user_data["top_store"] = store

    # Zakazlarni ko'rsatish (faqat ma'lumot uchun, tasdiqlash shart emas)
    orders = [r for r in db_all("Buyurtmalar")
              if str(r.get("Dokon_ID",""))==store_id and r.get("Status","")=="Yangi"]
    ctx.user_data["store_orders"] = orders
    debt = get_debt(store_id)

    info = f"🏪 <b>{store.get('Nomi','')}</b>\n"
    if debt > 0: info += f"💸 Qarz: {debt:,.0f} so'm\n"
    if orders:
        info += "\n📋 <b>Aktiv zakazlar:</b>\n"
        for r in orders:
            pn=r.get("Mahsulot",""); qty=float(r.get("Miqdor",0) or 0)
            # Brinza zakaz dona, boshqalar dona
            if is_brinza(pn): qty_str=f"{int(round(qty))} dona"
            else: qty_str=fmt_qty(qty,'dona',pn,False)
            info += f"  • {pn}: {qty_str}\n"
    info += "\nMahsulotni tanlang:"

    # Faqat ombordagi mahsulotlar
    ombor = get_ombor(uid)
    prods = get_products()
    ombor_prods = [p for p in prods if ombor.get(p["uz"],ombor.get(p["ru"],0)) > 0.001]
    if not ombor_prods:
        await q.edit_message_text("❌ Ombor bo'sh!", reply_markup=back_ik("m:topshir"))
        return ST_MAIN

    rows = []
    for i in range(0, len(ombor_prods), 2):
        p = ombor_prods[i]
        ov = ombor.get(p["uz"],ombor.get(p["ru"],0))
        row = [(f"{p[la_]} ({fmt_qty(ov,p['unit'],p[la_],True)})", f"top_p:{p['id']}")]
        if i+1 < len(ombor_prods):
            p2 = ombor_prods[i+1]
            ov2 = ombor.get(p2["uz"],ombor.get(p2["ru"],0))
            row.append((f"{p2[la_]} ({fmt_qty(ov2,p2['unit'],p2[la_],True)})", f"top_p:{p2['id']}"))
        rows.append(row)
    rows.append([("🔙 Orqaga","m:topshir")])
    await q.edit_message_text(info, reply_markup=ikr(*rows), parse_mode="HTML")
    return ST_TOP_PROD

async def top_prod_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q)
    la_ = la(ctx); uid = str(upd.effective_user.id)
    pid = int(q.data.split(":")[1])
    prods = get_products()
    p = next((x for x in prods if x["id"]==pid), None)
    if not p: return ST_MAIN
    ctx.user_data["top_p"] = p
    brinza = is_brinza(p[la_])
    if brinza:
        await q.edit_message_text(
            f"🧀 <b>Brinza topshirish</b>\n"
            f"📸 Tarozi rasmini yuboring (MAJBURIY)\n"
            f"Yoki og'irlikni kg da yozing (masalan: 3.455):",
            reply_markup=back_ik(f"top_s:{ctx.user_data.get('top_store',{}).get('ID','')}"),
            parse_mode="HTML")
    else:
        await q.edit_message_text(
            f"🚚 <b>{p[la_]}</b>\n\nMiqdorni kiriting:" if la_=="uz"
            else f"🚚 <b>{p[la_]}</b>\n\nВведите количество:",
            reply_markup=back_ik(f"top_s:{ctx.user_data.get('top_store',{}).get('ID','')}"),
            parse_mode="HTML")
    return ST_TOP_PHOTO

async def top_photo(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id)
    p = ctx.user_data.get("top_p",{})
    brinza = is_brinza(p.get(la_,""))

    if upd.message.photo:
        await upd.message.reply_text("⏳ Rasm o'qilmoqda..." if la_=="uz" else "⏳ Читаю...")
        file = await ctx.bot.get_file(upd.message.photo[-1].file_id)
        img = bytes(await file.download_as_bytearray())
        raw = await vision_ocr(img); w = parse_scale(raw)
        ctx.user_data["scale_photo"] = upd.message.photo[-1].file_id
        if w > 0:
            ctx.user_data["_ocr_w"] = w
            await upd.message.reply_text(
                f"📸 O'qildi: {fmt_qty(w,'kg',p[la_],True)}\nTo'g'rimi?" if la_=="uz"
                else f"📸 Считано: {fmt_qty(w,'kg',p[la_],True)}\nВерно?",
                reply_markup=ikr(
                    [(f"✅ Ha, {fmt_qty(w,'kg',p[la_],True)}","ocr_yes")],
                    [("✏️ Boshqa raqam kiriting" if la_=="uz" else "✏️ Ввести другое","ocr_no")]
                ))
        else:
            ctx.user_data.pop("_ocr_w",None)
            await upd.message.reply_text(
                "📷 Rasm saqlandi. Og'irlikni kg da kiriting:" if la_=="uz"
                else "📷 Фото сохранено. Введите вес в кг:")
        return ST_TOP_PHOTO

    t = upd.message.text or ""
    # OCR tasdiq tugmasi (callback emas, matn orqali kelar qilib qo'yamiz)
    qty = parse_weight(t)
    if qty <= 0:
        if brinza and not ctx.user_data.get("scale_photo"):
            await upd.message.reply_text(
                "🧀 Brinza uchun tarozi rasmi MAJBURIY!\nRasm yuboring:" if la_=="uz"
                else "🧀 Фото весов ОБЯЗАТЕЛЬНО для брынзы!")
            return ST_TOP_PHOTO
        await upd.message.reply_text("❌ Noto'g'ri. Qaytadan kiriting:")
        return ST_TOP_PHOTO
    ctx.user_data["top_qty"] = qty
    return await top_pay_menu(upd, ctx)

async def top_photo_ocr_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q)
    la_ = la(ctx)
    if q.data == "ocr_yes":
        qty = ctx.user_data.pop("_ocr_w", 0)
        ctx.user_data["top_qty"] = qty
        await q.edit_message_text(f"✅ Miqdor: {fmt_qty(qty,'kg',ctx.user_data.get('top_p',{}).get(la_,''),True)}")
        return await top_pay_menu_edit(upd, ctx)
    else:
        ctx.user_data.pop("_ocr_w",None)
        await q.edit_message_text("✏️ Og'irlikni kg da kiriting (masalan: 3.455):")
        return ST_TOP_PHOTO

async def top_pay_menu(upd, ctx):
    la_ = la(ctx); p = ctx.user_data.get("top_p",{})
    qty = ctx.user_data.get("top_qty",0)
    qty_str = fmt_qty(qty, p.get("unit",""), p.get(la_,""), True)
    await upd.message.reply_text(
        f"💳 <b>To'lov usuli</b>\n{p.get(la_,'')}: {qty_str}\n\nQaysi usulda?" if la_=="uz"
        else f"💳 <b>Способ оплаты</b>\n{p.get(la_,'')}: {qty_str}\n\nКак оплата?",
        reply_markup=ikr(
            [("💵 Naqd" if la_=="uz" else "💵 Наличные","pay:naqd")],
            [("📝 Realizatsiya" if la_=="uz" else "📝 Реализация","pay:real")]
        ), parse_mode="HTML")
    return ST_TOP_PAY

async def top_pay_menu_edit(upd, ctx):
    q = upd.callback_query; la_ = la(ctx); p = ctx.user_data.get("top_p",{})
    qty = ctx.user_data.get("top_qty",0)
    qty_str = fmt_qty(qty, p.get("unit",""), p.get(la_,""), True)
    await q.edit_message_text(
        f"💳 <b>To'lov usuli</b>\n{p.get(la_,'')}: {qty_str}\n\nQaysi usulda?" if la_=="uz"
        else f"💳 <b>Способ оплаты</b>\n{p.get(la_,'')}: {qty_str}\n\nКак оплата?",
        reply_markup=ikr(
            [("💵 Naqd" if la_=="uz" else "💵 Наличные","pay:naqd")],
            [("📝 Realizatsiya","pay:real")]
        ), parse_mode="HTML")
    return ST_TOP_PAY

async def top_pay_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    data = q.data  # "pay:naqd" or "pay:real" or "voz:yes" or "voz:no" or "pay_ok:..."
    uid = str(upd.effective_user.id)

    if data == "pay:real":
        ctx.user_data["pay_type"] = "realizatsiya"; ctx.user_data["top_naqd"] = 0.0
        # Vozvrat so'rash
        await q.edit_message_text(
            "📦 Qaytarilgan tovar (vozvrat) bormi?" if la_=="uz"
            else "📦 Есть возврат товара?",
            reply_markup=ikr(
                [("✅ Ha, vozvrat bor" if la_=="uz" else "✅ Да","voz:yes")],
                [("❌ Yo'q","voz:no")]
            ))
        return ST_TOP_PAY

    if data == "pay:naqd":
        ctx.user_data["pay_type"] = "naqd"
        await q.edit_message_text(
            "💵 Naqd summani kiriting:\n(0 kiriting = to'liq realizatsiya)" if la_=="uz"
            else "💵 Введите сумму наличных:\n(0 = полностью в долг)")
        return ST_TOP_NAQD

    if data == "voz:no":
        ctx.user_data["voz_jami"] = 0.0
        return await _save_topshirish(upd, ctx)

    if data == "voz:yes":
        # Vozvrat mahsulot tanlash - bu do'konga topshirilgan mahsulotlar
        store = ctx.user_data.get("top_store",{})
        store_id = str(store.get("ID",""))
        tops = [r for r in db_all("Topshirish")
                if str(r.get("Dokon_ID",""))==store_id and r.get("Status","")=="tasdiqlangan"]
        prods_set = {}
        for r in tops:
            pn=r.get("Mahsulot",""); prods_set[pn]=prods_set.get(pn,0)+float(r.get("Miqdor",0) or 0)
        if not prods_set:
            ctx.user_data["voz_jami"]=0.0
            await q.edit_message_text(
                "Bu do'konga hali tovar topshirilmagan. Vozvrat yo'q.")
            return await _save_topshirish(upd, ctx)
        ctx.user_data["voz_prods"] = prods_set
        rows = [[(pn, f"voz_p:{pn}")] for pn in prods_set.keys()]
        rows.append([("🔙 Orqaga","voz:no")])
        await q.edit_message_text(
            "Qaysi mahsulot qaytarildi?" if la_=="uz" else "Какой товар возвращается?",
            reply_markup=ikr(*rows))
        return ST_VOZ_PROD

    return ST_TOP_PAY

async def top_naqd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx)
    amount = parse_money(upd.message.text)
    if amount < 0:
        await upd.message.reply_text("❌ Noto'g'ri summa:"); return ST_TOP_NAQD
    ctx.user_data["top_naqd"] = amount
    # Vozvrat so'rash
    await upd.message.reply_text(
        "📦 Vozvrat bormi?" if la_=="uz" else "📦 Есть возврат?",
        reply_markup=ikr(
            [("✅ Ha","voz:yes")],
            [("❌ Yo'q","voz:no")]
        ))
    return ST_TOP_PAY

async def voz_prod_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    prod_name = q.data.split(":",1)[1]
    ctx.user_data["voz_prod"] = prod_name
    brinza = is_brinza(prod_name)
    await q.edit_message_text(
        f"{'🧀' if brinza else '📦'} <b>{prod_name}</b>\n\n"
        f"{'Nechta? (dona, butun son)' if brinza else 'Necha kg?'}" if la_=="uz"
        else f"{'🧀' if brinza else '📦'} <b>{prod_name}</b>\n\n"
        f"{'Сколько штук? (целое)' if brinza else 'Сколько кг?'}",
        parse_mode="HTML")
    return ST_VOZ_QTY

async def voz_qty(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id)
    prod_name = ctx.user_data.get("voz_prod","")
    store = ctx.user_data.get("top_store",{})
    store_id=str(store.get("ID","")); store_name=store.get("Nomi","")
    brinza = is_brinza(prod_name)
    if brinza:
        try: qty=int(float(upd.message.text.strip().replace(",",".")))
        except: qty=0
        if qty<=0:
            await upd.message.reply_text("Butun son kiriting:"); return ST_VOZ_QTY
        birlik="dona"
    else:
        qty=parse_weight(upd.message.text)
        if qty<=0:
            await upd.message.reply_text("❌ Noto'g'ri:"); return ST_VOZ_QTY
        birlik="kg"
    p_obj=next((p for p in get_products() if p["uz"]==prod_name or p["ru"]==prod_name),None)
    pid=p_obj["id"] if p_obj else 0
    price,_=get_price(pid,dist_id=uid,dokon_id=store_id)
    if price==0: price,_=get_price(pid,dist_id=uid)
    if price==0: price,_=get_price(pid)
    jami=qty*price; voz_id=make_id("VOZ")
    db_append("Vozvrat",[now_str(),uid,store_name,store_id,prod_name,qty,birlik,price,jami,"tasdiqlangan",voz_id])
    ctx.user_data["voz_jami"]=jami
    qty_str=fmt_qty(qty,birlik,prod_name,birlik=="kg")
    await upd.message.reply_text(f"✅ Vozvrat: {prod_name} {qty_str}\n💰 Summa: {jami:,.0f}")
    return await _save_topshirish(upd, ctx)

async def _save_topshirish(upd, ctx):
    la_ = la(ctx); uid = str(upd.effective_user.id)
    p=ctx.user_data.get("top_p",{}); store=ctx.user_data.get("top_store",{})
    qty=ctx.user_data.get("top_qty",0); naqd=ctx.user_data.get("top_naqd",0.0)
    voz_jami=ctx.user_data.get("voz_jami",0.0); pay_type=ctx.user_data.get("pay_type","naqd")
    store_id=str(store.get("ID","")); store_name=store.get("Nomi","")
    pid=p.get("id",0)
    price,_=get_price(pid,dist_id=uid,dokon_id=store_id)
    if price==0: price,_=get_price(pid,dist_id=uid)
    if price==0: price,_=get_price(pid)
    jami=qty*price; effective_naqd=min(naqd+voz_jami,jami); qarz=max(0.0,jami-effective_naqd)
    top_id=make_id("T")
    u=get_user(uid); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else str(uid)
    db_append("Topshirish",[now_str(),uid,store_name,store_id,p.get(la_,""),qty,p.get("unit",""),
              price,jami,pay_type,effective_naqd,qarz,"tasdiqlangan",top_id])
    qty_str=fmt_qty(qty,p.get("unit",""),p.get(la_,""),True)
    msg=(f"✅ <b>Mol topshirildi!</b>\n"
         f"🏪 {store_name}\n📦 {p.get(la_,'')}: {qty_str}\n"
         f"💰 Jami: {jami:,.0f}\n💵 Naqd: {effective_naqd:,.0f}\n"
         f"📝 Qarz: {qarz:,.0f}")
    if voz_jami>0: msg+=f"\n↩️ Vozvrat: -{voz_jami:,.0f}"
    sid=get_sid(upd.effective_user.id)
    if upd.callback_query:
        await upd.callback_query.edit_message_text(msg,parse_mode="HTML")
    else:
        await upd.message.reply_text(msg,parse_mode="HTML")
    await ctx.bot.send_message(upd.effective_user.id,
        f"📋 <b>Asosiy menyu</b>\n🔑 ID: <b>{sid}</b>",
        reply_markup=main_kb(upd.effective_user.id,la_), parse_mode="HTML")
    # State tozalash
    for k in ["top_p","top_store","top_qty","top_naqd","voz_jami","pay_type","store_orders","voz_prods","voz_prod","scale_photo","_ocr_w"]:
        ctx.user_data.pop(k,None)
    return ST_MAIN


# ── BUYURTMALAR ────────────────────────────────────────────────────────────────
async def buyurtma_show(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id)
    my_stores = get_stores(dist_id=uid)
    my_store_ids = {str(s.get("ID","")) for s in my_stores}
    orders = [r for r in db_all("Buyurtmalar")
              if (str(r.get("Dist_ID",""))==uid or str(r.get("Dokon_ID","")) in my_store_ids)
              and r.get("Status","")=="Yangi"]
    if not orders:
        await edit_or_send(upd,ctx,
            "📋 Yangi buyurtma yo'q." if la_=="uz" else "📋 Новых заказов нет.",
            back_ik("m:main"))
        return ST_MAIN
    by_dokon={}; jami_prod={}
    for r in orders:
        dk=r.get("Dokon","?"); by_dokon.setdefault(dk,[]).append(r)
        pn=r.get("Mahsulot",""); qty=float(r.get("Miqdor",0) or 0)
        jami_prod[pn]=jami_prod.get(pn,0)+qty
    lines=[f"📋 <b>Buyurtmalar: {len(orders)} ta</b>","━━━━━━━━━━━━━━━━"]
    for dokon,recs in by_dokon.items():
        lines.append(f"\n🏪 <b>{dokon}:</b>")
        for r in recs:
            pn=r.get("Mahsulot",""); qty=float(r.get("Miqdor",0) or 0)
            # Brinza - dona, boshqalar - o'z birligi
            if is_brinza(pn): qty_str=f"{int(round(qty))} dona"
            else: qty_str=fmt_qty(qty, r.get("Birlik","") or "dona", pn, False)
            lines.append(f"  • {pn}: {qty_str}")
    lines.append("\n━━━━━━━━━━━━━━━━")
    lines.append("📊 <b>Jami kerak:</b>")
    for pn,qty in jami_prod.items():
        if is_brinza(pn): lines.append(f"  • {pn}: {int(round(qty))} dona")
        else: lines.append(f"  • {pn}: {fmt_qty(qty,'dona',pn,False)}")
    await edit_or_send(upd,ctx,"\n".join(lines),back_ik("m:main"),parse_mode="HTML")
    return ST_MAIN





async def tolov_show(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """To'lov qabul qilish — do'konlarni inline tugmalar bilan ko'rsatish"""
    la_ = la(ctx); uid = str(upd.effective_user.id)
    stores = get_stores(dist_id=uid)
    qarzdor = [(s, get_debt(str(s.get("ID","")))) for s in stores]
    qarzdor = [(s,d) for s,d in qarzdor if d > 0]

    if not qarzdor:
        await edit_or_send(upd, ctx,
            "✅ Barcha do'konlar qarzini to'lagan!" if la_=="uz" else "✅ Все долги погашены!",
            back_ik("m:main"))
        return ST_MAIN

    total = sum(d for _,d in qarzdor)
    lines = ["💸 <b>Qarzdorlar:</b>", "━━━━━━━━━━━━━━━━"]
    rows = []
    for s, debt in qarzdor:
        sid = str(s.get("ID",""))
        lines.append(f"• {s.get('Nomi','')}: {debt:,.0f} so'm")
        rows.append([(f"💵 {s.get('Nomi','')} — to'lov qabul", f"tolov_s:{sid}")])
    lines.append(f"━━━━━━━━━━━━━━━━\nJami: {total:,.0f}")
    rows.append([("🔙 Orqaga", "m:main")])

    await edit_or_send(upd, ctx, "\n".join(lines), ikr(*rows))
    return ST_MAIN

async def tolov_store_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Do'kon tanlandi — summa kiritish yoki to'liq yopish"""
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    store_id = q.data.split(":")[1]
    uid = str(upd.effective_user.id)
    stores = get_stores(dist_id=uid)
    store = next((s for s in stores if str(s.get("ID",""))==store_id), None)
    if not store: return ST_MAIN
    debt = get_debt(store_id)
    ctx.user_data["tolov_store"] = store
    ctx.user_data["tolov_store_id"] = store_id
    ctx.user_data["tolov_debt"] = debt
    await q.edit_message_text(
        f"💵 <b>{store.get('Nomi','')}</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💸 Joriy qarz: {debt:,.0f} so'm\n\n"
        f"Qabul qilinadigan summani kiriting\n"
        f"(To'liq yopish uchun {debt:,.0f} ni kiriting):",
        reply_markup=ikr(
            [(f"✅ To'liq yopish ({debt:,.0f})", f"tolov_full:{store_id}")],
            [("🔙 Orqaga", "m:tolov")]
        ),
        parse_mode="HTML")
    return ST_TOLOV_SUMMA

async def tolov_full_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """To'liq qarzni yopish"""
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    store_id = q.data.split(":")[1]
    store = ctx.user_data.get("tolov_store", {})
    debt = ctx.user_data.get("tolov_debt", 0)
    uid = str(upd.effective_user.id)
    await _save_tolov(ctx, uid, store, store_id, debt)
    await q.edit_message_text(
        f"✅ <b>To'lov qabul qilindi!</b>\n"
        f"🏪 {store.get('Nomi','')}\n"
        f"💰 Summa: {debt:,.0f} so'm\n"
        f"✅ Qarz to'liq yopildi!",
        reply_markup=back_ik("m:tolov"), parse_mode="HTML")
    return ST_MAIN

async def tolov_summa(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Qisman to'lov summasi kiritildi"""
    la_ = la(ctx); uid = str(upd.effective_user.id)
    summa = parse_money(upd.message.text)
    if summa <= 0:
        await upd.message.reply_text("❌ Noto'g'ri summa. Qaytadan kiriting:")
        return ST_TOLOV_SUMMA
    store = ctx.user_data.get("tolov_store", {})
    store_id = ctx.user_data.get("tolov_store_id", "")
    debt = ctx.user_data.get("tolov_debt", 0)
    if summa > debt:
        await upd.message.reply_text(
            f"⚠️ Kiritilgan summa ({summa:,.0f}) qarzdan ({debt:,.0f}) ko'p!\n"
            f"Qaytadan kiriting:")
        return ST_TOLOV_SUMMA
    await _save_tolov(ctx, uid, store, store_id, summa)
    qoldi = max(0.0, debt - summa)
    msg = (f"✅ <b>To'lov qabul qilindi!</b>\n"
           f"🏪 {store.get('Nomi','')}\n"
           f"💰 Summa: {summa:,.0f} so'm\n")
    if qoldi > 0:
        msg += f"💸 Qolgan qarz: {qoldi:,.0f} so'm"
    else:
        msg += "✅ Qarz to'liq yopildi!"
    await upd.message.reply_text(msg, reply_markup=main_kb(upd.effective_user.id, la_), parse_mode="HTML")
    return ST_MAIN

async def _save_tolov(ctx, uid, store, store_id, summa):
    """To'lovni Sheets ga saqlash"""
    store_name = store.get("Nomi","")
    tolov_id = make_id("TOL")
    db_append("Tolov", [now_str(), uid, store_name, store_id, summa, "tasdiqlangan", tolov_id])
    logger.info(f"To'lov saqlandi: {store_name} | {summa:,.0f} | {tolov_id}")


async def vok_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/vok_(\w+)',upd.message.text or "")
    if not m: return
    db_update("Tolov","Tolov_ID",m.group(1),"Status","tasdiqlangan")
    await upd.message.reply_text("✅ To'lov tasdiqlandi!")

async def vrad_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/vrad_(\w+)',upd.message.text or "")
    if not m: return
    db_update("Tolov","Tolov_ID",m.group(1),"Status","rad_etildi")
    await upd.message.reply_text("❌ To'lov rad etildi.")

async def tok_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/tok_(\w+)',upd.message.text or "")
    if not m: return
    db_update("Topshirish","Top_ID",m.group(1),"Status","tasdiqlangan")
    await upd.message.reply_text("✅ Topshirish tasdiqlandi!")

async def trad_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/trad_(\w+)',upd.message.text or "")
    if not m: return
    db_update("Topshirish","Top_ID",m.group(1),"Status","rad_etildi")
    await upd.message.reply_text("❌ Topshirish rad etildi.")

# ── KUNLIK NATIJA ──────────────────────────────────────────────────────────────
async def daily_show(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id); today=today_str()
    tops=[r for r in db_all("Topshirish") if str(r.get("Sana","")).startswith(today) and str(r.get("Dist_ID",""))==uid]
    ins =[r for r in db_all("Qabul") if str(r.get("Sana","")).startswith(today) and str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan"]
    ts=sum(float(r.get("Jami",0) or 0) for r in tops)
    tn=sum(float(r.get("Naqd",0) or 0) for r in tops)
    tq=sum(float(r.get("Qarz",0) or 0) for r in tops)
    ti=sum(float(r.get("Jami",0) or 0) for r in ins)
    foyda=ts-ti; stores=get_stores(dist_id=uid)
    jami_qarz=sum(get_debt(str(s.get("ID",""))) for s in stores)
    dc=len({r.get("Dokon","") for r in tops})
    msg=(f"📊 <b>Kunlik natija — {today}</b>\n━━━━━━━━━━━━━━━━\n"
         f"📥 Zavod: {ti:,.0f}\n🚚 Sotuv: {ts:,.0f}\n"
         f"💵 Naqd: {tn:,.0f}\n📝 Qarz: {tq:,.0f}\n"
         f"💸 Jami qarz: {jami_qarz:,.0f}\n💰 Foyda: {foyda:,.0f}\n🏪 Do'konlar: {dc}")
    await edit_or_send(upd,ctx,msg,back_ik("m:main"))
    return ST_MAIN

# ── OMBOR ─────────────────────────────────────────────────────────────────────
async def ombor_show(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id)
    st = get_ombor(uid); prods = get_products()
    prod_map={p["uz"].lower():p for p in prods}
    prod_map.update({p["ru"].lower():p for p in prods})
    lines=["📦 <b>Ombor:</b>","━━━━━━━━━━━━━━━━"]
    total_sotuv=0; total_tn=0
    for k,v in st.items():
        if v<=0.001: continue
        p_obj=prod_map.get(k.lower()); unit=p_obj["unit"] if p_obj else "kg"
        pid=p_obj["id"] if p_obj else 0
        narx,tn=get_price(pid,dist_id=uid)
        if narx==0: narx,tn=get_price(pid)
        # Omborda brinza dona ko'rsatiladi (topshirish=False)
        qty_str=fmt_qty(v,unit,k,topshirish=False)
        # Narx hisoblash: brinza kg narxi bo'yicha (sotuv kg da)
        sotuv=v*narx; tan=v*tn; foyda=sotuv-tan
        total_sotuv+=sotuv; total_tn+=tan
        narx_str=f"{narx:,.0f}" if narx else "belgilanmagan"
        lines.append(f"• <b>{k}</b>: {qty_str}\n  💰 {narx_str} → {sotuv:,.0f} | Foyda: {foyda:,.0f}")
    if len(lines)==2: lines.append("Ombor bo'sh!")
    else:
        lines.append("━━━━━━━━━━━━━━━━")
        lines.append(f"📊 <b>Jami:</b> Sotuv: {total_sotuv:,.0f} | Foyda: {total_sotuv-total_tn:,.0f}")
    await edit_or_send(upd,ctx,"\n".join(lines),back_ik("m:main"))
    return ST_MAIN

# ── MARSHRUT ───────────────────────────────────────────────────────────────────
async def marshrut_show(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id)
    stores = get_stores(dist_id=uid)
    if not stores:
        await edit_or_send(upd,ctx,"⚠️ Do'konlar yo'q.",back_ik("m:main")); return ST_MAIN
    lines=["🗺 <b>Marshrut:</b>","━━━━━━━━━━━━━━━━"]
    for i,s in enumerate(stores,1):
        debt=get_debt(str(s.get("ID",""))); d=f" ⚠️{debt:,.0f}" if debt>0 else ""
        lines.append(f"{i}. {s.get('Nomi','')}{d}\n   📞 {s.get('Tel1','-')}")
    await edit_or_send(upd,ctx,"\n".join(lines),back_ik("m:main"))
    return ST_MAIN

# ── HISOBOT ────────────────────────────────────────────────────────────────────
async def hisobot_menu(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx)
    await edit_or_send(upd,ctx,
        "📈 <b>Hisobot</b>\nDavrni tanlang:" if la_=="uz" else "📈 <b>Отчёт</b>\nВыберите период:",
        ikr([("7 kun","his:7")],[("30 kun","his:30")],[("🔙 Orqaga","m:main")]))
    return ST_MAIN

async def hisobot_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    uid = str(upd.effective_user.id)
    days = int(q.data.split(":")[1])
    from_dt = (datetime.now()-timedelta(days=days)).strftime("%Y-%m-%d")
    tops=[r for r in db_all("Topshirish") if str(r.get("Sana",""))>=from_dt and str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan"]
    ins =[r for r in db_all("Qabul")       if str(r.get("Sana",""))>=from_dt and str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan"]
    vozlar=[r for r in db_all("Vozvrat")   if str(r.get("Sana",""))>=from_dt and str(r.get("Dist_ID",""))==uid]
    ts=sum(float(r.get("Jami",0) or 0) for r in tops)
    tn=sum(float(r.get("Naqd",0) or 0) for r in tops)
    tq=sum(float(r.get("Qarz",0) or 0) for r in tops)
    ti=sum(float(r.get("Jami",0) or 0) for r in ins)
    voz_s=sum(float(r.get("Jami",0) or 0) for r in vozlar)
    stores=get_stores(dist_id=uid); jami_qarz=sum(get_debt(str(s.get("ID",""))) for s in stores)
    foyda=ts-ti
    msg=(f"📈 <b>Hisobot: {days} kun</b>\n━━━━━━━━━━━━━━━━\n"
         f"📥 Qabul: {ti:,.0f}\n🚚 Sotuv: {ts:,.0f}\n"
         f"💵 Naqd: {tn:,.0f}\n📝 Qarz (davr): {tq:,.0f}\n"
         f"↩️ Vozvrat: {voz_s:,.0f}\n💸 Jami qarz: {jami_qarz:,.0f}\n"
         f"💰 Foyda: {foyda:,.0f}")
    await q.edit_message_text(msg,reply_markup=back_ik("m:hisobot"),parse_mode="HTML")
    return ST_MAIN


# ── DO'KONLAR ──────────────────────────────────────────────────────────────────
async def dokonlar_show(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id)
    stores = get_stores(dist_id=uid)
    if not stores:
        await edit_or_send(upd,ctx,
            "🏪 Do'konlar yo'q.",
            ikr([("➕ Do'kon qo'shish","dokon:add")],[("🔙 Orqaga","m:main")]))
        return ST_MAIN
    # Har do'kon uchun karta
    if upd.callback_query:
        await upd.callback_query.edit_message_text("🏪 <b>Do'konlarim</b>", parse_mode="HTML")
    for s in stores:
        did=str(s.get("ID","")); debt=get_debt(did)
        debt_str=f"\n💸 Qarz: {debt:,.0f}" if debt>0 else ""
        card=(f"🏪 <b>{s.get('Nomi','')}</b>{debt_str}\n"
              f"━━━━━━━━━━━━━━━━\n"
              f"📍 {s.get('Adres','—')}\n"
              f"🏢 MCHJ: {s.get('MCHJ','—') or '—'}\n"
              f"📞 {s.get('Tel1','—')}\n"
              f"📞 {s.get('Tel2','—') or '—'}\n"
              f"👤 Ega: {s.get('Ega_Ismi','—') or '—'}")
        kb=ikr(
            [(f"📋 Zakaz qo'shish","dzak:"+did)],
            [(f"✏️ Ma'lumot o'zgartirish","dedit:"+did)]
        )
        await ctx.bot.send_message(upd.effective_user.id, card, reply_markup=kb, parse_mode="HTML")
    await ctx.bot.send_message(upd.effective_user.id, "Yangi do'kon qo'shish:",
        reply_markup=ikr([("➕ Yangi do'kon","dokon:add")],[("🔙 Asosiy menyu","m:main")]))
    return ST_MAIN

async def dokon_add_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    await q.edit_message_text("🏪 <b>Yangi do'kon</b>\n\nDo'kon nomini kiriting:", parse_mode="HTML")
    return ST_DI_NAME

async def di_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); t = upd.message.text.strip()
    ctx.user_data["di_name"] = t
    await upd.message.reply_text(
        "📍 Manzilini kiriting:" if la_=="uz" else "📍 Введите адрес:")
    return ST_DI_ADDR

async def di_addr(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); ctx.user_data["di_addr"] = upd.message.text.strip()
    await upd.message.reply_text(
        "🏢 MCHJ nomini kiriting (yoki o'tkazib yuborish):",
        reply_markup=ikr([("⏭ O'tkazib yuborish","di_skip:mchj")]))
    return ST_DI_MCHJ

async def di_mchj(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx)
    if upd.callback_query:
        await upd.callback_query.answer(); ctx.user_data["di_mchj"]=""
        await upd.callback_query.edit_message_text("📞 Telefon 1 kiriting:")
    else:
        ctx.user_data["di_mchj"] = upd.message.text.strip()
        await upd.message.reply_text("📞 Telefon 1 kiriting:",reply_markup=phone_kb(la_))
    return ST_DI_TEL1

async def di_tel1(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx)
    if upd.message.contact: phone=upd.message.contact.phone_number
    else:
        phone=clean_phone(upd.message.text)
        if len(phone.replace("+",""))<7:
            await upd.message.reply_text("❌ Noto'g'ri! Qaytadan:",reply_markup=phone_kb(la_)); return ST_DI_TEL1
    ctx.user_data["di_tel1"]=phone
    await upd.message.reply_text("📞 Telefon 2 (yoki o'tkazib yuborish):",
        reply_markup=ikr([("⏭ O'tkazib yuborish","di_skip:tel2")]))
    return ST_DI_TEL2

async def di_tel2(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx)
    if upd.callback_query:
        await upd.callback_query.answer(); ctx.user_data["di_tel2"]=""
        await upd.callback_query.edit_message_text("👤 Do'kon egasining ismini kiriting:")
    else:
        ctx.user_data["di_tel2"]=clean_phone(upd.message.text)
        await upd.message.reply_text("👤 Do'kon egasining ismini kiriting:")
    return ST_DI_EGA

async def di_ega(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); ctx.user_data["di_ega"]=upd.message.text.strip()
    await upd.message.reply_text(
        "📸 Do'kon rasmini yuboring (MAJBURIY):" if la_=="uz"
        else "📸 Отправьте фото магазина (ОБЯЗАТЕЛЬНО):")
    return ST_DI_PHOTO

async def di_photo(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = upd.effective_user.id
    if not upd.message.photo:
        await upd.message.reply_text("❗ Rasm yuboring! Rasmsiz bo'lmaydi."); return ST_DI_PHOTO
    photo_id = upd.message.photo[-1].file_id
    name=ctx.user_data.get("di_name",""); addr=ctx.user_data.get("di_addr","")
    mchj=ctx.user_data.get("di_mchj",""); tel1=ctx.user_data.get("di_tel1","")
    tel2=ctx.user_data.get("di_tel2",""); ega=ctx.user_data.get("di_ega","")
    u=get_user(uid); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else str(uid)
    cnt=len(db_all("Dokonlar"))+1
    db_append("Dokonlar",[str(cnt),name,addr,mchj,tel1,tel2,ega,str(uid),dn,now_str()])
    card=(f"🏪 <b>{name}</b>\n━━━━━━━━━━━━━━━━\n"
          f"📍 {addr}\n🏢 {mchj or '—'}\n"
          f"📞 {tel1}\n📞 {tel2 or '—'}\n👤 {ega or '—'}\n🚚 {dn}")
    for adm in ADMIN_IDS:
        try:
            await ctx.bot.send_photo(adm,photo_id,caption=card,parse_mode="HTML")
        except Exception: pass
    await upd.message.reply_photo(photo_id, caption=f"✅ Do'kon qo'shildi!\n\n{card}", parse_mode="HTML")
    sid=get_sid(uid)
    await ctx.bot.send_message(uid,f"📋 <b>Asosiy menyu</b>\n🔑 ID: <b>{sid}</b>",
        reply_markup=main_kb(uid,la_), parse_mode="HTML")
    for k in ["di_name","di_addr","di_mchj","di_tel1","di_tel2","di_ega"]: ctx.user_data.pop(k,None)
    return ST_MAIN

# Do'kon zakaz qo'shish
async def dokon_zakaz_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    store_id = q.data.split(":")[1]
    stores = get_stores(dist_id=str(upd.effective_user.id))
    store = next((s for s in stores if str(s.get("ID",""))==store_id), None)
    if not store: return ST_MAIN
    ctx.user_data["zakaz_store"] = store
    prods = get_products()
    await q.edit_message_text(
        f"📋 {store.get('Nomi','')} uchun zakaz\nMahsulotni tanlang:",
        reply_markup=prod_kb(prods, la_, prefix="zprod", back_cb="m:dokonlar"))
    return ST_MAIN

async def zakaz_prod_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    pid = int(q.data.split(":")[1]); prods = get_products()
    p = next((x for x in prods if x["id"]==pid), None)
    if not p: return ST_MAIN
    ctx.user_data["zakaz_p"] = p
    brinza = is_brinza(p[la_])
    await q.edit_message_text(
        f"📦 <b>{p[la_]}</b>\n\n"
        f"{'Nechta? (dona, butun son)' if brinza else 'Miqdorni kiriting (masalan: 5)'}:",
        parse_mode="HTML")
    return ST_ZAKAZ_FROM_QTY

async def zakaz_from_qty(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id)
    p = ctx.user_data.get("zakaz_p",{})
    store = ctx.user_data.get("zakaz_store",{})
    prod_name = p.get(la_,""); brinza = is_brinza(prod_name)
    if brinza:
        try: qty=int(float(upd.message.text.strip().replace(",",".")))
        except: qty=0
        if qty<=0:
            await upd.message.reply_text("Butun son kiriting:"); return ST_ZAKAZ_FROM_QTY
        birlik="dona"
    else:
        qty=parse_weight(upd.message.text)
        if qty<=0:
            await upd.message.reply_text("❌ Noto'g'ri:"); return ST_ZAKAZ_FROM_QTY
        birlik=p.get("unit","kg")
    store_id=str(store.get("ID","")); store_name=store.get("Nomi","")
    dist_id=str(store.get("Dist_ID","")) or uid
    zakaz_id=make_id("Z")
    db_append("Buyurtmalar",[now_str(),store_id,store_name,dist_id,prod_name,qty,"Yangi","",zakaz_id])
    qty_str=fmt_qty(qty,birlik,prod_name,birlik=="kg")
    await upd.message.reply_text(
        f"✅ Zakaz qo'shildi!\n🏪 {store_name}\n📦 {prod_name}: {qty_str}",
        reply_markup=main_kb(upd.effective_user.id,la_))
    return ST_MAIN

# Do'kon ma'lumot o'zgartirish
async def dokon_edit_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    store_id = q.data.split(":")[1]
    stores = db_all("Dokonlar")
    store = next((s for s in stores if str(s.get("ID",""))==store_id), None)
    if not store: return ST_MAIN
    ctx.user_data["edit_dokon_id"] = store_id
    ctx.user_data["edit_dokon"] = dict(store)
    fields={"1":"Nomi","2":"Adres","3":"MCHJ","4":"Tel1","5":"Tel2","6":"Ega_Ismi"}
    lines=[f"✏️ <b>{store.get('Nomi','')} — tahrirlash</b>\n"]
    for k,v in fields.items():
        lines.append(f"{k}. {v}: <i>{store.get(v,'—') or '—'}</i>")
    lines.append("\nRaqamni bosing:")
    rows=[[("1","ef:1"),("2","ef:2"),("3","ef:3")],[("4","ef:4"),("5","ef:5"),("6","ef:6")],[("🔙 Orqaga","m:dokonlar")]]
    await q.edit_message_text("\n".join(lines),reply_markup=ikr(*rows),parse_mode="HTML")
    return ST_MAIN

async def dokon_edit_field_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    num = q.data.split(":")[1]
    fields={"1":"Nomi","2":"Adres","3":"MCHJ","4":"Tel1","5":"Tel2","6":"Ega_Ismi"}
    field = fields.get(num,"Nomi")
    ctx.user_data["edit_field"] = field
    store = ctx.user_data.get("edit_dokon",{})
    await q.edit_message_text(
        f"✏️ <b>{field}</b>\nJoriy: <i>{store.get(field,'—') or '—'}</i>\n\nYangi qiymat kiriting:",
        parse_mode="HTML", reply_markup=back_ik("m:dokonlar"))
    return ST_DOKON_EDIT_VAL

async def dokon_edit_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx)
    field = ctx.user_data.get("edit_field","")
    store_id = ctx.user_data.get("edit_dokon_id","")
    new_val = upd.message.text.strip()
    try:
        w = get_ws("Dokonlar")
        if w:
            headers = w.row_values(1)
            if field in headers:
                recs = w.get_all_records()
                for i,r in enumerate(recs):
                    if str(r.get("ID",""))==store_id:
                        w.update_cell(i+2, headers.index(field)+1, new_val); break
    except Exception as e: logger.error(f"dokon_edit: {e}")
    ctx.user_data["edit_dokon"][field] = new_val
    await upd.message.reply_text(
        f"✅ {field} yangilandi: <b>{new_val}</b>", parse_mode="HTML",
        reply_markup=main_kb(upd.effective_user.id, la_))
    return ST_MAIN


# ── NARXLAR ────────────────────────────────────────────────────────────────────
async def narxlar_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id); prods = get_products()
    await edit_or_send(upd,ctx,
        "💰 <b>Narxlarim</b>\nMahsulotni tanlang:" if la_=="uz" else "💰 <b>Мои цены</b>\nВыберите товар:",
        prod_kb(prods, la_, prefix="narx", back_cb="m:main"))
    return ST_MAIN

async def narx_prod_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx); uid = str(upd.effective_user.id)
    pid = int(q.data.split(":")[1]); prods = get_products()
    p = next((x for x in prods if x["id"]==pid), None)
    if not p: return ST_MAIN
    ctx.user_data["narx_p"] = p
    narx, tn = get_price(pid, dist_id=uid)
    if narx==0: narx, tn = get_price(pid)
    stores = get_stores(dist_id=uid)
    await q.edit_message_text(
        f"💰 <b>{p[la_]}</b>\nJoriy: {narx:,.0f} / Tannarx: {tn:,.0f}\n\nQaysi narxni o'zgartirish?",
        reply_markup=ikr(
            [("🔵 Barcha do'konlar","narx_type:all")],
            [("🟡 Bitta do'kon uchun","narx_type:one")] if stores else [],
            [("🔙 Orqaga","m:narxlar")]
        ) if stores else ikr([("🔵 Barcha do'konlar","narx_type:all")],[("🔙 Orqaga","m:narxlar")]),
        parse_mode="HTML")
    return ST_MAIN

async def narx_type_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx); uid = str(upd.effective_user.id)
    ntype = q.data.split(":")[1]
    if ntype == "all":
        ctx.user_data["narx_dokon_id"] = ""
        await q.edit_message_text("💰 Yangi narxni kiriting (masalan: 15000):",
            reply_markup=back_ik("m:narxlar"))
        return ST_NARX_VAL
    else:
        stores = get_stores(dist_id=uid)
        await q.edit_message_text("Do'konni tanlang:",
            reply_markup=store_kb(stores, prefix="narx_d", back_cb="m:narxlar"))
        return ST_MAIN

async def narx_dokon_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    store_id = q.data.split(":")[1]
    ctx.user_data["narx_dokon_id"] = store_id
    await q.edit_message_text("💰 Ushbu do'kon uchun narxni kiriting:",
        reply_markup=back_ik("m:narxlar"))
    return ST_NARX_DOKON_VAL

async def narx_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id)
    price = parse_money(upd.message.text)
    if price <= 0:
        await upd.message.reply_text("❌ Noto'g'ri summa:"); return ST_NARX_VAL
    ctx.user_data["narx_val"] = price
    await upd.message.reply_text("📉 Tannarxni kiriting (masalan: 12000):")
    return ST_NARX_COST

async def narx_dokon_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx)
    price = parse_money(upd.message.text)
    if price <= 0:
        await upd.message.reply_text("❌ Noto'g'ri:"); return ST_NARX_DOKON_VAL
    ctx.user_data["narx_val"] = price
    await upd.message.reply_text("📉 Tannarxni kiriting:")
    return ST_NARX_DOKON_COST

async def narx_cost(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = str(upd.effective_user.id)
    cost = parse_money(upd.message.text)
    p = ctx.user_data.get("narx_p",{}); price = ctx.user_data.get("narx_val",0)
    dokon_id = ctx.user_data.get("narx_dokon_id","")
    set_price(p.get("id",0), p.get(la_,""), price, cost, dist_id=uid, dokon_id=dokon_id)
    await upd.message.reply_text(
        f"✅ Narx yangilandi!\n{p.get(la_,'')}: {price:,.0f} / Tannarx: {cost:,.0f}",
        reply_markup=main_kb(upd.effective_user.id, la_))
    return ST_MAIN

async def narx_dokon_cost(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    return await narx_cost(upd, ctx)

# ── ADMIN PANEL ────────────────────────────────────────────────────────────────
async def admin_menu(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); uid = upd.effective_user.id
    if uid not in ADMIN_IDS:
        await edit_or_send(upd,ctx,"🚫 Ruxsat yo'q!"); return ST_MAIN
    await edit_or_send(upd,ctx,
        "⚙️ <b>Admin panel</b>",
        ikr(
            [("➕ Mahsulot qo'shish","adm:prod")],
            [("💰 Umumiy narxlar","adm:price")],
            [("🏪 Do'kon qo'shish","adm:store")],
            [("🚚 Distribyutor qo'shish","adm:dist")],
            [("📊 Statistika","adm:stats"),("💸 Qarzdorlar","adm:debt")],
            [("🏪 Do'konlar ro'yxati","adm:stores"),("🚚 Distribyutorlar","adm:dists")],
            [("📦 Zavod so'rovlari","adm:zavod")],
            [("📢 Xabar yuborish","adm:bc")],
            [("🔙 Orqaga","m:main")]
        ))
    return ST_MAIN

async def admin_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q)
    uid = upd.effective_user.id; la_ = la(ctx)
    if uid not in ADMIN_IDS: return ST_MAIN
    data = q.data.split(":")[1]


    if data == "prod":
        await q.edit_message_text("➕ Mahsulot nomi (o'zbekcha):",reply_markup=back_ik("m:admin"))
        return ST_ADM_MAHSULOT_UZ

    if data == "price":
        prods = get_products()
        await q.edit_message_text("💰 Narx o'zgartirish\nMahsulotni tanlang:",
            reply_markup=prod_kb(prods,la_,prefix="adm_narx",back_cb="m:admin"))
        return ST_MAIN

    if data == "store":
        await q.edit_message_text("🏪 Yangi do'kon nomi:",reply_markup=back_ik("m:admin"))
        ctx.user_data["adm_mode"]="store"
        return ST_ADM_DOKON_NAME

    if data == "dist":
        await q.edit_message_text("🚚 Distribyutor ismi:",reply_markup=back_ik("m:admin"))
        return ST_ADM_DIST_NAME

    if data == "stats":   return await adm_stats(upd,ctx)
    if data == "debt":    return await adm_debt(upd,ctx)
    if data == "stores":  return await adm_stores_list(upd,ctx)
    if data == "dists":   return await adm_dists_list(upd,ctx)
    if data == "zavod":   return await adm_zavod_list(upd,ctx)
    if data == "bc":
        await q.edit_message_text("📢 Xabar matnini kiriting:",reply_markup=back_ik("m:admin"))
        return ST_ADM_BROADCAST
    return ST_MAIN

async def adm_narx_prod_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    pid = int(q.data.split(":")[1]); prods = get_products()
    p = next((x for x in prods if x["id"]==pid), None)
    if not p: return ST_MAIN
    ctx.user_data["narx_p"]=p; ctx.user_data["narx_dokon_id"]=""
    narx,tn=get_price(pid)
    await q.edit_message_text(
        f"💰 <b>{p[la_]}</b>\nJoriy: {narx:,.0f} / {tn:,.0f}\n\nYangi narx kiriting:",
        parse_mode="HTML",reply_markup=back_ik("m:admin"))
    return ST_ADM_NARX_VAL

async def adm_narx_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); price=parse_money(upd.message.text)
    if price<=0: await upd.message.reply_text("❌ Noto'g'ri:"); return ST_ADM_NARX_VAL
    ctx.user_data["narx_val"]=price
    await upd.message.reply_text("📉 Tannarx kiriting:")
    return ST_ADM_NARX_COST

async def adm_narx_cost(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); cost=parse_money(upd.message.text)
    p=ctx.user_data.get("narx_p",{}); price=ctx.user_data.get("narx_val",0)
    set_price(p.get("id",0),p.get(la_,""),price,cost)
    await upd.message.reply_text(
        f"✅ Narx: {p.get(la_,'')} = {price:,.0f} / Tannarx: {cost:,.0f}",
        reply_markup=main_kb(upd.effective_user.id,la_))
    return ST_MAIN

# Mahsulot qo'shish
async def adm_prod_uz(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["m_uz"]=upd.message.text.strip()
    await upd.message.reply_text("Mahsulot nomi (ruscha):"); return ST_ADM_MAHSULOT_RU

async def adm_prod_ru(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["m_ru"]=upd.message.text.strip()
    await upd.message.reply_text("Birligini tanlang:",
        reply_markup=ikr([("kg","unit:kg"),("litr","unit:litr")],[("dona","unit:dona"),("g","unit:g")]))
    return ST_ADM_MAHSULOT_UNIT

async def adm_prod_unit_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await answer(q); la_ = la(ctx)
    unit = q.data.split(":")[1]
    uz=ctx.user_data.get("m_uz",""); ru=ctx.user_data.get("m_ru","")
    recs=db_all("Mahsulotlar"); nid=max([int(r.get("ID",0)) for r in recs],default=0)+1
    db_append("Mahsulotlar",[str(nid),uz,ru,unit,"1",now_str()])
    await q.edit_message_text(f"✅ Mahsulot qo'shildi: {uz}",reply_markup=back_ik("m:admin"))
    return ST_MAIN

# Admin do'kon qo'shish
async def adm_dokon_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["adm_d_name"]=upd.message.text.strip()
    await upd.message.reply_text("📍 Manzil:"); return ST_ADM_DOKON_ADDR

async def adm_dokon_addr(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["adm_d_addr"]=upd.message.text.strip()
    await upd.message.reply_text("🚚 Distribyutor Telegram ID:"); return ST_ADM_DOKON_DIST

async def adm_dokon_dist(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); dist_id=upd.message.text.strip()
    u=get_user(dist_id); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else str(dist_id)
    name=ctx.user_data.get("adm_d_name",""); addr=ctx.user_data.get("adm_d_addr","")
    cnt=len(db_all("Dokonlar"))+1
    db_append("Dokonlar",[str(cnt),name,addr,"","","","",dist_id,dn,now_str()])
    await upd.message.reply_text(f"✅ Do'kon qo'shildi: {name}",reply_markup=main_kb(upd.effective_user.id,la_))
    return ST_MAIN

# Distribyutor qo'shish
async def adm_dist_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["adm_dist_name"]=upd.message.text.strip()
    await upd.message.reply_text("Telegram ID:"); return ST_ADM_DIST_TG

async def adm_dist_tg(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); tg=upd.message.text.strip(); name=ctx.user_data.get("adm_dist_name","")
    sid=make_sid()
    db_append("Foydalanuvchilar",[tg,name,"","","distributor","uz","","tasdiqlangan",sid,now_str()])
    await upd.message.reply_text(f"✅ Distribyutor: {name} | ID: {sid}",reply_markup=main_kb(upd.effective_user.id,la_))
    return ST_MAIN

# Broadcast
async def adm_broadcast(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la_ = la(ctx); t=upd.message.text
    users=db_all("Foydalanuvchilar"); sent=0; failed=0
    for u in users:
        try: await ctx.bot.send_message(int(u.get("TG_ID",0)),t); sent+=1
        except Exception: failed+=1
    await upd.message.reply_text(f"✅ Yuborildi: {sent} | Xato: {failed}",reply_markup=main_kb(upd.effective_user.id,la_))
    return ST_MAIN

# Admin statistika funksiyalari
async def adm_stats(upd, ctx):
    q = upd.callback_query
    tops=db_all("Topshirish"); ins=db_all("Qabul"); stores=db_all("Dokonlar"); users=db_all("Foydalanuvchilar")
    ts=sum(float(r.get("Jami",0) or 0) for r in tops if r.get("Status","")=="tasdiqlangan")
    ti=sum(float(r.get("Jami",0) or 0) for r in ins if r.get("Status","")=="tasdiqlangan")
    jq=sum(get_debt(str(s.get("ID",""))) for s in stores)
    dists=[u for u in users if u.get("Rol","")=="distributor"]
    foyda=ts-ti; zakaz=len([r for r in db_all("Buyurtmalar") if r.get("Status","")=="Yangi"])
    msg=(f"📊 <b>Statistika</b>\n━━━━━━━━━━━━━━━━\n"
         f"📥 Zavod: {ti:,.0f}\n🚚 Sotuv: {ts:,.0f}\n"
         f"💸 Jami qarz: {jq:,.0f}\n💰 Foyda: {foyda:,.0f}\n"
         f"🏪 Do'konlar: {len(stores)}\n🚚 Distribyutorlar: {len(dists)}\n"
         f"📋 Yangi zakazlar: {zakaz}")
    await q.edit_message_text(msg,reply_markup=back_ik("m:admin"),parse_mode="HTML")
    return ST_MAIN

async def adm_debt(upd, ctx):
    q = upd.callback_query; stores=db_all("Dokonlar"); lines=["💸 <b>Qarzdorlar:</b>","━━━━━━━━━━━━━━━━"]; total=0
    for s in stores:
        debt=get_debt(str(s.get("ID","")))
        if debt>0: lines.append(f"• {s.get('Nomi','')}: {debt:,.0f}\n  🚚 {s.get('Dist_Ism','')}"); total+=debt
    if len(lines)==2: lines.append("Qarz yo'q!")
    else: lines.append(f"━━━━━━━━━━━━━━━━\nJami: {total:,.0f}")
    await q.edit_message_text("\n".join(lines),reply_markup=back_ik("m:admin"),parse_mode="HTML")
    return ST_MAIN

async def adm_stores_list(upd, ctx):
    q = upd.callback_query; stores=db_all("Dokonlar")
    if not stores:
        await q.edit_message_text("Do'konlar yo'q.",reply_markup=back_ik("m:admin")); return ST_MAIN
    lines=[f"🏪 <b>Do'konlar: {len(stores)} ta</b>","━━━━━━━━━━━━━━━━"]
    for s in stores:
        debt=get_debt(str(s.get("ID",""))); d=f" | {debt:,.0f}" if debt>0 else ""
        lines.append(f"• {s.get('Nomi','')}{d}\n  📍 {s.get('Adres','-')}\n  📞 {s.get('Tel1','-')}\n  🚚 {s.get('Dist_Ism','')}")
    text="\n".join(lines)
    for i in range(0,len(text),3500): await ctx.bot.send_message(upd.effective_user.id,text[i:i+3500],parse_mode="HTML")
    await ctx.bot.send_message(upd.effective_user.id,"⬆️",reply_markup=back_ik("m:admin"))
    return ST_MAIN

async def adm_dists_list(upd, ctx):
    q = upd.callback_query; dists=[u for u in db_all("Foydalanuvchilar") if u.get("Rol","")=="distributor"]
    if not dists:
        await q.edit_message_text("Distribyutorlar yo'q.",reply_markup=back_ik("m:admin")); return ST_MAIN
    lines=[f"🚚 <b>Distribyutorlar: {len(dists)} ta</b>","━━━━━━━━━━━━━━━━"]
    for u in dists:
        uid_d=u.get("TG_ID",""); sid=u.get("Short_ID","?")
        name=f"{u.get('Ism','')} {u.get('Familiya','')}".strip()
        stores=get_stores(dist_id=uid_d); jq=sum(get_debt(str(s.get("ID",""))) for s in stores)
        lines.append(f"• {name} (ID:{sid})\n  📞 {u.get('Telefon','')}\n  🏪 {len(stores)} do'kon | 💸 {jq:,.0f}")
    text="\n".join(lines)
    for i in range(0,len(text),3500): await ctx.bot.send_message(upd.effective_user.id,text[i:i+3500],parse_mode="HTML")
    await ctx.bot.send_message(upd.effective_user.id,"⬆️",reply_markup=back_ik("m:admin"))
    return ST_MAIN

async def adm_zavod_list(upd, ctx):
    q = upd.callback_query; recs=[r for r in db_all("Qabul") if r.get("Status","")=="kutilmoqda"]
    if not recs:
        await q.edit_message_text("📦 Kutilayotgan so'rov yo'q.",reply_markup=back_ik("m:admin")); return ST_MAIN
    lines=[f"📦 <b>Kutilmoqda: {len(recs)} ta</b>","━━━━━━━━━━━━━━━━"]
    for r in recs:
        qid=r.get("Qabul_ID","")
        lines.append(f"• {r.get('Dist_Ism','')} | {r.get('Mahsulot','')} {r.get('Miqdor','')}\n  Jami: {float(r.get('Jami',0) or 0):,.0f}\n  ✅ /zok_{qid} | ❌ /zrad_{qid}")
    text="\n".join(lines)
    for i in range(0,len(text),3500): await ctx.bot.send_message(upd.effective_user.id,text[i:i+3500],parse_mode="HTML")
    await ctx.bot.send_message(upd.effective_user.id,"⬆️",reply_markup=back_ik("m:admin"))
    return ST_MAIN


# ── SCHEDULER ──────────────────────────────────────────────────────────────────
async def debt_reminder(ctx: ContextTypes.DEFAULT_TYPE):
    """Har kuni 09:00 qarz eslatmasi"""
    try:
        for u in db_all("Foydalanuvchilar"):
            if u.get("Rol","")!="distributor" or not is_approved(u.get("TG_ID","")): continue
            uid=str(u.get("TG_ID",""))
            stores=get_stores(dist_id=uid)
            debts=[(s.get("Nomi",""),get_debt(str(s.get("ID","")))) for s in stores]
            debts=[(n,d) for n,d in debts if d>0]
            if not debts: continue
            try:
                lines=["💸 <b>Bugungi qarzlar:</b>","━━━━━━━━━━━━━━━━"]
                for name,debt in debts: lines.append(f"• {name}: {debt:,.0f}")
                await ctx.bot.send_message(int(uid),"\n".join(lines),parse_mode="HTML")
            except Exception: pass
    except Exception as e: logger.error(f"debt_reminder: {e}")

async def auto_zakaz_reminder(ctx: ContextTypes.DEFAULT_TYPE):
    """Har kuni 20:00 kecha mol berilgan do'konlar eslatmasi"""
    try:
        kecha=(datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
        tops=db_all("Topshirish"); dist_map={}
        for r in tops:
            if not str(r.get("Sana","")).startswith(kecha): continue
            if r.get("Status","")!="tasdiqlangan": continue
            did=str(r.get("Dist_ID",""))
            if not did or did=="0": continue
            did_key=did; dokon=r.get("Dokon",""); dokon_id=str(r.get("Dokon_ID",""))
            dist_map.setdefault(did_key,{})[f"{dokon}:{dokon_id}"]=(dokon,dokon_id)
        for did,dokonlar in dist_map.items():
            try:
                du=get_user(did)
                if not du: continue
                lines=["📋 <b>KECHA MOL BERILGAN DO'KONLAR:</b>\n(Zakaz olish kerakmi?)\n","━━━━━━━━━━━━━━━━"]
                for dk_name, dk_id in dokonlar.values():
                    stores=db_all("Dokonlar"); store=next((s for s in stores if str(s.get("ID",""))==dk_id),None)
                    tel=store.get("Tel1","") if store else ""
                    tel2=store.get("Tel2","") if store else ""
                    tel_str=f"📞 {tel}" if tel else ""
                    if tel2: tel_str+=f" / {tel2}"
                    lines.append(f"• <b>{dk_name}</b>\n  {tel_str}")
                lines.append("\nUlar bilan bog'laning!")
                await ctx.bot.send_message(int(did),"\n".join(lines),parse_mode="HTML")
            except Exception as e: logger.error(f"zakaz remind: {e}")
    except Exception as e: logger.error(f"auto_zakaz: {e}")

async def tovar_24h_reminder(ctx: ContextTypes.DEFAULT_TYPE):
    """Har soatda: 24 soat oldin topshirilgan tovarlar eslatmasi"""
    try:
        now=datetime.now(); tops=db_all("Topshirish")
        for r in tops:
            if r.get("Status","")!="tasdiqlangan": continue
            try: top_time=datetime.strptime(r.get("Sana",""),"%Y-%m-%d %H:%M")
            except Exception: continue
            hours=(now-top_time).total_seconds()/3600
            if not (23.5<=hours<=24.5): continue
            did=str(r.get("Dist_ID",""))
            if not did or did=="0": continue
            try:
                du=get_user(did)
                if not du: continue
                dokon=r.get("Dokon",""); prod=r.get("Mahsulot",""); miqdor=r.get("Miqdor","")
                dokon_id=str(r.get("Dokon_ID",""))
                store=next((s for s in db_all("Dokonlar") if str(s.get("ID",""))==dokon_id),None)
                tel=store.get("Tel1","") if store else ""
                await ctx.bot.send_message(int(did),
                    f"📋 <b>24 SOAT ESLATMASI</b>\n"
                    f"🏪 {dokon}\n📞 {tel}\n"
                    f"📦 {prod}: {miqdor}\n\n"
                    f"Yangi zakaz bormi? Do'kon bilan bog'laning!",
                    parse_mode="HTML")
            except Exception as e: logger.error(f"24h: {e}")
    except Exception as e: logger.error(f"tovar_24h: {e}")


async def tovar_5kun_almashtirish(ctx: ContextTypes.DEFAULT_TYPE):
    """
    Har kuni tekshiradi: 5 kun oldin mol berilgan do'konlar uchun
    yangi buyurtma bo'lmasa — tovarni almashtirish eslatmasi yuboriladi.
    """
    try:
        now = datetime.now()
        besh_kun_oldin = (now - timedelta(days=5)).strftime("%Y-%m-%d")
        olti_kun_oldin = (now - timedelta(days=6)).strftime("%Y-%m-%d")

        tops = db_all("Topshirish")
        buyurtmalar = db_all("Buyurtmalar")

        # 5-6 kun oldin topshirilgan (aniq 5 kun o'tgan)
        relevant_tops = [
            r for r in tops
            if r.get("Status","")=="tasdiqlangan"
            and olti_kun_oldin <= str(r.get("Sana",""))[:10] <= besh_kun_oldin
        ]

        for top in relevant_tops:
            dist_id = str(top.get("Dist_ID",""))
            dokon_id = str(top.get("Dokon_ID",""))
            dokon = top.get("Dokon","")
            mahsulot = top.get("Mahsulot","")

            if not dist_id or not dokon_id: continue

            # Shu do'kondan shu mahsulot uchun so'nggi 5 kunda yangi buyurtma bormi?
            yangi_zakaz = any(
                b for b in buyurtmalar
                if str(b.get("Dokon_ID",""))==dokon_id
                and b.get("Mahsulot","")==mahsulot
                and str(b.get("Sana",""))[:10] >= besh_kun_oldin
            )

            if yangi_zakaz:
                continue  # Buyurtma bor — eslatma shart emas

            try:
                du = get_user(dist_id)
                if not du: continue
                qty = float(top.get("Miqdor",0) or 0)
                unit = top.get("Birlik","")
                qty_str = fmt_qty(qty, unit, mahsulot, topshirish=False)
                sana = str(top.get("Sana",""))[:10]

                msg = (
                    f"🔄 <b>TOVAR ALMASHTIRISH ESLATMASI</b>\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"🏪 Do'kon: <b>{dokon}</b>\n"
                    f"📦 Mahsulot: {mahsulot} — {qty_str}\n"
                    f"📅 Berilgan: {sana}\n\n"
                    f"⚠️ 5 kun o'tdi, yangi buyurtma yo'q!\n"
                    f"Tovarni almashtirish yoki tekshirish vaqti bo'lishi mumkin.\n\n"
                    f"Do'kon bilan bog'laning! 📞"
                )

                stores = db_all("Dokonlar")
                store = next((s for s in stores if str(s.get("ID",""))==dokon_id), None)
                if store:
                    tel = store.get("Tel1","")
                    if tel: msg += f"\n📞 {tel}"

                await ctx.bot.send_message(int(dist_id), msg, parse_mode="HTML")

            except Exception as e:
                logger.error(f"5kun_almashtirish dist {dist_id}: {e}")

    except Exception as e:
        logger.error(f"tovar_5kun_almashtirish: {e}")

def main():
    if not BOT_TOKEN: print("BOT_TOKEN yo'q!"); return
    app = Application.builder().token(BOT_TOKEN).build()

    # Scheduler
    app.job_queue.run_daily(debt_reminder, time=dtime(9,0))
    app.job_queue.run_daily(auto_zakaz_reminder, time=dtime(20,0))
    app.job_queue.run_repeating(tovar_24h_reminder, interval=3600, first=60)
    # Har kuni 10:00 da 5 kunlik almashtirish eslatmasi
    app.job_queue.run_daily(tovar_5kun_almashtirish, time=dtime(10,0))

    txt  = filters.TEXT & ~filters.COMMAND
    photo_txt = (filters.PHOTO | filters.TEXT) & ~filters.COMMAND
    cont_txt  = (filters.CONTACT | filters.TEXT) & ~filters.COMMAND

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            ST_LANG: [CallbackQueryHandler(lang_cb, pattern="^lang:")],
            ST_REG_NAME:    [MessageHandler(txt, reg_name)],
            ST_REG_FNAME:   [MessageHandler(txt, reg_fname)],
            ST_REG_PHONE:   [MessageHandler(cont_txt, reg_phone)],
            ST_REG_PASSPORT:[
                MessageHandler((filters.PHOTO|filters.TEXT)&~filters.COMMAND, reg_passport),
                CallbackQueryHandler(reg_passport, pattern="^skip_passport$"),
            ],
            ST_WAIT_APPROVE:[
                MessageHandler(txt, wait_approve),
                CallbackQueryHandler(wait_approve, pattern="^resend$"),
            ],
            ST_MAIN: [
                CallbackQueryHandler(main_cb,         pattern="^m:"),
                CallbackQueryHandler(zavod_prod_cb,   pattern="^zav:"),
                CallbackQueryHandler(top_store_cb,    pattern="^top_s:"),
                CallbackQueryHandler(top_prod_cb,     pattern="^top_p:"),
                CallbackQueryHandler(top_photo_ocr_cb,pattern="^ocr_"),
                CallbackQueryHandler(top_pay_cb,      pattern="^(pay:|voz:)"),
                CallbackQueryHandler(voz_prod_cb,     pattern="^voz_p:"),

                CallbackQueryHandler(dokon_add_cb,    pattern="^dokon:add$"),
                CallbackQueryHandler(dokon_zakaz_cb,  pattern="^dzak:"),
                CallbackQueryHandler(zakaz_prod_cb,   pattern="^zprod:"),
                CallbackQueryHandler(dokon_edit_cb,   pattern="^dedit:"),
                CallbackQueryHandler(dokon_edit_field_cb, pattern="^ef:"),
                CallbackQueryHandler(narx_prod_cb,    pattern="^narx:"),
                CallbackQueryHandler(narx_type_cb,    pattern="^narx_type:"),
                CallbackQueryHandler(narx_dokon_cb,   pattern="^narx_d:"),
                CallbackQueryHandler(admin_cb,        pattern="^adm:"),
                CallbackQueryHandler(adm_narx_prod_cb,pattern="^adm_narx:"),
                CallbackQueryHandler(adm_prod_unit_cb,pattern="^unit:"),
                CallbackQueryHandler(tolov_store_cb,  pattern="^tolov_s:"),
                CallbackQueryHandler(tolov_full_cb,   pattern="^tolov_full:"),
                MessageHandler(txt, lambda u,c: ST_MAIN),
            ],
            ST_ZAVOD_QTY:         [MessageHandler(txt, zavod_qty)],
            ST_TOP_STORE:         [CallbackQueryHandler(top_store_cb, pattern="^top_s:")],
            ST_TOP_PROD:          [CallbackQueryHandler(top_prod_cb, pattern="^top_p:")],
            ST_TOP_PHOTO:         [
                MessageHandler(photo_txt, top_photo),
                CallbackQueryHandler(top_photo_ocr_cb, pattern="^ocr_"),
            ],
            ST_TOP_PAY:           [
                CallbackQueryHandler(top_pay_cb, pattern="^(pay:|voz:|voz_p:)"),
                CallbackQueryHandler(voz_prod_cb, pattern="^voz_p:"),
                MessageHandler(txt, top_naqd),
            ],
            ST_TOP_NAQD:          [MessageHandler(txt, top_naqd)],
            ST_VOZ_PROD:          [CallbackQueryHandler(voz_prod_cb, pattern="^voz_p:")],
            ST_VOZ_QTY:           [MessageHandler(txt, voz_qty)],

            ST_DI_NAME:  [MessageHandler(txt, di_name)],
            ST_DI_ADDR:  [MessageHandler(txt, di_addr)],
            ST_DI_MCHJ:  [
                MessageHandler(txt, di_mchj),
                CallbackQueryHandler(di_mchj, pattern="^di_skip:mchj$"),
            ],
            ST_DI_TEL1:  [MessageHandler(cont_txt, di_tel1)],
            ST_DI_TEL2:  [
                MessageHandler(cont_txt, di_tel2),
                CallbackQueryHandler(di_tel2, pattern="^di_skip:tel2$"),
            ],
            ST_DI_EGA:   [MessageHandler(txt, di_ega)],
            ST_DI_PHOTO: [MessageHandler(photo_txt, di_photo)],
            ST_NARX_VAL:       [MessageHandler(txt, narx_val)],
            ST_NARX_COST:      [MessageHandler(txt, narx_cost)],
            ST_NARX_DOKON_VAL: [MessageHandler(txt, narx_dokon_val)],
            ST_NARX_DOKON_COST:[MessageHandler(txt, narx_dokon_cost)],
            ST_ZAKAZ_FROM_QTY: [MessageHandler(txt, zakaz_from_qty)],
            ST_DOKON_EDIT_VAL: [MessageHandler(txt, dokon_edit_val)],
            ST_ADM_MAHSULOT_UZ:  [MessageHandler(txt, adm_prod_uz)],
            ST_ADM_MAHSULOT_RU:  [MessageHandler(txt, adm_prod_ru)],
            ST_ADM_MAHSULOT_UNIT:[CallbackQueryHandler(adm_prod_unit_cb, pattern="^unit:")],
            ST_ADM_NARX_VAL:     [MessageHandler(txt, adm_narx_val)],
            ST_ADM_NARX_COST:    [MessageHandler(txt, adm_narx_cost)],
            ST_ADM_DOKON_NAME:   [MessageHandler(txt, adm_dokon_name)],
            ST_ADM_DOKON_ADDR:   [MessageHandler(txt, adm_dokon_addr)],
            ST_ADM_DOKON_DIST:   [MessageHandler(txt, adm_dokon_dist)],
            ST_ADM_DIST_NAME:    [MessageHandler(txt, adm_dist_name)],
            ST_ADM_DIST_TG:      [MessageHandler(txt, adm_dist_tg)],
            ST_ADM_BROADCAST:    [MessageHandler(txt, adm_broadcast)],
            ST_TOLOV_SUMMA: [
                MessageHandler(txt, tolov_summa),
                CallbackQueryHandler(tolov_full_cb, pattern="^tolov_full:"),
                CallbackQueryHandler(tolov_show,    pattern="^m:tolov$"),
            ],
            ST_ZAKAZ_EDIT_QTY:   [MessageHandler(txt, lambda u,c: ST_MAIN)],
        },
        fallbacks=[CommandHandler("start", start), CommandHandler("cancel", start)],
        allow_reentry=True,
    )

    # Command handlers
    for pattern, handler in [
        (r'^/approve_\d+$',  approve_cmd),
        (r'^/reject_\d+$',   reject_cmd),
        (r'^/zok_\w+$',      zok_cmd),
        (r'^/zrad_\w+$',     zrad_cmd),
        (r'^/tok_\w+$',      tok_cmd),
        (r'^/trad_\w+$',     trad_cmd),
        (r'^/vok_\w+$',      vok_cmd),
        (r'^/vrad_\w+$',     vrad_cmd),
    ]:
        app.add_handler(MessageHandler(filters.Regex(pattern), handler))

    app.add_handler(conv)
    print("Alba Milk Bot v4.0 — Inline Keyboard")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
