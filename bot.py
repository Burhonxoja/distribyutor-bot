import os, logging, json, re, base64, random
from datetime import datetime, timedelta, time as dtime
import gspread
from google.oauth2.service_account import Credentials
import google.auth.transport.requests
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, KeyboardButton
from telegram.ext import (Application, CommandHandler, MessageHandler, CallbackQueryHandler,
                          ConversationHandler, filters, ContextTypes)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN         = os.environ.get("BOT_TOKEN", "")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON", "")
SPREADSHEET_ID    = os.environ.get("SPREADSHEET_ID", "")
ADMIN_IDS         = [int(x) for x in os.environ.get("ADMIN_IDS","0").split(",") if x.strip()]

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

def get_products():
    try:
        recs = db_all("Mahsulotlar")
        if recs:
            return [{"id":int(r.get("ID",0)),"uz":r.get("Nomi_UZ",""),"ru":r.get("Nomi_RU",""),"unit":r.get("Birlik","kg")}
                    for r in recs if str(r.get("Faol","1"))=="1"]
    except Exception: pass
    return DEFAULT_PRODUCTS

# ── STATES ────────────────────────────────────────────────────────────────────
(
    LANG_SELECT, ROLE_SELECT,
    REG_NAME, REG_FNAME, REG_PHONE, REG_PASSPORT,
    WAIT_APPROVE, MAIN_MENU,
    DIST_LINK_ID, DOKON_LINK_ID,
    ZAVOD_PROD, ZAVOD_QTY,
    TOP_STORE, TOP_PROD, TOP_PHOTO, TOP_PAY_TYPE, TOP_PAY_AMOUNT,
    ZAKAZ_COMMENT,
    DI_NAME, DI_ADDR, DI_MCHJ, DI_TEL1, DI_TEL2, DI_PHOTO, DI_LOC,
    NARX_PROD, NARX_TYPE, NARX_VAL, NARX_COST,
    NARX_DOKON, NARX_DOKON_VAL, NARX_DOKON_COST,
    HISOBOT_MENU,
    DOKON_MENU, DOKON_ZAKAZ_PROD, DOKON_ZAKAZ_QTY, DOKON_TOLOV_AMOUNT,
    ADMIN_MENU,
    ADM_MAHSULOT_NOM, ADM_MAHSULOT_RU, ADM_MAHSULOT_UNIT,
    ADM_PRICE_PROD, ADM_PRICE_VAL, ADM_COST_VAL,
    ADM_STORE_NAME, ADM_STORE_ADDR, ADM_STORE_DIST, ADM_STORE_LOC,
    ADM_DIST_NAME, ADM_DIST_ID,
    ADM_DOKON_EGA_NAME, ADM_DOKON_EGA_ID,
    ADM_BROADCAST,
) = range(53)

# ── SHEETS ────────────────────────────────────────────────────────────────────
def get_creds_dict():
    return json.loads(GOOGLE_CREDS_JSON) if GOOGLE_CREDS_JSON else {}

def get_sheet():
    if not GOOGLE_CREDS_JSON: return None
    try:
        creds = Credentials.from_service_account_info(
            get_creds_dict(),
            scopes=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)
    except Exception as e:
        logger.error(f"Sheet: {e}"); return None

SHEET_HEADERS = {
    "Foydalanuvchilar": ["TG_ID","Ism","Familiya","Telefon","Rol","Til","Passport","Status","Short_ID","Bog_Dist_ID","Sana"],
    "Mahsulotlar":      ["ID","Nomi_UZ","Nomi_RU","Birlik","Faol","Sana"],
    "Dokonlar":         ["ID","Short_ID","Nomi","Adres","MCHJ","Tel1","Tel2","Dist_ID","Dist_Ism","Egasi_ID","Egasi_Ism","Lat","Lng","Sana"],
    "Narxlar":          ["Mahsulot_ID","Mahsulot","Narx","Tannarx","Dist_ID","Dokon_ID","Sana"],
    "Qabul":            ["Sana","Dist_ID","Dist_Ism","Mahsulot","Miqdor","Birlik","Narx","Jami","Status","Qabul_ID"],
    "Topshirish":       ["Sana","Dist_ID","Dokon","Dokon_ID","Mahsulot","Miqdor","Birlik","Narx","Jami","Pay_Type","Naqd","Qarz","Status","Top_ID"],
    "Tolov":            ["Sana","Dist_ID","Dokon","Dokon_ID","Summa","Status","Tolov_ID"],
    "Buyurtmalar":      ["Sana","Dokon_ID","Dokon","Dist_ID","Mahsulot","Miqdor","Status","Izoh","Zakaz_ID"],
    "Bog_Sorov":        ["Sana","From_TG_ID","From_Short_ID","From_Rol","To_TG_ID","To_Short_ID","Status","Req_ID"],
}

def get_ws(name):
    wb = get_sheet()
    if not wb: return None
    try: return wb.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        w = wb.add_worksheet(name, rows=3000, cols=25)
        w.append_row(SHEET_HEADERS.get(name, ["Data"]))
        return w
    except Exception as e: logger.error(f"get_ws {name}: {e}"); return None

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
            if str(r.get(sc,"")).strip()==str(sv).strip():
                w.update_cell(i+2, headers.index(uc)+1, str(uv)); return True
        return False
    except Exception as e: logger.error(f"db_update: {e}"); return False

def db_delete_row(tab, sc, sv):
    try:
        w = get_ws(tab)
        if not w: return
        for i, r in enumerate(w.get_all_records()):
            if str(r.get(sc,"")).strip()==str(sv).strip():
                w.delete_rows(i+2); return
    except Exception as e: logger.error(f"db_delete_row: {e}")

def make_short_id():
    existing = set(str(r.get("Short_ID","")) for r in db_all("Foydalanuvchilar"))
    while True:
        sid = str(random.randint(100000, 999999))
        if sid not in existing: return sid

def make_op_id(prefix=""):
    return prefix + datetime.now().strftime("%m%d%H%M%S") + str(random.randint(10,99))

def now_str():   return datetime.now().strftime("%Y-%m-%d %H:%M")
def today_str(): return datetime.now().strftime("%Y-%m-%d")

# ── DB HELPERS ────────────────────────────────────────────────────────────────
def get_user(uid):
    try:
        for r in db_all("Foydalanuvchilar"):
            if str(r.get("TG_ID","")).strip()==str(uid).strip(): return r
        return None
    except Exception: return None

def get_user_by_short(sid):
    try:
        for r in db_all("Foydalanuvchilar"):
            if str(r.get("Short_ID","")).strip()==str(sid).strip(): return r
        return None
    except Exception: return None

def is_approved(uid):
    if int(uid) in ADMIN_IDS: return True
    u = get_user(uid)
    if not u: return False
    return str(u.get("Status","")).strip().lower() in ["tasdiqlangan","approved","1"]

def is_rejected(uid):
    u = get_user(uid)
    if not u: return False
    return str(u.get("Status","")).strip().lower() in ["rad_etildi","rejected"]

def get_short_id(uid):
    u = get_user(uid)
    return u.get("Short_ID","?") if u else "?"

def get_price(pid, dist_id=None, dokon_id=None):
    try:
        recs = db_all("Narxlar")
        if dist_id and dokon_id:
            for r in recs:
                if str(r.get("Mahsulot_ID",""))==str(pid) and str(r.get("Dist_ID",""))==str(dist_id) and str(r.get("Dokon_ID",""))==str(dokon_id):
                    return float(r.get("Narx",0) or 0), float(r.get("Tannarx",0) or 0)
        if dist_id:
            for r in recs:
                if str(r.get("Mahsulot_ID",""))==str(pid) and str(r.get("Dist_ID",""))==str(dist_id) and not str(r.get("Dokon_ID","")).strip():
                    return float(r.get("Narx",0) or 0), float(r.get("Tannarx",0) or 0)
        for r in recs:
            if str(r.get("Mahsulot_ID",""))==str(pid) and not str(r.get("Dist_ID","")).strip() and not str(r.get("Dokon_ID","")).strip():
                return float(r.get("Narx",0) or 0), float(r.get("Tannarx",0) or 0)
    except Exception as e: logger.error(f"get_price: {e}")
    return 0.0, 0.0

def set_price(pid, pname, price, cost, dist_id="", dokon_id=""):
    try:
        w = get_ws("Narxlar")
        if not w: return
        recs = w.get_all_records(); now = now_str()
        for i, r in enumerate(recs):
            if str(r.get("Mahsulot_ID",""))==str(pid) and str(r.get("Dist_ID",""))==str(dist_id) and str(r.get("Dokon_ID",""))==str(dokon_id):
                w.update(f"A{i+2}:G{i+2}", [[str(pid),pname,str(price),str(cost),str(dist_id),str(dokon_id),now]]); return
        w.append_row([str(pid),pname,str(price),str(cost),str(dist_id),str(dokon_id),now])
    except Exception as e: logger.error(f"set_price: {e}")

def get_stores(dist_id=None, egasi_id=None):
    try:
        recs = db_all("Dokonlar")
        if dist_id: return [r for r in recs if str(r.get("Dist_ID","")).strip()==str(dist_id).strip()]
        if egasi_id: return [r for r in recs if str(r.get("Egasi_ID","")).strip()==str(egasi_id).strip()]
        return recs
    except Exception: return []

def get_debt(dokon_id):
    try:
        qarz = sum(float(r.get("Qarz",0) or 0) for r in db_all("Topshirish")
                   if str(r.get("Dokon_ID",""))==str(dokon_id) and r.get("Status","")=="tasdiqlangan")
        paid = sum(float(r.get("Summa",0) or 0) for r in db_all("Tolov")
                   if str(r.get("Dokon_ID",""))==str(dokon_id) and r.get("Status","")=="tasdiqlangan")
        return max(0.0, qarz - paid)
    except Exception: return 0.0

def calc_foyda(dist_uid_str, from_date_str=None):
    """Distribyutor foydasi = sotuv summasi - zavod tannarxi"""
    try:
        tops = db_all("Topshirish")
        ins  = db_all("Qabul")
        if from_date_str:
            tops = [r for r in tops if str(r.get("Sana",""))>=from_date_str]
            ins  = [r for r in ins  if str(r.get("Sana",""))>=from_date_str]
        my_tops = [r for r in tops if str(r.get("Dist_ID",""))==dist_uid_str and r.get("Status","")=="tasdiqlangan"]
        my_ins  = [r for r in ins  if str(r.get("Dist_ID",""))==dist_uid_str and r.get("Status","")=="tasdiqlangan"]
        sotuv = sum(float(r.get("Jami",0) or 0) for r in my_tops)
        zavod = sum(float(r.get("Jami",0) or 0) for r in my_ins)
        return sotuv - zavod
    except Exception: return 0.0

def parse_weight(text):
    t = str(text).strip().replace(" ","").replace(",",".")
    try:
        val = float(t)
        return round(val/1000,3) if val>=100 else round(val,3)
    except: return 0.0

def parse_money(text):
    t = str(text).strip().replace(" ","")
    if re.match(r'^\d+[.,]\d{3}$', t): t = re.sub(r'[.,]','',t)
    else: t = t.replace(",",".")
    try: return float(t)
    except: return 0.0

def clean_phone(text):
    return re.sub(r'[^\d+]','',str(text).strip())

async def vision_ocr(image_bytes):
    try:
        import httpx
        creds = Credentials.from_service_account_info(
            get_creds_dict(), scopes=["https://www.googleapis.com/auth/cloud-vision"])
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
    if not text: return 0.0
    nums = re.findall(r'\d+', text)
    if not nums: return 0.0
    try:
        val = int(nums[0])
        return round(val/1000,3) if val>=100 else float(val)
    except: return 0.0


# ── MATNLAR ───────────────────────────────────────────────────────────────────
T = {
    "start":            {"uz":"Tilni tanlang:","ru":"Выберите язык:"},
    "role_select":      {"uz":"Kim sifatida ro'yxatdan o'tmoqchisiz?","ru":"Кем хотите зарегистрироваться?"},
    "role_dist":        {"uz":"🚚 Distribyutor","ru":"🚚 Дистрибьютор"},
    "role_dokon":       {"uz":"🏪 Do'kon egasi","ru":"🏪 Владелец магазина"},
    "reg_name":         {"uz":"Ismingizni kiriting:","ru":"Введите имя:"},
    "reg_fname":        {"uz":"Familiyangizni kiriting:","ru":"Введите фамилию:"},
    "reg_phone":        {"uz":"Telefon raqamingizni yuboring:","ru":"Отправьте номер телефона:"},
    "reg_passport":     {"uz":"Passport rasmini yuboring:\n(yoki Otkazib yuborish)","ru":"Фото паспорта:\n(или Пропустить)"},
    "reg_ok":           {"uz":"✅ Ro'yxatdan o'tdingiz {name}!\n🔑 Sizning ID: <b>{sid}</b>\nBu IDni saqlang!\n\nAdmin tasdiqlashini kuting...","ru":"✅ Вы зарегистрированы {name}!\n🔑 Ваш ID: <b>{sid}</b>\nСохраните ID!\n\nОжидайте подтверждения..."},
    "wait_approve":     {"uz":"⏳ Hisobingiz tasdiqlanmagan. Admin tasdiqlashini kuting.","ru":"⏳ Аккаунт не подтверждён. Ожидайте."},
    "resend_btn":       {"uz":"📤 Ma'lumotlarni qayta yuborish","ru":"📤 Повторно отправить"},
    "resent_ok":        {"uz":"Adminga yuborildi. Kuting.","ru":"Отправлено. Ожидайте."},
    "reg_admin_msg":    {"uz":"👤 YANGI {rol}:\nIsm: {name}\nTel: {phone}\nTG_ID: {uid}\nID: <b>{sid}</b>\n\n✅ /approve_{uid}\n❌ /reject_{uid}","ru":"👤 НОВЫЙ {rol}:\nИмя: {name}\nТел: {phone}\nTG_ID: {uid}\nID: <b>{sid}</b>\n\n✅ /approve_{uid}\n❌ /reject_{uid}"},
    "approved_msg":     {"uz":"✅ Hisobingiz tasdiqlandi!\n🔑 Sizning ID: <b>{sid}</b>\n\n/start bosing.","ru":"✅ Аккаунт подтверждён!\n🔑 Ваш ID: <b>{sid}</b>\n\nНажмите /start."},
    "rejected_msg":     {"uz":"❌ Hisobingiz rad etildi.","ru":"❌ Аккаунт отклонён."},
    "rejected_retry":   {"uz":"Qayta kiriting:","ru":"Введите заново:"},
    "main":             {"uz":"📋 Asosiy menyu | 🔑 ID: <b>{sid}</b>","ru":"📋 Главное меню | 🔑 ID: <b>{sid}</b>"},
    "qabul":            {"uz":"📥 Zavoddan qabul","ru":"📥 Получить с завода"},
    "buyurtma":         {"uz":"📋 Buyurtmalar","ru":"📋 Заказы"},
    "topshir":          {"uz":"🚚 Mol topshirish","ru":"🚚 Передать товар"},
    "tolov_qabul":      {"uz":"💵 Tolov qabul","ru":"💵 Принять оплату"},
    "natija":           {"uz":"📊 Kunlik natija","ru":"📊 Итог дня"},
    "ombor":            {"uz":"📦 Ombor","ru":"📦 Склад"},
    "marshrut":         {"uz":"🗺 Marshrut","ru":"🗺 Маршрут"},
    "hisobot":          {"uz":"📈 Hisobot","ru":"📈 Отчёт"},
    "my_stores":        {"uz":"🏪 Do'konlarim","ru":"🏪 Мои магазины"},
    "my_prices":        {"uz":"💰 Narxlarim","ru":"💰 Мои цены"},
    "link_dokon_btn":   {"uz":"🔗 Do'kon egasini bog'lash","ru":"🔗 Привязать владельца"},
    "admin":            {"uz":"⚙️ Admin panel","ru":"⚙️ Админ панель"},
    "back":             {"uz":"🔙 Orqaga","ru":"🔙 Назад"},
    "dokon_main":       {"uz":"🏪 Do'kon menyusi | 🔑 ID: <b>{sid}</b>","ru":"🏪 Меню магазина | 🔑 ID: <b>{sid}</b>"},
    "dokon_zakaz":      {"uz":"📋 Zakaz berish","ru":"📋 Сделать заказ"},
    "dokon_qarz":       {"uz":"💸 Qarzim","ru":"💸 Мой долг"},
    "dokon_tarix":      {"uz":"📜 Tarix","ru":"📜 История"},
    "dokon_confirm":    {"uz":"✅ Molni tasdiqlash","ru":"✅ Подтвердить поставку"},
    "dokon_tolov":      {"uz":"💵 To'lov yuborish","ru":"💵 Отправить оплату"},
    "dokon_link_btn":   {"uz":"🔗 Distribyutorga bog'lanish","ru":"🔗 Привязаться к дистрибьютору"},
    "enter_dist_id":    {"uz":"Distribyutor ID sini kiriting (6 raqam):","ru":"Введите ID дистрибьютора (6 цифр):"},
    "enter_dokon_id":   {"uz":"Do'kon egasi ID sini kiriting (6 raqam):","ru":"Введите ID владельца (6 цифр):"},
    "id_not_found":     {"uz":"❌ Bu ID topilmadi. Qayta kiriting:","ru":"❌ ID не найден. Введите заново:"},
    "id_wrong_role":    {"uz":"❌ Bu ID noto'g'ri!","ru":"❌ Неверная роль для этого ID!"},
    "already_linked":   {"uz":"⚠️ Siz allaqachon bog'langansiz!","ru":"⚠️ Вы уже привязаны!"},
    "link_req_sent":    {"uz":"✅ So'rov yuborildi! {name} tasdiqlashini kuting.","ru":"✅ Запрос отправлен! Ожидайте подтверждения {name}."},
    "link_req_to_dist": {"uz":"🔗 BOG'LANISH SO'ROVI:\nDo'kon egasi: {name}\nID: {sid}\nTel: {phone}\n\n✅ /lok_{rid} — Tasdiqlash (keyin do'kon ma'lumot kiritasiz)\n❌ /lrad_{rid} — Rad etish","ru":"🔗 ЗАПРОС:\nВладелец: {name}\nID: {sid}\nТел: {phone}\n\n✅ /lok_{rid} — Подтвердить (потом введёте данные)\n❌ /lrad_{rid} — Отклонить"},
    "link_req_to_dokon":{"uz":"🔗 BOG'LANISH SO'ROVI:\nDistribYutor: {name}\nID: {sid}\nTel: {phone}\n\n✅ /lok_{rid} — Tasdiqlash\n❌ /lrad_{rid} — Rad etish","ru":"🔗 ЗАПРОС:\nДистрибьютор: {name}\nID: {sid}\nТел: {phone}\n\n✅ /lok_{rid} — Подтвердить\n❌ /lrad_{rid} — Отклонить"},
    "link_ok_enter_store":{"uz":"✅ Tasdiqlandi! Endi do'kon ma'lumotlarini kiriting:","ru":"✅ Подтверждено! Введите данные магазина:"},
    "link_ok_dokon":    {"uz":"✅ Distribyutor tasdiqladi! U do'kon ma'lumotlarini kiritadi.","ru":"✅ Дистрибьютор подтвердил! Он введёт данные."},
    "link_rad":         {"uz":"❌ So'rov rad etildi.","ru":"❌ Запрос отклонён."},
    "dokon_name":       {"uz":"Do'kon nomini kiriting:","ru":"Название магазина:"},
    "dokon_addr":       {"uz":"Manzilini kiriting:","ru":"Адрес магазина:"},
    "dokon_mchj":       {"uz":"MCHJ nomi (yoki Otkazib yuborish):","ru":"ООО (или Пропустить):"},
    "dokon_tel1":       {"uz":"Telefon 1 (faqat raqamlar):","ru":"Телефон 1 (только цифры):"},
    "dokon_tel2":       {"uz":"Telefon 2 (yoki Otkazib yuborish):","ru":"Телефон 2 (или Пропустить):"},
    "dokon_photo_q":    {"uz":"Do'kon rasmini yuboring (yoki Otkazib yuborish):","ru":"Фото магазина (или Пропустить):"},
    "dokon_loc_q":      {"uz":"Lokatsiyasini yuboring (yoki Otkazib yuborish):","ru":"Локацию (или Пропустить):"},
    "dokon_saved":      {"uz":"✅ Do'kon saqlandi: {name}","ru":"✅ Магазин сохранён: {name}"},
    "zavod_req":        {"uz":"⏳ ZAVOD SO'ROVI:\nDist: {dist} (ID:{sid})\n{prod}: {qty} {unit}\nNarx: {narx:,.0f}\nJami: {jami:,.0f}\nRef: {id}\n\n✅ /zok_{id}\n❌ /zrad_{id}","ru":"⏳ ЗАВОД:\nДист: {dist} (ID:{sid})\n{prod}: {qty} {unit}\nЦена: {narx:,.0f}\nИтог: {jami:,.0f}\nRef: {id}\n\n✅ /zok_{id}\n❌ /zrad_{id}"},
    "zavod_wait":       {"uz":"⏳ So'rov adminga yuborildi. Kuting...","ru":"⏳ Запрос отправлен. Ожидайте..."},
    "zavod_ok":         {"uz":"✅ Zavod so'rovi tasdiqlandi!","ru":"✅ Запрос подтверждён!"},
    "zavod_rad":        {"uz":"❌ Zavod so'rovi rad etildi.","ru":"❌ Запрос отклонён."},
    "top_pay_type":     {"uz":"💳 Tolov usulini tanlang:","ru":"💳 Способ оплаты:"},
    "naqd":             {"uz":"💵 Naqd","ru":"💵 Наличные"},
    "realizatsiya":     {"uz":"📝 Realizatsiya","ru":"📝 Реализация"},
    "top_naqd_sum":     {"uz":"💵 Naqd summani kiriting:\n(0 = to'liq realizatsiya)","ru":"💵 Сумма наличных:\n(0 = полностью в долг)"},
    "photo_scale":      {"uz":"📸 Tarozi rasmini yuboring\nYOKI ⌨️ og'irlikni kiriting (masalan: 3.455):","ru":"📸 Фото весов\nИЛИ ⌨️ введите вес (например: 3.455):"},
    "ocr_ok":           {"uz":"📸 Rasmdan o'qildi: {v} kg\nTo'g'ri? HA bosing yoki to'g'ri raqamni kiriting:","ru":"📸 Считано: {v} кг\nВерно? ДА или введите правильное:"},
    "ocr_fail":         {"uz":"❌ O'qib bo'lmadi. Og'irlikni kiriting:","ru":"❌ Не удалось. Введите вес:"},
    "reading":          {"uz":"⏳ Rasm o'qilmoqda...","ru":"⏳ Читаю изображение..."},
    "top_dokon_msg":    {"uz":"📦 MOL KELDI:\n{dist}\n{prod}: {qty} {unit}\nJami: {jami:,.0f}\nNaqd: {naqd:,.0f}\nQarz: {qarz:,.0f}\n\n✅ /tok_{id} — Qabul\n❌ /trad_{id} — Rad","ru":"📦 ПОСТАВКА:\n{dist}\n{prod}: {qty} {unit}\nИтог: {jami:,.0f}\nНал: {naqd:,.0f}\nДолг: {qarz:,.0f}\n\n✅ /tok_{id} — Принять\n❌ /trad_{id} — Отклонить"},
    "top_ok_dist":      {"uz":"✅ {dokon} molni qabul qildi!","ru":"✅ {dokon} принял товар!"},
    "top_rad_dist":     {"uz":"❌ {dokon} molni rad etdi!","ru":"❌ {dokon} отклонил товар!"},
    "tolov_dist_msg":   {"uz":"💵 TOLOV:\n{dokon}\nSumma: {summa:,.0f}\n\n✅ /vok_{id}\n❌ /vrad_{id}","ru":"💵 ОПЛАТА:\n{dokon}\nСумма: {summa:,.0f}\n\n✅ /vok_{id}\n❌ /vrad_{id}"},
    "tolov_ok":         {"uz":"✅ Tolov tasdiqlandi!","ru":"✅ Оплата подтверждена!"},
    "tolov_rad":        {"uz":"❌ Tolov rad etildi!","ru":"❌ Оплата отклонена!"},
    "zakaz_sent":       {"uz":"✅ Zakaz yuborildi!","ru":"✅ Заказ отправлен!"},
    "zakaz_dist_new":   {"uz":"📋 YANGI ZAKAZ:\nDo'kon: {dokon}\n{prod}: {qty} {unit}\n\n✅ /zqabul_{id} — Qabul + izoh\n❌ /zrad_z_{id} — Rad + izoh","ru":"📋 НОВЫЙ ЗАКАЗ:\nМаг: {dokon}\n{prod}: {qty} {unit}\n\n✅ /zqabul_{id} — Принять + комментарий\n❌ /zrad_z_{id} — Отклонить + комментарий"},
    "zakaz_reminder":   {"uz":"⏰ Eslatma! Zakaz kutilmoqda:\n{dokon}: {prod} {qty}\nID: {id}","ru":"⏰ Напоминание! Ожидает заказ:\n{dokon}: {prod} {qty}\nID: {id}"},
    "zakaz_timeout":    {"uz":"⚠️ Zakazingiz ({prod} {qty}) 2 soat ichida qabul qilinmadi.\n\nDist bilan bog'laning:\n📞 {phone}\n👤 {name}","ru":"⚠️ Ваш заказ ({prod} {qty}) не принят в течение 2 часов.\n\nСвяжитесь с дистрибьютором:\n📞 {phone}\n👤 {name}"},
    "zakaz_comment_q":  {"uz":"📝 Izoh yozing (yoki Otkazib yuborish):","ru":"📝 Комментарий (или Пропустить):"},
    "zakaz_acc_dist":   {"uz":"✅ Zakaz qabul qilindi!\nIzoh: {izoh}","ru":"✅ Заказ принят!\nКомментарий: {izoh}"},
    "zakaz_rad_dist":   {"uz":"❌ Zakaz rad etildi.\nIzoh: {izoh}","ru":"❌ Заказ отклонён.\nКомментарий: {izoh}"},
    "zakaz_acc_dokon":  {"uz":"✅ Zakazingiz qabul qilindi!\nDist izohi: {izoh}","ru":"✅ Ваш заказ принят!\nКомментарий дистрибьютора: {izoh}"},
    "zakaz_rad_dokon":  {"uz":"❌ Zakazingiz rad etildi.\nDist izohi: {izoh}","ru":"❌ Ваш заказ отклонён.\nКомментарий: {izoh}"},
    "narx_prod":        {"uz":"Mahsulotni tanlang:","ru":"Выберите товар:"},
    "narx_type":        {"uz":"Qaysi narx?","ru":"Какую цену?"},
    "narx_umumiy":      {"uz":"🔵 Barcha do'konlar","ru":"🔵 Для всех магазинов"},
    "narx_maxsus":      {"uz":"🟡 Bitta do'kon uchun","ru":"🟡 Для одного магазина"},
    "narx_val":         {"uz":"Yangi narx (masalan: 15000):","ru":"Новая цена (например: 15000):"},
    "tannarx_val":      {"uz":"Tannarx (masalan: 12000):","ru":"Себестоимость (например: 12000):"},
    "narx_updated":     {"uz":"✅ Narx yangilandi!","ru":"✅ Цена обновлена!"},
    "prod":             {"uz":"Mahsulotni tanlang:","ru":"Выберите товар:"},
    "store":            {"uz":"Do'konni tanlang:","ru":"Выберите магазин:"},
    "no_store":         {"uz":"⚠️ Do'konlar yo'q.\nDo'kon egasini bog'lang.","ru":"⚠️ Магазины не найдены.\nПривяжите владельца."},
    "qty":              {"uz":"Miqdorni kiriting (masalan: 5 yoki 5.5):","ru":"Количество (например: 5 или 5.5):"},
    "ok":               {"uz":"✅ Saqlandi!","ru":"✅ Сохранено!"},
    "err_num":          {"uz":"❌ Raqam kiriting!","ru":"❌ Введите число!"},
    "err_weight":       {"uz":"❌ Og'irlikni kiriting! Masalan: 3.455","ru":"❌ Введите вес! Например: 3.455"},
    "err_money":        {"uz":"❌ Summani kiriting! Masalan: 15000","ru":"❌ Введите сумму! Например: 15000"},
    "err_phone":        {"uz":"❌ Faqat raqam!","ru":"❌ Только цифры!"},
    "skip":             {"uz":"⏭ Otkazib yuborish","ru":"⏭ Пропустить"},
    "loc_btn":          {"uz":"📍 Lokatsiyani yuborish","ru":"📍 Отправить геолокацию"},
    "phone_btn":        {"uz":"📱 Telefon raqamni yuborish","ru":"📱 Отправить номер телефона"},
    "send_loc":         {"uz":"📍 Lokatsiyangizni yuboring:","ru":"📍 Отправьте геолокацию:"},
    "no_admin":         {"uz":"🚫 Siz admin emassiz!","ru":"🚫 Вы не администратор!"},
    "adm":              {"uz":"⚙️ Admin paneli:","ru":"⚙️ Админ панель:"},
    "adm_mahsulot":     {"uz":"➕ Mahsulot qo'shish","ru":"➕ Добавить товар"},
    "adm_price":        {"uz":"💰 Umumiy narxlar","ru":"💰 Общие цены"},
    "adm_add_store":    {"uz":"🏪 Do'kon qo'shish","ru":"🏪 Добавить магазин"},
    "adm_add_dist":     {"uz":"🚚 Distribyutor qo'shish","ru":"🚚 Добавить дистрибьютора"},
    "adm_add_dokon_ega":{"uz":"👤 Do'kon egasi qo'shish","ru":"👤 Добавить владельца"},
    "adm_stats":        {"uz":"📊 Statistika","ru":"📊 Статистика"},
    "adm_broadcast":    {"uz":"📢 Xabar yuborish","ru":"📢 Рассылка"},
    "adm_debtors":      {"uz":"💸 Qarzdorlar","ru":"💸 Должники"},
    "adm_list_stores":  {"uz":"🏪 Do'konlar","ru":"🏪 Магазины"},
    "adm_list_dists":   {"uz":"🚚 Distribyutorlar","ru":"🚚 Дистрибьюторы"},
    "adm_zavod_list":   {"uz":"📦 Zavod so'rovlari","ru":"📦 Запросы завода"},
    "broadcast_msg":    {"uz":"Xabar matnini kiriting:","ru":"Введите текст рассылки:"},
    "mahsulot_nom_uz":  {"uz":"Mahsulot nomini kiriting (o'zbekcha):","ru":"Название товара (по-узбекски):"},
    "mahsulot_nom_ru":  {"uz":"Mahsulot nomini kiriting (ruscha):","ru":"Название товара (по-русски):"},
    "mahsulot_unit":    {"uz":"Birligini tanlang:","ru":"Единица измерения:"},
    "mahsulot_ok":      {"uz":"✅ Mahsulot qo'shildi: {name}","ru":"✅ Товар добавлен: {name}"},
    "week":             {"uz":"Haftalik","ru":"Недельный"},
    "month":            {"uz":"Oylik","ru":"Месячный"},
}

def tx(k, la="uz", **kw):
    t = T.get(k,{}).get(la,k)
    return t.format(**kw) if kw else t

def lg(ctx):     return ctx.user_data.get("lang","uz")
def is_adm(ctx): return ctx.user_data.get("is_admin", False)
def uname(upd):
    u = upd.effective_user
    return u.full_name or u.username or str(u.id)
def find_prod(name, la):
    return next((p for p in get_products() if p[la]==name), None)

def main_kb(la, sid="", admin=False):
    rows = [
        [tx("qabul",la), tx("buyurtma",la)],
        [tx("topshir",la), tx("tolov_qabul",la)],
        [tx("natija",la), tx("ombor",la)],
        [tx("marshrut",la), tx("hisobot",la)],
        [tx("my_stores",la), tx("my_prices",la)],
        [tx("link_dokon_btn",la)],
    ]
    if admin: rows.append([tx("admin",la)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def dokon_kb(la):
    return ReplyKeyboardMarkup([
        [tx("dokon_zakaz",la)],
        [tx("dokon_qarz",la), tx("dokon_tarix",la)],
        [tx("dokon_confirm",la), tx("dokon_tolov",la)],
        [tx("dokon_link_btn",la)],
    ], resize_keyboard=True)

def admin_kb(la):
    return ReplyKeyboardMarkup([
        [tx("adm_mahsulot",la), tx("adm_price",la)],
        [tx("adm_add_store",la), tx("adm_add_dist",la)],
        [tx("adm_add_dokon_ega",la), tx("adm_stats",la)],
        [tx("adm_debtors",la), tx("adm_list_stores",la)],
        [tx("adm_list_dists",la), tx("adm_zavod_list",la)],
        [tx("adm_broadcast",la), tx("back",la)],
    ], resize_keyboard=True)

def prod_kb(la):
    prods = get_products(); rows = []
    for i in range(0, len(prods), 2):
        r = [prods[i][la]]
        if i+1 < len(prods): r.append(prods[i+1][la])
        rows.append(r)
    rows.append([tx("back",la)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def store_kb(stores, la):
    rows = [[s.get("Nomi","")] for s in stores if s.get("Nomi","")]
    rows.append([tx("back",la)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def back_kb(la):  return ReplyKeyboardMarkup([[tx("back",la)]], resize_keyboard=True)
def skip_kb(la):  return ReplyKeyboardMarkup([[tx("skip",la)],[tx("back",la)]], resize_keyboard=True)
def wait_kb(la):  return ReplyKeyboardMarkup([[tx("resend_btn",la)]], resize_keyboard=True)
def yes_kb(la):   return ReplyKeyboardMarkup([["HA" if la=="uz" else "ДА", tx("back",la)]], resize_keyboard=True)
def phone_kb(la): return ReplyKeyboardMarkup([[KeyboardButton(tx("phone_btn",la), request_contact=True)]], resize_keyboard=True)
def loc_kb(la):
    return ReplyKeyboardMarkup([[KeyboardButton(tx("loc_btn",la), request_location=True)],[tx("skip",la)],[tx("back",la)]], resize_keyboard=True)


# ── START / LANG ──────────────────────────────────────────────────────────────
async def start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = upd.effective_user.id
    ctx.user_data["is_admin"] = uid in ADMIN_IDS
    if uid in ADMIN_IDS:
        la = ctx.user_data.get("lang","uz")
        await upd.message.reply_text(tx("adm",la), reply_markup=admin_kb(la), parse_mode="HTML")
        return ADMIN_MENU
    user = get_user(uid)
    if user:
        la = user.get("Til","uz"); ctx.user_data["lang"] = la
        role = user.get("Rol",""); sid = user.get("Short_ID","?")
        if is_approved(uid):
            name = user.get("Ism","")
            if role == "dokon_ega":
                await upd.message.reply_text(tx("dokon_main",la,sid=sid), reply_markup=dokon_kb(la), parse_mode="HTML")
                return DOKON_MENU
            await upd.message.reply_text(tx("main",la,sid=sid), reply_markup=main_kb(la,sid,False), parse_mode="HTML")
            return MAIN_MENU
        elif is_rejected(uid):
            await upd.message.reply_text(tx("rejected_msg",la)+"\n\n"+tx("rejected_retry",la))
            return REG_NAME
        else:
            await upd.message.reply_text(tx("wait_approve",la), reply_markup=wait_kb(la))
            return WAIT_APPROVE
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("O'zbek", callback_data="lang_uz"),
        InlineKeyboardButton("Русский", callback_data="lang_ru"),
    ]])
    await upd.message.reply_text(tx("start"), reply_markup=kb)
    return LANG_SELECT

async def lang_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await q.answer()
    la = q.data.replace("lang_",""); ctx.user_data["lang"] = la
    uid = upd.effective_user.id; ctx.user_data["is_admin"] = uid in ADMIN_IDS
    await q.edit_message_text("Til tanlandi!" if la=="uz" else "Язык выбран!")
    if uid in ADMIN_IDS:
        await ctx.bot.send_message(uid, tx("adm",la), reply_markup=admin_kb(la), parse_mode="HTML")
        return ADMIN_MENU
    user = get_user(uid)
    if user:
        la = user.get("Til",la); ctx.user_data["lang"]=la
        role = user.get("Rol",""); sid = user.get("Short_ID","?")
        if is_approved(uid):
            if role=="dokon_ega":
                await ctx.bot.send_message(uid,tx("dokon_main",la,sid=sid),reply_markup=dokon_kb(la),parse_mode="HTML")
                return DOKON_MENU
            await ctx.bot.send_message(uid,tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML")
            return MAIN_MENU
        elif is_rejected(uid):
            await ctx.bot.send_message(uid,tx("rejected_msg",la)+"\n\n"+tx("rejected_retry",la))
            return REG_NAME
        else:
            await ctx.bot.send_message(uid,tx("wait_approve",la),reply_markup=wait_kb(la))
            return WAIT_APPROVE
    await ctx.bot.send_message(uid, tx("role_select",la), reply_markup=ReplyKeyboardMarkup(
        [[tx("role_dist",la)],[tx("role_dokon",la)]], resize_keyboard=True))
    return ROLE_SELECT

# ── RO'YXAT ───────────────────────────────────────────────────────────────────
async def role_select(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("role_dist",la): ctx.user_data["reg_role"]="distributor"
    elif t==tx("role_dokon",la): ctx.user_data["reg_role"]="dokon_ega"
    else: await upd.message.reply_text(tx("role_select",la)); return ROLE_SELECT
    await upd.message.reply_text(tx("reg_name",la),reply_markup=ReplyKeyboardMarkup([[]], resize_keyboard=True))
    return REG_NAME

async def reg_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["reg_name"]=upd.message.text.strip()
    await upd.message.reply_text(tx("reg_fname",lg(ctx))); return REG_FNAME

async def reg_fname(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["reg_fname"]=upd.message.text.strip(); la=lg(ctx)
    await upd.message.reply_text(tx("reg_phone",la),reply_markup=phone_kb(la)); return REG_PHONE

async def reg_phone(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    if upd.message.contact: phone=upd.message.contact.phone_number
    else:
        phone=clean_phone(upd.message.text)
        if len(phone.replace("+",""))<7:
            await upd.message.reply_text(tx("err_phone",la),reply_markup=phone_kb(la)); return REG_PHONE
    ctx.user_data["reg_phone"]=phone
    await upd.message.reply_text(tx("reg_passport",la),
        reply_markup=ReplyKeyboardMarkup([[tx("skip",la)]], resize_keyboard=True))
    return REG_PASSPORT

async def reg_passport(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id
    name=ctx.user_data.get("reg_name",""); fname=ctx.user_data.get("reg_fname","")
    phone=ctx.user_data.get("reg_phone",""); role=ctx.user_data.get("reg_role","distributor")
    full_name=f"{name} {fname}".strip()
    passport="rasm_bor" if upd.message.photo else (upd.message.text or "otkazildi")
    sid=make_short_id()
    db_delete_row("Foydalanuvchilar","TG_ID",str(uid))
    db_append("Foydalanuvchilar",[str(uid),name,fname,phone,role,la,passport,"kutilmoqda",sid,"",now_str()])
    rol_text="DISTRIBYUTOR" if role=="distributor" else "DO'KON EGASI"
    for admin_id in ADMIN_IDS:
        try:
            await ctx.bot.send_message(admin_id,
                tx("reg_admin_msg",la,rol=rol_text,name=full_name,phone=phone,uid=str(uid),sid=sid),
                parse_mode="HTML")
            if upd.message.photo:
                await ctx.bot.send_photo(admin_id,upd.message.photo[-1].file_id,
                    caption=f"Passport: {full_name}|{uid}|{sid}")
        except Exception as e: logger.error(f"Admin notify: {e}")
    await upd.message.reply_text(tx("reg_ok",la,name=name,sid=sid),reply_markup=wait_kb(la),parse_mode="HTML")
    return WAIT_APPROVE

async def wait_approve_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id; t=upd.message.text or ""
    if is_approved(uid):
        user=get_user(uid); role=user.get("Rol","") if user else ""; sid=user.get("Short_ID","?") if user else "?"
        if role=="dokon_ega":
            await upd.message.reply_text(tx("dokon_main",la,sid=sid),reply_markup=dokon_kb(la),parse_mode="HTML")
            return DOKON_MENU
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML")
        return MAIN_MENU
    if is_rejected(uid):
        await upd.message.reply_text(tx("rejected_msg",la)+"\n\n"+tx("rejected_retry",la)); return REG_NAME
    if t==tx("resend_btn",la):
        user=get_user(uid)
        if user:
            fn=f"{user.get('Ism','')} {user.get('Familiya','')}".strip()
            ph=user.get("Telefon",""); sid=user.get("Short_ID","?"); role=user.get("Rol","")
            rol_text="DISTRIBYUTOR" if role=="distributor" else "DO'KON EGASI"
            for admin_id in ADMIN_IDS:
                try: await ctx.bot.send_message(admin_id,tx("reg_admin_msg",la,rol=rol_text,name=fn,phone=ph,uid=str(uid),sid=sid),parse_mode="HTML")
                except Exception: pass
        await upd.message.reply_text(tx("resent_ok",la),reply_markup=wait_kb(la)); return WAIT_APPROVE
    await upd.message.reply_text(tx("wait_approve",la),reply_markup=wait_kb(la)); return WAIT_APPROVE

# ── APPROVE/REJECT ────────────────────────────────────────────────────────────
async def approve_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m=re.search(r'/approve_(\d+)',upd.message.text or "")
    if not m: return
    target=m.group(1); db_update("Foydalanuvchilar","TG_ID",target,"Status","tasdiqlangan")
    await upd.message.reply_text(f"✅ Tasdiqlandi: {target}")
    try:
        u=get_user(target); la=u.get("Til","uz") if u else "uz"; sid=u.get("Short_ID","?") if u else "?"
        await ctx.bot.send_message(int(target),tx("approved_msg",la,sid=sid),parse_mode="HTML")
    except Exception as e: logger.error(f"approve: {e}")

async def reject_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m=re.search(r'/reject_(\d+)',upd.message.text or "")
    if not m: return
    target=m.group(1); db_update("Foydalanuvchilar","TG_ID",target,"Status","rad_etildi")
    await upd.message.reply_text(f"❌ Rad etildi: {target}")
    try:
        u=get_user(target); la=u.get("Til","uz") if u else "uz"
        await ctx.bot.send_message(int(target),tx("rejected_msg",la)+"\n\n"+tx("rejected_retry",la))
    except Exception as e: logger.error(f"reject: {e}")

# ── BOG'LANISH ────────────────────────────────────────────────────────────────
async def dist_link_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    await upd.message.reply_text(tx("enter_dokon_id",la),reply_markup=back_kb(la)); return DIST_LINK_ID

async def dist_link_id(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text or ""; uid=upd.effective_user.id
    if t==tx("back",la):
        sid=get_short_id(uid)
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU
    target=get_user_by_short(t.strip())
    if not target: await upd.message.reply_text(tx("id_not_found",la)); return DIST_LINK_ID
    if target.get("Rol","")!="dokon_ega": await upd.message.reply_text(tx("id_wrong_role",la)+" (Do'kon egasi bo'lishi kerak)"); return DIST_LINK_ID
    if str(target.get("Bog_Dist_ID","")).strip(): await upd.message.reply_text("⚠️ Bu do'kon egasi allaqachon bog'langan!"); return DIST_LINK_ID
    req_id=make_op_id("L")
    du=get_user(uid); dn=f"{du.get('Ism','')} {du.get('Familiya','')}".strip() if du else str(uid)
    dsid=du.get("Short_ID","?") if du else "?"; dphone=du.get("Telefon","") if du else ""
    dokon_tg=str(target.get("TG_ID",""))
    db_append("Bog_Sorov",[now_str(),str(uid),dsid,"distributor",dokon_tg,t.strip(),"kutilmoqda",req_id])
    ctx.user_data["link_dokon_tg"]=dokon_tg; ctx.user_data["link_dist_tg"]=str(uid)
    try:
        dkla=target.get("Til","uz")
        await ctx.bot.send_message(int(dokon_tg),tx("link_req_to_dokon",dkla,name=dn,sid=dsid,phone=dphone,rid=req_id),parse_mode="HTML")
    except Exception as e: logger.error(f"link notify: {e}")
    tname=f"{target.get('Ism','')} {target.get('Familiya','')}".strip()
    await upd.message.reply_text(tx("link_req_sent",la,name=tname)); return MAIN_MENU

async def dokon_link_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id
    u=get_user(uid)
    if u and str(u.get("Bog_Dist_ID","")).strip(): await upd.message.reply_text(tx("already_linked",la)); return DOKON_MENU
    await upd.message.reply_text(tx("enter_dist_id",la),reply_markup=back_kb(la)); return DOKON_LINK_ID

async def dokon_link_id(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text or ""; uid=upd.effective_user.id
    if t==tx("back",la):
        sid=get_short_id(uid)
        await upd.message.reply_text(tx("dokon_main",la,sid=sid),reply_markup=dokon_kb(la),parse_mode="HTML"); return DOKON_MENU
    target=get_user_by_short(t.strip())
    if not target: await upd.message.reply_text(tx("id_not_found",la)); return DOKON_LINK_ID
    if target.get("Rol","")!="distributor": await upd.message.reply_text(tx("id_wrong_role",la)); return DOKON_LINK_ID
    req_id=make_op_id("L")
    dku=get_user(uid); dkn=f"{dku.get('Ism','')} {dku.get('Familiya','')}".strip() if dku else str(uid)
    dksid=dku.get("Short_ID","?") if dku else "?"; dkphone=dku.get("Telefon","") if dku else ""
    dist_tg=str(target.get("TG_ID",""))
    db_append("Bog_Sorov",[now_str(),str(uid),dksid,"dokon_ega",dist_tg,t.strip(),"kutilmoqda",req_id])
    ctx.user_data["link_dokon_tg"]=str(uid); ctx.user_data["link_dist_tg"]=dist_tg
    try:
        dla=target.get("Til","uz")
        await ctx.bot.send_message(int(dist_tg),tx("link_req_to_dist",dla,name=dkn,sid=dksid,phone=dkphone,rid=req_id),parse_mode="HTML")
    except Exception as e: logger.error(f"link notify: {e}")
    tname=f"{target.get('Ism','')} {target.get('Familiya','')}".strip()
    await upd.message.reply_text(tx("link_req_sent",la,name=tname)); return DOKON_MENU

async def lok_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/lok_(\w+)',upd.message.text or "")
    if not m: return
    req_id=m.group(1); uid=str(upd.effective_user.id)
    sorov=next((r for r in db_all("Bog_Sorov") if r.get("Req_ID","")==req_id and r.get("Status","")=="kutilmoqda"),None)
    if not sorov: await upd.message.reply_text("So'rov topilmadi."); return
    from_tg=str(sorov.get("From_TG_ID","")); from_rol=sorov.get("From_Rol","")
    db_update("Bog_Sorov","Req_ID",req_id,"Status","tasdiqlangan")
    la=lg(ctx)
    if from_rol=="dokon_ega":
        dokon_tg=from_tg; dist_tg=uid
        db_update("Foydalanuvchilar","TG_ID",dokon_tg,"Bog_Dist_ID",dist_tg)
        ctx.user_data["link_dokon_tg"]=dokon_tg; ctx.user_data["link_dist_tg"]=dist_tg
        await upd.message.reply_text(tx("link_ok_enter_store",la))
        await upd.message.reply_text(tx("dokon_name",la),reply_markup=back_kb(la))
        try:
            dku=get_user(dokon_tg); dkla=dku.get("Til","uz") if dku else "uz"
            await ctx.bot.send_message(int(dokon_tg),tx("link_ok_dokon",dkla))
        except Exception: pass
        return DI_NAME
    else:
        dist_tg=from_tg; dokon_tg=uid
        db_update("Foydalanuvchilar","TG_ID",dokon_tg,"Bog_Dist_ID",dist_tg)
        ctx.user_data["link_dokon_tg"]=dokon_tg; ctx.user_data["link_dist_tg"]=dist_tg
        dku=get_user(dokon_tg); dkla=dku.get("Til","uz") if dku else "uz"
        await upd.message.reply_text(tx("link_ok_dokon",dkla))
        try:
            du=get_user(dist_tg); dla=du.get("Til","uz") if du else "uz"
            await ctx.bot.send_message(int(dist_tg),tx("link_ok_enter_store",dla))
        except Exception: pass

async def lrad_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/lrad_(\w+)',upd.message.text or "")
    if not m: return
    req_id=m.group(1); db_update("Bog_Sorov","Req_ID",req_id,"Status","rad_etildi")
    sorov=next((r for r in db_all("Bog_Sorov") if r.get("Req_ID","")==req_id),None)
    if sorov:
        try:
            u=get_user(str(sorov.get("From_TG_ID",""))); la=u.get("Til","uz") if u else "uz"
            await ctx.bot.send_message(int(sorov.get("From_TG_ID",0)),tx("link_rad",la))
        except Exception: pass
    await upd.message.reply_text("❌ Rad etildi.")

# ── DO'KON MA'LUMOT (distribyutor tomonidan) ─────────────────────────────────
async def di_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text or ""
    if t==tx("back",la): return MAIN_MENU
    ctx.user_data["di_name"]=t.strip()
    await upd.message.reply_text(tx("dokon_addr",la),reply_markup=back_kb(la)); return DI_ADDR

async def di_addr(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); ctx.user_data["di_addr"]=upd.message.text.strip()
    await upd.message.reply_text(tx("dokon_mchj",la),reply_markup=skip_kb(la)); return DI_MCHJ

async def di_mchj(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text or ""
    ctx.user_data["di_mchj"]="" if t==tx("skip",la) else t.strip()
    await upd.message.reply_text(tx("dokon_tel1",la),reply_markup=phone_kb(la)); return DI_TEL1

async def di_tel1(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    if upd.message.contact: phone=upd.message.contact.phone_number
    else:
        phone=clean_phone(upd.message.text or "")
        if len(phone.replace("+",""))<7:
            await upd.message.reply_text(tx("err_phone",la),reply_markup=phone_kb(la)); return DI_TEL1
    ctx.user_data["di_tel1"]=phone
    await upd.message.reply_text(tx("dokon_tel2",la),reply_markup=skip_kb(la)); return DI_TEL2

async def di_tel2(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text or ""
    ctx.user_data["di_tel2"]="" if t==tx("skip",la) else clean_phone(t)
    await upd.message.reply_text(tx("dokon_photo_q",la),reply_markup=skip_kb(la)); return DI_PHOTO

async def di_photo(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    ctx.user_data["di_photo"]=upd.message.photo[-1].file_id if upd.message.photo else ""
    await upd.message.reply_text(tx("dokon_loc_q",la),reply_markup=loc_kb(la)); return DI_LOC

async def di_loc(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id
    dist_tg=ctx.user_data.get("link_dist_tg",str(uid)); dokon_tg=ctx.user_data.get("link_dokon_tg","")
    lat,lng="",""
    if upd.message.location: lat=str(upd.message.location.latitude); lng=str(upd.message.location.longitude)
    name=ctx.user_data.get("di_name",""); addr=ctx.user_data.get("di_addr","")
    mchj=ctx.user_data.get("di_mchj",""); tel1=ctx.user_data.get("di_tel1","")
    tel2=ctx.user_data.get("di_tel2",""); photo=ctx.user_data.get("di_photo","")
    du=get_user(dist_tg); dn=f"{du.get('Ism','')} {du.get('Familiya','')}".strip() if du else str(dist_tg)
    dku=get_user(dokon_tg); dkn=f"{dku.get('Ism','')} {dku.get('Familiya','')}".strip() if dku else ""
    dksid=dku.get("Short_ID","") if dku else ""
    cnt=len(db_all("Dokonlar"))+1
    db_append("Dokonlar",[str(cnt),dksid,name,addr,mchj,tel1,tel2,dist_tg,dn,dokon_tg,dkn,lat,lng,now_str()])
    await upd.message.reply_text(tx("dokon_saved",la,name=name))
    for admin_id in ADMIN_IDS:
        try:
            await ctx.bot.send_message(admin_id,f"🏪 YANGI DO'KON:\n{name}\nAdres: {addr}\nDist: {dn}\nEga: {dkn}")
            if lat and lng: await ctx.bot.send_location(admin_id,float(lat),float(lng))
            if photo: await ctx.bot.send_photo(admin_id,photo,caption=f"Do'kon: {name}")
        except Exception: pass
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU

# ── ZAVOD ─────────────────────────────────────────────────────────────────────
async def zavod_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    await upd.message.reply_text(tx("prod",la),reply_markup=prod_kb(la)); return ZAVOD_PROD

async def zavod_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if t==tx("back",la):
        sid=get_short_id(uid)
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU
    p=find_prod(t,la)
    if not p: await upd.message.reply_text(tx("prod",la),reply_markup=prod_kb(la)); return ZAVOD_PROD
    ctx.user_data["p"]=p
    await upd.message.reply_text(f"{t}\n\n{tx('qty',la)}",reply_markup=back_kb(la)); return ZAVOD_QTY

async def zavod_qty(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if t==tx("back",la): await upd.message.reply_text(tx("prod",la),reply_markup=prod_kb(la)); return ZAVOD_PROD
    qty=parse_weight(t)
    if qty<=0: await upd.message.reply_text(tx("err_num",la)); return ZAVOD_QTY
    p=ctx.user_data["p"]; price,_=get_price(p["id"],dist_id=str(uid))
    jami=qty*price; qid=make_op_id("Q")
    u=get_user(uid); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else str(uid)
    sid=u.get("Short_ID","?") if u else "?"
    db_append("Qabul",[now_str(),str(uid),dn,p[la],qty,p["unit"],price,jami,"kutilmoqda",qid])
    for admin_id in ADMIN_IDS:
        try: await ctx.bot.send_message(admin_id,tx("zavod_req",la,dist=dn,sid=sid,prod=p[la],qty=qty,unit=p["unit"],narx=price,jami=jami,id=qid),parse_mode="HTML")
        except Exception as e: logger.error(f"zavod admin: {e}")
    await upd.message.reply_text(tx("zavod_wait",la))
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU

async def zok_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m=re.search(r'/zok_(\w+)',upd.message.text or "")
    if not m: return
    qid=m.group(1); db_update("Qabul","Qabul_ID",qid,"Status","tasdiqlangan")
    await upd.message.reply_text(f"✅ Zavod tasdiqlandi: {qid}")
    for r in db_all("Qabul"):
        if r.get("Qabul_ID","")==qid:
            try:
                u=get_user(r.get("Dist_ID","")); la=u.get("Til","uz") if u else "uz"
                await ctx.bot.send_message(int(r.get("Dist_ID",0)),tx("zavod_ok",la))
            except Exception: pass
            break

async def zrad_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m=re.search(r'/zrad_(\w+)',upd.message.text or "")
    if not m: return
    qid=m.group(1); db_update("Qabul","Qabul_ID",qid,"Status","rad_etildi")
    await upd.message.reply_text(f"❌ Zavod rad etildi: {qid}")
    for r in db_all("Qabul"):
        if r.get("Qabul_ID","")==qid:
            try:
                u=get_user(r.get("Dist_ID","")); la=u.get("Til","uz") if u else "uz"
                await ctx.bot.send_message(int(r.get("Dist_ID",0)),tx("zavod_rad",la))
            except Exception: pass
            break

# ── TOPSHIRISH ────────────────────────────────────────────────────────────────
async def top_store(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if t==tx("back",la):
        sid=get_short_id(uid)
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU
    stores=ctx.user_data.get("stores",[])
    if t not in [s.get("Nomi","") for s in stores]:
        await upd.message.reply_text(tx("store",la),reply_markup=store_kb(stores,la)); return TOP_STORE
    store=next(s for s in stores if s.get("Nomi","")==t); ctx.user_data["s"]=store
    debt=get_debt(str(store.get("ID",""))); msg=tx("prod",la)
    if debt>0: msg=f"⚠️ Qarz: {debt:,.0f}\n\n"+msg
    await upd.message.reply_text(msg,reply_markup=prod_kb(la)); return TOP_PROD

async def top_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la):
        stores=ctx.user_data.get("stores",[])
        await upd.message.reply_text(tx("store",la),reply_markup=store_kb(stores,la)); return TOP_STORE
    p=find_prod(t,la)
    if not p: await upd.message.reply_text(tx("prod",la),reply_markup=prod_kb(la)); return TOP_PROD
    ctx.user_data["p"]=p
    await upd.message.reply_text(f"{t}\n\n{tx('photo_scale',la)}",reply_markup=back_kb(la)); return TOP_PHOTO

async def top_photo(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    if upd.message.photo:
        await upd.message.reply_text(tx("reading",la))
        file=await ctx.bot.get_file(upd.message.photo[-1].file_id)
        img=bytes(await file.download_as_bytearray()); raw=await vision_ocr(img); w=parse_scale(raw)
        if w>0:
            ctx.user_data["_w"]=w
            await upd.message.reply_text(tx("ocr_ok",la,v=w),reply_markup=yes_kb(la)); return TOP_PHOTO
        await upd.message.reply_text(tx("ocr_fail",la),reply_markup=back_kb(la)); return TOP_PHOTO
    t=upd.message.text or ""
    if t==tx("back",la): await upd.message.reply_text(tx("prod",la),reply_markup=prod_kb(la)); return TOP_PROD
    if "_w" in ctx.user_data:
        if t.upper() in ["HA","ДА","YES","OK"]: qty=ctx.user_data.pop("_w")
        else:
            ctx.user_data.pop("_w",None); qty=parse_weight(t)
            if qty<=0: await upd.message.reply_text(tx("err_weight",la)); return TOP_PHOTO
    else:
        qty=parse_weight(t)
        if qty<=0: await upd.message.reply_text(tx("err_weight",la)); return TOP_PHOTO
    ctx.user_data["top_qty"]=qty
    await upd.message.reply_text(
        f"{ctx.user_data['p'][la]}: {qty} {ctx.user_data['p']['unit']}\n\n{tx('top_pay_type',la)}",
        reply_markup=ReplyKeyboardMarkup([[tx("naqd",la)],[tx("realizatsiya",la)],[tx("back",la)]],resize_keyboard=True))
    return TOP_PAY_TYPE

async def top_pay_type(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return TOP_PHOTO
    if t==tx("realizatsiya",la):
        ctx.user_data["pay_type"]="realizatsiya"; ctx.user_data["top_naqd"]=0.0
        await _save_top(upd,ctx); return TOP_STORE
    if t==tx("naqd",la):
        ctx.user_data["pay_type"]="naqd"
        await upd.message.reply_text(tx("top_naqd_sum",la),reply_markup=back_kb(la)); return TOP_PAY_AMOUNT
    return TOP_PAY_TYPE

async def top_pay_amount(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return TOP_PAY_TYPE
    amount=parse_money(t)
    if amount<0: await upd.message.reply_text(tx("err_money",la)); return TOP_PAY_AMOUNT
    ctx.user_data["top_naqd"]=amount
    await _save_top(upd,ctx); return TOP_STORE

async def _save_top(upd, ctx):
    la=lg(ctx); uid=upd.effective_user.id
    p=ctx.user_data["p"]; store=ctx.user_data["s"]
    qty=ctx.user_data["top_qty"]; naqd=ctx.user_data.get("top_naqd",0.0)
    pay_type=ctx.user_data.get("pay_type","naqd")
    store_id=str(store.get("ID","")); store_name=store.get("Nomi","")
    egasi_id=str(store.get("Egasi_ID",""))
    price,_=get_price(p["id"],dist_id=str(uid),dokon_id=store_id)
    jami=qty*price; qarz=max(0.0,jami-naqd)
    top_id=make_op_id("T")
    u=get_user(uid); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else str(uid)
    db_append("Topshirish",[now_str(),str(uid),store_name,store_id,p[la],qty,p["unit"],price,jami,pay_type,naqd,qarz,"kutilmoqda",top_id])
    if egasi_id:
        try:
            eu=get_user(egasi_id); ela=eu.get("Til","uz") if eu else la
            pname=p.get(ela,p[la])
            await ctx.bot.send_message(int(egasi_id),tx("top_dokon_msg",ela,dist=dn,prod=pname,qty=qty,unit=p["unit"],jami=jami,naqd=naqd,qarz=qarz,id=top_id))
        except Exception as e: logger.error(f"top dokon: {e}")
    await upd.message.reply_text(
        f"✅ {store_name}\n{p[la]}: {qty} {p['unit']}\nJami: {jami:,.0f}\nNaqd: {naqd:,.0f}\nQarz: {qarz:,.0f}",
        reply_markup=store_kb(ctx.user_data.get("stores",[]),la))

async def tok_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/tok_(\w+)',upd.message.text or "")
    if not m: return
    top_id=m.group(1); db_update("Topshirish","Top_ID",top_id,"Status","tasdiqlangan")
    la=lg(ctx); await upd.message.reply_text("✅ Topshirish tasdiqlandi!")
    for r in db_all("Topshirish"):
        if r.get("Top_ID","")==top_id:
            try:
                du=get_user(r.get("Dist_ID","")); dla=du.get("Til","uz") if du else "uz"; dokon=r.get("Dokon","")
                await ctx.bot.send_message(int(r.get("Dist_ID",0)),tx("top_ok_dist",dla,dokon=dokon))
            except Exception: pass
            break

async def trad_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/trad_(\w+)',upd.message.text or "")
    if not m: return
    top_id=m.group(1); db_update("Topshirish","Top_ID",top_id,"Status","rad_etildi")
    la=lg(ctx); await upd.message.reply_text("❌ Topshirish rad etildi!")
    for r in db_all("Topshirish"):
        if r.get("Top_ID","")==top_id:
            try:
                du=get_user(r.get("Dist_ID","")); dla=du.get("Til","uz") if du else "uz"; dokon=r.get("Dokon","")
                await ctx.bot.send_message(int(r.get("Dist_ID",0)),tx("top_rad_dist",dla,dokon=dokon))
            except Exception: pass
            break

async def vok_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/vok_(\w+)',upd.message.text or "")
    if not m: return
    db_update("Tolov","Tolov_ID",m.group(1),"Status","tasdiqlangan")
    la=lg(ctx); await upd.message.reply_text(tx("tolov_ok",la))

async def vrad_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/vrad_(\w+)',upd.message.text or "")
    if not m: return
    db_update("Tolov","Tolov_ID",m.group(1),"Status","rad_etildi")
    la=lg(ctx); await upd.message.reply_text(tx("tolov_rad",la))

# ── ZAKAZ QABUL/RAD (kommentariya bilan) ─────────────────────────────────────
async def zqabul_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/zqabul_(\w+)',upd.message.text or "")
    if not m: return
    ctx.user_data["zakaz_id"]=m.group(1); ctx.user_data["zakaz_action"]="qabul"
    la=lg(ctx); await upd.message.reply_text(tx("zakaz_comment_q",la),reply_markup=skip_kb(la)); return ZAKAZ_COMMENT

async def zrad_z_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/zrad_z_(\w+)',upd.message.text or "")
    if not m: return
    ctx.user_data["zakaz_id"]=m.group(1); ctx.user_data["zakaz_action"]="rad"
    la=lg(ctx); await upd.message.reply_text(tx("zakaz_comment_q",la),reply_markup=skip_kb(la)); return ZAKAZ_COMMENT

async def zakaz_comment(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text or ""; uid=upd.effective_user.id
    izoh="" if t==tx("skip",la) else t.strip()
    zakaz_id=ctx.user_data.get("zakaz_id",""); action=ctx.user_data.get("zakaz_action","qabul")
    status="Qabul_qilindi" if action=="qabul" else "Rad_etildi"
    db_update("Buyurtmalar","Zakaz_ID",zakaz_id,"Status",status)
    db_update("Buyurtmalar","Zakaz_ID",zakaz_id,"Izoh",izoh or "-")
    if action=="qabul": await upd.message.reply_text(tx("zakaz_acc_dist",la,izoh=izoh or "-"))
    else: await upd.message.reply_text(tx("zakaz_rad_dist",la,izoh=izoh or "-"))
    # Do'kon egasiga xabar
    try:
        for r in db_all("Buyurtmalar"):
            if r.get("Zakaz_ID","")==zakaz_id:
                dokon_id=str(r.get("Dokon_ID",""))
                for s in db_all("Dokonlar"):
                    if str(s.get("ID",""))==dokon_id:
                        egasi_id=str(s.get("Egasi_ID",""))
                        if egasi_id:
                            eu=get_user(egasi_id); ela=eu.get("Til","uz") if eu else la
                            if action=="qabul": await ctx.bot.send_message(int(egasi_id),tx("zakaz_acc_dokon",ela,izoh=izoh or "-"))
                            else: await ctx.bot.send_message(int(egasi_id),tx("zakaz_rad_dokon",ela,izoh=izoh or "-"))
                        break
                break
    except Exception as e: logger.error(f"zakaz notify: {e}")
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU

# ── NARX ──────────────────────────────────────────────────────────────────────
async def narx_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    await upd.message.reply_text(tx("narx_prod",la),reply_markup=prod_kb(la)); return NARX_PROD

async def narx_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if t==tx("back",la):
        sid=get_short_id(uid)
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU
    p=find_prod(t,la)
    if not p: await upd.message.reply_text(tx("narx_prod",la),reply_markup=prod_kb(la)); return NARX_PROD
    ctx.user_data["p"]=p; price,cost=get_price(p["id"],dist_id=str(uid))
    await upd.message.reply_text(
        f"{t}\n💰 Joriy narx: {price:,.0f} / Tannarx: {cost:,.0f}\n\n{tx('narx_type',la)}",
        reply_markup=ReplyKeyboardMarkup([[tx("narx_umumiy",la)],[tx("narx_maxsus",la)],[tx("back",la)]],resize_keyboard=True))
    return NARX_TYPE

async def narx_type(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if t==tx("back",la): return NARX_PROD
    if t==tx("narx_maxsus",la):
        stores=get_stores(dist_id=uid)
        if not stores: await upd.message.reply_text(tx("no_store",la)); return NARX_PROD
        ctx.user_data["narx_type"]="maxsus"; ctx.user_data["stores"]=stores
        await upd.message.reply_text(tx("store",la),reply_markup=store_kb(stores,la)); return NARX_DOKON
    ctx.user_data["narx_type"]="umumiy"
    await upd.message.reply_text(tx("narx_val",la),reply_markup=back_kb(la)); return NARX_VAL

async def narx_dokon(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; stores=ctx.user_data.get("stores",[])
    if t==tx("back",la): return NARX_TYPE
    store=next((s for s in stores if s.get("Nomi","")==t),None)
    if not store: await upd.message.reply_text(tx("store",la),reply_markup=store_kb(stores,la)); return NARX_DOKON
    ctx.user_data["narx_dokon"]=store
    await upd.message.reply_text(tx("narx_val",la),reply_markup=back_kb(la)); return NARX_DOKON_VAL

async def narx_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return NARX_PROD
    price=parse_money(t)
    if price<=0: await upd.message.reply_text(tx("err_money",la)); return NARX_VAL
    ctx.user_data["new_price"]=price
    await upd.message.reply_text(tx("tannarx_val",la)); return NARX_COST

async def narx_dokon_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return NARX_TYPE
    price=parse_money(t)
    if price<=0: await upd.message.reply_text(tx("err_money",la)); return NARX_DOKON_VAL
    ctx.user_data["new_price"]=price
    await upd.message.reply_text(tx("tannarx_val",la)); return NARX_DOKON_COST

async def narx_cost(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id
    cost=parse_money(upd.message.text); p=ctx.user_data["p"]; price=ctx.user_data["new_price"]
    set_price(p["id"],p[la],price,cost,dist_id=str(uid))
    await upd.message.reply_text(f"{tx('narx_updated',la)}\n{p[la]}: {price:,.0f} / {cost:,.0f}")
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU

async def narx_dokon_cost(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id
    cost=parse_money(upd.message.text); p=ctx.user_data["p"]; price=ctx.user_data["new_price"]
    store=ctx.user_data.get("narx_dokon",{})
    set_price(p["id"],p[la],price,cost,dist_id=str(uid),dokon_id=str(store.get("ID","")))
    await upd.message.reply_text(f"{tx('narx_updated',la)}\n{p[la]}\nDo'kon: {store.get('Nomi','')}\n{price:,.0f} / {cost:,.0f}")
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU

# ── DO'KON MENYU ──────────────────────────────────────────────────────────────
async def dokon_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if not is_approved(uid):
        await upd.message.reply_text(tx("wait_approve",la),reply_markup=wait_kb(la)); return WAIT_APPROVE
    sid=get_short_id(uid)
    if t==tx("dokon_zakaz",la):
        stores=get_stores(egasi_id=uid)
        if not stores: await upd.message.reply_text("Do'koningiz topilmadi. Distribyutorga bog'laning."); return DOKON_MENU
        ctx.user_data["my_store"]=stores[0]
        await upd.message.reply_text(tx("prod",la),reply_markup=prod_kb(la)); return DOKON_ZAKAZ_PROD
    if t==tx("dokon_qarz",la):
        stores=get_stores(egasi_id=uid)
        if stores:
            debt=get_debt(str(stores[0].get("ID","")))
            await upd.message.reply_text(f"💸 Qarzingiz: {debt:,.0f} so'm" if la=="uz" else f"💸 Ваш долг: {debt:,.0f} сум")
        return DOKON_MENU
    if t==tx("dokon_tarix",la):
        await _dokon_tarix(upd,ctx); return DOKON_MENU
    if t==tx("dokon_confirm",la):
        await _dokon_confirm_list(upd,ctx); return DOKON_MENU
    if t==tx("dokon_tolov",la):
        stores=get_stores(egasi_id=uid)
        if not stores: await upd.message.reply_text("Do'kon topilmadi"); return DOKON_MENU
        store=stores[0]; debt=get_debt(str(store.get("ID","")))
        if debt<=0: await upd.message.reply_text("Qarzingiz yo'q!" if la=="uz" else "Долга нет!"); return DOKON_MENU
        ctx.user_data["my_store"]=store
        await upd.message.reply_text(f"Qarz: {debt:,.0f}\n\nTo'lov summasini kiriting:",reply_markup=back_kb(la))
        return DOKON_TOLOV_AMOUNT
    if t==tx("dokon_link_btn",la):
        return await dokon_link_start(upd,ctx)
    await upd.message.reply_text(tx("dokon_main",la,sid=sid),reply_markup=dokon_kb(la),parse_mode="HTML"); return DOKON_MENU

async def _dokon_tarix(upd,ctx):
    la=lg(ctx); uid=str(upd.effective_user.id)
    stores=get_stores(egasi_id=uid)
    if not stores: return
    store_id=str(stores[0].get("ID",""))
    tops=[r for r in db_all("Topshirish") if str(r.get("Dokon_ID",""))==store_id][-10:]
    if not tops: await upd.message.reply_text("Tarix yo'q" if la=="uz" else "История пуста"); return
    lines=["📜 So'nggi 10:","---"]
    for r in reversed(tops):
        s=r.get("Status",""); icon="✅" if s=="tasdiqlangan" else ("❌" if s=="rad_etildi" else "⏳")
        lines.append(f"{icon} {r.get('Sana','')[:10]}: {r.get('Mahsulot','')} {r.get('Miqdor','')} | {float(r.get('Jami',0) or 0):,.0f}")
    await upd.message.reply_text("\n".join(lines))

async def _dokon_confirm_list(upd,ctx):
    la=lg(ctx); uid=str(upd.effective_user.id)
    stores=get_stores(egasi_id=uid)
    if not stores: return
    store_id=str(stores[0].get("ID",""))
    pending=[r for r in db_all("Topshirish") if str(r.get("Dokon_ID",""))==store_id and r.get("Status","")=="kutilmoqda"]
    if not pending: await upd.message.reply_text("Kutilayotgan mol yo'q" if la=="uz" else "Нет ожидающих поставок"); return
    for r in pending:
        tid=r.get("Top_ID","")
        await upd.message.reply_text(
            f"📦 {r.get('Mahsulot','')} {r.get('Miqdor','')} {r.get('Birlik','')}\n"
            f"Jami: {float(r.get('Jami',0) or 0):,.0f}\nNaqd: {float(r.get('Naqd',0) or 0):,.0f}\nQarz: {float(r.get('Qarz',0) or 0):,.0f}\n\n"
            f"✅ /tok_{tid} — Qabul\n❌ /trad_{tid} — Rad")

async def dokon_tolov_amount(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text or ""; uid=upd.effective_user.id
    if t==tx("back",la): return DOKON_MENU
    amount=parse_money(t)
    if amount<=0: await upd.message.reply_text(tx("err_money",la)); return DOKON_TOLOV_AMOUNT
    store=ctx.user_data.get("my_store",{})
    store_id=str(store.get("ID","")); store_name=store.get("Nomi","")
    dist_id=str(store.get("Dist_ID","")); tolov_id=make_op_id("V")
    db_append("Tolov",[now_str(),str(uid),store_name,store_id,amount,"kutilmoqda",tolov_id])
    try:
        du=get_user(dist_id); dla=du.get("Til","uz") if du else la
        await ctx.bot.send_message(int(dist_id),tx("tolov_dist_msg",dla,dokon=store_name,summa=amount,id=tolov_id))
    except Exception as e: logger.error(f"tolov dist: {e}")
    await upd.message.reply_text(f"✅ To'lov so'rovi yuborildi: {amount:,.0f}")
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("dokon_main",la,sid=sid),reply_markup=dokon_kb(la),parse_mode="HTML"); return DOKON_MENU

async def dokon_zakaz_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return DOKON_MENU
    p=find_prod(t,la)
    if not p: await upd.message.reply_text(tx("prod",la),reply_markup=prod_kb(la)); return DOKON_ZAKAZ_PROD
    ctx.user_data["p"]=p
    await upd.message.reply_text(f"{t}\n\n{tx('qty',la)}",reply_markup=back_kb(la)); return DOKON_ZAKAZ_QTY

async def dokon_zakaz_qty(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if t==tx("back",la): await upd.message.reply_text(tx("prod",la),reply_markup=prod_kb(la)); return DOKON_ZAKAZ_PROD
    qty=parse_weight(t)
    if qty<=0: await upd.message.reply_text(tx("err_num",la)); return DOKON_ZAKAZ_QTY
    p=ctx.user_data["p"]; store=ctx.user_data.get("my_store",{})
    store_id=str(store.get("ID","")); store_name=store.get("Nomi",""); dist_id=str(store.get("Dist_ID",""))
    zakaz_id=make_op_id("Z")
    db_append("Buyurtmalar",[now_str(),store_id,store_name,dist_id,p[la],qty,"Yangi","",zakaz_id])
    if dist_id:
        try:
            du=get_user(dist_id); dla=du.get("Til","uz") if du else la
            pname=p.get(dla,p[la])
            await ctx.bot.send_message(int(dist_id),tx("zakaz_dist_new",dla,dokon=store_name,prod=pname,qty=qty,unit=p["unit"],id=zakaz_id))
        except Exception as e: logger.error(f"zakaz dist: {e}")
    await upd.message.reply_text(tx("zakaz_sent",la))
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("dokon_main",la,sid=sid),reply_markup=dokon_kb(la),parse_mode="HTML"); return DOKON_MENU

# ── MAIN MENU (DIST) ──────────────────────────────────────────────────────────
async def main_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if not is_approved(uid):
        if is_rejected(uid):
            await upd.message.reply_text(tx("rejected_msg",la)+"\n\n"+tx("rejected_retry",la)); return REG_NAME
        await upd.message.reply_text(tx("wait_approve",la),reply_markup=wait_kb(la)); return WAIT_APPROVE
    sid=get_short_id(uid)
    if t==tx("qabul",la):         return await zavod_start(upd,ctx)
    if t==tx("my_prices",la):     return await narx_start(upd,ctx)
    if t==tx("link_dokon_btn",la):return await dist_link_start(upd,ctx)
    if t==tx("tolov_qabul",la):   await _show_qarzdorlar(upd,ctx); return MAIN_MENU
    if t==tx("topshir",la):
        stores=get_stores(dist_id=uid)
        if not stores: await upd.message.reply_text(tx("no_store",la)); return MAIN_MENU
        ctx.user_data["stores"]=stores
        await upd.message.reply_text(tx("store",la),reply_markup=store_kb(stores,la)); return TOP_STORE
    if t==tx("buyurtma",la):      await _show_buyurtmalar(upd,ctx); return MAIN_MENU
    if t==tx("natija",la):        await daily(upd,ctx); return MAIN_MENU
    if t==tx("ombor",la):         await stock(upd,ctx); return MAIN_MENU
    if t==tx("marshrut",la):      await marshrut_start(upd,ctx); return MAIN_MENU
    if t==tx("my_stores",la):     await _show_my_stores(upd,ctx); return MAIN_MENU
    if t==tx("hisobot",la):
        await upd.message.reply_text("Hisobot:",
            reply_markup=ReplyKeyboardMarkup([[tx("week",la),tx("month",la)],[tx("back",la)]],resize_keyboard=True))
        return HISOBOT_MENU
    if t==tx("admin",la) and is_adm(ctx):
        await upd.message.reply_text(tx("adm",la),reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,uid in ADMIN_IDS),parse_mode="HTML")
    return MAIN_MENU

async def _show_qarzdorlar(upd,ctx):
    la=lg(ctx); uid=str(upd.effective_user.id)
    stores=get_stores(dist_id=uid); lines=["💸 Qarzdorlar:","---"]; total=0
    for s in stores:
        debt=get_debt(str(s.get("ID","")))
        if debt>0: lines.append(f"• {s.get('Nomi','')}: {debt:,.0f}"); total+=debt
    if len(lines)==2: lines.append("Qarz yo'q!")
    else: lines.append(f"---\nJami: {total:,.0f}\n\nTo'lovni tasdiqlash: /vok_TOLOV_ID")
    await upd.message.reply_text("\n".join(lines))

async def _show_buyurtmalar(upd,ctx):
    la=lg(ctx); uid=str(upd.effective_user.id)
    orders=[r for r in db_all("Buyurtmalar") if str(r.get("Dist_ID",""))==uid and r.get("Status","")=="Yangi"]
    if not orders: await upd.message.reply_text("📋 Yangi zakaz yo'q" if la=="uz" else "📋 Новых заказов нет"); return
    lines=[f"📋 Yangi zakazlar: {len(orders)}","---"]
    for r in orders:
        zid=r.get("Zakaz_ID","")
        lines.append(f"• {r.get('Dokon','')} | {r.get('Mahsulot','')} {r.get('Miqdor','')} | {r.get('Sana','')[:10]}\n  ✅ /zqabul_{zid} | ❌ /zrad_z_{zid}")
    await upd.message.reply_text("\n".join(lines))

async def _show_my_stores(upd,ctx):
    la=lg(ctx); uid=str(upd.effective_user.id)
    stores=get_stores(dist_id=uid); lines=["🏪 Mening do'konlarim:","---"]
    for s in stores:
        debt=get_debt(str(s.get("ID",""))); d=f" | Qarz: {debt:,.0f}" if debt>0 else ""
        lat=s.get("Lat",""); lng=s.get("Lng",""); loc=f"\n  📍 {lat},{lng}" if lat and lng else ""
        lines.append(f"• {s.get('Nomi','')}{d}\n  📞 {s.get('Tel1','')}\n  👤 {s.get('Egasi_Ism','Tayinlanmagan')}{loc}")
    if len(lines)==2: lines.append("Do'konlar yo'q")
    await upd.message.reply_text("\n".join(lines))

# ── HISOBOT ───────────────────────────────────────────────────────────────────
async def hisobot_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=str(upd.effective_user.id)
    if t==tx("back",la):
        sid=get_short_id(upd.effective_user.id)
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU
    days=7 if t==tx("week",la) else 30
    from_dt=(datetime.now()-timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        tops=[r for r in db_all("Topshirish") if str(r.get("Sana",""))>=from_dt and str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan"]
        ins =[r for r in db_all("Qabul")       if str(r.get("Sana",""))>=from_dt and str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan"]
        ts=sum(float(r.get("Jami",0) or 0) for r in tops)
        tn=sum(float(r.get("Naqd",0) or 0) for r in tops)
        tq=sum(float(r.get("Qarz",0) or 0) for r in tops)
        ti=sum(float(r.get("Jami",0) or 0) for r in ins)
        stores=get_stores(dist_id=uid)
        jami_qarz=sum(get_debt(str(s.get("ID",""))) for s in stores)
        foyda=calc_foyda(uid,from_dt)
        period=("7 kun" if days==7 else "30 kun") if la=="uz" else ("7 дней" if days==7 else "30 дней")
        if la=="uz":
            msg=(f"📈 Hisobot: {period}\n---\n"
                 f"📥 Qabul (zavod): {ti:,.0f} so'm\n"
                 f"🚚 Sotuv: {ts:,.0f} so'm\n"
                 f"💵 Naqd: {tn:,.0f} so'm\n"
                 f"📝 Davr qarzi: {tq:,.0f} so'm\n"
                 f"💸 Jami qarzdorlik: {jami_qarz:,.0f} so'm\n"
                 f"💰 Foyda: {foyda:,.0f} so'm")
        else:
            msg=(f"📈 Отчёт: {period}\n---\n"
                 f"📥 Получено (завод): {ti:,.0f} сум\n"
                 f"🚚 Продажи: {ts:,.0f} сум\n"
                 f"💵 Наличные: {tn:,.0f} сум\n"
                 f"📝 Долг (период): {tq:,.0f} сум\n"
                 f"💸 Общий долг: {jami_qarz:,.0f} сум\n"
                 f"💰 Прибыль: {foyda:,.0f} сум")
    except Exception as e: msg=f"Xatolik: {e}"
    sid=get_short_id(upd.effective_user.id)
    await upd.message.reply_text(msg,reply_markup=main_kb(la,sid,False)); return MAIN_MENU

# ── KUNLIK ────────────────────────────────────────────────────────────────────
async def daily(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=str(upd.effective_user.id); today=today_str()
    try:
        tops=[r for r in db_all("Topshirish") if str(r.get("Sana","")).startswith(today) and str(r.get("Dist_ID",""))==uid]
        ins =[r for r in db_all("Qabul")       if str(r.get("Sana","")).startswith(today) and str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan"]
        ts=sum(float(r.get("Jami",0) or 0) for r in tops)
        tn=sum(float(r.get("Naqd",0) or 0) for r in tops)
        tq=sum(float(r.get("Qarz",0) or 0) for r in tops)
        ti=sum(float(r.get("Jami",0) or 0) for r in ins)
        foyda=ts-ti  # bugungi sotuv - bugungi zavod xarajati
        stores=get_stores(dist_id=uid)
        jami_qarz=sum(get_debt(str(s.get("ID",""))) for s in stores)
        dc=len(set(r.get("Dokon","") for r in tops))
        if la=="uz":
            msg=(f"📊 Kunlik natija - {today}\n---\n"
                 f"📥 Zavod: {ti:,.0f} so'm\n"
                 f"🚚 Sotuv: {ts:,.0f} so'm\n"
                 f"💵 Naqd: {tn:,.0f} so'm\n"
                 f"📝 Qarz: {tq:,.0f} so'm\n"
                 f"💸 Jami qarzdorlik: {jami_qarz:,.0f} so'm\n"
                 f"💰 Bugungi foyda: {foyda:,.0f} so'm\n"
                 f"🏪 Do'konlar: {dc}")
        else:
            msg=(f"📊 Итог дня - {today}\n---\n"
                 f"📥 Завод: {ti:,.0f} сум\n"
                 f"🚚 Продажи: {ts:,.0f} сум\n"
                 f"💵 Наличные: {tn:,.0f} сум\n"
                 f"📝 Долг: {tq:,.0f} сум\n"
                 f"💸 Общий долг: {jami_qarz:,.0f} сум\n"
                 f"💰 Прибыль за день: {foyda:,.0f} сум\n"
                 f"🏪 Магазинов: {dc}")
    except Exception as e: msg=f"Xatolik: {e}"
    await upd.message.reply_text(msg)

async def stock(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=str(upd.effective_user.id)
    try:
        st={}
        for r in db_all("Qabul"):
            if str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan":
                k=r.get("Mahsulot",""); st[k]=st.get(k,0)+float(r.get("Miqdor",0) or 0)
        for r in db_all("Topshirish"):
            if str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan":
                k=r.get("Mahsulot",""); st[k]=st.get(k,0)-float(r.get("Miqdor",0) or 0)
        lines=["📦 Ombor:","---"]
        for k,v in st.items():
            if v>0.001: lines.append(f"• {k}: {v:.3f}")
        if len(lines)==2: lines.append("Hammasi topshirilgan!" if la=="uz" else "Всё сдано!")
        await upd.message.reply_text("\n".join(lines))
    except Exception as e: await upd.message.reply_text(f"Xatolik: {e}")

# ── MARSHRUT ──────────────────────────────────────────────────────────────────
async def marshrut_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id
    stores=get_stores(dist_id=uid)
    if not stores: await upd.message.reply_text(tx("no_store",la)); return
    ctx.user_data["m_stores"]=stores
    await upd.message.reply_text(tx("send_loc",la),
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton(tx("loc_btn",la),request_location=True)],[tx("back",la)]],resize_keyboard=True))

async def marshrut_loc(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id
    stores=ctx.user_data.get("m_stores",get_stores(dist_id=uid))
    lat,lng=0,0
    if upd.message.location: lat=upd.message.location.latitude; lng=upd.message.location.longitude
    lines=["🗺 Bugungi marshrut:" if la=="uz" else "🗺 Маршрут:","---"]
    for i,s in enumerate(stores,1):
        debt=get_debt(str(s.get("ID",""))); d=f" (⚠️ {debt:,.0f})" if debt>0 else ""
        lines.append(f"{i}. {s.get('Nomi','')}{d}")
    if lat and lng and stores:
        wps="|".join([s.get("Nomi","").replace(" ","+") for s in stores])
        dest=stores[-1].get("Nomi","").replace(" ","+")
        url=f"https://www.google.com/maps/dir/?api=1&origin={lat},{lng}&destination={dest}&waypoints={wps}&travelmode=driving"
        lines.append(f"\n{url}")
    sid=get_short_id(uid)
    await upd.message.reply_text("\n".join(lines),reply_markup=main_kb(la,sid,False))


# ── ADMIN ─────────────────────────────────────────────────────────────────────
async def admin_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if uid not in ADMIN_IDS: await upd.message.reply_text(tx("no_admin",la)); return MAIN_MENU
    if t==tx("back",la): await upd.message.reply_text(tx("adm",la),reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU
    if t==tx("adm_mahsulot",la):
        await upd.message.reply_text(tx("mahsulot_nom_uz",la),reply_markup=back_kb(la)); return ADM_MAHSULOT_NOM
    if t==tx("adm_price",la):
        await upd.message.reply_text(tx("narx_prod",la),reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    if t==tx("adm_add_store",la):
        await upd.message.reply_text("Do'kon nomi:",reply_markup=back_kb(la)); return ADM_STORE_NAME
    if t==tx("adm_add_dist",la):
        ctx.user_data["adm_role"]="distributor"
        await upd.message.reply_text("Distribyutor ismi:",reply_markup=back_kb(la)); return ADM_DIST_NAME
    if t==tx("adm_add_dokon_ega",la):
        ctx.user_data["adm_role"]="dokon_ega"
        await upd.message.reply_text("Do'kon egasi ismi:",reply_markup=back_kb(la)); return ADM_DOKON_EGA_NAME
    if t==tx("adm_stats",la):       await a_stats(upd,ctx); return ADMIN_MENU
    if t==tx("adm_debtors",la):     await a_debtors(upd,ctx); return ADMIN_MENU
    if t==tx("adm_list_stores",la): await a_list_stores(upd,ctx); return ADMIN_MENU
    if t==tx("adm_list_dists",la):  await a_list_dists(upd,ctx); return ADMIN_MENU
    if t==tx("adm_zavod_list",la):  await a_zavod_list(upd,ctx); return ADMIN_MENU
    if t==tx("adm_broadcast",la):
        await upd.message.reply_text(tx("broadcast_msg",la),reply_markup=back_kb(la)); return ADM_BROADCAST
    return ADMIN_MENU

# ── MAHSULOT QO'SHISH ─────────────────────────────────────────────────────────
async def adm_mahsulot_nom(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["m_uz"]=t.strip()
    await upd.message.reply_text(tx("mahsulot_nom_ru",la)); return ADM_MAHSULOT_RU

async def adm_mahsulot_ru(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); ctx.user_data["m_ru"]=upd.message.text.strip()
    await upd.message.reply_text(tx("mahsulot_unit",la),
        reply_markup=ReplyKeyboardMarkup([["kg","litr"],["dona","g"],[tx("back",la)]],resize_keyboard=True))
    return ADM_MAHSULOT_UNIT

async def adm_mahsulot_unit(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    uz=ctx.user_data.get("m_uz",""); ru=ctx.user_data.get("m_ru",""); unit=t.strip()
    recs=db_all("Mahsulotlar")
    new_id=max([int(r.get("ID",0)) for r in recs],default=0)+1
    db_append("Mahsulotlar",[str(new_id),uz,ru,unit,"1",now_str()])
    await upd.message.reply_text(tx("mahsulot_ok",la,name=uz),reply_markup=admin_kb(la),parse_mode="HTML")
    return ADMIN_MENU

# ── ADMIN NARX ────────────────────────────────────────────────────────────────
async def adm_price_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    p=find_prod(t,la)
    if not p: await upd.message.reply_text(tx("narx_prod",la),reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    ctx.user_data["p"]=p; price,cost=get_price(p["id"])
    await upd.message.reply_text(f"{t}\n💰 Joriy: {price:,.0f} / Tannarx: {cost:,.0f}\n\n{tx('narx_val',la)}",reply_markup=back_kb(la))
    return ADM_PRICE_VAL

async def adm_price_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADM_PRICE_PROD
    price=parse_money(t)
    if price<=0: await upd.message.reply_text(tx("err_money",la)); return ADM_PRICE_VAL
    ctx.user_data["np"]=price
    await upd.message.reply_text(tx("tannarx_val",la)); return ADM_COST_VAL

async def adm_cost_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); cost=parse_money(upd.message.text); p=ctx.user_data["p"]; price=ctx.user_data["np"]
    set_price(p["id"],p[la],price,cost)
    await upd.message.reply_text(f"{tx('narx_updated',la)}\n{p[la]}: {price:,.0f} / {cost:,.0f}",reply_markup=admin_kb(la),parse_mode="HTML")
    return ADMIN_MENU

# ── ADMIN DO'KON ──────────────────────────────────────────────────────────────
async def adm_store_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["ns"]=t.strip()
    await upd.message.reply_text("Manzil:",reply_markup=back_kb(la)); return ADM_STORE_ADDR

async def adm_store_addr(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); ctx.user_data["ns_addr"]=upd.message.text.strip()
    await upd.message.reply_text("Distribyutor Telegram ID:",reply_markup=back_kb(la)); return ADM_STORE_DIST

async def adm_store_dist(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); ctx.user_data["ns_dist"]=upd.message.text.strip()
    await upd.message.reply_text("Lokatsiya (yoki Otkazib yuborish):",reply_markup=loc_kb(la)); return ADM_STORE_LOC

async def adm_store_loc(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    store=ctx.user_data.get("ns",""); addr=ctx.user_data.get("ns_addr",""); dist_id=ctx.user_data.get("ns_dist","")
    lat,lng="",""
    if upd.message.location: lat=str(upd.message.location.latitude); lng=str(upd.message.location.longitude)
    cnt=len(db_all("Dokonlar"))+1
    db_append("Dokonlar",[str(cnt),"",store,addr,"","","",str(dist_id),"","","",lat,lng,now_str()])
    await upd.message.reply_text(f"✅ Do'kon qo'shildi: {store}",reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU

# ── ADMIN FOYDALANUVCHI ───────────────────────────────────────────────────────
async def adm_dist_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["nd"]=t.strip()
    await upd.message.reply_text("Telegram ID:",reply_markup=back_kb(la)); return ADM_DIST_ID

async def adm_dist_id(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); name=ctx.user_data.get("nd",""); role=ctx.user_data.get("adm_role","distributor"); sid=make_short_id()
    db_append("Foydalanuvchilar",[upd.message.text.strip(),name,"","",role,la,"","tasdiqlangan",sid,"",now_str()])
    await upd.message.reply_text(f"✅ Qo'shildi: {name} | ID: {sid}",reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU

async def adm_dokon_ega_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["nd"]=t.strip()
    await upd.message.reply_text("Telegram ID:",reply_markup=back_kb(la)); return ADM_DOKON_EGA_ID

async def adm_dokon_ega_id(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); name=ctx.user_data.get("nd",""); sid=make_short_id()
    db_append("Foydalanuvchilar",[upd.message.text.strip(),name,"","","dokon_ega",la,"","tasdiqlangan",sid,"",now_str()])
    await upd.message.reply_text(f"✅ Do'kon egasi qo'shildi: {name} | ID: {sid}",reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU

async def adm_broadcast(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): await upd.message.reply_text(tx("adm",la),reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU
    users=db_all("Foydalanuvchilar"); sent=0; failed=0
    for u in users:
        try: await ctx.bot.send_message(int(u.get("TG_ID",0)),t); sent+=1
        except Exception: failed+=1
    await upd.message.reply_text(f"✅ Yuborildi: {sent} | Xato: {failed}",reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU

# ── ADMIN HISOBOTLAR ──────────────────────────────────────────────────────────
async def a_zavod_list(upd,ctx):
    la=lg(ctx); recs=[r for r in db_all("Qabul") if r.get("Status","")=="kutilmoqda"]
    if not recs: await upd.message.reply_text("📦 Kutilayotgan so'rov yo'q"); return
    lines=[f"📦 Kutilmoqda: {len(recs)}","---"]
    for r in recs:
        qid=r.get("Qabul_ID","")
        lines.append(f"• {r.get('Dist_Ism','')} | {r.get('Mahsulot','')} {r.get('Miqdor','')} {r.get('Birlik','')}\nJami: {float(r.get('Jami',0) or 0):,.0f}\n✅ /zok_{qid} | ❌ /zrad_{qid}")
    text="\n".join(lines)
    for i in range(0,len(text),3500): await upd.message.reply_text(text[i:i+3500])

async def a_list_stores(upd,ctx):
    stores=db_all("Dokonlar")
    if not stores: await upd.message.reply_text("🏪 Do'konlar yo'q"); return
    lines=[f"🏪 Jami: {len(stores)}","---"]
    for s in stores:
        debt=get_debt(str(s.get("ID",""))); d=f" | Qarz: {debt:,.0f}" if debt>0 else ""
        lat=s.get("Lat",""); lng=s.get("Lng",""); loc=f"\n  📍{lat},{lng}" if lat and lng else ""
        lines.append(f"• {s.get('Nomi','')}{d}\n  📍 {s.get('Adres','-')}\n  📞 {s.get('Tel1','')}\n  🚚 {s.get('Dist_Ism','')} ({s.get('Dist_ID','')})\n  👤 {s.get('Egasi_Ism','Tayinlanmagan')}{loc}")
    text="\n".join(lines)
    for i in range(0,len(text),3500): await upd.message.reply_text(text[i:i+3500])

async def a_list_dists(upd,ctx):
    la=lg(ctx)
    dists=[u for u in db_all("Foydalanuvchilar") if u.get("Rol","")=="distributor"]
    if not dists: await upd.message.reply_text("🚚 Distribyutorlar yo'q"); return
    lines=[f"🚚 Jami: {len(dists)}","---"]
    for u in dists:
        uid_d=u.get("TG_ID",""); sid=u.get("Short_ID","?")
        name=f"{u.get('Ism','')} {u.get('Familiya','')}".strip()
        stores=get_stores(dist_id=uid_d)
        jq=sum(get_debt(str(s.get("ID",""))) for s in stores)
        foyda=calc_foyda(str(uid_d))
        lines.append(f"• {name} (ID:{sid})\n  📞 {u.get('Telefon','')}\n  TG: {uid_d}\n  Status: {u.get('Status','')}\n  🏪 {len(stores)} do'kon\n  💸 Qarz: {jq:,.0f}\n  💰 Foyda: {foyda:,.0f}")
    text="\n".join(lines)
    for i in range(0,len(text),3500): await upd.message.reply_text(text[i:i+3500])

async def a_stats(upd,ctx):
    la=lg(ctx)
    try:
        tops=db_all("Topshirish"); ins=db_all("Qabul"); stores=db_all("Dokonlar"); users=db_all("Foydalanuvchilar")
        ts=sum(float(r.get("Jami",0) or 0) for r in tops if r.get("Status","")=="tasdiqlangan")
        tn=sum(float(r.get("Naqd",0) or 0) for r in tops if r.get("Status","")=="tasdiqlangan")
        ti=sum(float(r.get("Jami",0) or 0) for r in ins if r.get("Status","")=="tasdiqlangan")
        jq=sum(get_debt(str(s.get("ID",""))) for s in stores)
        dists_list=[u for u in users if u.get("Rol","")=="distributor"]
        dokon_egas=[u for u in users if u.get("Rol","")=="dokon_ega"]
        # Barcha distribyutorlar foydasi
        total_foyda=sum(calc_foyda(str(u.get("TG_ID",""))) for u in dists_list)
        zakaz_yangi=len([r for r in db_all("Buyurtmalar") if r.get("Status","")=="Yangi"])
        msg=(f"📊 Umumiy statistika\n---\n"
             f"📥 Zavod: {ti:,.0f} so'm\n"
             f"🚚 Sotuv: {ts:,.0f} so'm\n"
             f"💵 Naqd: {tn:,.0f} so'm\n"
             f"💸 Jami qarzdorlik: {jq:,.0f} so'm\n"
             f"💰 Jami foyda: {total_foyda:,.0f} so'm\n"
             f"🏪 Do'konlar: {len(stores)}\n"
             f"🚚 Distribyutorlar: {len(dists_list)}\n"
             f"👤 Do'kon egalari: {len(dokon_egas)}\n"
             f"📋 Yangi zakazlar: {zakaz_yangi}")
        await upd.message.reply_text(msg)
    except Exception as e: await upd.message.reply_text(f"Xatolik: {e}")

async def a_debtors(upd,ctx):
    stores=db_all("Dokonlar"); lines=["💸 Qarzdorlar:","---"]; total=0
    for s in stores:
        debt=get_debt(str(s.get("ID","")))
        if debt>0: lines.append(f"• {s.get('Nomi','')}: {debt:,.0f}\n  Dist: {s.get('Dist_Ism','')}"); total+=debt
    if len(lines)==2: lines.append("Qarz yo'q!")
    else: lines.append(f"---\nJami: {total:,.0f} so'm")
    await upd.message.reply_text("\n".join(lines))

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
async def check_zakaz_timeouts(ctx: ContextTypes.DEFAULT_TYPE):
    """Har 15 daqiqada tekshirish"""
    try:
        orders=db_all("Buyurtmalar"); now=datetime.now()
        for r in orders:
            if r.get("Status","")!="Yangi": continue
            try: zakaz_time=datetime.strptime(r.get("Sana",""),"%Y-%m-%d %H:%M")
            except Exception: continue
            mins=(now-zakaz_time).total_seconds()/60
            dist_id=str(r.get("Dist_ID","")); zakaz_id=r.get("Zakaz_ID","")
            dokon=r.get("Dokon",""); prod=r.get("Mahsulot",""); miqdor=r.get("Miqdor","")
            # 2 soat = 120 min → do'kon egasiga xabar
            if mins>=120:
                dokon_id=str(r.get("Dokon_ID",""))
                for s in db_all("Dokonlar"):
                    if str(s.get("ID",""))==dokon_id:
                        egasi_id=str(s.get("Egasi_ID",""))
                        if egasi_id:
                            try:
                                du=get_user(dist_id)
                                dist_name=f"{du.get('Ism','')} {du.get('Familiya','')}".strip() if du else str(dist_id)
                                dist_phone=du.get("Telefon","") if du else ""
                                eu=get_user(egasi_id); ela=eu.get("Til","uz") if eu else "uz"
                                await ctx.bot.send_message(int(egasi_id),
                                    tx("zakaz_timeout",ela,prod=prod,qty=miqdor,phone=dist_phone,name=dist_name))
                                db_update("Buyurtmalar","Zakaz_ID",zakaz_id,"Status","Eslatildi")
                            except Exception as e: logger.error(f"timeout: {e}")
                        break
            # Har 15 daqiqada distribyutorga (2 soatgacha)
            elif 15<=mins<120:
                if dist_id:
                    try:
                        du=get_user(dist_id); dla=du.get("Til","uz") if du else "uz"
                        await ctx.bot.send_message(int(dist_id),
                            tx("zakaz_reminder",dla,dokon=dokon,prod=prod,qty=miqdor,id=zakaz_id))
                    except Exception as e: logger.error(f"reminder: {e}")
    except Exception as e: logger.error(f"check_zakaz: {e}")

async def debt_reminder(ctx: ContextTypes.DEFAULT_TYPE):
    """Har kuni 09:00"""
    try:
        for u in db_all("Foydalanuvchilar"):
            if u.get("Rol","")!="distributor": continue
            uid=str(u.get("TG_ID",""))
            stores=get_stores(dist_id=uid)
            debts=[(s.get("Nomi",""),get_debt(str(s.get("ID","")))) for s in stores]
            debts=[(n,d) for n,d in debts if d>0]
            if not debts: continue
            try:
                la=u.get("Til","uz"); lines=["💸 Bugungi qarzlar:","---"]
                for name,debt in debts: lines.append(f"• {name}: {debt:,.0f} so'm")
                await ctx.bot.send_message(int(uid),"\n".join(lines))
            except Exception: pass
    except Exception as e: logger.error(f"debt_reminder: {e}")

async def cancel(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id
    user=get_user(uid); role=user.get("Rol","") if user else ""
    if role=="dokon_ega":
        sid=get_short_id(uid)
        await upd.message.reply_text(tx("dokon_main",la,sid=sid),reply_markup=dokon_kb(la),parse_mode="HTML"); return DOKON_MENU
    if uid in ADMIN_IDS:
        await upd.message.reply_text(tx("adm",la),reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN: print("BOT_TOKEN topilmadi!"); return
    app=Application.builder().token(BOT_TOKEN).build()
    app.job_queue.run_daily(debt_reminder, time=dtime(9,0))
    app.job_queue.run_repeating(check_zakaz_timeouts, interval=900, first=60)

    txt      = filters.TEXT & ~filters.COMMAND
    photo_txt= (filters.PHOTO | filters.TEXT) & ~filters.COMMAND
    loc_txt  = (filters.LOCATION | filters.TEXT) & ~filters.COMMAND
    cont_txt = (filters.CONTACT | filters.TEXT) & ~filters.COMMAND

    conv=ConversationHandler(
        entry_points=[CommandHandler("start",start)],
        states={
            LANG_SELECT:        [CallbackQueryHandler(lang_cb,pattern="^lang_"),CommandHandler("start",start)],
            ROLE_SELECT:        [MessageHandler(txt,role_select)],
            REG_NAME:           [MessageHandler(txt,reg_name)],
            REG_FNAME:          [MessageHandler(txt,reg_fname)],
            REG_PHONE:          [MessageHandler(cont_txt,reg_phone)],
            REG_PASSPORT:       [MessageHandler((filters.PHOTO|filters.TEXT)&~filters.COMMAND,reg_passport)],
            WAIT_APPROVE:       [MessageHandler(txt,wait_approve_h)],
            MAIN_MENU:          [MessageHandler(txt,main_h)],
            DIST_LINK_ID:       [MessageHandler(txt,dist_link_id)],
            DOKON_LINK_ID:      [MessageHandler(txt,dokon_link_id)],
            ZAVOD_PROD:         [MessageHandler(txt,zavod_prod)],
            ZAVOD_QTY:          [MessageHandler(txt,zavod_qty)],
            TOP_STORE:          [MessageHandler(txt,top_store)],
            TOP_PROD:           [MessageHandler(txt,top_prod)],
            TOP_PHOTO:          [MessageHandler(photo_txt,top_photo)],
            TOP_PAY_TYPE:       [MessageHandler(txt,top_pay_type)],
            TOP_PAY_AMOUNT:     [MessageHandler(txt,top_pay_amount)],
            ZAKAZ_COMMENT:      [MessageHandler(txt,zakaz_comment)],
            DI_NAME:            [MessageHandler(txt,di_name)],
            DI_ADDR:            [MessageHandler(txt,di_addr)],
            DI_MCHJ:            [MessageHandler(txt,di_mchj)],
            DI_TEL1:            [MessageHandler(cont_txt,di_tel1)],
            DI_TEL2:            [MessageHandler(cont_txt,di_tel2)],
            DI_PHOTO:           [MessageHandler(photo_txt,di_photo)],
            DI_LOC:             [MessageHandler(loc_txt,di_loc)],
            NARX_PROD:          [MessageHandler(txt,narx_prod)],
            NARX_TYPE:          [MessageHandler(txt,narx_type)],
            NARX_VAL:           [MessageHandler(txt,narx_val)],
            NARX_COST:          [MessageHandler(txt,narx_cost)],
            NARX_DOKON:         [MessageHandler(txt,narx_dokon)],
            NARX_DOKON_VAL:     [MessageHandler(txt,narx_dokon_val)],
            NARX_DOKON_COST:    [MessageHandler(txt,narx_dokon_cost)],
            HISOBOT_MENU:       [MessageHandler(txt,hisobot_h)],
            DOKON_MENU:         [MessageHandler(txt,dokon_h)],
            DOKON_ZAKAZ_PROD:   [MessageHandler(txt,dokon_zakaz_prod)],
            DOKON_ZAKAZ_QTY:    [MessageHandler(txt,dokon_zakaz_qty)],
            DOKON_TOLOV_AMOUNT: [MessageHandler(txt,dokon_tolov_amount)],
            ADMIN_MENU:         [MessageHandler(txt,admin_h)],
            ADM_MAHSULOT_NOM:   [MessageHandler(txt,adm_mahsulot_nom)],
            ADM_MAHSULOT_RU:    [MessageHandler(txt,adm_mahsulot_ru)],
            ADM_MAHSULOT_UNIT:  [MessageHandler(txt,adm_mahsulot_unit)],
            ADM_PRICE_PROD:     [MessageHandler(txt,adm_price_prod)],
            ADM_PRICE_VAL:      [MessageHandler(txt,adm_price_val)],
            ADM_COST_VAL:       [MessageHandler(txt,adm_cost_val)],
            ADM_STORE_NAME:     [MessageHandler(txt,adm_store_name)],
            ADM_STORE_ADDR:     [MessageHandler(txt,adm_store_addr)],
            ADM_STORE_DIST:     [MessageHandler(txt,adm_store_dist)],
            ADM_STORE_LOC:      [MessageHandler(loc_txt,adm_store_loc)],
            ADM_DIST_NAME:      [MessageHandler(txt,adm_dist_name)],
            ADM_DIST_ID:        [MessageHandler(txt,adm_dist_id)],
            ADM_DOKON_EGA_NAME: [MessageHandler(txt,adm_dokon_ega_name)],
            ADM_DOKON_EGA_ID:   [MessageHandler(txt,adm_dokon_ega_id)],
            ADM_BROADCAST:      [MessageHandler(txt,adm_broadcast)],
        },
        fallbacks=[CommandHandler("cancel",cancel),CommandHandler("start",start)],
        allow_reentry=True,
    )
    for pattern,handler in [
        (r'^/approve_\d+$', approve_cmd),
        (r'^/reject_\d+$',  reject_cmd),
        (r'^/zok_\w+$',     zok_cmd),
        (r'^/zrad_\w+$',    zrad_cmd),
        (r'^/tok_\w+$',     tok_cmd),
        (r'^/trad_\w+$',    trad_cmd),
        (r'^/vok_\w+$',     vok_cmd),
        (r'^/vrad_\w+$',    vrad_cmd),
        (r'^/lok_\w+$',     lok_cmd),
        (r'^/lrad_\w+$',    lrad_cmd),
        (r'^/zqabul_\w+$',  zqabul_cmd),
        (r'^/zrad_z_\w+$',  zrad_z_cmd),
    ]:
        app.add_handler(MessageHandler(filters.Regex(pattern),handler))
    app.add_handler(MessageHandler(filters.LOCATION,marshrut_loc))
    app.add_handler(conv)
    print("Bot ishga tushdi! v3.0")
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__":
    main()
