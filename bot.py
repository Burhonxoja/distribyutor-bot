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
    LANG_SELECT,
    REG_NAME, REG_FNAME, REG_PHONE, REG_PASSPORT,
    WAIT_APPROVE, MAIN_MENU,
    DIST_LINK_ID,
    ZAVOD_PROD, ZAVOD_QTY,
    TOP_STORE, TOP_PROD, TOP_PHOTO, TOP_PAY_TYPE, TOP_PAY_AMOUNT,
    ZAKAZ_COMMENT,
    DI_NAME, DI_ADDR, DI_MCHJ, DI_TEL1, DI_TEL2, DI_PHOTO, DI_LOC,
    NARX_PROD, NARX_TYPE, NARX_VAL, NARX_COST,
    NARX_DOKON, NARX_DOKON_VAL, NARX_DOKON_COST,
    HISOBOT_MENU,
    ADMIN_MENU,
    ADM_MAHSULOT_NOM, ADM_MAHSULOT_RU, ADM_MAHSULOT_UNIT,
    ADM_PRICE_PROD, ADM_PRICE_VAL, ADM_COST_VAL,
    ADM_STORE_NAME, ADM_STORE_ADDR, ADM_STORE_DIST, ADM_STORE_LOC,
    ADM_DIST_NAME, ADM_DIST_ID,
    ADM_BROADCAST,
) = range(44)

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
    "Foydalanuvchilar": ["TG_ID","Ism","Familiya","Telefon","Rol","Til","Passport","Status","Short_ID","Sana"],
    "Mahsulotlar":      ["ID","Nomi_UZ","Nomi_RU","Birlik","Faol","Sana"],
    "Dokonlar":         ["ID","Short_ID","Nomi","Adres","MCHJ","Tel1","Tel2","Dist_ID","Dist_Ism","Lat","Lng","Sana"],
    "Narxlar":          ["Mahsulot_ID","Mahsulot","Narx","Tannarx","Dist_ID","Dokon_ID","Sana"],
    "Qabul":            ["Sana","Dist_ID","Dist_Ism","Mahsulot","Miqdor","Birlik","Narx","Jami","Status","Qabul_ID"],
    "Topshirish":       ["Sana","Dist_ID","Dokon","Dokon_ID","Mahsulot","Miqdor","Birlik","Narx","Jami","Pay_Type","Naqd","Qarz","Status","Top_ID"],
    "Tolov":            ["Sana","Dist_ID","Dokon","Dokon_ID","Summa","Status","Tolov_ID"],
    "Buyurtmalar":      ["Sana","Dokon_ID","Dokon","Dist_ID","Mahsulot","Miqdor","Status","Izoh","Zakaz_ID"],
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
    if int(uid) in ADMIN_IDS: return "ADMIN"
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

def get_stores(dist_id=None):
    try:
        recs = db_all("Dokonlar")
        if dist_id: return [r for r in recs if str(r.get("Dist_ID","")).strip()==str(dist_id).strip()]
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
    try:
        tops = [r for r in db_all("Topshirish") if str(r.get("Dist_ID",""))==dist_uid_str and r.get("Status","")=="tasdiqlangan"]
        ins  = [r for r in db_all("Qabul")       if str(r.get("Dist_ID",""))==dist_uid_str and r.get("Status","")=="tasdiqlangan"]
        if from_date_str:
            tops = [r for r in tops if str(r.get("Sana",""))>=from_date_str]
            ins  = [r for r in ins  if str(r.get("Sana",""))>=from_date_str]
        return sum(float(r.get("Jami",0) or 0) for r in tops) - sum(float(r.get("Jami",0) or 0) for r in ins)
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
    "reg_name":         {"uz":"Ismingizni kiriting:","ru":"Введите имя:"},
    "reg_fname":        {"uz":"Familiyangizni kiriting:","ru":"Введите фамилию:"},
    "reg_phone":        {"uz":"Telefon raqamingizni yuboring:","ru":"Отправьте номер телефона:"},
    "reg_passport":     {"uz":"Passport rasmini yuboring:\n(yoki Otkazib yuborish)","ru":"Фото паспорта:\n(или Пропустить)"},
    "reg_ok":           {"uz":"✅ Ro'yxatdan o'tdingiz {name}!\n🔑 Sizning ID: <b>{sid}</b>\nBu IDni saqlang!\n\nAdmin tasdiqlashini kuting...","ru":"✅ Вы зарегистрированы {name}!\n🔑 Ваш ID: <b>{sid}</b>\nСохраните ID!\n\nОжидайте подтверждения..."},
    "wait_approve":     {"uz":"⏳ Hisobingiz tasdiqlanmagan. Admin tasdiqlashini kuting.","ru":"⏳ Аккаунт не подтверждён. Ожидайте."},
    "resend_btn":       {"uz":"📤 Ma'lumotlarni qayta yuborish","ru":"📤 Повторно отправить"},
    "resent_ok":        {"uz":"Adminga yuborildi. Kuting.","ru":"Отправлено. Ожидайте."},
    "reg_admin_msg":    {"uz":"👤 YANGI DISTRIBYUTOR:\nIsm: {name}\nTel: {phone}\nTG_ID: {uid}\nID: <b>{sid}</b>\n\n✅ /approve_{uid}\n❌ /reject_{uid}","ru":"👤 НОВЫЙ ДИСТРИБЬЮТОР:\nИмя: {name}\nТел: {phone}\nTG_ID: {uid}\nID: <b>{sid}</b>\n\n✅ /approve_{uid}\n❌ /reject_{uid}"},
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
    "admin":            {"uz":"⚙️ Admin panel","ru":"⚙️ Админ панель"},
    "back":             {"uz":"🔙 Orqaga","ru":"🔙 Назад"},
    "prod":             {"uz":"Mahsulotni tanlang:","ru":"Выберите товар:"},
    "store":            {"uz":"Do'konni tanlang:","ru":"Выберите магазин:"},
    "no_store":         {"uz":"⚠️ Do'konlar yo'q.\nAdmin panel orqali qo'shing.","ru":"⚠️ Магазины не найдены.\nДобавьте через админ панель."},
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
    "top_pay_type":     {"uz":"💳 Tolov usulini tanlang:","ru":"💳 Способ оплаты:"},
    "naqd":             {"uz":"💵 Naqd","ru":"💵 Наличные"},
    "realizatsiya":     {"uz":"📝 Realizatsiya","ru":"📝 Реализация"},
    "top_naqd_sum":     {"uz":"💵 Naqd summani kiriting:\n(0 = to'liq realizatsiya)","ru":"💵 Сумма наличных:\n(0 = полностью в долг)"},
    "photo_scale":      {"uz":"📸 Tarozi rasmini yuboring\nYOKI ⌨️ og'irlikni kiriting (masalan: 3.455):","ru":"📸 Фото весов\nИЛИ ⌨️ введите вес (например: 3.455):"},
    "ocr_ok":           {"uz":"📸 Rasmdan o'qildi: {v} kg\nTo'g'ri? HA bosing yoki to'g'ri raqamni kiriting:","ru":"📸 Считано: {v} кг\nВерно? ДА или введите правильное:"},
    "ocr_fail":         {"uz":"❌ O'qib bo'lmadi. Og'irlikni kiriting:","ru":"❌ Не удалось. Введите вес:"},
    "reading":          {"uz":"⏳ Rasm o'qilmoqda...","ru":"⏳ Читаю изображение..."},
    "top_dokon_msg":    {"uz":"📦 MOL KELDI:\n{dist}\n{prod}: {qty} {unit}\nJami: {jami:,.0f}\nNaqd: {naqd:,.0f}\nQarz: {qarz:,.0f}\n\n✅ /tok_{id}\n❌ /trad_{id}","ru":"📦 ПОСТАВКА:\n{dist}\n{prod}: {qty} {unit}\nИтог: {jami:,.0f}\nНал: {naqd:,.0f}\nДолг: {qarz:,.0f}\n\n✅ /tok_{id}\n❌ /trad_{id}"},
    "top_ok_dist":      {"uz":"✅ {dokon} molni qabul qildi!","ru":"✅ {dokon} принял товар!"},
    "top_rad_dist":     {"uz":"❌ {dokon} molni rad etdi!","ru":"❌ {dokon} отклонил!"},
    "tolov_dist_msg":   {"uz":"💵 TOLOV:\n{dokon}\nSumma: {summa:,.0f}\n\n✅ /vok_{id}\n❌ /vrad_{id}","ru":"💵 ОПЛАТА:\n{dokon}\nСумма: {summa:,.0f}\n\n✅ /vok_{id}\n❌ /vrad_{id}"},
    "tolov_ok":         {"uz":"✅ Tolov tasdiqlandi!","ru":"✅ Оплата подтверждена!"},
    "tolov_rad":        {"uz":"❌ Tolov rad etildi!","ru":"❌ Оплата отклонена!"},
    "zakaz_dist_new":   {"uz":"📋 YANGI ZAKAZ:\nDo'kon: {dokon}\n{prod}: {qty} {unit}\n\n✅ /zqabul_{id}\n❌ /zrad_z_{id}","ru":"📋 НОВЫЙ ЗАКАЗ:\nМаг: {dokon}\n{prod}: {qty} {unit}\n\n✅ /zqabul_{id}\n❌ /zrad_z_{id}"},
    "zakaz_reminder":   {"uz":"⏰ Eslatma! Zakaz kutilmoqda:\n{dokon}: {prod} {qty}\nID: {id}","ru":"⏰ Напоминание!\n{dokon}: {prod} {qty}\nID: {id}"},
    "zakaz_timeout":    {"uz":"⚠️ Zakazingiz ({prod} {qty}) 2 soat ichida qabul qilinmadi.\n\nDist bilan bog'laning:\n📞 {phone}\n👤 {name}","ru":"⚠️ Ваш заказ ({prod} {qty}) не принят 2 часа.\n\n📞 {phone}\n👤 {name}"},
    "zakaz_comment_q":  {"uz":"📝 Izoh yozing (yoki Otkazib yuborish):","ru":"📝 Комментарий (или Пропустить):"},
    "zakaz_acc_dist":   {"uz":"✅ Zakaz qabul qilindi!\nIzoh: {izoh}","ru":"✅ Заказ принят!\nКомментарий: {izoh}"},
    "zakaz_rad_dist":   {"uz":"❌ Zakaz rad etildi.\nIzoh: {izoh}","ru":"❌ Заказ отклонён.\nКомментарий: {izoh}"},
    "narx_prod":        {"uz":"Mahsulotni tanlang:","ru":"Выберите товар:"},
    "narx_type":        {"uz":"Qaysi narx?","ru":"Какую цену?"},
    "narx_umumiy":      {"uz":"🔵 Barcha do'konlar","ru":"🔵 Для всех магазинов"},
    "narx_maxsus":      {"uz":"🟡 Bitta do'kon uchun","ru":"🟡 Для одного магазина"},
    "narx_val":         {"uz":"Yangi narx (masalan: 15000):","ru":"Новая цена (например: 15000):"},
    "tannarx_val":      {"uz":"Tannarx (masalan: 12000):","ru":"Себестоимость (например: 12000):"},
    "narx_updated":     {"uz":"✅ Narx yangilandi!","ru":"✅ Цена обновлена!"},
    "no_admin":         {"uz":"🚫 Siz admin emassiz!","ru":"🚫 Вы не администратор!"},
    "adm":              {"uz":"⚙️ Admin paneli:","ru":"⚙️ Админ панель:"},
    "adm_mahsulot":     {"uz":"➕ Mahsulot qo'shish","ru":"➕ Добавить товар"},
    "adm_price":        {"uz":"💰 Umumiy narxlar","ru":"💰 Общие цены"},
    "adm_add_store":    {"uz":"🏪 Do'kon qo'shish","ru":"🏪 Добавить магазин"},
    "adm_add_dist":     {"uz":"🚚 Distribyutor qo'shish","ru":"🚚 Добавить дистрибьютора"},
    "adm_stats":        {"uz":"📊 Statistika","ru":"📊 Статистика"},
    "adm_broadcast":    {"uz":"📢 Xabar yuborish","ru":"📢 Рассылка"},
    "adm_debtors":      {"uz":"💸 Qarzdorlar","ru":"💸 Должники"},
    "adm_list_stores":  {"uz":"🏪 Do'konlar","ru":"🏪 Магазины"},
    "adm_list_dists":   {"uz":"🚚 Distribyutorlar","ru":"🚚 Дистрибьюторы"},
    "adm_zavod_list":   {"uz":"📦 Zavod so'rovlari","ru":"📦 Запросы завода"},
    "broadcast_msg":    {"uz":"Xabar matnini kiriting:","ru":"Введите текст рассылки:"},
    "mahsulot_nom_uz":  {"uz":"Mahsulot nomini kiriting (o'zbekcha):","ru":"Название (по-узбекски):"},
    "mahsulot_nom_ru":  {"uz":"Mahsulot nomini kiriting (ruscha):","ru":"Название (по-русски):"},
    "mahsulot_unit":    {"uz":"Birligini tanlang:","ru":"Единица измерения:"},
    "mahsulot_ok":      {"uz":"✅ Mahsulot qo'shildi: {name}","ru":"✅ Товар добавлен: {name}"},
    "week":             {"uz":"Haftalik","ru":"Недельный"},
    "month":            {"uz":"Oylik","ru":"Месячный"},
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
    # Avtomatik eslatma
    "auto_zakaz_remind":{"uz":"📋 KECHA MOL BERILGAN DO'KONLAR:\n(Zakaz olish kerakmi?)\n\n{dokonlar}\n\nAgar zakaz bo'lsa, ular bilan bog'laning!","ru":"📋 МАГАЗИНЫ, КОТОРЫМ ВЧЕРА ПОСТАВИЛИ ТОВАР:\n(Нужно ли сделать заказ?)\n\n{dokonlar}\n\nСвяжитесь с ними если нужен заказ!"},
}

def tx(k, la="uz", **kw):
    t = T.get(k,{}).get(la,k)
    return t.format(**kw) if kw else t

def lg(ctx):     return ctx.user_data.get("lang","uz")
def is_adm(ctx): return ctx.user_data.get("is_admin", False)
def find_prod(name, la):
    return next((p for p in get_products() if p[la]==name), None)

def main_kb(la, sid="", admin=False):
    rows = [
        [tx("qabul",la), tx("buyurtma",la)],
        [tx("topshir",la), tx("tolov_qabul",la)],
        [tx("natija",la), tx("ombor",la)],
        [tx("marshrut",la), tx("hisobot",la)],
        [tx("my_stores",la), tx("my_prices",la)],
    ]
    if admin: rows.append([tx("admin",la)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def admin_kb(la):
    return ReplyKeyboardMarkup([
        [tx("adm_mahsulot",la), tx("adm_price",la)],
        [tx("adm_add_store",la), tx("adm_add_dist",la)],
        [tx("adm_stats",la), tx("adm_debtors",la)],
        [tx("adm_list_stores",la), tx("adm_list_dists",la)],
        [tx("adm_zavod_list",la), tx("adm_broadcast",la)],
        [tx("back",la)],
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

# ── START ─────────────────────────────────────────────────────────────────────
async def start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = upd.effective_user.id
    ctx.user_data["is_admin"] = uid in ADMIN_IDS
    user = get_user(uid)
    if user:
        la = user.get("Til","uz"); ctx.user_data["lang"] = la
        sid = user.get("Short_ID","ADMIN") if uid not in ADMIN_IDS else "ADMIN"
        if is_approved(uid):
            admin = uid in ADMIN_IDS
            await upd.message.reply_text(tx("main",la,sid=sid), reply_markup=main_kb(la,sid,admin), parse_mode="HTML")
            return MAIN_MENU
        elif is_rejected(uid):
            await upd.message.reply_text(tx("rejected_msg",la)+"\n\n"+tx("rejected_retry",la)); return REG_NAME
        else:
            await upd.message.reply_text(tx("wait_approve",la), reply_markup=wait_kb(la)); return WAIT_APPROVE
    if uid in ADMIN_IDS:
        la = ctx.user_data.get("lang","uz")
        await upd.message.reply_text(tx("main",la,sid="ADMIN"), reply_markup=main_kb(la,"ADMIN",True), parse_mode="HTML")
        return MAIN_MENU
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
        await ctx.bot.send_message(uid, tx("main",la,sid="ADMIN"), reply_markup=main_kb(la,"ADMIN",True), parse_mode="HTML")
        return MAIN_MENU
    user = get_user(uid)
    if user:
        la = user.get("Til",la); ctx.user_data["lang"]=la; sid=user.get("Short_ID","?")
        if is_approved(uid):
            await ctx.bot.send_message(uid,tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU
        elif is_rejected(uid):
            await ctx.bot.send_message(uid,tx("rejected_msg",la)+"\n\n"+tx("rejected_retry",la)); return REG_NAME
        else:
            await ctx.bot.send_message(uid,tx("wait_approve",la),reply_markup=wait_kb(la)); return WAIT_APPROVE
    await ctx.bot.send_message(uid, tx("reg_name",la), reply_markup=ReplyKeyboardMarkup([[]], resize_keyboard=True))
    return REG_NAME

# ── RO'YXAT ───────────────────────────────────────────────────────────────────
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
        reply_markup=ReplyKeyboardMarkup([[tx("skip",la)]],resize_keyboard=True)); return REG_PASSPORT

async def reg_passport(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id
    name=ctx.user_data.get("reg_name",""); fname=ctx.user_data.get("reg_fname","")
    phone=ctx.user_data.get("reg_phone",""); full_name=f"{name} {fname}".strip()
    passport="rasm_bor" if upd.message.photo else (upd.message.text or "otkazildi")
    sid=make_short_id()
    db_delete_row("Foydalanuvchilar","TG_ID",str(uid))
    db_append("Foydalanuvchilar",[str(uid),name,fname,phone,"distributor",la,passport,"kutilmoqda",sid,now_str()])
    for admin_id in ADMIN_IDS:
        try:
            await ctx.bot.send_message(admin_id,tx("reg_admin_msg",la,name=full_name,phone=phone,uid=str(uid),sid=sid),parse_mode="HTML")
            if upd.message.photo:
                await ctx.bot.send_photo(admin_id,upd.message.photo[-1].file_id,caption=f"Passport: {full_name}|{uid}")
        except Exception as e: logger.error(f"Admin notify: {e}")
    await upd.message.reply_text(tx("reg_ok",la,name=name,sid=sid),reply_markup=wait_kb(la),parse_mode="HTML"); return WAIT_APPROVE

async def wait_approve_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id; t=upd.message.text or ""
    if is_approved(uid):
        user=get_user(uid); sid=user.get("Short_ID","?") if user else "?"
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,False),parse_mode="HTML"); return MAIN_MENU
    if is_rejected(uid):
        await upd.message.reply_text(tx("rejected_msg",la)+"\n\n"+tx("rejected_retry",la)); return REG_NAME
    if t==tx("resend_btn",la):
        user=get_user(uid)
        if user:
            fn=f"{user.get('Ism','')} {user.get('Familiya','')}".strip()
            ph=user.get("Telefon",""); sid=user.get("Short_ID","?")
            for admin_id in ADMIN_IDS:
                try: await ctx.bot.send_message(admin_id,tx("reg_admin_msg",la,name=fn,phone=ph,uid=str(uid),sid=sid),parse_mode="HTML")
                except Exception: pass
        await upd.message.reply_text(tx("resent_ok",la),reply_markup=wait_kb(la)); return WAIT_APPROVE
    await upd.message.reply_text(tx("wait_approve",la),reply_markup=wait_kb(la)); return WAIT_APPROVE

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

# ── ZAVOD ─────────────────────────────────────────────────────────────────────
async def zavod_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); await upd.message.reply_text(tx("prod",la),reply_markup=prod_kb(la)); return ZAVOD_PROD

async def zavod_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if t==tx("back",la):
        sid=get_short_id(uid)
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,uid in ADMIN_IDS),parse_mode="HTML"); return MAIN_MENU
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
    sid=u.get("Short_ID","?") if u else "ADMIN"
    db_append("Qabul",[now_str(),str(uid),dn,p[la],qty,p["unit"],price,jami,"kutilmoqda",qid])
    for admin_id in ADMIN_IDS:
        try: await ctx.bot.send_message(admin_id,tx("zavod_req",la,dist=dn,sid=sid,prod=p[la],qty=qty,unit=p["unit"],narx=price,jami=jami,id=qid),parse_mode="HTML")
        except Exception: pass
    await upd.message.reply_text(tx("zavod_wait",la))
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,uid in ADMIN_IDS),parse_mode="HTML"); return MAIN_MENU

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
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,uid in ADMIN_IDS),parse_mode="HTML"); return MAIN_MENU
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
        stores=ctx.user_data.get("stores",[]); await upd.message.reply_text(tx("store",la),reply_markup=store_kb(stores,la)); return TOP_STORE
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
            ctx.user_data["_w"]=w; await upd.message.reply_text(tx("ocr_ok",la,v=w),reply_markup=yes_kb(la)); return TOP_PHOTO
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
    ctx.user_data["top_naqd"]=amount; await _save_top(upd,ctx); return TOP_STORE

async def _save_top(upd, ctx):
    la=lg(ctx); uid=upd.effective_user.id
    p=ctx.user_data["p"]; store=ctx.user_data["s"]
    qty=ctx.user_data["top_qty"]; naqd=ctx.user_data.get("top_naqd",0.0)
    pay_type=ctx.user_data.get("pay_type","naqd")
    store_id=str(store.get("ID","")); store_name=store.get("Nomi","")
    price,_=get_price(p["id"],dist_id=str(uid),dokon_id=store_id)
    jami=qty*price; qarz=max(0.0,jami-naqd); top_id=make_op_id("T")
    u=get_user(uid); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else str(uid)
    db_append("Topshirish",[now_str(),str(uid),store_name,store_id,p[la],qty,p["unit"],price,jami,pay_type,naqd,qarz,"tasdiqlangan",top_id])
    await upd.message.reply_text(
        f"✅ {store_name}\n{p[la]}: {qty} {p['unit']}\nJami: {jami:,.0f}\nNaqd: {naqd:,.0f}\nQarz: {qarz:,.0f}",
        reply_markup=store_kb(ctx.user_data.get("stores",[]),la))

async def tok_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/tok_(\w+)',upd.message.text or "")
    if not m: return
    db_update("Topshirish","Top_ID",m.group(1),"Status","tasdiqlangan")
    await upd.message.reply_text("✅ Topshirish tasdiqlandi!")

async def trad_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/trad_(\w+)',upd.message.text or "")
    if not m: return
    db_update("Topshirish","Top_ID",m.group(1),"Status","rad_etildi")
    await upd.message.reply_text("❌ Topshirish rad etildi!")

async def vok_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/vok_(\w+)',upd.message.text or "")
    if not m: return
    db_update("Tolov","Tolov_ID",m.group(1),"Status","tasdiqlangan")
    await upd.message.reply_text(tx("tolov_ok",lg(ctx)))

async def vrad_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m=re.search(r'/vrad_(\w+)',upd.message.text or "")
    if not m: return
    db_update("Tolov","Tolov_ID",m.group(1),"Status","rad_etildi")
    await upd.message.reply_text(tx("tolov_rad",lg(ctx)))

# ── ZAKAZ ─────────────────────────────────────────────────────────────────────
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
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,uid in ADMIN_IDS),parse_mode="HTML"); return MAIN_MENU

# ── DO'KON QO'SHISH (distribyutor tomonidan) ─────────────────────────────────
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
    lat,lng="",""
    if upd.message.location: lat=str(upd.message.location.latitude); lng=str(upd.message.location.longitude)
    name=ctx.user_data.get("di_name",""); addr=ctx.user_data.get("di_addr","")
    mchj=ctx.user_data.get("di_mchj",""); tel1=ctx.user_data.get("di_tel1","")
    tel2=ctx.user_data.get("di_tel2",""); photo=ctx.user_data.get("di_photo","")
    u=get_user(uid); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else str(uid)
    cnt=len(db_all("Dokonlar"))+1
    sid=""
    db_append("Dokonlar",[str(cnt),sid,name,addr,mchj,tel1,tel2,str(uid),dn,lat,lng,now_str()])
    if photo:
        for admin_id in ADMIN_IDS:
            try: await ctx.bot.send_photo(admin_id,photo,caption=f"Yangi do'kon: {name} | Dist: {dn}")
            except Exception: pass
    await upd.message.reply_text(tx("dokon_saved",la,name=name))
    sid2=get_short_id(uid)
    await upd.message.reply_text(tx("main",la,sid=sid2),reply_markup=main_kb(la,sid2,uid in ADMIN_IDS),parse_mode="HTML"); return MAIN_MENU

# ── NARX ──────────────────────────────────────────────────────────────────────
async def narx_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); await upd.message.reply_text(tx("narx_prod",la),reply_markup=prod_kb(la)); return NARX_PROD

async def narx_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if t==tx("back",la):
        sid=get_short_id(uid)
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,uid in ADMIN_IDS),parse_mode="HTML"); return MAIN_MENU
    p=find_prod(t,la)
    if not p: await upd.message.reply_text(tx("narx_prod",la),reply_markup=prod_kb(la)); return NARX_PROD
    ctx.user_data["p"]=p; price,cost=get_price(p["id"],dist_id=str(uid))
    await upd.message.reply_text(
        f"{t}\n💰 Joriy: {price:,.0f} / Tannarx: {cost:,.0f}\n\n{tx('narx_type',la)}",
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
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,uid in ADMIN_IDS),parse_mode="HTML"); return MAIN_MENU

async def narx_dokon_cost(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id
    cost=parse_money(upd.message.text); p=ctx.user_data["p"]; price=ctx.user_data["new_price"]
    store=ctx.user_data.get("narx_dokon",{})
    set_price(p["id"],p[la],price,cost,dist_id=str(uid),dokon_id=str(store.get("ID","")))
    await upd.message.reply_text(f"{tx('narx_updated',la)}\n{p[la]}\nDo'kon: {store.get('Nomi','')}\n{price:,.0f} / {cost:,.0f}")
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,uid in ADMIN_IDS),parse_mode="HTML"); return MAIN_MENU

# ── MAIN MENU ─────────────────────────────────────────────────────────────────
async def main_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if uid not in ADMIN_IDS and not is_approved(uid):
        if is_rejected(uid):
            await upd.message.reply_text(tx("rejected_msg",la)+"\n\n"+tx("rejected_retry",la)); return REG_NAME
        await upd.message.reply_text(tx("wait_approve",la),reply_markup=wait_kb(la)); return WAIT_APPROVE
    sid=get_short_id(uid)
    if t==tx("qabul",la):         return await zavod_start(upd,ctx)
    if t==tx("my_prices",la):     return await narx_start(upd,ctx)
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
    if t==tx("admin",la) and uid in ADMIN_IDS:
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
        lines.append(f"• {s.get('Nomi','')}{d}\n  📞 {s.get('Tel1','')}{loc}")
    if len(lines)==2: lines.append("Do'konlar yo'q")
    await upd.message.reply_text("\n".join(lines))

# ── HISOBOT ───────────────────────────────────────────────────────────────────
async def hisobot_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=str(upd.effective_user.id)
    if t==tx("back",la):
        sid=get_short_id(upd.effective_user.id)
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,upd.effective_user.id in ADMIN_IDS),parse_mode="HTML"); return MAIN_MENU
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
        msg=(f"📈 Hisobot: {period}\n---\n"
             f"📥 Qabul: {ti:,.0f}\n🚚 Sotuv: {ts:,.0f}\n"
             f"💵 Naqd: {tn:,.0f}\n📝 Davr qarzi: {tq:,.0f}\n"
             f"💸 Jami qarz: {jami_qarz:,.0f}\n💰 Foyda: {foyda:,.0f}" if la=="uz" else
             f"📈 Отчёт: {period}\n---\n"
             f"📥 Получено: {ti:,.0f}\n🚚 Продажи: {ts:,.0f}\n"
             f"💵 Наличные: {tn:,.0f}\n📝 Долг (период): {tq:,.0f}\n"
             f"💸 Общий долг: {jami_qarz:,.0f}\n💰 Прибыль: {foyda:,.0f}")
    except Exception as e: msg=f"Xatolik: {e}"
    sid=get_short_id(upd.effective_user.id)
    await upd.message.reply_text(msg,reply_markup=main_kb(la,sid,upd.effective_user.id in ADMIN_IDS)); return MAIN_MENU

async def daily(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=str(upd.effective_user.id); today=today_str()
    try:
        tops=[r for r in db_all("Topshirish") if str(r.get("Sana","")).startswith(today) and str(r.get("Dist_ID",""))==uid]
        ins =[r for r in db_all("Qabul") if str(r.get("Sana","")).startswith(today) and str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan"]
        ts=sum(float(r.get("Jami",0) or 0) for r in tops)
        tn=sum(float(r.get("Naqd",0) or 0) for r in tops)
        tq=sum(float(r.get("Qarz",0) or 0) for r in tops)
        ti=sum(float(r.get("Jami",0) or 0) for r in ins)
        foyda=ts-ti
        stores=get_stores(dist_id=uid)
        jami_qarz=sum(get_debt(str(s.get("ID",""))) for s in stores)
        dc=len(set(r.get("Dokon","") for r in tops))
        msg=(f"📊 Kunlik natija - {today}\n---\n"
             f"📥 Zavod: {ti:,.0f}\n🚚 Sotuv: {ts:,.0f}\n"
             f"💵 Naqd: {tn:,.0f}\n📝 Qarz: {tq:,.0f}\n"
             f"💸 Jami qarz: {jami_qarz:,.0f}\n💰 Bugungi foyda: {foyda:,.0f}\n🏪 Do'konlar: {dc}" if la=="uz" else
             f"📊 Итог дня - {today}\n---\n"
             f"📥 Завод: {ti:,.0f}\n🚚 Продажи: {ts:,.0f}\n"
             f"💵 Наличные: {tn:,.0f}\n📝 Долг: {tq:,.0f}\n"
             f"💸 Общий долг: {jami_qarz:,.0f}\n💰 Прибыль: {foyda:,.0f}\n🏪 Магазинов: {dc}")
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
    await upd.message.reply_text("\n".join(lines),reply_markup=main_kb(la,sid,uid in ADMIN_IDS))

# ── ADMIN ─────────────────────────────────────────────────────────────────────
async def admin_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if uid not in ADMIN_IDS: await upd.message.reply_text(tx("no_admin",la)); return MAIN_MENU
    if t==tx("back",la):
        sid=get_short_id(uid)
        await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,True),parse_mode="HTML"); return MAIN_MENU
    if t==tx("adm_mahsulot",la):
        await upd.message.reply_text(tx("mahsulot_nom_uz",la),reply_markup=back_kb(la)); return ADM_MAHSULOT_NOM
    if t==tx("adm_price",la):
        await upd.message.reply_text(tx("narx_prod",la),reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    if t==tx("adm_add_store",la):
        ctx.user_data["di_dist_id"]=str(uid)
        await upd.message.reply_text(tx("dokon_name",la),reply_markup=back_kb(la)); return DI_NAME
    if t==tx("adm_add_dist",la):
        await upd.message.reply_text("Distribyutor ismi:",reply_markup=back_kb(la)); return ADM_DIST_NAME
    if t==tx("adm_stats",la):       await a_stats(upd,ctx); return ADMIN_MENU
    if t==tx("adm_debtors",la):     await a_debtors(upd,ctx); return ADMIN_MENU
    if t==tx("adm_list_stores",la): await a_list_stores(upd,ctx); return ADMIN_MENU
    if t==tx("adm_list_dists",la):  await a_list_dists(upd,ctx); return ADMIN_MENU
    if t==tx("adm_zavod_list",la):  await a_zavod_list(upd,ctx); return ADMIN_MENU
    if t==tx("adm_broadcast",la):
        await upd.message.reply_text(tx("broadcast_msg",la),reply_markup=back_kb(la)); return ADM_BROADCAST
    await upd.message.reply_text(tx("adm",la),reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU

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
    recs=db_all("Mahsulotlar"); new_id=max([int(r.get("ID",0)) for r in recs],default=0)+1
    db_append("Mahsulotlar",[str(new_id),uz,ru,unit,"1",now_str()])
    await upd.message.reply_text(tx("mahsulot_ok",la,name=uz),reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU

async def adm_price_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    p=find_prod(t,la)
    if not p: await upd.message.reply_text(tx("narx_prod",la),reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    ctx.user_data["p"]=p; price,cost=get_price(p["id"])
    await upd.message.reply_text(f"{t}\n💰 Joriy: {price:,.0f} / {cost:,.0f}\n\n{tx('narx_val',la)}",reply_markup=back_kb(la)); return ADM_PRICE_VAL

async def adm_price_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADM_PRICE_PROD
    price=parse_money(t)
    if price<=0: await upd.message.reply_text(tx("err_money",la)); return ADM_PRICE_VAL
    ctx.user_data["np"]=price; await upd.message.reply_text(tx("tannarx_val",la)); return ADM_COST_VAL

async def adm_cost_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); cost=parse_money(upd.message.text); p=ctx.user_data["p"]; price=ctx.user_data["np"]
    set_price(p["id"],p[la],price,cost)
    await upd.message.reply_text(f"{tx('narx_updated',la)}\n{p[la]}: {price:,.0f} / {cost:,.0f}",reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU

async def adm_store_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["ns"]=t.strip(); await upd.message.reply_text("Manzil:",reply_markup=back_kb(la)); return ADM_STORE_ADDR

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
    u=get_user(dist_id); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else str(dist_id)
    cnt=len(db_all("Dokonlar"))+1
    db_append("Dokonlar",[str(cnt),"",store,addr,"","","",str(dist_id),dn,lat,lng,now_str()])
    await upd.message.reply_text(f"✅ Do'kon qo'shildi: {store}",reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU

async def adm_dist_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["nd"]=t.strip(); await upd.message.reply_text("Telegram ID:",reply_markup=back_kb(la)); return ADM_DIST_ID

async def adm_dist_id(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); name=ctx.user_data.get("nd",""); sid=make_short_id()
    db_append("Foydalanuvchilar",[upd.message.text.strip(),name,"","","distributor",la,"","tasdiqlangan",sid,now_str()])
    await upd.message.reply_text(f"✅ Qo'shildi: {name} | ID: {sid}",reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU

async def adm_broadcast(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): await upd.message.reply_text(tx("adm",la),reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU
    users=db_all("Foydalanuvchilar"); sent=0; failed=0
    for u in users:
        try: await ctx.bot.send_message(int(u.get("TG_ID",0)),t); sent+=1
        except Exception: failed+=1
    await upd.message.reply_text(f"✅ {sent} | Xato: {failed}",reply_markup=admin_kb(la),parse_mode="HTML"); return ADMIN_MENU

async def a_zavod_list(upd,ctx):
    recs=[r for r in db_all("Qabul") if r.get("Status","")=="kutilmoqda"]
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
        lines.append(f"• {s.get('Nomi','')}{d}\n  📍 {s.get('Adres','-')}\n  📞 {s.get('Tel1','')}\n  🚚 {s.get('Dist_Ism','')} ({s.get('Dist_ID','')}){loc}")
    text="\n".join(lines)
    for i in range(0,len(text),3500): await upd.message.reply_text(text[i:i+3500])

async def a_list_dists(upd,ctx):
    dists=[u for u in db_all("Foydalanuvchilar") if u.get("Rol","")=="distributor"]
    if not dists: await upd.message.reply_text("🚚 Distribyutorlar yo'q"); return
    lines=[f"🚚 Jami: {len(dists)}","---"]
    for u in dists:
        uid_d=u.get("TG_ID",""); sid=u.get("Short_ID","?")
        name=f"{u.get('Ism','')} {u.get('Familiya','')}".strip()
        stores=get_stores(dist_id=uid_d)
        jq=sum(get_debt(str(s.get("ID",""))) for s in stores)
        foyda=calc_foyda(str(uid_d))
        lines.append(f"• {name} (ID:{sid})\n  📞 {u.get('Telefon','')}\n  TG: {uid_d}\n  🏪 {len(stores)} do'kon\n  💸 Qarz: {jq:,.0f}\n  💰 Foyda: {foyda:,.0f}")
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
        dists=[u for u in users if u.get("Rol","")=="distributor"]
        total_foyda=sum(calc_foyda(str(u.get("TG_ID",""))) for u in dists)
        zakaz=len([r for r in db_all("Buyurtmalar") if r.get("Status","")=="Yangi"])
        msg=(f"📊 Umumiy statistika\n---\n"
             f"📥 Zavod: {ti:,.0f}\n🚚 Sotuv: {ts:,.0f}\n"
             f"💵 Naqd: {tn:,.0f}\n💸 Jami qarz: {jq:,.0f}\n"
             f"💰 Jami foyda: {total_foyda:,.0f}\n"
             f"🏪 Do'konlar: {len(stores)}\n🚚 Distribyutorlar: {len(dists)}\n📋 Yangi zakazlar: {zakaz}")
        await upd.message.reply_text(msg)
    except Exception as e: await upd.message.reply_text(f"Xatolik: {e}")

async def a_debtors(upd,ctx):
    stores=db_all("Dokonlar"); lines=["💸 Qarzdorlar:","---"]; total=0
    for s in stores:
        debt=get_debt(str(s.get("ID","")))
        if debt>0: lines.append(f"• {s.get('Nomi','')}: {debt:,.0f}\n  🚚 {s.get('Dist_Ism','')}"); total+=debt
    if len(lines)==2: lines.append("Qarz yo'q!")
    else: lines.append(f"---\nJami: {total:,.0f}")
    await upd.message.reply_text("\n".join(lines))

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
async def auto_zakaz_reminder(ctx: ContextTypes.DEFAULT_TYPE):
    """
    Har kuni kechqurun 20:00 — kecha mol berilgan do'konlarni
    distribyutorga yuborish (zakaz olish kerakligini eslatish)
    """
    try:
        kecha = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        tops = db_all("Topshirish")
        dokonlar_by_dist = {}
        for r in tops:
            if not str(r.get("Sana","")).startswith(kecha): continue
            if r.get("Status","") != "tasdiqlangan": continue
            dist_id = str(r.get("Dist_ID",""))
            if not dist_id or dist_id=="0": continue
            dokon_id = str(r.get("Dokon_ID",""))
            key = f"{dist_id}:{dokon_id}"
            if key not in dokonlar_by_dist:
                dokonlar_by_dist[key] = {
                    "dist_id": dist_id,
                    "dokon": r.get("Dokon",""),
                    "dokon_id": dokon_id,
                }
        # dist_id bo'yicha guruhlash
        dist_dokonlar = {}
        for key, val in dokonlar_by_dist.items():
            did = val["dist_id"]
            if did not in dist_dokonlar: dist_dokonlar[did] = []
            dist_dokonlar[did].append(val)

        # Har bir distribyutorga xabar
        for dist_id, dokonlar in dist_dokonlar.items():
            try:
                du = get_user(dist_id)
                if not du: continue
                la = du.get("Til","uz")
                lines = []
                for dk in dokonlar:
                    # Do'kon telefon raqamini olish
                    dokon_info = next((s for s in db_all("Dokonlar") if str(s.get("ID",""))==dk["dokon_id"]), None)
                    tel = dokon_info.get("Tel1","") if dokon_info else ""
                    tel2 = dokon_info.get("Tel2","") if dokon_info else ""
                    addr = dokon_info.get("Adres","") if dokon_info else ""
                    tel_str = f"📞 {tel}" if tel else ""
                    if tel2: tel_str += f" / {tel2}"
                    lines.append(f"• {dk['dokon']}\n  {tel_str}\n  📍 {addr}")
                dokonlar_text = "\n".join(lines)
                await ctx.bot.send_message(int(dist_id), tx("auto_zakaz_remind",la,dokonlar=dokonlar_text))
            except Exception as e:
                logger.error(f"auto_zakaz_remind dist {dist_id}: {e}")
    except Exception as e:
        logger.error(f"auto_zakaz_reminder: {e}")

async def debt_reminder(ctx: ContextTypes.DEFAULT_TYPE):
    """Har kuni 09:00 qarz eslatmasi"""
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
                for name,debt in debts: lines.append(f"• {name}: {debt:,.0f}")
                await ctx.bot.send_message(int(uid),"\n".join(lines))
            except Exception: pass
    except Exception as e: logger.error(f"debt_reminder: {e}")

async def cancel(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=upd.effective_user.id
    sid=get_short_id(uid)
    await upd.message.reply_text(tx("main",la,sid=sid),reply_markup=main_kb(la,sid,uid in ADMIN_IDS),parse_mode="HTML"); return MAIN_MENU

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN: print("BOT_TOKEN topilmadi!"); return
    app = Application.builder().token(BOT_TOKEN).build()
    # Har kuni 09:00 qarz eslatmasi
    app.job_queue.run_daily(debt_reminder, time=dtime(9,0))
    # Har kuni 20:00 zakaz eslatmasi (kecha mol berilgan do'konlar)
    app.job_queue.run_daily(auto_zakaz_reminder, time=dtime(20,0))

    txt      = filters.TEXT & ~filters.COMMAND
    photo_txt= (filters.PHOTO | filters.TEXT) & ~filters.COMMAND
    loc_txt  = (filters.LOCATION | filters.TEXT) & ~filters.COMMAND
    cont_txt = (filters.CONTACT | filters.TEXT) & ~filters.COMMAND

    conv = ConversationHandler(
        entry_points=[CommandHandler("start",start)],
        states={
            LANG_SELECT:        [CallbackQueryHandler(lang_cb,pattern="^lang_"),CommandHandler("start",start)],
            REG_NAME:           [MessageHandler(txt,reg_name)],
            REG_FNAME:          [MessageHandler(txt,reg_fname)],
            REG_PHONE:          [MessageHandler(cont_txt,reg_phone)],
            REG_PASSPORT:       [MessageHandler((filters.PHOTO|filters.TEXT)&~filters.COMMAND,reg_passport)],
            WAIT_APPROVE:       [MessageHandler(txt,wait_approve_h)],
            MAIN_MENU:          [MessageHandler(txt,main_h)],
            DIST_LINK_ID:       [MessageHandler(txt,lambda u,c: MAIN_MENU)],  # placeholder
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
        (r'^/zqabul_\w+$',  zqabul_cmd),
        (r'^/zrad_z_\w+$',  zrad_z_cmd),
    ]:
        app.add_handler(MessageHandler(filters.Regex(pattern),handler))
    app.add_handler(MessageHandler(filters.LOCATION,marshrut_loc))
    app.add_handler(conv)
    print("Bot ishga tushdi! v3.1")
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__":
    main()
