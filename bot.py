import os, logging, json, re, base64
from datetime import datetime, timedelta, time as dtime
import gspread
from google.oauth2.service_account import Credentials
import google.auth.transport.requests
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ConversationHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN         = os.environ.get("BOT_TOKEN", "")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON", "")
SPREADSHEET_ID    = os.environ.get("SPREADSHEET_ID", "")
ADMIN_IDS         = [int(x) for x in os.environ.get("ADMIN_IDS","0").split(",") if x.strip()]

PRODUCTS = [
    {"id":1,"uz":"Tvorog","ru":"Tvorog","unit":"kg"},
    {"id":2,"uz":"Sut","ru":"Sut","unit":"litr"},
    {"id":3,"uz":"Qatiq","ru":"Qatiq","unit":"kg"},
    {"id":4,"uz":"Brinza","ru":"Brinza","unit":"kg"},
    {"id":5,"uz":"Qaymoq 0.4 kg","ru":"Qaymoq 0.4 kg","unit":"dona"},
    {"id":6,"uz":"Qaymoq 0.2 kg","ru":"Qaymoq 0.2 kg","unit":"dona"},
    {"id":7,"uz":"Suzma 0.5 kg","ru":"Suzma 0.5 kg","unit":"kg"},
    {"id":8,"uz":"Qurt","ru":"Qurt","unit":"dona"},
    {"id":9,"uz":"Tosh qurt","ru":"Tosh qurt","unit":"dona"},
]

(
    LANG_SELECT,
    REG_NAME, REG_FNAME, REG_PHONE, REG_PASSPORT,
    WAIT_APPROVE,
    MAIN_MENU,
    QABUL_PROD, QABUL_QTY,
    TOPSHIR_STORE, TOPSHIR_PROD, TOPSHIR_PHOTO,
    TOLOV_STORE, TOLOV_AMOUNT, TOLOV_METHOD,
    BUYURTMA_STORE, BUYURTMA_PROD, BUYURTMA_QTY,
    MY_STORE_NAME, MY_STORE_ADDR, MY_STORE_MCHJ, MY_STORE_TEL1, MY_STORE_TEL2, MY_STORE_PHOTO, MY_STORE_LOC,
    HISOBOT_MENU,
    ADMIN_MENU,
    ADM_PRICE_PROD, ADM_PRICE_VAL, ADM_COST_VAL,
    ADM_STORE_NAME, ADM_STORE_MANZIL, ADM_STORE_DIST, ADM_STORE_LOC,
    ADM_DIST_NAME, ADM_DIST_ID,
    ADM_BROADCAST,
) = range(36)

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
def get_creds_dict():
    return json.loads(GOOGLE_CREDS_JSON) if GOOGLE_CREDS_JSON else {}

def get_sheet():
    if not GOOGLE_CREDS_JSON: return None
    try:
        creds = Credentials.from_service_account_info(
            get_creds_dict(),
            scopes=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        )
        return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)
    except Exception as e:
        logger.error(f"Sheet: {e}"); return None

SHEET_HEADERS = {
    "Foydalanuvchilar": ["TG_ID","Ism","Familiya","Telefon","Rol","Til","Passport","Status","Sana"],
    "Dokonlar":         ["ID","Nomi","Adres","MCHJ","Tel1","Tel2","Dist_ID","Dist_Ism","Lat","Lng","Sana"],
    "Narxlar":          ["Mahsulot_ID","Mahsulot","Narx","Tannarx","Sana"],
    "Qabul":            ["Sana","Dist_ID","Ism","Mahsulot","Miqdor","Birlik","Narx","Jami"],
    "Topshirish":       ["Sana","Dist_ID","Dokon","Mahsulot","Miqdor","Birlik","Narx","Jami"],
    "Tolov":            ["Sana","Dist_ID","Dokon","Summa","Usul","Izoh"],
    "Buyurtmalar":      ["Sana","Dist_ID","Dokon","Mahsulot","Miqdor","Status"],
}

def get_ws(name):
    wb = get_sheet()
    if not wb: return None
    try:
        return wb.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        w = wb.add_worksheet(name, rows=2000, cols=25)
        w.append_row(SHEET_HEADERS.get(name, ["Data"]))
        return w
    except Exception as e:
        logger.error(f"get_ws {name}: {e}"); return None

def db_append(tab, row):
    try:
        w = get_ws(tab)
        if w: w.append_row([str(x) for x in row])
    except Exception as e:
        logger.error(f"db_append {tab}: {e}")

def db_all(tab):
    try:
        w = get_ws(tab)
        return w.get_all_records() if w else []
    except Exception as e:
        logger.error(f"db_all {tab}: {e}"); return []

def db_update(tab, search_col, search_val, update_col, update_val):
    try:
        w = get_ws(tab)
        if not w: return
        headers = w.row_values(1)
        recs = w.get_all_records()
        for i, r in enumerate(recs):
            if str(r.get(search_col,"")).strip() == str(search_val).strip():
                col_idx = headers.index(update_col) + 1
                w.update_cell(i+2, col_idx, str(update_val))
                return
    except Exception as e:
        logger.error(f"db_update {tab}: {e}")

def get_user(uid):
    try:
        for r in db_all("Foydalanuvchilar"):
            if str(r.get("TG_ID","")).strip() == str(uid).strip():
                return r
        return None
    except Exception:
        return None

def is_registered(uid):
    return get_user(uid) is not None

def is_approved(uid):
    if int(uid) in ADMIN_IDS:
        return True
    u = get_user(uid)
    if not u: return False
    return str(u.get("Status","")).strip().lower() in ["tasdiqlangan","approved","ok","ha","yes","1"]

def get_price(pid):
    try:
        for r in db_all("Narxlar"):
            if str(r.get("Mahsulot_ID","")).strip() == str(pid).strip():
                return float(r.get("Narx",0) or 0), float(r.get("Tannarx",0) or 0)
    except Exception:
        pass
    return 0.0, 0.0

def set_price(pid, pname, price, cost):
    try:
        w = get_ws("Narxlar")
        if not w: return
        recs = w.get_all_records()
        now = now_str()
        for i, r in enumerate(recs):
            if str(r.get("Mahsulot_ID","")).strip() == str(pid).strip():
                w.update(f"A{i+2}:E{i+2}", [[str(pid), pname, str(price), str(cost), now]])
                return
        w.append_row([str(pid), pname, str(price), str(cost), now])
    except Exception as e:
        logger.error(f"set_price: {e}")

def get_stores(dist_id=None):
    try:
        recs = db_all("Dokonlar")
        if dist_id is None:
            return recs
        result = [r for r in recs if str(r.get("Dist_ID","")).strip() == str(dist_id).strip()]
        logger.info(f"get_stores({dist_id}): {len(result)}/{len(recs)}")
        return result
    except Exception as e:
        logger.error(f"get_stores: {e}"); return []

def get_debt(store_name):
    try:
        sold = sum(float(r.get("Jami",0) or 0)
                   for r in db_all("Topshirish") if r.get("Dokon","").strip()==store_name.strip())
        paid = sum(float(r.get("Summa",0) or 0)
                   for r in db_all("Tolov")
                   if r.get("Dokon","").strip()==store_name.strip() and r.get("Usul","")=="Naqd")
        return max(0.0, sold - paid)
    except Exception:
        return 0.0

def now_str():   return datetime.now().strftime("%Y-%m-%d %H:%M")
def today_str(): return datetime.now().strftime("%Y-%m-%d")

# ── RAQAM PARSING ─────────────────────────────────────────────────────────────
def parse_weight(text: str) -> float:
    """
    Og'irlik uchun: faqat nuqta decimal ajratuvchi sifatida.
    3.455 → 3.455 kg
    3,455 → 3.455 kg (vergul ham decimal)
    HECH QACHON 3455 ga aylantirmasin!
    """
    t = text.strip().replace(" ","")
    # Vergulni nuqtaga o'zgartir
    t = t.replace(",",".")
    try:
        val = float(t)
        # Agar 100 dan katta bo'lsa — gramm deb qabul qil
        if val >= 100:
            return round(val / 1000, 3)
        return round(val, 3)
    except Exception:
        return 0.0

def parse_money(text: str) -> float:
    """
    Pul uchun: 15000, 15,000, 15.000 → 15000
    """
    t = text.strip().replace(" ","")
    # Minglik ajratuvchi: X,000 yoki X.000 (3 ta nol)
    if re.match(r'^\d+[.,]\d{3}$', t):
        t = re.sub(r'[.,]', '', t)
        return float(t)
    # Decimal: 15000.50
    t = t.replace(",",".")
    try:
        return float(t)
    except Exception:
        return 0.0

def parse_qty(text: str) -> float:
    """Miqdor uchun — parse_weight bilan bir xil"""
    return parse_weight(text)

def clean_phone(text: str) -> str:
    """Faqat raqamlarni qoldiradi: +998901234567 → 998901234567"""
    return re.sub(r'[^\d+]', '', text.strip())

# ── GOOGLE VISION OCR ─────────────────────────────────────────────────────────
async def vision_ocr(image_bytes: bytes) -> str:
    try:
        import httpx
        creds = Credentials.from_service_account_info(
            get_creds_dict(),
            scopes=["https://www.googleapis.com/auth/cloud-vision"]
        )
        creds.refresh(google.auth.transport.requests.Request())
        b64 = base64.b64encode(image_bytes).decode()
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://vision.googleapis.com/v1/images:annotate",
                headers={"Authorization": f"Bearer {creds.token}"},
                json={"requests":[{"image":{"content":b64},"features":[{"type":"TEXT_DETECTION"}]}]}
            )
            data = resp.json()
            text = data["responses"][0].get("fullTextAnnotation",{}).get("text","").strip()
            logger.info(f"OCR raw: {repr(text)}")
            return text
    except Exception as e:
        logger.error(f"OCR: {e}"); return ""

def parse_scale_weight(text: str) -> float:
    """
    Tarozi: CHAP=og'irlik(gramm) | O'RTA=narx | O'NG=jami
    Birinchi raqam = gramm → kg ga o'girish
    3455 → 3.455 kg
    """
    if not text: return 0.0
    nums = re.findall(r'\d+', text)
    if not nums: return 0.0
    logger.info(f"Scale nums: {nums}")
    first = nums[0]
    try:
        val = int(first)
        if val >= 100:
            return round(val / 1000, 3)
        return float(val)
    except Exception:
        pass
    dec = re.findall(r'\d+[.,]\d+', text)
    if dec:
        try:
            return float(dec[0].replace(",","."))
        except Exception:
            pass
    return 0.0

# ── MATNLAR ───────────────────────────────────────────────────────────────────
T = {
    "start":          {"uz":"Tilni tanlang:","ru":"Выберите язык:"},
    "reg_name":       {"uz":"Ismingizni kiriting:","ru":"Введите имя:"},
    "reg_fname":      {"uz":"Familiyangizni kiriting:","ru":"Введите фамилию:"},
    "reg_phone":      {"uz":"Telefon raqamingizni yuboring (faqat raqamlar):","ru":"Отправьте номер телефона (только цифры):"},
    "reg_passport":   {"uz":"Passport rasmini yuboring (2 betini birga):","ru":"Фото паспорта (оба разворота):"},
    "reg_ok":         {"uz":"Royxatdan otdingiz {name}!\nAdmin tasdiqlashini kuting...","ru":"Вы зарегистрированы {name}!\nОжидайте подтверждения..."},
    "wait_approve":   {"uz":"Hisobingiz hali tasdiqlanmagan.\nAdmin tasdiqlashini kuting.\n\nQayta yuborish uchun tugmani bosing:","ru":"Аккаунт не подтверждён.\nОжидайте подтверждения.\n\nДля повторной отправки нажмите кнопку:"},
    "resend_btn":     {"uz":"Maʼlumotlarni qayta yuborish","ru":"Повторно отправить данные"},
    "resent_ok":      {"uz":"Maʼlumotlar adminga yuborildi. Kuting.","ru":"Данные отправлены администратору. Ожидайте."},
    "reg_admin_msg":  {"uz":"YANGI DISTRIBYUTOR:\nIsm: {name}\nTel: {phone}\nID: {uid}\n\nTasdiqlash: /approve_{uid}\nRad etish: /reject_{uid}","ru":"НОВЫЙ ДИСТРИБЬЮТОР:\nИмя: {name}\nТел: {phone}\nID: {uid}\n\nПодтвердить: /approve_{uid}\nОтклонить: /reject_{uid}"},
    "approved_msg":   {"uz":"Hisobingiz tasdiqlandi! Botdan foydalanishingiz mumkin.","ru":"Ваш аккаунт подтверждён! Можете пользоваться ботом."},
    "rejected_msg":   {"uz":"Hisobingiz rad etildi. Admin bilan bogʻlaning.","ru":"Ваш аккаунт отклонён. Свяжитесь с администратором."},
    "main":           {"uz":"Asosiy menyu:","ru":"Главное меню:"},
    "qabul":          {"uz":"Zavoddan qabul","ru":"Получить с завода"},
    "buyurtma":       {"uz":"Buyurtmalar","ru":"Заказы"},
    "topshir":        {"uz":"Mahsulot topshirish","ru":"Передать товар"},
    "tolov":          {"uz":"Tolov","ru":"Оплата"},
    "natija":         {"uz":"Kunlik natija","ru":"Итог дня"},
    "ombor":          {"uz":"Ombor","ru":"Склад"},
    "marshrut":       {"uz":"Marshrut","ru":"Маршрут"},
    "hisobot":        {"uz":"Hisobot","ru":"Отчёт"},
    "my_stores":      {"uz":"Mening dokonlarim","ru":"Мои магазины"},
    "admin":          {"uz":"Admin panel","ru":"Админ панель"},
    "back":           {"uz":"Orqaga","ru":"Назад"},
    "naqd":           {"uz":"Naqd","ru":"Наличные"},
    "qarz_btn":       {"uz":"Qarz","ru":"Долг"},
    "prod":           {"uz":"Mahsulotni tanlang:","ru":"Выберите товар:"},
    "store":          {"uz":"Dokonni tanlang:","ru":"Выберите магазин:"},
    "no_store":       {"uz":"Dokonlar topilmadi.\nQoshish: Mening dokonlarim","ru":"Магазины не найдены.\nДобавить: Мои магазины"},
    "sum":            {"uz":"Summa kiriting (masalan: 15000):","ru":"Введите сумму (например: 15000):"},
    "qty":            {"uz":"Miqdorni kiriting (masalan: 5 yoki 5.5):","ru":"Введите количество (например: 5 или 5.5):"},
    "pay":            {"uz":"Tolov usuli:","ru":"Способ оплаты:"},
    "ok":             {"uz":"Saqlandi!","ru":"Сохранено!"},
    "err_num":        {"uz":"Raqam kiriting!\nMasalan: 5 yoki 5.5","ru":"Введите число!\nНапример: 5 или 5.5"},
    "err_weight":     {"uz":"Ogirlikni kiriting!\nMasalan: 3.455 yoki 1.2","ru":"Введите вес!\nНапример: 3.455 или 1.2"},
    "err_money":      {"uz":"Summani kiriting!\nMasalan: 15000","ru":"Введите сумму!\nНапример: 15000"},
    "err_phone":      {"uz":"Faqat raqam kiriting!\nMasalan: 998901234567","ru":"Только цифры!\nНапример: 998901234567"},
    "no_admin":       {"uz":"Siz admin emassiz!","ru":"Вы не администратор!"},
    "adm":            {"uz":"Admin paneli:","ru":"Админ панель:"},
    "price_btn":      {"uz":"Narx ozgartirish","ru":"Изменить цены"},
    "add_store_adm":  {"uz":"Dokon qoshish (Admin)","ru":"Добавить магазин"},
    "add_dist":       {"uz":"Distribyutor qoshish","ru":"Добавить дистрибьютора"},
    "stats":          {"uz":"Statistika","ru":"Статистика"},
    "broadcast":      {"uz":"Hammaga xabar","ru":"Рассылка"},
    "debtors":        {"uz":"Qarzdorlar","ru":"Должники"},
    "list_stores":    {"uz":"Dokonlar royxati","ru":"Список магазинов"},
    "list_dists":     {"uz":"Distribyutorlar","ru":"Дистрибьюторы"},
    "new_price":      {"uz":"Yangi narx (masalan: 15000):","ru":"Новая цена (например: 15000):"},
    "tannarx":        {"uz":"Tannarx (masalan: 12000):","ru":"Себестоимость (например: 12000):"},
    "week":           {"uz":"Haftalik","ru":"Недельный"},
    "month":          {"uz":"Oylik","ru":"Месячный"},
    "send_loc":       {"uz":"Lokatsiyangizni yuboring:","ru":"Отправьте геолокацию:"},
    "loc_btn":        {"uz":"Lokatsiyani yuborish","ru":"Отправить геолокацию"},
    "skip":           {"uz":"Otkazib yuborish","ru":"Пропустить"},
    "broadcast_msg":  {"uz":"Xabar matnini kiriting:","ru":"Введите текст рассылки:"},
    "photo_scale":    {"uz":"Tarozi rasmini yuboring\nYOKI ogirlikni kiriting (masalan: 3.455):","ru":"Фото весов\nИЛИ введите вес (например: 3.455):"},
    "ocr_weight":     {"uz":"Rasmdan oqildi: {v} kg\nTogri? HA bosing yoki togri raqamni kiriting:","ru":"Считано: {v} кг\nВерно? ДА или введите правильное число:"},
    "ocr_fail":       {"uz":"Rasmdan oqib bolmadi.\nOgirlikni kiriting (masalan: 3.455):","ru":"Не удалось считать.\nВведите вес (например: 3.455):"},
    "reading":        {"uz":"Rasm oqilmoqda...","ru":"Читаю изображение..."},
    "ms_name":        {"uz":"Dokon nomini kiriting:","ru":"Название магазина:"},
    "ms_addr":        {"uz":"Dokon manzilini kiriting:","ru":"Адрес магазина:"},
    "ms_mchj":        {"uz":"MCHJ nomini kiriting\n(yoki Otkazib yuborish):","ru":"Название ООО\n(или Пропустить):"},
    "ms_tel1":        {"uz":"Dokon telefon 1 (faqat raqamlar):","ru":"Телефон магазина 1 (только цифры):"},
    "ms_tel2":        {"uz":"Telefon 2\n(yoki Otkazib yuborish):","ru":"Телефон 2\n(или Пропустить):"},
    "ms_photo":       {"uz":"Dokon tashqaridan rasmini yuboring\n(yoki Otkazib yuborish):","ru":"Фото магазина снаружи\n(или Пропустить):"},
    "ms_loc":         {"uz":"Dokon lokatsiyasini yuboring\n(yoki Otkazib yuborish):","ru":"Локация магазина\n(или Пропустить):"},
    "store_added":    {"uz":"Dokon qoshildi: {name}","ru":"Магазин добавлен: {name}"},
    "new_store_admin":{"uz":"YANGI DOKON:\nNomi: {name}\nAdres: {addr}\nDist: {dist}\nID: {uid}","ru":"НОВЫЙ МАГАЗИН:\nНазвание: {name}\nАдрес: {addr}\nДист: {dist}\nID: {uid}"},
    "adm_sname":      {"uz":"Dokon nomi:","ru":"Название магазина:"},
    "adm_smanzil":    {"uz":"Manzil:","ru":"Адрес:"},
    "adm_sdist":      {"uz":"Distribyutor Telegram ID:","ru":"Telegram ID дистрибьютора:"},
    "adm_sloc":       {"uz":"Lokatsiya (yoki Otkazib yuborish):","ru":"Локация (или Пропустить):"},
    "phone_btn":      {"uz":"Telefon raqamni yuborish","ru":"Отправить номер телефона"},
}

def tx(k, la="uz", **kw):
    t = T.get(k,{}).get(la, k)
    return t.format(**kw) if kw else t

def lg(ctx):     return ctx.user_data.get("lang","uz")
def is_adm(ctx): return ctx.user_data.get("is_admin", False)
def uname(upd):
    u = upd.effective_user
    return u.full_name or u.username or str(u.id)

def find_prod(name, la):
    return next((p for p in PRODUCTS if p[la]==name), None)

def main_kb(la, admin=False):
    rows = [
        [tx("qabul",la),    tx("buyurtma",la)],
        [tx("topshir",la),  tx("tolov",la)],
        [tx("natija",la),   tx("ombor",la)],
        [tx("marshrut",la), tx("hisobot",la)],
        [tx("my_stores",la)],
    ]
    if admin: rows.append([tx("admin",la)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def prod_kb(la):
    rows = []
    for i in range(0, len(PRODUCTS), 2):
        r = [PRODUCTS[i][la]]
        if i+1 < len(PRODUCTS): r.append(PRODUCTS[i+1][la])
        rows.append(r)
    rows.append([tx("back",la)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def store_kb(stores, la):
    rows = [[s.get("Nomi","")] for s in stores if s.get("Nomi","")]
    rows.append([tx("back",la)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def back_kb(la):
    return ReplyKeyboardMarkup([[tx("back",la)]], resize_keyboard=True)

def skip_kb(la):
    return ReplyKeyboardMarkup([[tx("skip",la)],[tx("back",la)]], resize_keyboard=True)

def loc_kb(la):
    btn = KeyboardButton(tx("loc_btn",la), request_location=True)
    return ReplyKeyboardMarkup([[btn],[tx("skip",la)],[tx("back",la)]], resize_keyboard=True)

def phone_kb(la):
    btn = KeyboardButton(tx("phone_btn",la), request_contact=True)
    return ReplyKeyboardMarkup([[btn]], resize_keyboard=True)

def yes_kb(la):
    return ReplyKeyboardMarkup([["HA" if la=="uz" else "ДА", tx("back",la)]], resize_keyboard=True)

def wait_kb(la):
    return ReplyKeyboardMarkup([[tx("resend_btn",la)]], resize_keyboard=True)

# ── START / LANG ──────────────────────────────────────────────────────────────
async def start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("O'zbek", callback_data="lang_uz"),
        InlineKeyboardButton("Русский", callback_data="lang_ru"),
    ]])
    await upd.message.reply_text(tx("start"), reply_markup=kb)
    return LANG_SELECT

async def lang_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = upd.callback_query; await q.answer()
    la = q.data.replace("lang_","")
    ctx.user_data["lang"] = la
    uid = upd.effective_user.id
    ctx.user_data["is_admin"] = uid in ADMIN_IDS
    await q.edit_message_text("Til tanlandi!" if la=="uz" else "Язык выбран!")

    if uid in ADMIN_IDS:
        await ctx.bot.send_message(uid, tx("main",la), reply_markup=main_kb(la, True))
        return MAIN_MENU

    user = get_user(uid)
    if user:
        if is_approved(uid):
            name = f"{user.get('Ism','')} {user.get('Familiya','')}".strip()
            await ctx.bot.send_message(uid,
                f"Xush kelibsiz, {name}!" if la=="uz" else f"Добро пожаловать, {name}!",
                reply_markup=main_kb(la, False))
            return MAIN_MENU
        else:
            await ctx.bot.send_message(uid, tx("wait_approve",la), reply_markup=wait_kb(la))
            return WAIT_APPROVE

    await ctx.bot.send_message(uid, tx("reg_name",la))
    return REG_NAME

# ── RO'YXATDAN O'TISH ─────────────────────────────────────────────────────────
async def reg_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["reg_name"] = upd.message.text.strip()
    la = lg(ctx)
    await upd.message.reply_text(tx("reg_fname",la))
    return REG_FNAME

async def reg_fname(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["reg_fname"] = upd.message.text.strip()
    la = lg(ctx)
    await upd.message.reply_text(tx("reg_phone",la), reply_markup=phone_kb(la))
    return REG_PHONE

async def reg_phone(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    if upd.message.contact:
        phone = upd.message.contact.phone_number
    else:
        phone = clean_phone(upd.message.text)
        if not phone.replace("+","").isdigit() or len(phone) < 9:
            await upd.message.reply_text(tx("err_phone",la), reply_markup=phone_kb(la))
            return REG_PHONE
    ctx.user_data["reg_phone"] = phone
    await upd.message.reply_text(tx("reg_passport",la),
        reply_markup=ReplyKeyboardMarkup([[tx("skip",la)]], resize_keyboard=True))
    return REG_PASSPORT

async def reg_passport(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    uid = upd.effective_user.id
    name  = ctx.user_data.get("reg_name","")
    fname = ctx.user_data.get("reg_fname","")
    phone = ctx.user_data.get("reg_phone","")
    full_name = f"{name} {fname}".strip()

    passport_info = "otkazildi"
    if upd.message.photo:
        passport_info = "rasm_bor"
    elif upd.message.text and upd.message.text != tx("skip",la):
        passport_info = upd.message.text

    db_append("Foydalanuvchilar",[
        str(uid), name, fname, phone,
        "distributor", la, passport_info, "kutilmoqda", now_str()
    ])

    # Adminlarga xabar
    await _notify_admins_new_dist(ctx, uid, full_name, phone, la, upd.message)

    await upd.message.reply_text(tx("reg_ok", la, name=name), reply_markup=wait_kb(la))
    return WAIT_APPROVE

async def _notify_admins_new_dist(ctx, uid, full_name, phone, la, message=None):
    for admin_id in ADMIN_IDS:
        try:
            msg = tx("reg_admin_msg", la, name=full_name, phone=phone, uid=str(uid))
            await ctx.bot.send_message(admin_id, msg)
            if message and message.photo:
                await ctx.bot.send_photo(admin_id, message.photo[-1].file_id,
                    caption=f"Passport: {full_name} | {phone} | ID:{uid}")
        except Exception as e:
            logger.error(f"Admin notify: {e}")

async def wait_approve_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Tasdiqlash kutish holatida"""
    la = lg(ctx)
    uid = upd.effective_user.id
    t = upd.message.text or ""

    if is_approved(uid):
        user = get_user(uid)
        name = f"{user.get('Ism','')}".strip() if user else ""
        await upd.message.reply_text(
            f"Xush kelibsiz, {name}!" if la=="uz" else f"Добро пожаловать, {name}!",
            reply_markup=main_kb(la, False))
        return MAIN_MENU

    if t == tx("resend_btn",la):
        user = get_user(uid)
        if user:
            full_name = f"{user.get('Ism','')} {user.get('Familiya','')}".strip()
            phone = user.get("Telefon","")
            await _notify_admins_new_dist(ctx, uid, full_name, phone, la)
        await upd.message.reply_text(tx("resent_ok",la), reply_markup=wait_kb(la))
        return WAIT_APPROVE

    await upd.message.reply_text(tx("wait_approve",la), reply_markup=wait_kb(la))
    return WAIT_APPROVE

# ── ADMIN APPROVE / REJECT COMMANDS ───────────────────────────────────────────
async def approve_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid_admin = upd.effective_user.id
    if uid_admin not in ADMIN_IDS: return
    text = upd.message.text or ""
    m = re.search(r'/approve_(\d+)', text)
    if not m: return
    target_uid = m.group(1)
    db_update("Foydalanuvchilar", "TG_ID", target_uid, "Status", "tasdiqlangan")
    await upd.message.reply_text(f"Tasdiqlandi: {target_uid}")
    try:
        user = get_user(target_uid)
        la = user.get("Til","uz") if user else "uz"
        await ctx.bot.send_message(int(target_uid), tx("approved_msg",la),
            reply_markup=main_kb(la, False))
    except Exception as e:
        logger.error(f"approve notify: {e}")

async def reject_cmd(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid_admin = upd.effective_user.id
    if uid_admin not in ADMIN_IDS: return
    text = upd.message.text or ""
    m = re.search(r'/reject_(\d+)', text)
    if not m: return
    target_uid = m.group(1)
    db_update("Foydalanuvchilar", "TG_ID", target_uid, "Status", "rad_etildi")
    await upd.message.reply_text(f"Rad etildi: {target_uid}")
    try:
        user = get_user(target_uid)
        la = user.get("Til","uz") if user else "uz"
        await ctx.bot.send_message(int(target_uid), tx("rejected_msg",la))
    except Exception as e:
        logger.error(f"reject notify: {e}")

# ── MAIN MENU ─────────────────────────────────────────────────────────────────
async def main_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text; uid = upd.effective_user.id

    # Tasdiqlangan bo'lmasa
    if not is_approved(uid):
        await upd.message.reply_text(tx("wait_approve",la), reply_markup=wait_kb(la))
        return WAIT_APPROVE

    if t == tx("qabul",la):
        await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la))
        return QABUL_PROD
    if t == tx("topshir",la):
        stores = get_stores(uid)
        if not stores: await upd.message.reply_text(tx("no_store",la)); return MAIN_MENU
        ctx.user_data["stores"] = stores
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return TOPSHIR_STORE
    if t == tx("tolov",la):
        stores = get_stores(uid)
        if not stores: await upd.message.reply_text(tx("no_store",la)); return MAIN_MENU
        ctx.user_data["stores"] = stores
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return TOLOV_STORE
    if t == tx("buyurtma",la):
        stores = get_stores(uid)
        if not stores: await upd.message.reply_text(tx("no_store",la)); return MAIN_MENU
        ctx.user_data["stores"] = stores
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return BUYURTMA_STORE
    if t == tx("natija",la):   await daily(upd, ctx); return MAIN_MENU
    if t == tx("ombor",la):    await stock(upd, ctx); return MAIN_MENU
    if t == tx("marshrut",la): await marshrut_start(upd, ctx); return MAIN_MENU
    if t == tx("hisobot",la):
        await upd.message.reply_text("Hisobot:" if la=="uz" else "Отчёт:",
            reply_markup=ReplyKeyboardMarkup([[tx("week",la),tx("month",la)],[tx("back",la)]], resize_keyboard=True))
        return HISOBOT_MENU
    if t == tx("my_stores",la):
        await my_stores_show(upd, ctx); return MY_STORE_NAME
    if t == tx("admin",la) and is_adm(ctx):
        await upd.message.reply_text(tx("adm",la), reply_markup=ReplyKeyboardMarkup([
            [tx("price_btn",la),   tx("add_store_adm",la)],
            [tx("add_dist",la),    tx("stats",la)],
            [tx("debtors",la),     tx("broadcast",la)],
            [tx("list_stores",la), tx("list_dists",la)],
            [tx("back",la)],
        ], resize_keyboard=True))
        return ADMIN_MENU
    await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la, is_adm(ctx)))
    return MAIN_MENU

# ── MENING DO'KONLARIM ────────────────────────────────────────────────────────
async def my_stores_show(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); uid = upd.effective_user.id
    stores = get_stores(uid)
    lines = ["Mening dokonlarim:" if la=="uz" else "Мои магазины:", "---"]
    for s in stores:
        debt = get_debt(s.get("Nomi",""))
        d = f" | Qarz: {debt:,.0f}" if debt>0 else ""
        lat = s.get("Lat",""); lng = s.get("Lng","")
        loc_txt = f"\n  📍 {lat}, {lng}" if lat and lng else ""
        lines.append(f"• {s.get('Nomi','')}{d}\n  📞 {s.get('Tel1','')}{loc_txt}")
    lines.append("\n" + ("Yangi dokon qoshish uchun nomini kiriting:" if la=="uz"
                         else "Для добавления введите название:"))
    await upd.message.reply_text("\n".join(lines), reply_markup=back_kb(la))

async def my_store_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx)))
        return MAIN_MENU
    ctx.user_data["ns"] = t.strip()
    await upd.message.reply_text(tx("ms_addr",la), reply_markup=back_kb(la))
    return MY_STORE_ADDR

async def my_store_addr(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("ms_name",la), reply_markup=back_kb(la))
        return MY_STORE_NAME
    ctx.user_data["ns_addr"] = t.strip()
    await upd.message.reply_text(tx("ms_mchj",la), reply_markup=skip_kb(la))
    return MY_STORE_MCHJ

async def my_store_mchj(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    ctx.user_data["ns_mchj"] = "" if t==tx("skip",la) else t.strip()
    await upd.message.reply_text(tx("ms_tel1",la), reply_markup=phone_kb(la))
    return MY_STORE_TEL1

async def my_store_tel1(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    if upd.message.contact:
        phone = upd.message.contact.phone_number
    else:
        phone = clean_phone(upd.message.text or "")
        if not phone.replace("+","").isdigit() or len(phone) < 7:
            await upd.message.reply_text(tx("err_phone",la), reply_markup=phone_kb(la))
            return MY_STORE_TEL1
    ctx.user_data["ns_tel1"] = phone
    await upd.message.reply_text(tx("ms_tel2",la), reply_markup=skip_kb(la))
    return MY_STORE_TEL2

async def my_store_tel2(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text or ""
    if t == tx("skip",la):
        ctx.user_data["ns_tel2"] = ""
    else:
        phone = clean_phone(t)
        ctx.user_data["ns_tel2"] = phone
    await upd.message.reply_text(tx("ms_photo",la), reply_markup=skip_kb(la))
    return MY_STORE_PHOTO

async def my_store_photo(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    ctx.user_data["ns_photo_id"] = ""
    if upd.message.photo:
        ctx.user_data["ns_photo_id"] = upd.message.photo[-1].file_id
    await upd.message.reply_text(tx("ms_loc",la), reply_markup=loc_kb(la))
    return MY_STORE_LOC

async def my_store_loc(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); uid = upd.effective_user.id
    store    = ctx.user_data.get("ns","")
    addr     = ctx.user_data.get("ns_addr","")
    mchj     = ctx.user_data.get("ns_mchj","")
    tel1     = ctx.user_data.get("ns_tel1","")
    tel2     = ctx.user_data.get("ns_tel2","")
    photo_id = ctx.user_data.get("ns_photo_id","")
    lat, lng = "", ""

    if upd.message.location:
        lat = str(upd.message.location.latitude)
        lng = str(upd.message.location.longitude)

    # Distribyutor ismini olish
    user = get_user(uid)
    dist_name = f"{user.get('Ism','')} {user.get('Familiya','')}".strip() if user else str(uid)

    cnt = len(db_all("Dokonlar")) + 1
    row = [str(cnt), store, addr, mchj, tel1, tel2, str(uid), dist_name, lat, lng, now_str()]
    db_append("Dokonlar", row)
    logger.info(f"Store saved: {row}")

    # Adminlarga xabar
    for admin_id in ADMIN_IDS:
        try:
            msg = tx("new_store_admin", la, name=store, addr=addr, dist=dist_name, uid=str(uid))
            await ctx.bot.send_message(admin_id, msg)
            if lat and lng:
                await ctx.bot.send_location(admin_id, float(lat), float(lng))
            if photo_id:
                await ctx.bot.send_photo(admin_id, photo_id, caption=f"Dokon: {store}")
        except Exception as e:
            logger.error(f"Store admin notify: {e}")

    await upd.message.reply_text(tx("store_added", la, name=store),
        reply_markup=main_kb(la, is_adm(ctx)))
    return MAIN_MENU

# ── QABUL ─────────────────────────────────────────────────────────────────────
async def qabul_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU
    p = find_prod(t, la)
    if not p: await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return QABUL_PROD
    ctx.user_data["p"] = p
    await upd.message.reply_text(f"{t}\n\n{tx('qty',la)}", reply_markup=back_kb(la))
    return QABUL_QTY

async def qabul_qty(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return QABUL_PROD
    qty = parse_qty(t)
    if qty <= 0: await upd.message.reply_text(tx("err_num",la)); return QABUL_QTY
    p = ctx.user_data["p"]; uid = upd.effective_user.id
    price, _ = get_price(p["id"])
    total = qty * price
    db_append("Qabul",[now_str(), str(uid), uname(upd), p[la], qty, p["unit"], price, total])
    await upd.message.reply_text(
        f"{tx('ok',la)}\n{p[la]}: {qty} {p['unit']}\nNarx: {price:,.0f}\nJami: {total:,.0f} som",
        reply_markup=prod_kb(la))
    return QABUL_PROD

# ── TOPSHIRISH ────────────────────────────────────────────────────────────────
async def topshir_store(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU
    stores = ctx.user_data.get("stores",[])
    if t not in [s.get("Nomi","") for s in stores]:
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la)); return TOPSHIR_STORE
    ctx.user_data["s"] = t
    debt = get_debt(t)
    msg = tx("prod",la)
    if debt > 0:
        msg = (f"⚠️ Qarz: {debt:,.0f} som\n\n" if la=="uz" else f"⚠️ Долг: {debt:,.0f} сум\n\n") + msg
    await upd.message.reply_text(msg, reply_markup=prod_kb(la))
    return TOPSHIR_PROD

async def topshir_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        stores = ctx.user_data.get("stores",[])
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la)); return TOPSHIR_STORE
    p = find_prod(t, la)
    if not p: await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return TOPSHIR_PROD
    ctx.user_data["p"] = p
    await upd.message.reply_text(f"{t}\n\n{tx('photo_scale',la)}", reply_markup=back_kb(la))
    return TOPSHIR_PHOTO

async def topshir_photo(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    p = ctx.user_data["p"]; store = ctx.user_data["s"]; uid = upd.effective_user.id

    if upd.message.photo:
        await upd.message.reply_text(tx("reading",la))
        file = await ctx.bot.get_file(upd.message.photo[-1].file_id)
        img_bytes = bytes(await file.download_as_bytearray())
        raw = await vision_ocr(img_bytes)
        weight = parse_scale_weight(raw)
        if weight > 0:
            ctx.user_data["_w"] = weight
            await upd.message.reply_text(tx("ocr_weight",la,v=weight), reply_markup=yes_kb(la))
            return TOPSHIR_PHOTO
        await upd.message.reply_text(tx("ocr_fail",la), reply_markup=back_kb(la))
        return TOPSHIR_PHOTO

    t = upd.message.text or ""
    if t == tx("back",la):
        await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return TOPSHIR_PROD

    if "_w" in ctx.user_data:
        if t.upper() in ["HA","ДА","YES","OK"]:
            qty = ctx.user_data.pop("_w")
        else:
            ctx.user_data.pop("_w", None)
            qty = parse_weight(t)
            if qty <= 0: await upd.message.reply_text(tx("err_weight",la)); return TOPSHIR_PHOTO
    else:
        qty = parse_weight(t)
        if qty <= 0: await upd.message.reply_text(tx("err_weight",la)); return TOPSHIR_PHOTO

    price, _ = get_price(p["id"])
    total = qty * price
    db_append("Topshirish",[now_str(), str(uid), store, p[la], qty, p["unit"], price, total])
    await upd.message.reply_text(
        f"{tx('ok',la)}\nDokon: {store}\n{p[la]}: {qty} {p['unit']}\nJami: {total:,.0f} som",
        reply_markup=prod_kb(la))
    return TOPSHIR_PROD

# ── TOLOV ─────────────────────────────────────────────────────────────────────
async def tolov_store(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU
    stores = ctx.user_data.get("stores",[])
    if t not in [s.get("Nomi","") for s in stores]:
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la)); return TOLOV_STORE
    ctx.user_data["s"] = t
    debt = get_debt(t)
    debt_txt = (f"\n⚠️ Qarz: {debt:,.0f} som" if la=="uz" else f"\n⚠️ Долг: {debt:,.0f} сум") if debt>0 else ""
    await upd.message.reply_text(f"{t}{debt_txt}\n\n{tx('sum',la)}", reply_markup=back_kb(la))
    return TOLOV_AMOUNT

async def tolov_amount(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        stores = ctx.user_data.get("stores",[])
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la)); return TOLOV_STORE
    amount = parse_money(t)
    if amount <= 0: await upd.message.reply_text(tx("err_money",la)); return TOLOV_AMOUNT
    ctx.user_data["amount"] = amount
    await upd.message.reply_text(tx("pay",la), reply_markup=ReplyKeyboardMarkup([
        [tx("naqd",la), tx("qarz_btn",la)],[tx("back",la)]], resize_keyboard=True))
    return TOLOV_METHOD

async def tolov_method(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("sum",la), reply_markup=back_kb(la)); return TOLOV_AMOUNT
    store = ctx.user_data["s"]; amount = ctx.user_data["amount"]; uid = upd.effective_user.id
    method = "Naqd" if t==tx("naqd",la) else "Qarz"
    db_append("Tolov",[now_str(), str(uid), store, amount, method, ""])
    await upd.message.reply_text(
        f"{tx('ok',la)}\n{store}\n{amount:,.0f} som - {method}",
        reply_markup=main_kb(la,is_adm(ctx)))
    return MAIN_MENU

# ── BUYURTMA ──────────────────────────────────────────────────────────────────
async def buyurtma_store(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU
    stores = ctx.user_data.get("stores",[])
    if t not in [s.get("Nomi","") for s in stores]:
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la)); return BUYURTMA_STORE
    ctx.user_data["s"] = t
    await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return BUYURTMA_PROD

async def buyurtma_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        stores = ctx.user_data.get("stores",[])
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la)); return BUYURTMA_STORE
    p = find_prod(t, la)
    if not p: await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return BUYURTMA_PROD
    ctx.user_data["p"] = p
    await upd.message.reply_text(f"{t}\n{tx('qty',la)}", reply_markup=back_kb(la)); return BUYURTMA_QTY

async def buyurtma_qty(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return BUYURTMA_PROD
    qty = parse_qty(t)
    if qty <= 0: await upd.message.reply_text(tx("err_num",la)); return BUYURTMA_QTY
    p = ctx.user_data["p"]; store = ctx.user_data["s"]; uid = upd.effective_user.id
    db_append("Buyurtmalar",[now_str(), str(uid), store, p[la], qty, "Kutilmoqda"])
    await upd.message.reply_text(
        f"{tx('ok',la)}\nDokon: {store}\n{p[la]}: {qty} {p['unit']}",
        reply_markup=prod_kb(la)); return BUYURTMA_PROD

# ── MARSHRUT ──────────────────────────────────────────────────────────────────
async def marshrut_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); uid = upd.effective_user.id
    stores = get_stores(uid)
    if not stores: await upd.message.reply_text(tx("no_store",la)); return
    ctx.user_data["m_stores"] = stores
    await upd.message.reply_text(tx("send_loc",la), reply_markup=loc_kb(la))

async def marshrut_loc(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); uid = upd.effective_user.id
    stores = ctx.user_data.get("m_stores", get_stores(uid))
    lat, lng = 0, 0
    if upd.message.location:
        lat = upd.message.location.latitude
        lng = upd.message.location.longitude
    lines = ["Bugungi marshrut:" if la=="uz" else "Маршрут:","---"]
    for i, s in enumerate(stores, 1):
        debt = get_debt(s.get("Nomi",""))
        d = f" (⚠️ Qarz: {debt:,.0f})" if debt>0 else ""
        slat = s.get("Lat",""); slng = s.get("Lng","")
        loc_info = f" 📍" if slat and slng else ""
        lines.append(f"{i}. {s.get('Nomi','')}{d}{loc_info}")
    if lat and lng and stores:
        wps = "|".join([s.get("Nomi","").replace(" ","+") for s in stores])
        dest = stores[-1].get("Nomi","").replace(" ","+")
        url = f"https://www.google.com/maps/dir/?api=1&origin={lat},{lng}&destination={dest}&waypoints={wps}&travelmode=driving"
        lines.append(f"\n🗺 Google Maps:\n{url}")
    await upd.message.reply_text("\n".join(lines), reply_markup=main_kb(la, is_adm(ctx)))

# ── HISOBOT ───────────────────────────────────────────────────────────────────
async def hisobot_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text; uid = str(upd.effective_user.id)
    if t == tx("back",la):
        await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU
    days = 7 if t==tx("week",la) else 30
    from_dt = (datetime.now()-timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        sales=[r for r in db_all("Topshirish") if str(r.get("Sana",""))>=from_dt and str(r.get("Dist_ID",""))==uid]
        pays =[r for r in db_all("Tolov")      if str(r.get("Sana",""))>=from_dt and str(r.get("Dist_ID",""))==uid]
        ins  =[r for r in db_all("Qabul")       if str(r.get("Sana",""))>=from_dt and str(r.get("Dist_ID",""))==uid]
        ts=sum(float(r.get("Jami",0) or 0) for r in sales)
        tc=sum(float(r.get("Summa",0) or 0) for r in pays if r.get("Usul")=="Naqd")
        td=sum(float(r.get("Summa",0) or 0) for r in pays if r.get("Usul")=="Qarz")
        ti=sum(float(r.get("Jami",0) or 0) for r in ins)
        ps={}
        for r in sales: k=r.get("Mahsulot",""); ps[k]=ps.get(k,0)+float(r.get("Miqdor",0) or 0)
        top=sorted(ps.items(),key=lambda x:x[1],reverse=True)[:3]
        top_txt="\n".join([f"  {p}: {q:.3f}" for p,q in top]) or "  -"
        period=("7 kun" if days==7 else "30 kun") if la=="uz" else ("7 дней" if days==7 else "30 дней")
        if la=="uz":
            msg=(f"Hisobot: {period}\n---\nQabul: {ti:,.0f}\nSotuv: {ts:,.0f}\n"
                 f"Naqd: {tc:,.0f}\nQarz: {td:,.0f}\n---\nTop:\n{top_txt}")
        else:
            msg=(f"Отчёт: {period}\n---\nПолучено: {ti:,.0f}\nПродажи: {ts:,.0f}\n"
                 f"Наличные: {tc:,.0f}\nДолг: {td:,.0f}\n---\nТоп:\n{top_txt}")
    except Exception as e:
        msg=f"Xatolik: {e}"
    await upd.message.reply_text(msg, reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU

# ── KUNLIK / OMBOR ────────────────────────────────────────────────────────────
async def daily(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=str(upd.effective_user.id); today=today_str()
    try:
        sales=[r for r in db_all("Topshirish") if str(r.get("Sana","")).startswith(today) and str(r.get("Dist_ID",""))==uid]
        pays =[r for r in db_all("Tolov")      if str(r.get("Sana","")).startswith(today) and str(r.get("Dist_ID",""))==uid]
        ins  =[r for r in db_all("Qabul")       if str(r.get("Sana","")).startswith(today) and str(r.get("Dist_ID",""))==uid]
        ts=sum(float(r.get("Jami",0) or 0) for r in sales)
        tc=sum(float(r.get("Summa",0) or 0) for r in pays if r.get("Usul")=="Naqd")
        td=sum(float(r.get("Summa",0) or 0) for r in pays if r.get("Usul")=="Qarz")
        ti=sum(float(r.get("Jami",0) or 0) for r in ins)
        dc=len(set(r.get("Dokon","") for r in sales))
        if la=="uz":
            msg=f"Kunlik natija - {today}\n---\nQabul: {ti:,.0f} som\nSotuv: {ts:,.0f} som\nNaqd: {tc:,.0f} som\nQarz: {td:,.0f} som\nDokonlar: {dc}"
        else:
            msg=f"Итог дня - {today}\n---\nПолучено: {ti:,.0f} сум\nПродажи: {ts:,.0f} сум\nНаличные: {tc:,.0f} сум\nДолг: {td:,.0f} сум\nМагазинов: {dc}"
    except Exception as e:
        msg=f"Xatolik: {e}"
    await upd.message.reply_text(msg)

async def stock(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); uid=str(upd.effective_user.id)
    try:
        st={}
        for r in db_all("Qabul"):
            if str(r.get("Dist_ID",""))==uid:
                k=r.get("Mahsulot",""); st[k]=st.get(k,0)+float(r.get("Miqdor",0) or 0)
        for r in db_all("Topshirish"):
            if str(r.get("Dist_ID",""))==uid:
                k=r.get("Mahsulot",""); st[k]=st.get(k,0)-float(r.get("Miqdor",0) or 0)
        lines=["Ombor:" if la=="uz" else "Склад:","---"]
        for k,v in st.items():
            if v>0.001: lines.append(f"{k}: {v:.3f}")
        if len(lines)==2: lines.append("Hammasi topshirilgan!" if la=="uz" else "Всё сдано!")
        await upd.message.reply_text("\n".join(lines))
    except Exception as e:
        await upd.message.reply_text(f"Xatolik: {e}")

# ── ADMIN ─────────────────────────────────────────────────────────────────────
async def admin_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if uid not in ADMIN_IDS: await upd.message.reply_text(tx("no_admin",la)); return MAIN_MENU
    if t==tx("back",la):
        await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,True)); return MAIN_MENU
    if t==tx("price_btn",la):
        await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    if t==tx("add_store_adm",la):
        await upd.message.reply_text(tx("adm_sname",la), reply_markup=back_kb(la)); return ADM_STORE_NAME
    if t==tx("add_dist",la):
        await upd.message.reply_text("Distribyutor ismi:", reply_markup=back_kb(la)); return ADM_DIST_NAME
    if t==tx("stats",la):     await a_stats(upd,ctx); return ADMIN_MENU
    if t==tx("debtors",la):   await a_debtors(upd,ctx); return ADMIN_MENU
    if t==tx("broadcast",la):
        await upd.message.reply_text(tx("broadcast_msg",la), reply_markup=back_kb(la)); return ADM_BROADCAST
    if t==tx("list_stores",la): await a_list_stores(upd,ctx); return ADMIN_MENU
    if t==tx("list_dists",la):  await a_list_dists(upd,ctx); return ADMIN_MENU
    return ADMIN_MENU

async def a_list_stores(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    stores = db_all("Dokonlar")
    if not stores:
        await upd.message.reply_text("Dokonlar yoq" if la=="uz" else "Магазинов нет"); return
    lines = [f"Jami: {len(stores)} dokon","---"]
    for s in stores:
        debt = get_debt(s.get("Nomi",""))
        d = f" | Qarz: {debt:,.0f}" if debt>0 else ""
        lat = s.get("Lat",""); lng = s.get("Lng","")
        loc = f"\n  📍 {lat}, {lng}" if lat and lng else ""
        lines.append(
            f"• {s.get('Nomi','')}{d}\n"
            f"  Adres: {s.get('Adres','') or s.get('MCHJ','') or '-'}\n"
            f"  Tel: {s.get('Tel1','')}\n"
            f"  Dist: {s.get('Dist_Ism','')} ({s.get('Dist_ID','')}){loc}"
        )
    text = "\n".join(lines)
    for i in range(0, len(text), 3500):
        await upd.message.reply_text(text[i:i+3500])

async def a_list_dists(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    users = db_all("Foydalanuvchilar")
    dists = [u for u in users if u.get("Rol","") == "distributor"]
    if not dists:
        await upd.message.reply_text("Distribyutorlar yoq" if la=="uz" else "Нет дистрибьюторов"); return
    lines = [f"Jami: {len(dists)}","---"]
    for u in dists:
        name = f"{u.get('Ism','')} {u.get('Familiya','')}".strip()
        status = u.get("Status","")
        stores_count = len(get_stores(u.get("TG_ID","")))
        lines.append(
            f"• {name}\n"
            f"  Tel: {u.get('Telefon','')}\n"
            f"  ID: {u.get('TG_ID','')}\n"
            f"  Status: {status}\n"
            f"  Dokonlar: {stores_count}"
        )
    text = "\n".join(lines)
    for i in range(0, len(text), 3500):
        await upd.message.reply_text(text[i:i+3500])

async def a_price_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    p=find_prod(t,la)
    if not p: await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    ctx.user_data["p"]=p; price,_=get_price(p["id"])
    await upd.message.reply_text(f"{t}\nJoriy narx: {price:,.0f}\n\n{tx('new_price',la)}", reply_markup=back_kb(la))
    return ADM_PRICE_VAL

async def a_price_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    price=parse_money(t)
    if price<=0: await upd.message.reply_text(tx("err_money",la)); return ADM_PRICE_VAL
    ctx.user_data["np"]=price
    await upd.message.reply_text(tx("tannarx",la)); return ADM_COST_VAL

async def a_cost_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    cost=parse_money(t)
    p=ctx.user_data["p"]; price=ctx.user_data["np"]
    set_price(p["id"],p[la],price,cost)
    await upd.message.reply_text(
        f"Yangilandi!\n{p[la]}\nNarx: {price:,.0f}\nTannarx: {cost:,.0f}",
        reply_markup=main_kb(la,True))
    return MAIN_MENU

async def a_store_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["ns"]=t.strip()
    await upd.message.reply_text(tx("adm_smanzil",la), reply_markup=back_kb(la)); return ADM_STORE_MANZIL

async def a_store_manzil(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    ctx.user_data["ns_manzil"]=upd.message.text.strip()
    await upd.message.reply_text(tx("adm_sdist",la)); return ADM_STORE_DIST

async def a_store_dist(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    ctx.user_data["ns_dist"]=upd.message.text.strip()
    await upd.message.reply_text(tx("adm_sloc",la), reply_markup=loc_kb(la)); return ADM_STORE_LOC

async def a_store_loc(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    store=ctx.user_data.get("ns",""); manzil=ctx.user_data.get("ns_manzil",""); dist_id=ctx.user_data.get("ns_dist","")
    lat,lng="",""
    if upd.message.location: lat=str(upd.message.location.latitude); lng=str(upd.message.location.longitude)
    cnt=len(db_all("Dokonlar"))+1
    db_append("Dokonlar",[str(cnt), store, manzil, "", "", "", str(dist_id), "", lat, lng, now_str()])
    await upd.message.reply_text(
        f"Dokon qoshildi: {store}" if la=="uz" else f"Магазин добавлен: {store}",
        reply_markup=main_kb(la,True))
    return MAIN_MENU

async def a_dist_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["nd"]=t.strip()
    await upd.message.reply_text("Telegram ID:", reply_markup=back_kb(la)); return ADM_DIST_ID

async def a_dist_id(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); name=ctx.user_data.get("nd","")
    db_append("Foydalanuvchilar",[upd.message.text.strip(), name, "", "", "distributor", la, "", "tasdiqlangan", now_str()])
    await upd.message.reply_text(f"Qoshildi: {name}", reply_markup=main_kb(la,True)); return MAIN_MENU

async def a_stats(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    try:
        sales=db_all("Topshirish"); pays=db_all("Tolov"); ins=db_all("Qabul"); stores=db_all("Dokonlar")
        users=db_all("Foydalanuvchilar")
        ts=sum(float(r.get("Jami",0) or 0) for r in sales)
        tc=sum(float(r.get("Summa",0) or 0) for r in pays if r.get("Usul")=="Naqd")
        td=sum(float(r.get("Summa",0) or 0) for r in pays if r.get("Usul")=="Qarz")
        ti=sum(float(r.get("Jami",0) or 0) for r in ins)
        ps={}
        for r in sales: k=r.get("Mahsulot",""); ps[k]=ps.get(k,0)+float(r.get("Miqdor",0) or 0)
        top=sorted(ps.items(),key=lambda x:x[1],reverse=True)[:3]
        top_txt="\n".join([f"  {p}: {q:.3f}" for p,q in top]) or "  -"
        dists=[u for u in users if u.get("Rol")=="distributor"]
        if la=="uz":
            msg=(f"Umumiy statistika\n---\n"
                 f"Qabul: {ti:,.0f} som\nSotuv: {ts:,.0f} som\n"
                 f"Naqd: {tc:,.0f} som\nQarz: {td:,.0f} som\n"
                 f"Dokonlar: {len(stores)}\nDistribytorlar: {len(dists)}\n---\nTop:\n{top_txt}")
        else:
            msg=(f"Общая статистика\n---\n"
                 f"Получено: {ti:,.0f} сум\nПродажи: {ts:,.0f} сум\n"
                 f"Наличные: {tc:,.0f} сум\nДолг: {td:,.0f} сум\n"
                 f"Магазинов: {len(stores)}\nДистрибьюторов: {len(dists)}\n---\nТоп:\n{top_txt}")
        await upd.message.reply_text(msg)
    except Exception as e:
        await upd.message.reply_text(f"Xatolik: {e}")

async def a_debtors(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    try:
        stores=db_all("Dokonlar"); lines=["Qarzdorlar:","---"]; total=0
        for s in stores:
            name=s.get("Nomi",""); debt=get_debt(name)
            if debt>0:
                dist=s.get("Dist_Ism","")
                lines.append(f"{name}: {debt:,.0f} som\n  Dist: {dist}"); total+=debt
        if len(lines)==2: lines.append("Qarz yoq!" if la=="uz" else "Долгов нет!")
        else: lines.append(f"---\nJami: {total:,.0f} som")
        await upd.message.reply_text("\n".join(lines))
    except Exception as e:
        await upd.message.reply_text(f"Xatolik: {e}")

async def a_broadcast(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,True)); return MAIN_MENU
    users=db_all("Foydalanuvchilar"); sent=0; failed=0
    for u in users:
        try: await ctx.bot.send_message(int(u.get("TG_ID",0)),t); sent+=1
        except Exception: failed+=1
    await upd.message.reply_text(f"Yuborildi: {sent}, Xato: {failed}", reply_markup=main_kb(la,True))
    return MAIN_MENU

# ── QARZ ESLATMASI ────────────────────────────────────────────────────────────
async def debt_reminder(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        stores=db_all("Dokonlar"); dist_debts={}
        for s in stores:
            debt=get_debt(s.get("Nomi",""))
            if debt>0:
                did=str(s.get("Dist_ID","")).strip()
                if did and did!="0":
                    if did not in dist_debts: dist_debts[did]=[]
                    dist_debts[did].append((s.get("Nomi",""),debt))
        for did,debts in dist_debts.items():
            try:
                lines=["Bugungi qarzlar:","---"]
                for name,debt in debts: lines.append(f"{name}: {debt:,.0f} som")
                await ctx.bot.send_message(int(did),"\n".join(lines))
            except Exception:
                pass
    except Exception as e:
        logger.error(f"debt_reminder: {e}")

async def cancel(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN: print("BOT_TOKEN topilmadi!"); return
    app = Application.builder().token(BOT_TOKEN).build()
    app.job_queue.run_daily(debt_reminder, time=dtime(9,0))

    txt      = filters.TEXT & ~filters.COMMAND
    photo_txt= (filters.PHOTO | filters.TEXT) & ~filters.COMMAND
    loc_txt  = (filters.LOCATION | filters.TEXT) & ~filters.COMMAND
    cont_txt = (filters.CONTACT | filters.TEXT) & ~filters.COMMAND

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            LANG_SELECT:      [CallbackQueryHandler(lang_cb, pattern="^lang_"), CommandHandler("start",start)],
            REG_NAME:         [MessageHandler(txt, reg_name)],
            REG_FNAME:        [MessageHandler(txt, reg_fname)],
            REG_PHONE:        [MessageHandler(cont_txt, reg_phone)],
            REG_PASSPORT:     [MessageHandler((filters.PHOTO|filters.TEXT)&~filters.COMMAND, reg_passport)],
            WAIT_APPROVE:     [MessageHandler(txt, wait_approve_h)],
            MAIN_MENU:        [MessageHandler(txt, main_h)],
            QABUL_PROD:       [MessageHandler(txt, qabul_prod)],
            QABUL_QTY:        [MessageHandler(txt, qabul_qty)],
            TOPSHIR_STORE:    [MessageHandler(txt, topshir_store)],
            TOPSHIR_PROD:     [MessageHandler(txt, topshir_prod)],
            TOPSHIR_PHOTO:    [MessageHandler(photo_txt, topshir_photo)],
            TOLOV_STORE:      [MessageHandler(txt, tolov_store)],
            TOLOV_AMOUNT:     [MessageHandler(txt, tolov_amount)],
            TOLOV_METHOD:     [MessageHandler(txt, tolov_method)],
            BUYURTMA_STORE:   [MessageHandler(txt, buyurtma_store)],
            BUYURTMA_PROD:    [MessageHandler(txt, buyurtma_prod)],
            BUYURTMA_QTY:     [MessageHandler(txt, buyurtma_qty)],
            MY_STORE_NAME:    [MessageHandler(txt, my_store_name)],
            MY_STORE_ADDR:    [MessageHandler(txt, my_store_addr)],
            MY_STORE_MCHJ:    [MessageHandler(txt, my_store_mchj)],
            MY_STORE_TEL1:    [MessageHandler(cont_txt, my_store_tel1)],
            MY_STORE_TEL2:    [MessageHandler(cont_txt, my_store_tel2)],
            MY_STORE_PHOTO:   [MessageHandler(photo_txt, my_store_photo)],
            MY_STORE_LOC:     [MessageHandler(loc_txt, my_store_loc)],
            HISOBOT_MENU:     [MessageHandler(txt, hisobot_h)],
            ADMIN_MENU:       [MessageHandler(txt, admin_h)],
            ADM_PRICE_PROD:   [MessageHandler(txt, a_price_prod)],
            ADM_PRICE_VAL:    [MessageHandler(txt, a_price_val)],
            ADM_COST_VAL:     [MessageHandler(txt, a_cost_val)],
            ADM_STORE_NAME:   [MessageHandler(txt, a_store_name)],
            ADM_STORE_MANZIL: [MessageHandler(txt, a_store_manzil)],
            ADM_STORE_DIST:   [MessageHandler(txt, a_store_dist)],
            ADM_STORE_LOC:    [MessageHandler(loc_txt, a_store_loc)],
            ADM_DIST_NAME:    [MessageHandler(txt, a_dist_name)],
            ADM_DIST_ID:      [MessageHandler(txt, a_dist_id)],
            ADM_BROADCAST:    [MessageHandler(txt, a_broadcast)],
        },
        fallbacks=[CommandHandler("cancel", cancel), CommandHandler("start", start)],
        allow_reentry=True,
    )
    app.add_handler(MessageHandler(filters.Regex(r'^/approve_\d+$'), approve_cmd))
    app.add_handler(MessageHandler(filters.Regex(r'^/reject_\d+$'), reject_cmd))
    app.add_handler(MessageHandler(filters.LOCATION, marshrut_loc))
    app.add_handler(conv)
    print("Bot ishga tushdi!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
