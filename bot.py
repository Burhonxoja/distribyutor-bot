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
    MAIN_MENU,
    QABUL_PROD, QABUL_QTY,
    TOPSHIR_STORE, TOPSHIR_PROD, TOPSHIR_PHOTO,
    TOLOV_STORE, TOLOV_AMOUNT, TOLOV_METHOD,
    BUYURTMA_STORE, BUYURTMA_PROD, BUYURTMA_QTY,
    MY_STORE_NAME, MY_STORE_MCHJ, MY_STORE_TEL1, MY_STORE_TEL2, MY_STORE_LOC,
    HISOBOT_MENU,
    ADMIN_MENU,
    ADM_PRICE_PROD, ADM_PRICE_VAL, ADM_COST_VAL,
    ADM_STORE_NAME, ADM_STORE_MANZIL, ADM_STORE_DIST, ADM_STORE_LOC,
    ADM_DIST_NAME, ADM_DIST_ID,
    ADM_BROADCAST,
) = range(34)

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
def get_creds():
    return json.loads(GOOGLE_CREDS_JSON) if GOOGLE_CREDS_JSON else {}

def get_sheet():
    if not GOOGLE_CREDS_JSON: return None
    try:
        creds = Credentials.from_service_account_info(
            get_creds(),
            scopes=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        )
        return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)
    except Exception as e:
        logger.error(f"Sheet: {e}"); return None

# Har bir varaqning sarlavhalari
SHEET_HEADERS = {
    "Foydalanuvchilar": ["TG_ID","Ism","Familiya","Telefon","Rol","Til","Passport","Sana"],
    "Dokonlar":         ["ID","Nomi","MCHJ","Tel1","Tel2","Dist_ID","Lat","Lng","Sana"],
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
        w = wb.worksheet(name)
        # Sarlavha to'g'riligini tekshir
        existing = w.row_values(1)
        expected = SHEET_HEADERS.get(name, [])
        if not existing and expected:
            w.append_row(expected)
        return w
    except gspread.exceptions.WorksheetNotFound:
        w = wb.add_worksheet(name, rows=2000, cols=25)
        headers = SHEET_HEADERS.get(name, ["Data"])
        w.append_row(headers)
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
        if not w: return []
        return w.get_all_records()
    except Exception as e:
        logger.error(f"db_all {tab}: {e}"); return []

def db_update_row(tab, col_name, col_val, update_data: dict):
    """Bir qatorni yangilash"""
    try:
        w = get_ws(tab)
        if not w: return
        recs = w.get_all_records()
        headers = w.row_values(1)
        for i, r in enumerate(recs):
            if str(r.get(col_name,"")) == str(col_val):
                row_num = i + 2
                for k, v in update_data.items():
                    if k in headers:
                        col_idx = headers.index(k) + 1
                        w.update_cell(row_num, col_idx, str(v))
                return
    except Exception as e:
        logger.error(f"db_update_row {tab}: {e}")

# ── DATABASE FUNKSIYALAR ───────────────────────────────────────────────────────
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

def get_price(pid):
    try:
        for r in db_all("Narxlar"):
            if str(r.get("Mahsulot_ID","")).strip() == str(pid).strip():
                return float(r.get("Narx",0)), float(r.get("Tannarx",0))
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
    """
    Faqat shu distribyutorning do'konlarini qaytaradi.
    dist_id None bo'lsa - hammasi (faqat admin uchun)
    """
    try:
        recs = db_all("Dokonlar")
        if dist_id is None:
            return recs
        result = []
        for r in recs:
            stored_id = str(r.get("Dist_ID","")).strip()
            search_id = str(dist_id).strip()
            if stored_id == search_id:
                result.append(r)
        logger.info(f"get_stores({dist_id}): found {len(result)} of {len(recs)}")
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

def fmt_num(text: str) -> float:
    """10000, 10,000, 10.000, 10.5 — barchasini float ga o'giradi"""
    try:
        t = str(text).strip().replace(" ","")
        # Minglik ajratuvchi: 10,000 yoki 10.000
        if re.match(r'^\d{1,3}[.,]\d{3}$', t):
            t = t.replace(",","").replace(".","")
        else:
            t = t.replace(",",".")
        return float(t)
    except Exception:
        return 0.0

# ── GOOGLE VISION OCR ─────────────────────────────────────────────────────────
async def vision_ocr(image_bytes: bytes) -> str:
    try:
        import httpx
        creds = Credentials.from_service_account_info(
            get_creds(),
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
    Tarozi ekrani: CHAP=og'irlik | O'RTA=1kg narxi | O'NG=jami
    Chap raqam = og'irlik (gramm), masalan 3455 = 3.455 kg
    """
    if not text: return 0.0
    # Barcha raqamlarni top
    nums = re.findall(r'\d+', text.replace("\n"," "))
    if not nums: return 0.0
    # Birinchi raqam = og'irlik (gramm)
    first = nums[0]
    logger.info(f"Scale parse: first num = {first}")
    try:
        val = int(first)
        # 3-4 raqam = gramm → kg
        if 100 <= val <= 99999:
            return round(val / 1000, 3)
        # Allaqachon kg (masalan 3.455 yoki 3,455)
    except Exception:
        pass
    # Decimal izla
    dec = re.findall(r'\d+[.,]\d+', text)
    if dec:
        try:
            return float(dec[0].replace(",","."))
        except Exception:
            pass
    return 0.0

# ── MATNLAR ───────────────────────────────────────────────────────────────────
T = {
    # Til
    "start":        {"uz":"Tilni tanlang:","ru":"Выберите язык:"},
    # Ro'yxat
    "reg_name":     {"uz":"Ismingizni kiriting:","ru":"Введите ваше имя:"},
    "reg_fname":    {"uz":"Familiyangizni kiriting:","ru":"Введите фамилию:"},
    "reg_phone":    {"uz":"Telefon raqamingizni yuboring:","ru":"Отправьте номер телефона:"},
    "reg_passport": {"uz":"Passport rasmini yuboring (2 betini birga):","ru":"Отправьте фото паспорта (оба разворота):"},
    "reg_ok":       {"uz":"Royxatdan otdingiz {name}! Admin tasdiqlashini kuting.","ru":"Вы зарегистрированы {name}! Ожидайте подтверждения."},
    "reg_new_admin":{"uz":"Yangi distribyutor:\nIsm: {name}\nTel: {phone}\nID: {uid}","ru":"Новый дистрибьютор:\nИмя: {name}\nТел: {phone}\nID: {uid}"},
    # Menyu
    "main":         {"uz":"Asosiy menyu:","ru":"Главное меню:"},
    "qabul":        {"uz":"Zavoddan qabul","ru":"Получить с завода"},
    "buyurtma":     {"uz":"Buyurtmalar","ru":"Заказы"},
    "topshir":      {"uz":"Mahsulot topshirish","ru":"Передать товар"},
    "tolov":        {"uz":"Tolov","ru":"Оплата"},
    "natija":       {"uz":"Kunlik natija","ru":"Итог дня"},
    "ombor":        {"uz":"Ombor","ru":"Склад"},
    "marshrut":     {"uz":"Marshrut","ru":"Маршрут"},
    "hisobot":      {"uz":"Hisobot","ru":"Отчёт"},
    "my_stores":    {"uz":"Mening dokonlarim","ru":"Мои магазины"},
    "admin":        {"uz":"Admin panel","ru":"Админ панель"},
    "back":         {"uz":"Orqaga","ru":"Назад"},
    "naqd":         {"uz":"Naqd","ru":"Наличные"},
    "qarz_btn":     {"uz":"Qarz","ru":"Долг"},
    # Umumiy
    "prod":         {"uz":"Mahsulotni tanlang:","ru":"Выберите товар:"},
    "store":        {"uz":"Dokonni tanlang:","ru":"Выберите магазин:"},
    "no_store":     {"uz":"Dokonlar topilmadi.\nQoshish: Mening dokonlarim tugmasi","ru":"Магазины не найдены.\nДобавить: кнопка Мои магазины"},
    "sum":          {"uz":"Summa kiriting (masalan: 15000):","ru":"Введите сумму (например: 15000):"},
    "qty":          {"uz":"Miqdorni kiriting (masalan: 5 yoki 5.5):","ru":"Введите количество (например: 5 или 5.5):"},
    "pay":          {"uz":"Tolov usuli:","ru":"Способ оплаты:"},
    "ok":           {"uz":"Saqlandi!","ru":"Сохранено!"},
    "err":          {"uz":"Raqam kiriting! Masalan: 5 yoki 5.5 yoki 15000","ru":"Введите число! Например: 5 или 5.5 или 15000"},
    # Admin
    "no_admin":     {"uz":"Siz admin emassiz!","ru":"Вы не администратор!"},
    "adm":          {"uz":"Admin paneli:","ru":"Админ панель:"},
    "price_btn":    {"uz":"Narx ozgartirish","ru":"Изменить цены"},
    "add_store_adm":{"uz":"Dokon qoshish","ru":"Добавить магазин"},
    "add_dist":     {"uz":"Distribyutor qoshish","ru":"Добавить дистрибьютора"},
    "stats":        {"uz":"Statistika","ru":"Статистика"},
    "broadcast":    {"uz":"Hammaga xabar","ru":"Рассылка"},
    "debtors":      {"uz":"Qarzdorlar","ru":"Должники"},
    "list_stores":  {"uz":"Dokonlar royxati","ru":"Список магазинов"},
    "list_dists":   {"uz":"Distribyutorlar","ru":"Дистрибьюторы"},
    "new_price":    {"uz":"Yangi narx (masalan: 15000):","ru":"Новая цена (например: 15000):"},
    "tannarx":      {"uz":"Tannarx (masalan: 12000):","ru":"Себестоимость (например: 12000):"},
    # Hisobot
    "week":         {"uz":"Haftalik","ru":"Недельный"},
    "month":        {"uz":"Oylik","ru":"Месячный"},
    # Marshrut
    "send_loc":     {"uz":"Lokatsiyangizni yuboring:","ru":"Отправьте геолокацию:"},
    "loc_btn":      {"uz":"Lokatsiyani yuborish","ru":"Отправить геолокацию"},
    "skip":         {"uz":"Otkazib yuborish","ru":"Пропустить"},
    # Broadcast
    "broadcast_msg":{"uz":"Xabar matnini kiriting:","ru":"Введите текст рассылки:"},
    # OCR
    "photo_scale":  {"uz":"Tarozi rasmini yuboring YOKI ogirlikni kiriting\n(masalan: 3.455):","ru":"Фото весов ИЛИ введите вес\n(например: 3.455):"},
    "ocr_weight":   {"uz":"Rasmdan oqildi: {v} kg\nTogri? HA bosing yoki togri raqamni kiriting:","ru":"Считано: {v} кг\nВерно? Нажмите ДА или введите правильное число:"},
    "ocr_fail":     {"uz":"Rasmdan oqib bolmadi.\nOgirlikni qolda kiriting (masalan: 3.455):","ru":"Не удалось считать.\nВведите вес вручную (например: 3.455):"},
    "reading":      {"uz":"Rasm oqilmoqda...","ru":"Читаю изображение..."},
    # Do'kon qo'shish (distribyutor)
    "ms_name":      {"uz":"Dokon nomini kiriting:","ru":"Название магазина:"},
    "ms_mchj":      {"uz":"MCHJ nomini kiriting\n(yoki Otkazib yuborish):","ru":"Название ООО\n(или Пропустить):"},
    "ms_tel1":      {"uz":"Dokon telefon raqami 1:","ru":"Телефон магазина 1:"},
    "ms_tel2":      {"uz":"Telefon raqami 2\n(yoki Otkazib yuborish):","ru":"Телефон 2\n(или Пропустить):"},
    "ms_loc":       {"uz":"Dokon lokatsiyasini yuboring\n(yoki Otkazib yuborish):","ru":"Локацию магазина\n(или Пропустить):"},
    "store_added":  {"uz":"Dokon qoshildi: {name}","ru":"Магазин добавлен: {name}"},
    # Admin do'kon
    "adm_sname":    {"uz":"Dokon nomi:","ru":"Название магазина:"},
    "adm_smanzil":  {"uz":"Manzil:","ru":"Адрес:"},
    "adm_sdist":    {"uz":"Distribyutor Telegram ID si:","ru":"Telegram ID дистрибьютора:"},
    "adm_sloc":     {"uz":"Lokatsiya yuboring (yoki Otkazib yuborish):","ru":"Локация (или Пропустить):"},
    "phone_btn":    {"uz":"Telefon raqamni yuborish","ru":"Отправить номер телефона"},
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

    if is_registered(uid):
        user = get_user(uid)
        name = f"{user.get('Ism','')} {user.get('Familiya','')}".strip()
        await ctx.bot.send_message(uid,
            f"Xush kelibsiz, {name}!" if la=="uz" else f"Добро пожаловать, {name}!",
            reply_markup=main_kb(la, False))
        return MAIN_MENU

    # Yangi foydalanuvchi
    await ctx.bot.send_message(uid, tx("reg_name",la), reply_markup=ReplyKeyboardMarkup([[]], resize_keyboard=True))
    return REG_NAME

# ── RO'YXATDAN O'TISH ─────────────────────────────────────────────────────────
async def reg_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    ctx.user_data["reg_name"] = upd.message.text.strip()
    await upd.message.reply_text(tx("reg_fname",la))
    return REG_FNAME

async def reg_fname(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    ctx.user_data["reg_fname"] = upd.message.text.strip()
    await upd.message.reply_text(tx("reg_phone",la), reply_markup=phone_kb(la))
    return REG_PHONE

async def reg_phone(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    if upd.message.contact:
        phone = upd.message.contact.phone_number
    else:
        phone = upd.message.text.strip()
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

    passport_info = ""
    if upd.message.photo:
        passport_info = "rasm_bor"
    elif upd.message.text == tx("skip",la):
        passport_info = "otkazildi"
    else:
        passport_info = upd.message.text

    # Saqlash
    db_append("Foydalanuvchilar",[
        str(uid), name, fname, phone, "distributor", la, passport_info, now_str()
    ])

    # Adminlarga xabar + passport rasm
    for admin_id in ADMIN_IDS:
        try:
            msg = tx("reg_new_admin", la, name=full_name, phone=phone, uid=str(uid))
            await ctx.bot.send_message(admin_id, msg)
            if upd.message.photo:
                await ctx.bot.send_photo(admin_id, upd.message.photo[-1].file_id,
                    caption=f"Passport: {full_name} ({uid})")
        except Exception as e:
            logger.error(f"Admin notify: {e}")

    await upd.message.reply_text(
        tx("reg_ok", la, name=name),
        reply_markup=main_kb(la, False))
    return MAIN_MENU

# ── MAIN MENU ─────────────────────────────────────────────────────────────────
async def main_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text; uid = upd.effective_user.id

    if t == tx("qabul",la):
        await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la))
        return QABUL_PROD

    if t == tx("topshir",la):
        stores = get_stores(uid)
        if not stores:
            await upd.message.reply_text(tx("no_store",la)); return MAIN_MENU
        ctx.user_data["stores"] = stores
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return TOPSHIR_STORE

    if t == tx("tolov",la):
        stores = get_stores(uid)
        if not stores:
            await upd.message.reply_text(tx("no_store",la)); return MAIN_MENU
        ctx.user_data["stores"] = stores
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return TOLOV_STORE

    if t == tx("buyurtma",la):
        stores = get_stores(uid)
        if not stores:
            await upd.message.reply_text(tx("no_store",la)); return MAIN_MENU
        ctx.user_data["stores"] = stores
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return BUYURTMA_STORE

    if t == tx("natija",la):   await daily(upd, ctx); return MAIN_MENU
    if t == tx("ombor",la):    await stock(upd, ctx); return MAIN_MENU
    if t == tx("marshrut",la): await marshrut_start(upd, ctx); return MAIN_MENU

    if t == tx("hisobot",la):
        await upd.message.reply_text(
            "Hisobot:" if la=="uz" else "Отчёт:",
            reply_markup=ReplyKeyboardMarkup(
                [[tx("week",la), tx("month",la)],[tx("back",la)]], resize_keyboard=True))
        return HISOBOT_MENU

    if t == tx("my_stores",la):
        await my_stores_show(upd, ctx)
        return MY_STORE_NAME

    if t == tx("admin",la) and is_adm(ctx):
        await upd.message.reply_text(tx("adm",la), reply_markup=ReplyKeyboardMarkup([
            [tx("price_btn",la),    tx("add_store_adm",la)],
            [tx("add_dist",la),     tx("stats",la)],
            [tx("debtors",la),      tx("broadcast",la)],
            [tx("list_stores",la),  tx("list_dists",la)],
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
    if stores:
        for s in stores:
            debt = get_debt(s.get("Nomi",""))
            d = f" | Qarz: {debt:,.0f}" if debt>0 else ""
            tel = s.get("Tel1","")
            lines.append(f"• {s.get('Nomi','')}{d}\n  Tel: {tel}")
    else:
        lines.append("Hozircha dokon yoq" if la=="uz" else "Магазинов пока нет")
    lines.append("\n" + ("Yangi dokon qoshish uchun nomini kiriting:" if la=="uz"
                         else "Для добавления введите название:"))
    await upd.message.reply_text("\n".join(lines), reply_markup=back_kb(la))

async def my_store_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx)))
        return MAIN_MENU
    ctx.user_data["ns"] = t.strip()
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
        ctx.user_data["ns_tel1"] = upd.message.contact.phone_number
    else:
        ctx.user_data["ns_tel1"] = upd.message.text.strip()
    await upd.message.reply_text(tx("ms_tel2",la), reply_markup=skip_kb(la))
    return MY_STORE_TEL2

async def my_store_tel2(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    ctx.user_data["ns_tel2"] = "" if t==tx("skip",la) else t.strip()
    await upd.message.reply_text(tx("ms_loc",la), reply_markup=loc_kb(la))
    return MY_STORE_LOC

async def my_store_loc(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); uid = upd.effective_user.id
    store = ctx.user_data.get("ns","")
    mchj  = ctx.user_data.get("ns_mchj","")
    tel1  = ctx.user_data.get("ns_tel1","")
    tel2  = ctx.user_data.get("ns_tel2","")
    lat, lng = "", ""

    if upd.message.location:
        lat = str(upd.message.location.latitude)
        lng = str(upd.message.location.longitude)

    existing = db_all("Dokonlar")
    cnt = len(existing) + 1

    # Dist_ID = distribyutorning o'z Telegram ID si
    row = [str(cnt), store, mchj, tel1, tel2, str(uid), lat, lng, now_str()]
    db_append("Dokonlar", row)
    logger.info(f"Store saved: {row}")

    # Adminlarga xabar
    for admin_id in ADMIN_IDS:
        try:
            await ctx.bot.send_message(admin_id,
                f"Yangi dokon qoshildi:\n{store}\nDist: {uid}\nTel: {tel1}"
                if la=="uz" else
                f"Добавлен новый магазин:\n{store}\nДист: {uid}\nТел: {tel1}")
        except Exception:
            pass

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
    qty = fmt_num(t)
    if qty <= 0: await upd.message.reply_text(tx("err",la)); return QABUL_QTY
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
        msg = (f"Qarz: {debt:,.0f} som\n\n" if la=="uz" else f"Долг: {debt:,.0f} сум\n\n") + msg
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
            qty = fmt_num(t)
            if qty <= 0: await upd.message.reply_text(tx("err",la)); return TOPSHIR_PHOTO
    else:
        qty = fmt_num(t)
        if qty <= 0: await upd.message.reply_text(tx("err",la)); return TOPSHIR_PHOTO

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
    debt_txt = (f"\nQarz: {debt:,.0f} som" if la=="uz" else f"\nДолг: {debt:,.0f} сум") if debt>0 else ""
    await upd.message.reply_text(f"{t}{debt_txt}\n\n{tx('sum',la)}", reply_markup=back_kb(la))
    return TOLOV_AMOUNT

async def tolov_amount(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        stores = ctx.user_data.get("stores",[])
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la)); return TOLOV_STORE
    amount = fmt_num(t)
    if amount <= 0: await upd.message.reply_text(tx("err",la)); return TOLOV_AMOUNT
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
    qty = fmt_num(t)
    if qty <= 0: await upd.message.reply_text(tx("err",la)); return BUYURTMA_QTY
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
    lines = ["Bugungi marshrut:" if la=="uz" else "Маршрут на сегодня:","---"]
    for i, s in enumerate(stores, 1):
        debt = get_debt(s.get("Nomi",""))
        d = f" (Qarz: {debt:,.0f})" if debt>0 else ""
        lines.append(f"{i}. {s.get('Nomi','')}{d}")
    if lat and lng and stores:
        wps = "|".join([s.get("Nomi","").replace(" ","+") for s in stores])
        dest = stores[-1].get("Nomi","").replace(" ","+")
        url = f"https://www.google.com/maps/dir/?api=1&origin={lat},{lng}&destination={dest}&waypoints={wps}&travelmode=driving"
        lines.append(f"\nGoogle Maps:\n{url}")
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
            msg=(f"Hisobot: {period}\n---\nQabul: {ti:,.0f} som\nSotuv: {ts:,.0f} som\n"
                 f"Naqd: {tc:,.0f} som\nQarz: {td:,.0f} som\n---\nTop:\n{top_txt}")
        else:
            msg=(f"Отчёт: {period}\n---\nПолучено: {ti:,.0f} сум\nПродажи: {ts:,.0f} сум\n"
                 f"Наличные: {tc:,.0f} сум\nДолг: {td:,.0f} сум\n---\nТоп:\n{top_txt}")
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
    lines = [f"Jami dokonlar: {len(stores)}" if la=="uz" else f"Всего магазинов: {len(stores)}", "---"]
    for s in stores:
        debt = get_debt(s.get("Nomi",""))
        d = f" | Qarz: {debt:,.0f}" if debt>0 else ""
        lines.append(
            f"• {s.get('Nomi','')}\n"
            f"  MCHJ: {s.get('MCHJ','') or '-'}\n"
            f"  Tel: {s.get('Tel1','')}\n"
            f"  Dist ID: {s.get('Dist_ID','')}{d}"
        )
    # Uzoq bo'lsa bo'laklarga bo'lib yuboramiz
    text = "\n".join(lines)
    for i in range(0, len(text), 3000):
        await upd.message.reply_text(text[i:i+3000])

async def a_list_dists(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    users = db_all("Foydalanuvchilar")
    dists = [u for u in users if u.get("Rol","") == "distributor"]
    if not dists:
        await upd.message.reply_text("Distribyutorlar yoq" if la=="uz" else "Дистрибьюторов нет"); return
    lines = [f"Jami: {len(dists)}" if la=="uz" else f"Всего: {len(dists)}", "---"]
    for u in dists:
        name = f"{u.get('Ism','')} {u.get('Familiya','')}".strip()
        stores = get_stores(u.get("TG_ID",""))
        lines.append(
            f"• {name}\n"
            f"  Tel: {u.get('Telefon','')}\n"
            f"  ID: {u.get('TG_ID','')}\n"
            f"  Dokonlar: {len(stores)}"
        )
    text = "\n".join(lines)
    for i in range(0, len(text), 3000):
        await upd.message.reply_text(text[i:i+3000])

async def a_price_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    p=find_prod(t,la)
    if not p: await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    ctx.user_data["p"]=p; price,_=get_price(p["id"])
    await upd.message.reply_text(f"{t}\nJoriy: {price:,.0f}\n\n{tx('new_price',la)}", reply_markup=back_kb(la))
    return ADM_PRICE_VAL

async def a_price_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    price=fmt_num(t)
    if price<=0: await upd.message.reply_text(tx("err",la)); return ADM_PRICE_VAL
    ctx.user_data["np"]=price
    await upd.message.reply_text(tx("tannarx",la)); return ADM_COST_VAL

async def a_cost_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    cost=fmt_num(t)
    if cost<0: await upd.message.reply_text(tx("err",la)); return ADM_COST_VAL
    p=ctx.user_data["p"]; price=ctx.user_data["np"]
    set_price(p["id"],p[la],price,cost)
    await upd.message.reply_text(f"Yangilandi!\n{p[la]}: {price:,.0f} / {cost:,.0f}", reply_markup=main_kb(la,True))
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
    db_append("Dokonlar",[str(cnt), store, manzil, "", "", str(dist_id), lat, lng, now_str()])
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
    db_append("Foydalanuvchilar",[upd.message.text.strip(), name, "", "", "distributor", la, "", now_str()])
    await upd.message.reply_text(f"Qoshildi: {name}", reply_markup=main_kb(la,True)); return MAIN_MENU

async def a_stats(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    try:
        sales=db_all("Topshirish"); pays=db_all("Tolov"); ins=db_all("Qabul"); stores=db_all("Dokonlar")
        ts=sum(float(r.get("Jami",0) or 0) for r in sales)
        tc=sum(float(r.get("Summa",0) or 0) for r in pays if r.get("Usul")=="Naqd")
        td=sum(float(r.get("Summa",0) or 0) for r in pays if r.get("Usul")=="Qarz")
        ti=sum(float(r.get("Jami",0) or 0) for r in ins)
        ps={}
        for r in sales: k=r.get("Mahsulot",""); ps[k]=ps.get(k,0)+float(r.get("Miqdor",0) or 0)
        top=sorted(ps.items(),key=lambda x:x[1],reverse=True)[:3]
        top_txt="\n".join([f"  {p}: {q:.3f}" for p,q in top]) or "  -"
        if la=="uz":
            msg=(f"Umumiy statistika\n---\nQabul: {ti:,.0f}\nSotuv: {ts:,.0f}\n"
                 f"Naqd: {tc:,.0f}\nQarz: {td:,.0f}\nDokonlar: {len(stores)}\n---\nTop:\n{top_txt}")
        else:
            msg=(f"Общая статистика\n---\nПолучено: {ti:,.0f}\nПродажи: {ts:,.0f}\n"
                 f"Наличные: {tc:,.0f}\nДолг: {td:,.0f}\nМагазинов: {len(stores)}\n---\nТоп:\n{top_txt}")
        await upd.message.reply_text(msg)
    except Exception as e:
        await upd.message.reply_text(f"Xatolik: {e}")

async def a_debtors(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    try:
        stores=db_all("Dokonlar"); lines=["Qarzdorlar:" if la=="uz" else "Должники:","---"]; total=0
        for s in stores:
            name=s.get("Nomi",""); debt=get_debt(name)
            if debt>0: lines.append(f"{name}: {debt:,.0f} som"); total+=debt
        if len(lines)==2: lines.append("Qarz yoq!" if la=="uz" else "Долгов нет!")
        else: lines.append(f"---\nJami: {total:,.0f}" if la=="uz" else f"---\nИтого: {total:,.0f}")
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
                if did not in dist_debts: dist_debts[did]=[]
                dist_debts[did].append((s.get("Nomi",""),debt))
        for did,debts in dist_debts.items():
            if not did or did=="0": continue
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
            MY_STORE_MCHJ:    [MessageHandler(txt, my_store_mchj)],
            MY_STORE_TEL1:    [MessageHandler(cont_txt, my_store_tel1)],
            MY_STORE_TEL2:    [MessageHandler(cont_txt, my_store_tel2)],
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
    app.add_handler(MessageHandler(filters.LOCATION, marshrut_loc))
    app.add_handler(conv)
    print("Bot ishga tushdi!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
