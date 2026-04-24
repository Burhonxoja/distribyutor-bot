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
    REGISTER_NAME, REGISTER_PHONE, REGISTER_PASSPORT,
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
) = range(33)

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
def get_sheet():
    if not GOOGLE_CREDS_JSON: return None
    try:
        creds = Credentials.from_service_account_info(
            json.loads(GOOGLE_CREDS_JSON),
            scopes=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        )
        return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)
    except Exception as e:
        logger.error(f"Sheet: {e}"); return None

HEADERS = {
    "Qabul":            ["Sana","Dist_ID","Ism","Mahsulot","Miqdor","Birlik","Narx","Jami"],
    "Topshirish":       ["Sana","Dist_ID","Dokon","Mahsulot","Miqdor","Birlik","Narx","Jami"],
    "Tolov":            ["Sana","Dist_ID","Dokon","Summa","Usul","Izoh"],
    "Foydalanuvchilar": ["TG_ID","Ism","Familiya","Telefon","Rol","Til","Passport","Sana"],
    "Dokonlar":         ["ID","Nomi","MCHJ","Tel1","Tel2","Dist_ID","Lat","Lng","Sana"],
    "Narxlar":          ["Mahsulot_ID","Mahsulot","Narx","Tannarx","Sana"],
    "Buyurtmalar":      ["Sana","Dist_ID","Dokon","Mahsulot","Miqdor","Status"],
}

def ws(name):
    wb = get_sheet()
    if not wb: return None
    try:
        return wb.worksheet(name)
    except Exception:
        w = wb.add_worksheet(name, rows=2000, cols=20)
        w.append_row(HEADERS.get(name, ["Data"]))
        return w

def db_append(tab, row):
    try:
        w = ws(tab)
        if w: w.append_row(row)
    except Exception as e:
        logger.error(f"append {tab}: {e}")

def db_all(tab):
    try:
        w = ws(tab)
        return w.get_all_records() if w else []
    except Exception:
        return []

def get_price(pid):
    try:
        for r in db_all("Narxlar"):
            if int(r.get("Mahsulot_ID",0)) == pid:
                return float(r.get("Narx",0)), float(r.get("Tannarx",0))
    except Exception:
        pass
    return 0.0, 0.0

def set_price(pid, pname, price, cost):
    try:
        w = ws("Narxlar")
        if not w: return
        recs = w.get_all_records()
        now = now_str()
        for i, r in enumerate(recs):
            if int(r.get("Mahsulot_ID",0)) == pid:
                w.update(f"A{i+2}:E{i+2}", [[pid, pname, price, cost, now]]); return
        w.append_row([pid, pname, price, cost, now])
    except Exception as e:
        logger.error(f"set_price: {e}")

def get_stores(dist_id=None):
    """Faqat shu distribyutorning do'konlari"""
    try:
        recs = db_all("Dokonlar")
        if dist_id:
            return [r for r in recs if str(r.get("Dist_ID","")).strip() == str(dist_id).strip()]
        return recs
    except Exception:
        return []

def get_debt(store):
    try:
        sold = sum(float(r.get("Jami",0)) for r in db_all("Topshirish") if r.get("Dokon")==store)
        paid = sum(float(r.get("Summa",0)) for r in db_all("Tolov") if r.get("Dokon")==store and r.get("Usul")=="Naqd")
        return max(0.0, sold - paid)
    except Exception:
        return 0.0

def is_registered(uid):
    try:
        for r in db_all("Foydalanuvchilar"):
            if str(r.get("TG_ID","")) == str(uid):
                return True
        return False
    except Exception:
        return False

def get_user(uid):
    try:
        for r in db_all("Foydalanuvchilar"):
            if str(r.get("TG_ID","")) == str(uid):
                return r
        return None
    except Exception:
        return None

def now_str():   return datetime.now().strftime("%Y-%m-%d %H:%M")
def today_str(): return datetime.now().strftime("%Y-%m-%d")

# ── GOOGLE VISION OCR ─────────────────────────────────────────────────────────
async def vision_ocr(image_bytes: bytes) -> str:
    try:
        creds = Credentials.from_service_account_info(
            json.loads(GOOGLE_CREDS_JSON),
            scopes=["https://www.googleapis.com/auth/cloud-vision"]
        )
        creds.refresh(google.auth.transport.requests.Request())
        token = creds.token
        b64 = base64.b64encode(image_bytes).decode()
        import httpx
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://vision.googleapis.com/v1/images:annotate",
                headers={"Authorization": f"Bearer {token}"},
                json={"requests":[{"image":{"content":b64},"features":[{"type":"TEXT_DETECTION"}]}]}
            )
            data = resp.json()
            return data["responses"][0].get("fullTextAnnotation",{}).get("text","").strip()
    except Exception as e:
        logger.error(f"Vision OCR: {e}"); return ""

def parse_scale_weight(text: str) -> float:
    """
    Tarozi ekranidagi matndan og'irlikni ajratib oladi.
    Tarozi ekranda 3 ta raqam ko'rsatadi:
      CHAP = og'irlik (kg)  ← biz shu kerak
      O'RTA = 1 kg narxi
      O'NG = umumiy summa
    
    Tarozi og'irlikni odatda 3-4 raqam bilan ko'rsatadi:
    masalan 3455 → 3.455 kg, 1200 → 1.200 kg, 500 → 0.500 kg
    """
    if not text:
        return 0.0

    # Barcha raqam guruhlarini topamiz
    all_nums = re.findall(r'\d+', text)
    if not all_nums:
        return 0.0

    # Birinchi raqam = chap ko'rsatgich = og'irlik
    first = all_nums[0]

    # Tarozi og'irlikni gramm sifatida ko'rsatadi (3455 = 3.455 kg)
    # 3-4 raqam bo'lsa → gramm → kg ga o'girish
    if len(first) >= 3:
        try:
            grams = int(first)
            if grams > 0:
                return round(grams / 1000, 3)
        except Exception:
            pass

    # Agar allaqachon decimal ko'rinishda bo'lsa (3.455)
    decimal_nums = re.findall(r'\d+[.,]\d+', text)
    if decimal_nums:
        try:
            return float(decimal_nums[0].replace(",", "."))
        except Exception:
            pass

    return 0.0

def fmt_price(text: str) -> float:
    """Narx matnini float ga o'giradi. 10000, 10,000, 10.000 hammasi ishlaydi"""
    try:
        # Vergul va nuqtani olib tashlash
        cleaned = text.strip().replace(" ", "")
        # Agar nuqta yoki vergul minglik ajratuvchi bo'lsa (3 ta raqam keyin)
        if re.match(r'^\d{1,3}[.,]\d{3}$', cleaned):
            cleaned = cleaned.replace(",", "").replace(".", "")
        else:
            cleaned = cleaned.replace(",", ".")
        return float(cleaned)
    except Exception:
        return 0.0

# ── MATNLAR ───────────────────────────────────────────────────────────────────
T = {
    "start":         {"uz":"Tilni tanlang:","ru":"Выберите язык:"},
    "register_name": {"uz":"Ismingizni kiriting:","ru":"Введите ваше имя:"},
    "register_fname":{"uz":"Familiyangizni kiriting:","ru":"Введите фамилию:"},
    "register_phone":{"uz":"Telefon raqamingizni yuboring:","ru":"Отправьте номер телефона:"},
    "register_pass": {"uz":"Passport rasmini yuboring:","ru":"Отправьте фото паспорта:"},
    "register_ok":   {"uz":"Royxatdan otdingiz! Xush kelibsiz, {name}!","ru":"Вы зарегистрированы! Добро пожаловать, {name}!"},
    "wait_approve":  {"uz":"Hisobingiz tekshirilmoqda. Admin tasdiqlashini kuting.","ru":"Аккаунт проверяется. Ожидайте подтверждения администратора."},
    "main":          {"uz":"Asosiy menyu:","ru":"Главное меню:"},
    "qabul":         {"uz":"Zavoddan qabul","ru":"Получить с завода"},
    "buyurtma":      {"uz":"Buyurtmalar","ru":"Заказы"},
    "topshir":       {"uz":"Mahsulot topshirish","ru":"Передать товар"},
    "tolov":         {"uz":"Tolov","ru":"Оплата"},
    "natija":        {"uz":"Kunlik natija","ru":"Итог дня"},
    "ombor":         {"uz":"Ombor","ru":"Склад"},
    "marshrut":      {"uz":"Marshrut","ru":"Маршрут"},
    "hisobot":       {"uz":"Hisobot","ru":"Отчёт"},
    "my_stores":     {"uz":"Mening dokonlarim","ru":"Мои магазины"},
    "admin":         {"uz":"Admin panel","ru":"Админ панель"},
    "back":          {"uz":"Orqaga","ru":"Назад"},
    "naqd":          {"uz":"Naqd","ru":"Наличные"},
    "qarz_btn":      {"uz":"Qarz","ru":"Долг"},
    "prod":          {"uz":"Mahsulotni tanlang:","ru":"Выберите товар:"},
    "store":         {"uz":"Dokonni tanlang:","ru":"Выберите магазин:"},
    "no_store":      {"uz":"Dokonlar topilmadi. Qoshish uchun Mening dokonlarim tugmasini bosing.","ru":"Магазины не найдены. Нажмите Мои магазины чтобы добавить."},
    "sum":           {"uz":"Summa kiriting (masalan: 15000):","ru":"Введите сумму (например: 15000):"},
    "qty":           {"uz":"Miqdorni kiriting (masalan: 5):","ru":"Введите количество (например: 5):"},
    "pay":           {"uz":"Tolov usuli:","ru":"Способ оплаты:"},
    "ok":            {"uz":"Saqlandi!","ru":"Сохранено!"},
    "err":           {"uz":"Raqam kiriting! Masalan: 5 yoki 5.5","ru":"Введите число! Например: 5 или 5.5"},
    "no_admin":      {"uz":"Siz admin emassiz!","ru":"Вы не администратор!"},
    "adm":           {"uz":"Admin paneli:","ru":"Админ панель:"},
    "price_btn":     {"uz":"Narx ozgartirish","ru":"Изменить цены"},
    "add_store":     {"uz":"Dokon qoshish (Admin)","ru":"Добавить магазин (Админ)"},
    "add_dist":      {"uz":"Distribyutor qoshish","ru":"Добавить дистрибьютора"},
    "stats":         {"uz":"Statistika","ru":"Статистика"},
    "broadcast":     {"uz":"Hammaga xabar","ru":"Рассылка"},
    "debtors":       {"uz":"Qarzdorlar","ru":"Должники"},
    "new_price":     {"uz":"Yangi narx (masalan: 15000):","ru":"Новая цена (например: 15000):"},
    "tannarx":       {"uz":"Tannarx (masalan: 12000):","ru":"Себестоимость (например: 12000):"},
    "week":          {"uz":"Haftalik","ru":"Недельный"},
    "month":         {"uz":"Oylik","ru":"Месячный"},
    "send_loc":      {"uz":"Lokatsiyangizni yuboring:","ru":"Отправьте геолокацию:"},
    "loc_btn":       {"uz":"Lokatsiyani yuborish","ru":"Отправить геолокацию"},
    "skip":          {"uz":"Otkazib yuborish","ru":"Пропустить"},
    "broadcast_msg": {"uz":"Xabar matnini kiriting:","ru":"Введите текст рассылки:"},
    "photo_scale":   {"uz":"Tarozi rasmini yuboring YOKI ogirlikni kiriting (masalan: 3.455):","ru":"Фото весов ИЛИ введите вес (например: 3.455):"},
    "ocr_weight":    {"uz":"Rasmdan oqildi: {v} kg\nTogri bo'lsa HA bosing, yoki to'g'ri raqamni kiriting:","ru":"С фото считано: {v} кг\nЕсли верно нажмите ДА, или введите правильное число:"},
    "ocr_fail":      {"uz":"Rasmdan oqib bolmadi. Ogirlikni qolda kiriting (masalan: 3.455):","ru":"Не удалось считать. Введите вес вручную (например: 3.455):"},
    "reading":       {"uz":"Rasm oqilmoqda...","ru":"Читаю изображение..."},
    # Do'kon qo'shish (distribyutor)
    "my_store_name": {"uz":"Dokon nomini kiriting:","ru":"Введите название магазина:"},
    "my_store_mchj": {"uz":"MCHJ nomini kiriting (yoki Otkazib yuborish):","ru":"Введите название ООО (или Пропустить):"},
    "my_store_tel1": {"uz":"Dokon telefon raqami 1:","ru":"Телефон магазина 1:"},
    "my_store_tel2": {"uz":"Telefon raqami 2 (yoki Otkazib yuborish):","ru":"Телефон 2 (или Пропустить):"},
    "my_store_loc":  {"uz":"Dokon lokatsiyasini yuboring (yoki Otkazib yuborish):","ru":"Отправьте локацию магазина (или Пропустить):"},
    "store_added":   {"uz":"Dokon qoshildi: {name}","ru":"Магазин добавлен: {name}"},
    # Admin do'kon qo'shish
    "adm_store_name":{"uz":"Dokon nomini kiriting:","ru":"Название магазина:"},
    "adm_store_manz":{"uz":"Manzilni kiriting:","ru":"Введите адрес:"},
    "adm_store_dist":{"uz":"Distribyutor Telegram ID:","ru":"Telegram ID дистрибьютора:"},
    "adm_store_loc": {"uz":"Lokatsiyani yuboring (yoki Otkazib yuborish):","ru":"Отправьте локацию (или Пропустить):"},
    "phone_btn":     {"uz":"Telefon raqamni yuborish","ru":"Отправить номер телефона"},
}

def tx(k, la="uz", **kw):
    t = T.get(k,{}).get(la, k)
    return t.format(**kw) if kw else t

def lg(ctx):     return ctx.user_data.get("lang","uz")
def is_adm(ctx): return ctx.user_data.get("is_admin", False)
def uname(upd):  u=upd.effective_user; return u.full_name or u.username or str(u.id)

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
    rows = [[s.get("Nomi","")] for s in stores]
    rows.append([tx("back",la)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def back_kb(la):
    return ReplyKeyboardMarkup([[tx("back",la)]], resize_keyboard=True)

def skip_back_kb(la):
    return ReplyKeyboardMarkup([[tx("skip",la)],[tx("back",la)]], resize_keyboard=True)

def loc_kb(la):
    btn = KeyboardButton(tx("loc_btn",la), request_location=True)
    return ReplyKeyboardMarkup([[btn],[tx("skip",la)],[tx("back",la)]], resize_keyboard=True)

def phone_kb(la):
    btn = KeyboardButton(tx("phone_btn",la), request_contact=True)
    return ReplyKeyboardMarkup([[btn],[tx("back",la)]], resize_keyboard=True)

def yes_kb(la):
    yes = "HA" if la=="uz" else "ДА"
    return ReplyKeyboardMarkup([[yes, tx("back",la)]], resize_keyboard=True)

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

    # Ro'yxatdan o'tganmi?
    if uid in ADMIN_IDS:
        await ctx.bot.send_message(uid, tx("main",la), reply_markup=main_kb(la, True))
        return MAIN_MENU

    if is_registered(uid):
        user = get_user(uid)
        name = user.get("Ism","") if user else ""
        await ctx.bot.send_message(uid, f"Xush kelibsiz, {name}!" if la=="uz" else f"Добро пожаловать, {name}!",
            reply_markup=main_kb(la, False))
        return MAIN_MENU

    # Yangi foydalanuvchi → ro'yxat
    await ctx.bot.send_message(uid, tx("register_name",la),
        reply_markup=ReplyKeyboardMarkup([[tx("back",la)]], resize_keyboard=True))
    return REGISTER_NAME

# ── RO'YXATDAN O'TISH ─────────────────────────────────────────────────────────
async def reg_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    ctx.user_data["reg_name"] = upd.message.text
    await upd.message.reply_text(tx("register_fname",la), reply_markup=back_kb(la))
    return REGISTER_PHONE

async def reg_phone(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    # Familiyani olish
    ctx.user_data["reg_fname"] = upd.message.text
    await upd.message.reply_text(tx("register_phone",la), reply_markup=phone_kb(la))
    return REGISTER_PASSPORT

async def reg_passport(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    # Telefon raqam
    if upd.message.contact:
        ctx.user_data["reg_phone"] = upd.message.contact.phone_number
    else:
        ctx.user_data["reg_phone"] = upd.message.text
    await upd.message.reply_text(tx("register_pass",la),
        reply_markup=ReplyKeyboardMarkup([[tx("skip",la)]], resize_keyboard=True))
    return MAIN_MENU

async def reg_finish(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    uid = upd.effective_user.id
    name = ctx.user_data.get("reg_name","")
    fname = ctx.user_data.get("reg_fname","")
    phone = ctx.user_data.get("reg_phone","")

    # Passport rasmi
    passport_info = ""
    if upd.message.photo:
        passport_info = "rasm_yuborildi"
    elif upd.message.text == tx("skip",la):
        passport_info = "otkazildi"
    else:
        passport_info = upd.message.text

    db_append("Foydalanuvchilar",[
        str(uid), name, fname, phone,
        "distributor", la, passport_info, now_str()
    ])

    # Adminlarga xabar
    for admin_id in ADMIN_IDS:
        try:
            await upd.get_bot().send_message(
                admin_id,
                f"Yangi distribyutor:\nIsm: {name} {fname}\nTel: {phone}\nID: {uid}"
                if la=="uz" else
                f"Новый дистрибьютор:\nИмя: {name} {fname}\nТел: {phone}\nID: {uid}"
            )
        except Exception:
            pass

    await upd.message.reply_text(
        tx("register_ok", la, name=name),
        reply_markup=main_kb(la, False)
    )
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
    if t == tx("marshrut",la): await marshrut(upd, ctx); return MAIN_MENU

    if t == tx("hisobot",la):
        await upd.message.reply_text(
            "Hisobot:" if la=="uz" else "Отчёт:",
            reply_markup=ReplyKeyboardMarkup(
                [[tx("week",la), tx("month",la)],[tx("back",la)]], resize_keyboard=True))
        return HISOBOT_MENU

    if t == tx("my_stores",la):
        await my_stores_menu(upd, ctx); return MY_STORE_NAME

    if t == tx("admin",la) and is_adm(ctx):
        await upd.message.reply_text(tx("adm",la), reply_markup=ReplyKeyboardMarkup([
            [tx("price_btn",la), tx("add_store",la)],
            [tx("add_dist",la),  tx("stats",la)],
            [tx("debtors",la),   tx("broadcast",la)],
            [tx("back",la)],
        ], resize_keyboard=True))
        return ADMIN_MENU

    await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la, is_adm(ctx)))
    return MAIN_MENU

# ── MENING DO'KONLARIM (Distribyutor o'zi qo'shadi) ──────────────────────────
async def my_stores_menu(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    uid = upd.effective_user.id
    stores = get_stores(uid)
    lines = ["Mening dokonlarim:" if la=="uz" else "Мои магазины:","---"]
    for s in stores:
        debt = get_debt(s.get("Nomi",""))
        d = f" (Qarz: {debt:,.0f})" if debt>0 else ""
        lines.append(f"• {s.get('Nomi','')}{d}")
    lines.append("\n" + ("Yangi dokon qoshish uchun nomini kiriting:" if la=="uz" else "Для добавления магазина введите название:"))
    await upd.message.reply_text("\n".join(lines), reply_markup=back_kb(la))

async def my_store_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU
    ctx.user_data["ns"] = t
    await upd.message.reply_text(tx("my_store_mchj",la), reply_markup=skip_back_kb(la))
    return MY_STORE_MCHJ

async def my_store_mchj(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    ctx.user_data["ns_mchj"] = "" if t==tx("skip",la) else t
    await upd.message.reply_text(tx("my_store_tel1",la), reply_markup=back_kb(la))
    return MY_STORE_TEL1

async def my_store_tel1(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    if upd.message.contact:
        ctx.user_data["ns_tel1"] = upd.message.contact.phone_number
    else:
        ctx.user_data["ns_tel1"] = upd.message.text
    await upd.message.reply_text(tx("my_store_tel2",la), reply_markup=skip_back_kb(la))
    return MY_STORE_TEL2

async def my_store_tel2(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    ctx.user_data["ns_tel2"] = "" if t==tx("skip",la) else t
    await upd.message.reply_text(tx("my_store_loc",la), reply_markup=loc_kb(la))
    return MY_STORE_LOC

async def my_store_loc(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    uid = upd.effective_user.id
    store  = ctx.user_data.get("ns","")
    mchj   = ctx.user_data.get("ns_mchj","")
    tel1   = ctx.user_data.get("ns_tel1","")
    tel2   = ctx.user_data.get("ns_tel2","")
    lat, lng = "", ""

    if upd.message.location:
        lat = str(upd.message.location.latitude)
        lng = str(upd.message.location.longitude)

    cnt = len(db_all("Dokonlar")) + 1
    # Dist_ID = distribyutorning o'z TG ID si
    db_append("Dokonlar",[cnt, store, mchj, tel1, tel2, str(uid), lat, lng, now_str()])

    await upd.message.reply_text(
        tx("store_added", la, name=store),
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
    try:
        qty = fmt_price(t)
        if qty <= 0: raise ValueError
    except Exception:
        await upd.message.reply_text(tx("err",la)); return QABUL_QTY
    p = ctx.user_data["p"]; uid = upd.effective_user.id
    price, _ = get_price(p["id"])
    total = qty * price
    db_append("Qabul",[now_str(), str(uid), uname(upd), p[la], qty, p["unit"], price, total])
    await upd.message.reply_text(
        f"{tx('ok',la)}\n{p[la]}: {qty} {p['unit']}\nNarx: {price:,.0f}\nJami: {total:,.0f} som",
        reply_markup=prod_kb(la))
    return QABUL_PROD

# ── TOPSHIRISH (TAROZI OCR) ───────────────────────────────────────────────────
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

    # Rasm yuborilgan
    if upd.message.photo:
        await upd.message.reply_text(tx("reading",la))
        photo = upd.message.photo[-1]
        file = await ctx.bot.get_file(photo.file_id)
        img_bytes = bytes(await file.download_as_bytearray())
        raw_text = await vision_ocr(img_bytes)
        logger.info(f"OCR raw text: {repr(raw_text)}")

        weight = parse_scale_weight(raw_text)
        if weight > 0:
            ctx.user_data["_ocr_weight"] = weight
            await upd.message.reply_text(
                tx("ocr_weight", la, v=weight),
                reply_markup=yes_kb(la))
            return TOPSHIR_PHOTO

        await upd.message.reply_text(tx("ocr_fail",la), reply_markup=back_kb(la))
        return TOPSHIR_PHOTO

    # Matn
    t = upd.message.text or ""
    if t == tx("back",la):
        await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return TOPSHIR_PROD

    # OCR tasdiqlash
    if "_ocr_weight" in ctx.user_data:
        if t.upper() in ["HA","ДА","YES","OK"]:
            qty = ctx.user_data.pop("_ocr_weight")
        else:
            ctx.user_data.pop("_ocr_weight", None)
            try:
                qty = fmt_price(t)
                if qty <= 0: raise ValueError
            except Exception:
                await upd.message.reply_text(tx("err",la)); return TOPSHIR_PHOTO
    else:
        try:
            qty = fmt_price(t)
            if qty <= 0: raise ValueError
        except Exception:
            await upd.message.reply_text(tx("err",la)); return TOPSHIR_PHOTO

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
    try:
        amount = fmt_price(t)
        if amount <= 0: raise ValueError
    except Exception:
        await upd.message.reply_text(tx("err",la)); return TOLOV_AMOUNT
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
    try:
        qty = fmt_price(t)
        if qty <= 0: raise ValueError
    except Exception:
        await upd.message.reply_text(tx("err",la)); return BUYURTMA_QTY
    p = ctx.user_data["p"]; store = ctx.user_data["s"]; uid = upd.effective_user.id
    db_append("Buyurtmalar",[now_str(), str(uid), store, p[la], qty, "Kutilmoqda"])
    await upd.message.reply_text(
        f"{tx('ok',la)}\nDokon: {store}\n{p[la]}: {qty} {p['unit']}",
        reply_markup=prod_kb(la)); return BUYURTMA_PROD

# ── MARSHRUT ──────────────────────────────────────────────────────────────────
async def marshrut(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
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
        sales=[r for r in db_all("Topshirish") if r.get("Sana","")>=from_dt and str(r.get("Dist_ID",""))==uid]
        pays =[r for r in db_all("Tolov")      if r.get("Sana","")>=from_dt and str(r.get("Dist_ID",""))==uid]
        ins  =[r for r in db_all("Qabul")       if r.get("Sana","")>=from_dt and str(r.get("Dist_ID",""))==uid]
        ts=sum(float(r.get("Jami",0)) for r in sales)
        tc=sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Naqd")
        td=sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Qarz")
        ti=sum(float(r.get("Jami",0)) for r in ins)
        ps={}
        for r in sales: k=r.get("Mahsulot",""); ps[k]=ps.get(k,0)+float(r.get("Miqdor",0))
        top=sorted(ps.items(),key=lambda x:x[1],reverse=True)[:3]
        top_txt="\n".join([f"  {p}: {q:.2f}" for p,q in top]) or "  -"
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
        sales=[r for r in db_all("Topshirish") if r.get("Sana","").startswith(today) and str(r.get("Dist_ID",""))==uid]
        pays =[r for r in db_all("Tolov")      if r.get("Sana","").startswith(today) and str(r.get("Dist_ID",""))==uid]
        ins  =[r for r in db_all("Qabul")       if r.get("Sana","").startswith(today) and str(r.get("Dist_ID",""))==uid]
        ts=sum(float(r.get("Jami",0)) for r in sales)
        tc=sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Naqd")
        td=sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Qarz")
        ti=sum(float(r.get("Jami",0)) for r in ins)
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
            if str(r.get("Dist_ID",""))==uid: k=r.get("Mahsulot",""); st[k]=st.get(k,0)+float(r.get("Miqdor",0))
        for r in db_all("Topshirish"):
            if str(r.get("Dist_ID",""))==uid: k=r.get("Mahsulot",""); st[k]=st.get(k,0)-float(r.get("Miqdor",0))
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
    if t==tx("back",la):      await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,True)); return MAIN_MENU
    if t==tx("price_btn",la): await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    if t==tx("add_store",la): await upd.message.reply_text(tx("adm_store_name",la), reply_markup=back_kb(la)); return ADM_STORE_NAME
    if t==tx("add_dist",la):  await upd.message.reply_text(tx("adm_dist_name",la) if "adm_dist_name" in T else "Ism:", reply_markup=back_kb(la)); return ADM_DIST_NAME
    if t==tx("stats",la):     await a_stats(upd,ctx); return ADMIN_MENU
    if t==tx("debtors",la):   await a_debtors(upd,ctx); return ADMIN_MENU
    if t==tx("broadcast",la): await upd.message.reply_text(tx("broadcast_msg",la), reply_markup=back_kb(la)); return ADM_BROADCAST
    return ADMIN_MENU

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
    try: price=fmt_price(t);
    except Exception: await upd.message.reply_text(tx("err",la)); return ADM_PRICE_VAL
    ctx.user_data["np"]=price
    await upd.message.reply_text(tx("tannarx",la)); return ADM_COST_VAL

async def a_cost_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    try: cost=fmt_price(t)
    except Exception: await upd.message.reply_text(tx("err",la)); return ADM_COST_VAL
    p=ctx.user_data["p"]; price=ctx.user_data["np"]
    set_price(p["id"],p[la],price,cost)
    await upd.message.reply_text(f"Yangilandi!\n{p[la]}: {price:,.0f} / {cost:,.0f}", reply_markup=main_kb(la,True))
    return MAIN_MENU

async def a_store_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["ns"]=t
    await upd.message.reply_text(tx("adm_store_manz",la), reply_markup=back_kb(la)); return ADM_STORE_MANZIL

async def a_store_manzil(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    ctx.user_data["ns_manzil"]=upd.message.text
    await upd.message.reply_text(tx("adm_store_dist",la)); return ADM_STORE_DIST

async def a_store_dist(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    ctx.user_data["ns_dist"]=upd.message.text
    await upd.message.reply_text(tx("adm_store_loc",la), reply_markup=loc_kb(la)); return ADM_STORE_LOC

async def a_store_loc(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    store=ctx.user_data.get("ns",""); manzil=ctx.user_data.get("ns_manzil",""); dist_id=ctx.user_data.get("ns_dist","")
    lat,lng="",""
    if upd.message.location: lat=str(upd.message.location.latitude); lng=str(upd.message.location.longitude)
    cnt=len(db_all("Dokonlar"))+1
    db_append("Dokonlar",[cnt, store, manzil, "", "", str(dist_id), lat, lng, now_str()])
    await upd.message.reply_text(f"Dokon qoshildi: {store}" if la=="uz" else f"Магазин добавлен: {store}", reply_markup=main_kb(la,True))
    return MAIN_MENU

async def a_dist_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["nd"]=t
    await upd.message.reply_text("Telegram ID:", reply_markup=back_kb(la)); return ADM_DIST_ID

async def a_dist_id(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); name=ctx.user_data.get("nd","")
    db_append("Foydalanuvchilar",[upd.message.text, name, "", "", "distributor", la, "", now_str()])
    await upd.message.reply_text(f"Qoshildi: {name}", reply_markup=main_kb(la,True)); return MAIN_MENU

async def a_stats(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    try:
        sales=db_all("Topshirish"); pays=db_all("Tolov"); ins=db_all("Qabul"); stores=db_all("Dokonlar")
        ts=sum(float(r.get("Jami",0)) for r in sales)
        tc=sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Naqd")
        td=sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Qarz")
        ti=sum(float(r.get("Jami",0)) for r in ins)
        ps={}
        for r in sales: k=r.get("Mahsulot",""); ps[k]=ps.get(k,0)+float(r.get("Miqdor",0))
        top=sorted(ps.items(),key=lambda x:x[1],reverse=True)[:3]
        top_txt="\n".join([f"  {p}: {q:.2f}" for p,q in top]) or "  -"
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
                did=str(s.get("Dist_ID",""))
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
            REGISTER_NAME:    [MessageHandler(txt, reg_name)],
            REGISTER_PHONE:   [MessageHandler(txt, reg_phone)],
            REGISTER_PASSPORT:[MessageHandler((filters.PHOTO|filters.TEXT|filters.CONTACT)&~filters.COMMAND, reg_finish)],
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
