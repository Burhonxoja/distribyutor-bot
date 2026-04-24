import os
import logging
import json
import re
import base64
from datetime import datetime, timedelta, time as dtime
import gspread
from google.oauth2.service_account import Credentials
from telegram import (
    Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, KeyboardButton
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, filters, ContextTypes
)
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── ENV ──────────────────────────────────────────────────────────
BOT_TOKEN         = os.environ.get("BOT_TOKEN", "")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON", "")
SPREADSHEET_ID    = os.environ.get("SPREADSHEET_ID", "")
ADMIN_IDS         = [int(x) for x in os.environ.get("ADMIN_IDS", "0").split(",") if x.strip()]

# ── MAHSULOTLAR ──────────────────────────────────────────────────
PRODUCTS = [
    {"id":1,  "uz":"Tvorog",        "ru":"Tvorog",        "unit":"kg"},
    {"id":2,  "uz":"Sut",           "ru":"Sut",           "unit":"litr"},
    {"id":3,  "uz":"Qatiq",         "ru":"Qatiq",         "unit":"kg"},
    {"id":4,  "uz":"Brinza",        "ru":"Brinza",        "unit":"kg"},
    {"id":5,  "uz":"Qaymoq 0.4 kg", "ru":"Qaymoq 0.4 kg","unit":"dona"},
    {"id":6,  "uz":"Qaymoq 0.2 kg", "ru":"Qaymoq 0.2 kg","unit":"dona"},
    {"id":7,  "uz":"Suzma 0.5 kg",  "ru":"Suzma 0.5 kg", "unit":"kg"},
    {"id":8,  "uz":"Qurt",          "ru":"Qurt",          "unit":"dona"},
    {"id":9,  "uz":"Tosh qurt",     "ru":"Tosh qurt",     "unit":"dona"},
]

# ── STATES ───────────────────────────────────────────────────────
(
    LANG_SELECT, MAIN_MENU,
    QABUL_PROD, QABUL_QTY,
    TOPSHIR_STORE, TOPSHIR_PROD, TOPSHIR_PHOTO,
    BUYURTMA_STORE, BUYURTMA_PROD, BUYURTMA_QTY,
    TOLOV_STORE, TOLOV_AMOUNT, TOLOV_METHOD,
    ADMIN_MENU,
    ADM_PRICE_PROD, ADM_PRICE_VAL, ADM_COST_VAL,
    ADM_STORE_NAME, ADM_STORE_MANZIL, ADM_STORE_DIST, ADM_STORE_LOC,
    ADM_DIST_NAME, ADM_DIST_ID,
    ADM_BROADCAST,
    HISOBOT_MENU,
) = range(25)

# ── GOOGLE SHEETS ────────────────────────────────────────────────
def get_sheet():
    if not GOOGLE_CREDS_JSON:
        return None
    try:
        creds = Credentials.from_service_account_info(
            json.loads(GOOGLE_CREDS_JSON),
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/cloud-vision",
            ]
        )
        return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)
    except Exception as e:
        logger.error(f"Sheet: {e}")
        return None

HEADERS = {
    "Qabul":            ["Sana","Dist_ID","Ism","Mahsulot","Miqdor","Birlik","Narx","Jami"],
    "Topshirish":       ["Sana","Dist_ID","Dokon","Mahsulot","Miqdor","Birlik","Narx","Jami"],
    "Tolov":            ["Sana","Dist_ID","Dokon","Summa","Usul","Izoh"],
    "Foydalanuvchilar": ["TG_ID","Ism","Rol","Til","Sana"],
    "Dokonlar":         ["ID","Nomi","Manzil","Dist_ID","Lat","Lng","Sana"],
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
            if int(r.get("Mahsulot_ID", 0)) == pid:
                return float(r.get("Narx", 0)), float(r.get("Tannarx", 0))
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
            if int(r.get("Mahsulot_ID", 0)) == pid:
                w.update(f"A{i+2}:E{i+2}", [[pid, pname, price, cost, now]])
                return
        w.append_row([pid, pname, price, cost, now])
    except Exception as e:
        logger.error(f"set_price: {e}")

def get_stores(dist_id=None):
    try:
        recs = db_all("Dokonlar")
        if dist_id:
            return [r for r in recs if str(r.get("Dist_ID","")) == str(dist_id)]
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

def now_str():  return datetime.now().strftime("%Y-%m-%d %H:%M")
def today_str():return datetime.now().strftime("%Y-%m-%d")

# ── GOOGLE VISION OCR ────────────────────────────────────────────
async def vision_ocr(image_bytes: bytes, mode: str = "scale") -> str:
    """
    mode: 'scale' = tarozi, 'check' = chek, 'address' = manzil, 'count' = soni
    Google Vision API ishlatadi — bepul
    """
    try:
        creds_dict = json.loads(GOOGLE_CREDS_JSON)
        # Access token olish
        import google.auth.transport.requests
        from google.oauth2.service_account import Credentials as GCreds
        gcreds = GCreds.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/cloud-vision"]
        )
        gcreds.refresh(google.auth.transport.requests.Request())
        token = gcreds.token

        b64 = base64.b64encode(image_bytes).decode()
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://vision.googleapis.com/v1/images:annotate",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "requests": [{
                        "image": {"content": b64},
                        "features": [{"type": "TEXT_DETECTION", "maxResults": 1}]
                    }]
                }
            )
            data = resp.json()
            text = data["responses"][0].get("fullTextAnnotation", {}).get("text", "")
            return text.strip()
    except Exception as e:
        logger.error(f"Vision OCR error: {e}")
        return ""

def parse_weight(text: str) -> float:
    """Matndan og'irlik raqamini ajratib oladi"""
    # kg, г, g, кг kabi belgilar oldidagi raqamlarni qidiradi
    patterns = [
        r'(\d+[\.,]\d+)\s*(?:kg|кг|KG)',
        r'(\d+[\.,]\d+)',
        r'(\d+)\s*(?:kg|кг|KG)',
        r'(\d+)',
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            try:
                return float(m.group(1).replace(",", "."))
            except Exception:
                pass
    return 0.0

def parse_sum(text: str) -> float:
    """Matndan summa raqamini ajratib oladi"""
    # Eng katta raqamni topadi (chek summasi)
    nums = re.findall(r'\d[\d\s]*[\.,]?\d*', text)
    values = []
    for n in nums:
        try:
            v = float(n.replace(" ","").replace(",","."))
            if v > 100:  # Juda kichik raqamlarni o'tkazib yuboramiz
                values.append(v)
        except Exception:
            pass
    return max(values) if values else 0.0

def parse_count(text: str) -> float:
    """Matndan soni ajratib oladi"""
    nums = re.findall(r'\d+[\.,]?\d*', text)
    for n in nums:
        try:
            v = float(n.replace(",","."))
            if 0 < v < 10000:
                return v
        except Exception:
            pass
    return 0.0

# ── MATNLAR ──────────────────────────────────────────────────────
T = {
    "start":       {"uz":"Tilni tanlang:","ru":"Выберите язык:"},
    "main":        {"uz":"Asosiy menyu:","ru":"Главное меню:"},
    "qabul":       {"uz":"Zavoddan qabul","ru":"Получить с завода"},
    "buyurtma":    {"uz":"Buyurtmalar","ru":"Заказы"},
    "topshir":     {"uz":"Mahsulot topshirish","ru":"Передать товар"},
    "tolov":       {"uz":"Tolov","ru":"Оплата"},
    "natija":      {"uz":"Kunlik natija","ru":"Итог дня"},
    "ombor":       {"uz":"Ombor","ru":"Склад"},
    "marshrut":    {"uz":"Marshrut","ru":"Маршрут"},
    "hisobot":     {"uz":"Hisobot","ru":"Отчёт"},
    "admin":       {"uz":"Admin panel","ru":"Админ панель"},
    "back":        {"uz":"Orqaga","ru":"Назад"},
    "naqd":        {"uz":"Naqd","ru":"Наличные"},
    "qarz_btn":    {"uz":"Qarz","ru":"Долг"},
    "prod":        {"uz":"Mahsulotni tanlang:","ru":"Выберите товар:"},
    "qty":         {"uz":"Miqdorni kiriting (masalan: 10):","ru":"Введите количество (например: 10):"},
    "store":       {"uz":"Dokonni tanlang:","ru":"Выберите магазин:"},
    "no_store":    {"uz":"Dokonlar topilmadi. Admin qoshsin.","ru":"Магазины не найдены."},
    "sum":         {"uz":"Summa kiriting:","ru":"Введите сумму:"},
    "pay":         {"uz":"Tolov usuli:","ru":"Способ оплаты:"},
    "ok":          {"uz":"Saqlandi!","ru":"Сохранено!"},
    "err":         {"uz":"Raqam kiriting!","ru":"Введите число!"},
    "no_admin":    {"uz":"Siz admin emassiz!","ru":"Вы не администратор!"},
    "adm":         {"uz":"Admin paneli:","ru":"Админ панель:"},
    "price_btn":   {"uz":"Narx ozgartirish","ru":"Изменить цены"},
    "add_store":   {"uz":"Dokon qoshish","ru":"Добавить магазин"},
    "add_dist":    {"uz":"Distribyutor qoshish","ru":"Добавить дистрибьютора"},
    "stats":       {"uz":"Statistika","ru":"Статистика"},
    "broadcast":   {"uz":"Hammaga xabar","ru":"Рассылка"},
    "debtors":     {"uz":"Qarzdorlar","ru":"Должники"},
    "new_price":   {"uz":"Yangi narx (som):","ru":"Новая цена (сум):"},
    "tannarx":     {"uz":"Tannarx (som):","ru":"Себестоимость (сум):"},
    "sname":       {"uz":"Dokon nomini kiriting:","ru":"Название магазина:"},
    "smanzil":     {"uz":"Manzilni kiriting:","ru":"Введите адрес:"},
    "sdist":       {"uz":"Distribyutor Telegram ID:","ru":"Telegram ID дистрибьютора:"},
    "sloc":        {"uz":"Lokatsiyani yuboring yoki otkazib yuboring:","ru":"Отправьте локацию или пропустите:"},
    "dname":       {"uz":"Distribyutor ismini kiriting:","ru":"Имя дистрибьютора:"},
    "week":        {"uz":"Haftalik","ru":"Недельный"},
    "month":       {"uz":"Oylik","ru":"Месячный"},
    "send_loc":    {"uz":"Lokatsiyangizni yuboring:","ru":"Отправьте геолокацию:"},
    "loc_btn":     {"uz":"Lokatsiyani yuborish","ru":"Отправить геолокацию"},
    "skip":        {"uz":"Otkazib yuborish","ru":"Пропустить"},
    "broadcast_msg":{"uz":"Xabar matnini kiriting:","ru":"Введите текст рассылки:"},
    # OCR
    "photo_scale": {"uz":"Tarozi rasmini yuboring YOKI ogirlikni qolda kiriting:","ru":"Фото весов ИЛИ введите вес вручную:"},
    "photo_check": {"uz":"Chek rasmini yuboring YOKI summani qolda kiriting:","ru":"Фото чека ИЛИ введите сумму вручную:"},
    "photo_addr":  {"uz":"Dokon rasmini yuboring (manzil aniqlanadi):","ru":"Фото магазина (определим адрес):"},
    "photo_count": {"uz":"Mahsulot rasmini yuboring YOKI sonini qolda kiriting:","ru":"Фото товара ИЛИ введите количество вручную:"},
    "ocr_weight":  {"uz":"Rasmdan oqildi: {v} kg. Togri? (ha / boshqa raqam kiriting)","ru":"С фото: {v} кг. Верно? (да / введите другое)"},
    "ocr_sum":     {"uz":"Rasmdan oqildi: {v} som. Togri? (ha / boshqa raqam kiriting)","ru":"С фото: {v} сум. Верно? (да / введите другое)"},
    "ocr_addr":    {"uz":"Manzil aniqlandi: {v}","ru":"Адрес определён: {v}"},
    "ocr_count":   {"uz":"Rasmdan oqildi: {v} dona. Togri? (ha / boshqa raqam kiriting)","ru":"С фото: {v} шт. Верно? (да / введите другое)"},
    "ocr_fail":    {"uz":"Rasmdan oqib bolmadi. Qolda kiriting:","ru":"Не удалось считать. Введите вручную:"},
    "reading":     {"uz":"Rasm oqilmoqda...","ru":"Читаю изображение..."},
}

def tx(k, la="uz", **kw):
    t = T.get(k, {}).get(la, k)
    return t.format(**kw) if kw else t

def lg(ctx):    return ctx.user_data.get("lang", "uz")
def is_adm(ctx):return ctx.user_data.get("is_admin", False)
def uname(upd): u=upd.effective_user; return u.full_name or u.username or str(u.id)
def find_prod(name, la): return next((p for p in PRODUCTS if p[la]==name), None)

def main_kb(la, admin=False):
    rows = [
        [tx("qabul",la),   tx("buyurtma",la)],
        [tx("topshir",la), tx("tolov",la)],
        [tx("natija",la),  tx("ombor",la)],
        [tx("marshrut",la),tx("hisobot",la)],
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

def loc_kb(la):
    btn = KeyboardButton(tx("loc_btn",la), request_location=True)
    return ReplyKeyboardMarkup([[btn],[tx("skip",la)],[tx("back",la)]], resize_keyboard=True)

def yes_kb(la):
    yes = "ha" if la=="uz" else "да"
    return ReplyKeyboardMarkup([[yes, tx("back",la)]], resize_keyboard=True)

# ── START ────────────────────────────────────────────────────────
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
    db_append("Foydalanuvchilar",[str(uid), uname(upd), "admin" if uid in ADMIN_IDS else "distributor", la, now_str()])
    await q.edit_message_text("Til tanlandi!" if la=="uz" else "Язык выбран!")
    await ctx.bot.send_message(uid, tx("main",la), reply_markup=main_kb(la, uid in ADMIN_IDS))
    return MAIN_MENU

# ── MAIN MENU ────────────────────────────────────────────────────
async def main_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text; uid = upd.effective_user.id

    if t == tx("qabul",la):
        await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return QABUL_PROD
    if t == tx("topshir",la):
        stores = get_stores(uid) or get_stores()
        if not stores: await upd.message.reply_text(tx("no_store",la)); return MAIN_MENU
        ctx.user_data["stores"] = stores
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la)); return TOPSHIR_STORE
    if t == tx("tolov",la):
        stores = get_stores(uid) or get_stores()
        if not stores: await upd.message.reply_text(tx("no_store",la)); return MAIN_MENU
        ctx.user_data["stores"] = stores
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la)); return TOLOV_STORE
    if t == tx("buyurtma",la):
        stores = get_stores(uid) or get_stores()
        if not stores: await upd.message.reply_text(tx("no_store",la)); return MAIN_MENU
        ctx.user_data["stores"] = stores
        await upd.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la)); return BUYURTMA_STORE
    if t == tx("natija",la):   await daily(upd, ctx); return MAIN_MENU
    if t == tx("ombor",la):    await stock(upd, ctx); return MAIN_MENU
    if t == tx("marshrut",la): await marshrut(upd, ctx); return MAIN_MENU
    if t == tx("hisobot",la):
        await upd.message.reply_text(
            "Hisobot turini tanlang:" if la=="uz" else "Выберите тип отчёта:",
            reply_markup=ReplyKeyboardMarkup([[tx("week",la), tx("month",la)],[tx("back",la)]], resize_keyboard=True)
        ); return HISOBOT_MENU
    if t == tx("admin",la) and is_adm(ctx):
        await upd.message.reply_text(tx("adm",la), reply_markup=ReplyKeyboardMarkup([
            [tx("price_btn",la), tx("add_store",la)],
            [tx("add_dist",la),  tx("stats",la)],
            [tx("debtors",la),   tx("broadcast",la)],
            [tx("back",la)],
        ], resize_keyboard=True)); return ADMIN_MENU
    await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la, is_adm(ctx)))
    return MAIN_MENU

# ── QABUL ────────────────────────────────────────────────────────
async def qabul_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text
    if t == tx("back",la):
        await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU
    p = find_prod(t, la)
    if not p: await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return QABUL_PROD
    ctx.user_data["p"] = p
    await upd.message.reply_text(
        f"{t}\n\n{tx('photo_count',la)}",
        reply_markup=back_kb(la)
    ); return QABUL_QTY

async def qabul_qty(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    p = ctx.user_data["p"]; uid = upd.effective_user.id

    qty = await _process_photo_or_text(upd, ctx, mode="count")
    if qty is None: return QABUL_QTY  # hali kutilmoqda

    price, _ = get_price(p["id"])
    total = qty * price
    db_append("Qabul",[now_str(), str(uid), uname(upd), p[la], qty, p["unit"], price, total])
    await upd.message.reply_text(
        f"{tx('ok',la)}\n{p[la]}: {qty} {p['unit']}\nNarx: {price:,.0f}\nJami: {total:,.0f} som",
        reply_markup=prod_kb(la))
    return QABUL_PROD

# ── TOPSHIRISH ───────────────────────────────────────────────────
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
    await upd.message.reply_text(msg, reply_markup=prod_kb(la)); return TOPSHIR_PROD

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

    qty = await _process_photo_or_text(upd, ctx, mode="scale")
    if qty is None: return TOPSHIR_PHOTO

    price, _ = get_price(p["id"])
    total = qty * price
    db_append("Topshirish",[now_str(), str(uid), store, p[la], qty, p["unit"], price, total])
    await upd.message.reply_text(
        f"{tx('ok',la)}\nDokon: {store}\n{p[la]}: {qty} {p['unit']}\nJami: {total:,.0f} som",
        reply_markup=prod_kb(la))
    return TOPSHIR_PROD

# ── TOLOV ────────────────────────────────────────────────────────
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
    await upd.message.reply_text(
        f"{t}{debt_txt}\n\n{tx('photo_check',la)}",
        reply_markup=back_kb(la)); return TOLOV_AMOUNT

async def tolov_amount(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    amount = await _process_photo_or_text(upd, ctx, mode="check")
    if amount is None: return TOLOV_AMOUNT
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
        reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU

# ── BUYURTMA ─────────────────────────────────────────────────────
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
    if t == tx("back",la): await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return BUYURTMA_PROD
    try:
        qty = float(t.replace(",","."))
    except Exception:
        await upd.message.reply_text(tx("err",la)); return BUYURTMA_QTY
    p = ctx.user_data["p"]; store = ctx.user_data["s"]; uid = upd.effective_user.id
    db_append("Buyurtmalar",[now_str(), str(uid), store, p[la], qty, "Kutilmoqda"])
    await upd.message.reply_text(
        f"{tx('ok',la)}\nDokon: {store}\n{p[la]}: {qty} {p['unit']}\nStatus: Kutilmoqda",
        reply_markup=prod_kb(la)); return BUYURTMA_PROD

# ── OCR UMUMIY FUNKSIYA ──────────────────────────────────────────
async def _process_photo_or_text(upd, ctx, mode="scale"):
    """
    Rasm yoki matn qayta ishlaydi.
    None qaytarsa — foydalanuvchi yana javob kutilmoqda.
    Raqam qaytarsa — qiymat tayyor.
    """
    la = lg(ctx)

    # Rasm yuborilgan
    if upd.message.photo:
        await upd.message.reply_text(tx("reading",la))
        photo = upd.message.photo[-1]
        file = await ctx.bot.get_file(photo.file_id)
        img_bytes = bytes(await file.download_as_bytearray())
        raw_text = await vision_ocr(img_bytes, mode)

        if raw_text:
            if mode == "scale":
                val = parse_weight(raw_text)
                if val > 0:
                    ctx.user_data["_ocr_val"] = val
                    ctx.user_data["_ocr_mode"] = mode
                    await upd.message.reply_text(tx("ocr_weight",la,v=val), reply_markup=yes_kb(la))
                    return None
            elif mode == "check":
                val = parse_sum(raw_text)
                if val > 0:
                    ctx.user_data["_ocr_val"] = val
                    ctx.user_data["_ocr_mode"] = mode
                    await upd.message.reply_text(tx("ocr_sum",la,v=f"{val:,.0f}"), reply_markup=yes_kb(la))
                    return None
            elif mode == "address":
                # Matndan manzilni oladi
                addr = raw_text[:100] if raw_text else ""
                if addr:
                    ctx.user_data["ns_manzil"] = addr
                    await upd.message.reply_text(tx("ocr_addr",la,v=addr))
                    return addr
            elif mode == "count":
                val = parse_count(raw_text)
                if val > 0:
                    ctx.user_data["_ocr_val"] = val
                    ctx.user_data["_ocr_mode"] = mode
                    await upd.message.reply_text(tx("ocr_count",la,v=val), reply_markup=yes_kb(la))
                    return None

        await upd.message.reply_text(tx("ocr_fail",la), reply_markup=back_kb(la))
        return None

    # Matn yuborilgan
    t = upd.message.text or ""
    if t == tx("back",la):
        return -1  # Orqaga signal

    # OCR tasdiqlash (ha/да)
    if "_ocr_val" in ctx.user_data:
        yes_words = ["ha","да","yes","1","ok","OK"]
        if t.lower() in [w.lower() for w in yes_words]:
            val = ctx.user_data.pop("_ocr_val")
            ctx.user_data.pop("_ocr_mode", None)
            return val
        else:
            # Boshqa raqam kiritdi
            ctx.user_data.pop("_ocr_val", None)
            ctx.user_data.pop("_ocr_mode", None)
            try:
                return float(t.replace(",",".").replace(" ",""))
            except Exception:
                await upd.message.reply_text(tx("err",la)); return None

    # Oddiy qo'lda kiritish
    try:
        return float(t.replace(",",".").replace(" ",""))
    except Exception:
        await upd.message.reply_text(tx("err",la)); return None

# ── MARSHRUT ─────────────────────────────────────────────────────
async def marshrut(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); uid = upd.effective_user.id
    stores = get_stores(uid) or get_stores()
    if not stores: await upd.message.reply_text(tx("no_store",la)); return
    ctx.user_data["m_stores"] = stores
    await upd.message.reply_text(tx("send_loc",la), reply_markup=loc_kb(la))

async def marshrut_loc(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx)
    stores = ctx.user_data.get("m_stores", get_stores())
    lat, lng = 0, 0
    if upd.message.location:
        lat = upd.message.location.latitude
        lng = upd.message.location.longitude

    lines = ["Bugungi marshrut:" if la=="uz" else "Маршрут на сегодня:", "---"]
    for i, s in enumerate(stores, 1):
        debt = get_debt(s.get("Nomi",""))
        d = f" (Qarz: {debt:,.0f})" if debt>0 else ""
        lines.append(f"{i}. {s.get('Nomi','')}{d}")

    if lat and lng:
        waypoints = "|".join([
            s.get("Nomi","").replace(" ","+") for s in stores
        ])
        maps_url = f"https://www.google.com/maps/dir/?api=1&origin={lat},{lng}&destination={stores[-1].get('Nomi','').replace(' ','+')}&waypoints={waypoints}&travelmode=driving"
        lines.append(f"\nGoogle Maps:\n{maps_url}")

    await upd.message.reply_text("\n".join(lines), reply_markup=main_kb(la, is_adm(ctx)))

# ── HISOBOT ──────────────────────────────────────────────────────
async def hisobot_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); t = upd.message.text; uid = str(upd.effective_user.id)
    if t == tx("back",la):
        await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU

    days = 7 if t==tx("week",la) else 30
    from_dt = (datetime.now()-timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        sales = [r for r in db_all("Topshirish") if r.get("Sana","")>=from_dt and str(r.get("Dist_ID",""))==uid]
        pays  = [r for r in db_all("Tolov")      if r.get("Sana","")>=from_dt and str(r.get("Dist_ID",""))==uid]
        ins   = [r for r in db_all("Qabul")       if r.get("Sana","")>=from_dt and str(r.get("Dist_ID",""))==uid]

        ts = sum(float(r.get("Jami",0)) for r in sales)
        tc = sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Naqd")
        td = sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Qarz")
        ti = sum(float(r.get("Jami",0)) for r in ins)

        prod_st = {}
        for r in sales:
            k=r.get("Mahsulot",""); prod_st[k]=prod_st.get(k,0)+float(r.get("Miqdor",0))
        top = sorted(prod_st.items(), key=lambda x:x[1], reverse=True)[:3]
        top_txt = "\n".join([f"  {p}: {q:.1f}" for p,q in top]) or "  -"

        period = ("7 kun" if days==7 else "30 kun") if la=="uz" else ("7 дней" if days==7 else "30 дней")
        if la=="uz":
            msg=(f"Hisobot: {period}\n---\nQabul: {ti:,.0f} som\nSotuv: {ts:,.0f} som\n"
                 f"Naqd: {tc:,.0f} som\nQarz: {td:,.0f} som\n---\nTop mahsulotlar:\n{top_txt}")
        else:
            msg=(f"Отчёт: {period}\n---\nПолучено: {ti:,.0f} сум\nПродажи: {ts:,.0f} сум\n"
                 f"Наличные: {tc:,.0f} сум\nДолг: {td:,.0f} сум\n---\nТоп товары:\n{top_txt}")
    except Exception as e:
        msg = f"Xatolik: {e}"
    await upd.message.reply_text(msg, reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU

# ── KUNLIK / OMBOR ───────────────────────────────────────────────
async def daily(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lg(ctx); uid = str(upd.effective_user.id); today = today_str()
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
            if v>0: lines.append(f"{k}: {v:.1f}")
        if len(lines)==2: lines.append("Hammasi topshirilgan!" if la=="uz" else "Всё сдано!")
        await upd.message.reply_text("\n".join(lines))
    except Exception as e:
        await upd.message.reply_text(f"Xatolik: {e}")

# ── ADMIN ────────────────────────────────────────────────────────
async def admin_h(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text; uid=upd.effective_user.id
    if uid not in ADMIN_IDS: await upd.message.reply_text(tx("no_admin",la)); return MAIN_MENU
    if t==tx("back",la):      await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,True)); return MAIN_MENU
    if t==tx("price_btn",la): await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    if t==tx("add_store",la): await upd.message.reply_text(tx("sname",la), reply_markup=back_kb(la)); return ADM_STORE_NAME
    if t==tx("add_dist",la):  await upd.message.reply_text(tx("dname",la), reply_markup=back_kb(la)); return ADM_DIST_NAME
    if t==tx("stats",la):     await a_stats(upd,ctx); return ADMIN_MENU
    if t==tx("debtors",la):   await a_debtors(upd,ctx); return ADMIN_MENU
    if t==tx("broadcast",la): await upd.message.reply_text(tx("broadcast_msg",la), reply_markup=back_kb(la)); return ADM_BROADCAST
    return ADMIN_MENU

async def a_price_prod(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    p=find_prod(t,la)
    if not p: await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    ctx.user_data["p"]=p
    price,_=get_price(p["id"])
    await upd.message.reply_text(f"{t}\nJoriy: {price:,.0f}\n\n{tx('new_price',la)}", reply_markup=back_kb(la)); return ADM_PRICE_VAL

async def a_price_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): await upd.message.reply_text(tx("prod",la), reply_markup=prod_kb(la)); return ADM_PRICE_PROD
    try: price=float(t.replace(",",".").replace(" ",""))
    except Exception: await upd.message.reply_text(tx("err",la)); return ADM_PRICE_VAL
    ctx.user_data["np"]=price
    await upd.message.reply_text(tx("tannarx",la)); return ADM_COST_VAL

async def a_cost_val(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    try: cost=float(t.replace(",",".").replace(" ",""))
    except Exception: await upd.message.reply_text(tx("err",la)); return ADM_COST_VAL
    p=ctx.user_data["p"]; price=ctx.user_data["np"]
    set_price(p["id"],p[la],price,cost)
    await upd.message.reply_text(f"Yangilandi!\n{p[la]}: {price:,.0f} / {cost:,.0f}", reply_markup=main_kb(la,True)); return MAIN_MENU

async def a_store_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["ns"]=t
    await upd.message.reply_text(tx("smanzil",la)); return ADM_STORE_MANZIL

async def a_store_manzil(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    ctx.user_data["ns_manzil"]=t
    await upd.message.reply_text(tx("sdist",la)); return ADM_STORE_DIST

async def a_store_dist(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    ctx.user_data["ns_dist"]=t
    await upd.message.reply_text(tx("sloc",la), reply_markup=loc_kb(la)); return ADM_STORE_LOC

async def a_store_loc(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    store=ctx.user_data.get("ns",""); manzil=ctx.user_data.get("ns_manzil",""); dist_id=ctx.user_data.get("ns_dist","")
    lat,lng="",""
    if upd.message.location: lat=upd.message.location.latitude; lng=upd.message.location.longitude
    cnt=len(db_all("Dokonlar"))+1
    db_append("Dokonlar",[cnt,store,manzil,dist_id,lat,lng,now_str()])
    await upd.message.reply_text(f"Dokon qoshildi: {store}" if la=="uz" else f"Магазин добавлен: {store}", reply_markup=main_kb(la,True)); return MAIN_MENU

async def a_dist_name(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); t=upd.message.text
    if t==tx("back",la): return ADMIN_MENU
    ctx.user_data["nd"]=t
    await upd.message.reply_text(tx("sdist",la)); return ADM_DIST_ID

async def a_dist_id(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx); name=ctx.user_data.get("nd","")
    db_append("Foydalanuvchilar",[upd.message.text,name,"distributor",la,now_str()])
    await upd.message.reply_text(f"Distribyutor qoshildi: {name}" if la=="uz" else f"Добавлен: {name}", reply_markup=main_kb(la,True)); return MAIN_MENU

async def a_stats(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    try:
        sales=db_all("Topshirish"); pays=db_all("Tolov"); ins=db_all("Qabul"); stores=db_all("Dokonlar")
        ts=sum(float(r.get("Jami",0)) for r in sales)
        tc=sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Naqd")
        td=sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Qarz")
        ti=sum(float(r.get("Jami",0)) for r in ins)
        prod_st={}
        for r in sales: k=r.get("Mahsulot",""); prod_st[k]=prod_st.get(k,0)+float(r.get("Miqdor",0))
        top=sorted(prod_st.items(),key=lambda x:x[1],reverse=True)[:3]
        top_txt="\n".join([f"  {p}: {q:.1f}" for p,q in top]) or "  -"
        if la=="uz":
            msg=(f"Umumiy statistika\n---\nQabul: {ti:,.0f} som\nSotuv: {ts:,.0f} som\n"
                 f"Naqd: {tc:,.0f} som\nQarz: {td:,.0f} som\nDokonlar: {len(stores)}\n---\nTop:\n{top_txt}")
        else:
            msg=(f"Общая статистика\n---\nПолучено: {ti:,.0f} сум\nПродажи: {ts:,.0f} сум\n"
                 f"Наличные: {tc:,.0f} сум\nДолг: {td:,.0f} сум\nМагазинов: {len(stores)}\n---\nТоп:\n{top_txt}")
        await upd.message.reply_text(msg)
    except Exception as e:
        await upd.message.reply_text(f"Xatolik: {e}")

async def a_debtors(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    try:
        stores=db_all("Dokonlar")
        lines=["Qarzdor dokonlar:" if la=="uz" else "Должники:","---"]
        total_debt=0
        for s in stores:
            name=s.get("Nomi",""); debt=get_debt(name)
            if debt>0: lines.append(f"{name}: {debt:,.0f} som"); total_debt+=debt
        if len(lines)==2: lines.append("Qarz yoq!" if la=="uz" else "Долгов нет!")
        else: lines.append(f"---\nJami: {total_debt:,.0f} som" if la=="uz" else f"---\nИтого: {total_debt:,.0f} сум")
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
    await upd.message.reply_text(
        f"Yuborildi: {sent}\nXato: {failed}" if la=="uz" else f"Отправлено: {sent}\nОшибок: {failed}",
        reply_markup=main_kb(la,True)); return MAIN_MENU

# ── QARZ ESLATMASI (har kuni 09:00) ─────────────────────────────
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

# ── CANCEL ───────────────────────────────────────────────────────
async def cancel(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la=lg(ctx)
    await upd.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm(ctx))); return MAIN_MENU

# ── MAIN ─────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN: print("BOT_TOKEN topilmadi!"); return
    app = Application.builder().token(BOT_TOKEN).build()

    # Har kuni 09:00 qarz eslatmasi
    app.job_queue.run_daily(debt_reminder, time=dtime(9, 0))

    txt  = filters.TEXT & ~filters.COMMAND
    photo_txt = (filters.PHOTO | filters.TEXT) & ~filters.COMMAND
    loc_txt   = (filters.LOCATION | filters.TEXT) & ~filters.COMMAND

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            LANG_SELECT:      [CallbackQueryHandler(lang_cb, pattern="^lang_"), CommandHandler("start",start)],
            MAIN_MENU:        [MessageHandler(txt, main_h)],
            QABUL_PROD:       [MessageHandler(txt, qabul_prod)],
            QABUL_QTY:        [MessageHandler(photo_txt, qabul_qty)],
            TOPSHIR_STORE:    [MessageHandler(txt, topshir_store)],
            TOPSHIR_PROD:     [MessageHandler(txt, topshir_prod)],
            TOPSHIR_PHOTO:    [MessageHandler(photo_txt, topshir_photo)],
            TOLOV_STORE:      [MessageHandler(txt, tolov_store)],
            TOLOV_AMOUNT:     [MessageHandler(photo_txt, tolov_amount)],
            TOLOV_METHOD:     [MessageHandler(txt, tolov_method)],
            BUYURTMA_STORE:   [MessageHandler(txt, buyurtma_store)],
            BUYURTMA_PROD:    [MessageHandler(txt, buyurtma_prod)],
            BUYURTMA_QTY:     [MessageHandler(txt, buyurtma_qty)],
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

    print("Bot ishga tushdi! To'liq versiya + Google Vision OCR")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
