import os
import logging
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, filters, ContextTypes
)
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "0").split(",") if x.strip()]

PRODUCTS = [
    {"id": 1, "uz": "Tvorog",        "ru": "Tvorog",         "unit": "kg"},
    {"id": 2, "uz": "Sut",           "ru": "Sut",            "unit": "litr"},
    {"id": 3, "uz": "Qatiq",         "ru": "Qatiq",          "unit": "kg"},
    {"id": 4, "uz": "Brinza",        "ru": "Brinza",         "unit": "kg"},
    {"id": 5, "uz": "Qaymoq 0.4 kg", "ru": "Qaymoq 0.4 kg", "unit": "dona"},
    {"id": 6, "uz": "Qaymoq 0.2 kg", "ru": "Qaymoq 0.2 kg", "unit": "dona"},
    {"id": 7, "uz": "Suzma 0.5 kg",  "ru": "Suzma 0.5 kg",  "unit": "kg"},
    {"id": 8, "uz": "Qurt",          "ru": "Qurt",           "unit": "dona"},
    {"id": 9, "uz": "Tosh qurt",     "ru": "Tosh qurt",      "unit": "dona"},
]

(
    LANG_SELECT, MAIN_MENU,
    QABUL_PRODUCT, QABUL_QTY,
    TOPSHIR_STORE, TOPSHIR_PRODUCT, TOPSHIR_QTY,
    TOLOV_STORE, TOLOV_AMOUNT, TOLOV_METHOD,
    ADMIN_MENU, ADMIN_PRICE_PRODUCT, ADMIN_PRICE_VALUE, ADMIN_COST_VALUE,
    ADMIN_ADD_STORE_NAME, ADMIN_ADD_STORE_DIST,
    ADMIN_ADD_DIST_NAME, ADMIN_ADD_DIST_ID,
) = range(18)

def get_sheet():
    if not GOOGLE_CREDS_JSON:
        return None
    try:
        creds_dict = json.loads(GOOGLE_CREDS_JSON)
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        return client.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        logger.error(f"Sheet error: {e}")
        return None

def ensure_ws(wb, name, headers):
    try:
        return wb.worksheet(name)
    except Exception:
        ws = wb.add_worksheet(name, rows=1000, cols=20)
        ws.append_row(headers)
        return ws

def sheet_append(tab, row):
    try:
        wb = get_sheet()
        if not wb:
            return
        hdrs = {
            "Qabul":             ["Sana","Dist_ID","Ism","Mahsulot","Miqdor","Birlik","Narx","Jami"],
            "Topshirish":        ["Sana","Dist_ID","Dokon","Mahsulot","Miqdor","Birlik","Narx","Jami"],
            "Tolov":             ["Sana","Dist_ID","Dokon","Summa","Usul","Izoh"],
            "Foydalanuvchilar":  ["TG_ID","Ism","Rol","Til","Sana"],
            "Dokonlar":          ["ID","Nomi","Manzil","Dist_ID","Sana"],
            "Narxlar":           ["Mahsulot_ID","Mahsulot","Narx","Tannarx","Sana"],
        }
        ws = ensure_ws(wb, tab, hdrs.get(tab, ["Data"]))
        ws.append_row(row)
    except Exception as e:
        logger.error(f"append error: {e}")

def sheet_get_all(tab):
    try:
        wb = get_sheet()
        if not wb:
            return []
        return wb.worksheet(tab).get_all_records()
    except Exception:
        return []

def get_price(pid):
    try:
        for r in sheet_get_all("Narxlar"):
            if int(r.get("Mahsulot_ID", 0)) == pid:
                return float(r.get("Narx", 0)), float(r.get("Tannarx", 0))
    except Exception:
        pass
    return 0.0, 0.0

def set_price(pid, pname, price, cost):
    try:
        wb = get_sheet()
        if not wb:
            return
        ws = ensure_ws(wb, "Narxlar", ["Mahsulot_ID","Mahsulot","Narx","Tannarx","Sana"])
        records = ws.get_all_records()
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        for i, r in enumerate(records):
            if int(r.get("Mahsulot_ID", 0)) == pid:
                ws.update(f"A{i+2}:E{i+2}", [[pid, pname, price, cost, now]])
                return
        ws.append_row([pid, pname, price, cost, now])
    except Exception as e:
        logger.error(f"set_price error: {e}")

def get_stores(dist_id=None):
    try:
        recs = sheet_get_all("Dokonlar")
        if dist_id:
            return [r for r in recs if str(r.get("Dist_ID","")) == str(dist_id)]
        return recs
    except Exception:
        return []

def get_debt(store):
    try:
        sold = sum(float(r.get("Jami",0)) for r in sheet_get_all("Topshirish") if r.get("Dokon")==store)
        paid = sum(float(r.get("Summa",0)) for r in sheet_get_all("Tolov") if r.get("Dokon")==store and r.get("Usul")=="Naqd")
        return max(0.0, sold - paid)
    except Exception:
        return 0.0

T = {
    "start":        {"uz":"Tilni tanlang:","ru":"Выберите язык:"},
    "main":         {"uz":"Asosiy menyu:","ru":"Главное меню:"},
    "qabul":        {"uz":"Zavoddan qabul","ru":"Получить с завода"},
    "buyurtma":     {"uz":"Buyurtmalar","ru":"Заказы"},
    "topshir":      {"uz":"Mahsulot topshirish","ru":"Передать товар"},
    "tolov":        {"uz":"Tolov","ru":"Оплата"},
    "natija":       {"uz":"Kunlik natija","ru":"Итог дня"},
    "ombor":        {"uz":"Ombor","ru":"Склад"},
    "admin":        {"uz":"Admin panel","ru":"Админ панель"},
    "back":         {"uz":"Orqaga","ru":"Назад"},
    "naqd":         {"uz":"Naqd","ru":"Наличные"},
    "qarz_btn":     {"uz":"Qarz","ru":"Долг"},
    "prod":         {"uz":"Mahsulotni tanlang:","ru":"Выберите товар:"},
    "qty":          {"uz":"Miqdorni kiriting (masalan: 10):","ru":"Введите количество (например: 10):"},
    "store":        {"uz":"Dokonni tanlang:","ru":"Выберите магазин:"},
    "no_store":     {"uz":"Dokonlar topilmadi. Admin qoshsin.","ru":"Магазины не найдены."},
    "sum":          {"uz":"Summa kiriting:","ru":"Введите сумму:"},
    "pay":          {"uz":"Tolov usuli:","ru":"Способ оплаты:"},
    "ok":           {"uz":"Saqlandi!","ru":"Сохранено!"},
    "err":          {"uz":"Raqam kiriting!","ru":"Введите число!"},
    "no_admin":     {"uz":"Siz admin emassiz!","ru":"Вы не администратор!"},
    "adm":          {"uz":"Admin paneli:","ru":"Админ панель:"},
    "price_btn":    {"uz":"Narx ozgartirish","ru":"Изменить цены"},
    "add_store":    {"uz":"Dokon qoshish","ru":"Добавить магазин"},
    "add_dist":     {"uz":"Distribyutor qoshish","ru":"Добавить дистрибьютора"},
    "stats":        {"uz":"Statistika","ru":"Статистика"},
    "new_price":    {"uz":"Yangi narx (som):","ru":"Новая цена (сум):"},
    "tannarx":      {"uz":"Tannarx (som):","ru":"Себестоимость (сум):"},
    "sname":        {"uz":"Dokon nomini kiriting:","ru":"Название магазина:"},
    "sdist":        {"uz":"Distribyutor Telegram ID:","ru":"Telegram ID дистрибьютора:"},
    "dname":        {"uz":"Distribyutor ismini kiriting:","ru":"Имя дистрибьютора:"},
}

def tx(k, lang="uz"):
    return T.get(k, {}).get(lang, k)

def lang(ctx):
    return ctx.user_data.get("lang", "uz")

def adm(ctx):
    return ctx.user_data.get("is_admin", False)

def uname(update):
    u = update.effective_user
    return u.full_name or u.username or str(u.id)

def main_kb(la, is_adm=False):
    rows = [
        [tx("qabul",la), tx("buyurtma",la)],
        [tx("topshir",la), tx("tolov",la)],
        [tx("natija",la), tx("ombor",la)],
    ]
    if is_adm:
        rows.append([tx("admin",la)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def prod_kb(la):
    rows = []
    for i in range(0, len(PRODUCTS), 2):
        r = [PRODUCTS[i][la]]
        if i+1 < len(PRODUCTS):
            r.append(PRODUCTS[i+1][la])
        rows.append(r)
    rows.append([tx("back",la)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def store_kb(stores, la):
    rows = [[s.get("Nomi","")] for s in stores]
    rows.append([tx("back",la)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def back_kb(la):
    return ReplyKeyboardMarkup([[tx("back",la)]], resize_keyboard=True)

def find_prod(name, la):
    return next((p for p in PRODUCTS if p[la]==name), None)

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("O'zbek", callback_data="lang_uz"),
        InlineKeyboardButton("Русский", callback_data="lang_ru"),
    ]])
    await update.message.reply_text(tx("start","uz"), reply_markup=kb)
    return LANG_SELECT

async def lang_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    la = q.data.replace("lang_","")
    ctx.user_data["lang"] = la
    uid = update.effective_user.id
    ctx.user_data["is_admin"] = uid in ADMIN_IDS
    sheet_append("Foydalanuvchilar",[
        str(uid), uname(update),
        "admin" if uid in ADMIN_IDS else "distributor",
        la, datetime.now().strftime("%Y-%m-%d %H:%M")
    ])
    await q.edit_message_text("Til tanlandi!" if la=="uz" else "Язык выбран!")
    await ctx.bot.send_message(uid, tx("main",la), reply_markup=main_kb(la, uid in ADMIN_IDS))
    return MAIN_MENU

async def main_h(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    uid = update.effective_user.id
    is_adm = adm(ctx)

    if t == tx("qabul",la):
        await update.message.reply_text(tx("prod",la), reply_markup=prod_kb(la))
        return QABUL_PRODUCT
    if t == tx("topshir",la):
        stores = get_stores(uid) or get_stores()
        if not stores:
            await update.message.reply_text(tx("no_store",la))
            return MAIN_MENU
        ctx.user_data["stores"] = stores
        await update.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return TOPSHIR_STORE
    if t == tx("tolov",la):
        stores = get_stores(uid) or get_stores()
        if not stores:
            await update.message.reply_text(tx("no_store",la))
            return MAIN_MENU
        ctx.user_data["stores"] = stores
        await update.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return TOLOV_STORE
    if t == tx("natija",la):
        await daily(update, ctx)
        return MAIN_MENU
    if t == tx("ombor",la):
        await stock(update, ctx)
        return MAIN_MENU
    if t == tx("buyurtma",la):
        await update.message.reply_text("Buyurtmalar moduli tez orada!" if la=="uz" else "Скоро!")
        return MAIN_MENU
    if t == tx("admin",la) and is_adm:
        await update.message.reply_text(tx("adm",la), reply_markup=ReplyKeyboardMarkup([
            [tx("price_btn",la), tx("add_store",la)],
            [tx("add_dist",la), tx("stats",la)],
            [tx("back",la)],
        ], resize_keyboard=True))
        return ADMIN_MENU
    await update.message.reply_text(tx("main",la), reply_markup=main_kb(la,is_adm))
    return MAIN_MENU

async def qabul_prod(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        await update.message.reply_text(tx("main",la), reply_markup=main_kb(la,adm(ctx)))
        return MAIN_MENU
    p = find_prod(t, la)
    if not p:
        await update.message.reply_text(tx("prod",la), reply_markup=prod_kb(la))
        return QABUL_PRODUCT
    ctx.user_data["p"] = p
    await update.message.reply_text(f"{t}\n{tx('qty',la)}", reply_markup=back_kb(la))
    return QABUL_QTY

async def qabul_qty(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        await update.message.reply_text(tx("prod",la), reply_markup=prod_kb(la))
        return QABUL_PRODUCT
    try:
        qty = float(t.replace(",","."))
    except Exception:
        await update.message.reply_text(tx("err",la))
        return QABUL_QTY
    p = ctx.user_data["p"]
    uid = update.effective_user.id
    price, _ = get_price(p["id"])
    total = qty * price
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    sheet_append("Qabul",[now, str(uid), uname(update), p[la], qty, p["unit"], price, total])
    await update.message.reply_text(
        f"{tx('ok',la)}\n{p[la]}: {qty} {p['unit']}\nNarx: {price:,.0f}\nJami: {total:,.0f} som",
        reply_markup=prod_kb(la)
    )
    return QABUL_PRODUCT

async def topshir_store(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        await update.message.reply_text(tx("main",la), reply_markup=main_kb(la,adm(ctx)))
        return MAIN_MENU
    stores = ctx.user_data.get("stores",[])
    if t not in [s.get("Nomi","") for s in stores]:
        await update.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return TOPSHIR_STORE
    ctx.user_data["s"] = t
    debt = get_debt(t)
    msg = tx("prod",la)
    if debt > 0:
        msg = f"Qarz: {debt:,.0f} som\n\n" + msg if la=="uz" else f"Долг: {debt:,.0f} сум\n\n" + msg
    await update.message.reply_text(msg, reply_markup=prod_kb(la))
    return TOPSHIR_PRODUCT

async def topshir_prod(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        stores = ctx.user_data.get("stores",[])
        await update.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return TOPSHIR_STORE
    p = find_prod(t, la)
    if not p:
        await update.message.reply_text(tx("prod",la), reply_markup=prod_kb(la))
        return TOPSHIR_PRODUCT
    ctx.user_data["p"] = p
    await update.message.reply_text(f"{t}\n{tx('qty',la)}", reply_markup=back_kb(la))
    return TOPSHIR_QTY

async def topshir_qty(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        await update.message.reply_text(tx("prod",la), reply_markup=prod_kb(la))
        return TOPSHIR_PRODUCT
    try:
        qty = float(t.replace(",","."))
    except Exception:
        await update.message.reply_text(tx("err",la))
        return TOPSHIR_QTY
    p = ctx.user_data["p"]
    store = ctx.user_data["s"]
    uid = update.effective_user.id
    price, _ = get_price(p["id"])
    total = qty * price
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    sheet_append("Topshirish",[now, str(uid), store, p[la], qty, p["unit"], price, total])
    await update.message.reply_text(
        f"{tx('ok',la)}\nDokon: {store}\n{p[la]}: {qty} {p['unit']}\nJami: {total:,.0f} som",
        reply_markup=prod_kb(la)
    )
    return TOPSHIR_PRODUCT

async def tolov_store(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        await update.message.reply_text(tx("main",la), reply_markup=main_kb(la,adm(ctx)))
        return MAIN_MENU
    stores = ctx.user_data.get("stores",[])
    if t not in [s.get("Nomi","") for s in stores]:
        await update.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return TOLOV_STORE
    ctx.user_data["s"] = t
    debt = get_debt(t)
    debt_txt = f"\nQarz: {debt:,.0f} som" if (debt > 0 and la=="uz") else (f"\nДолг: {debt:,.0f} сум" if debt > 0 else "")
    await update.message.reply_text(f"{t}{debt_txt}\n\n{tx('sum',la)}", reply_markup=back_kb(la))
    return TOLOV_AMOUNT

async def tolov_amount(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        stores = ctx.user_data.get("stores",[])
        await update.message.reply_text(tx("store",la), reply_markup=store_kb(stores,la))
        return TOLOV_STORE
    try:
        amount = float(t.replace(",",".").replace(" ",""))
    except Exception:
        await update.message.reply_text(tx("err",la))
        return TOLOV_AMOUNT
    ctx.user_data["amount"] = amount
    await update.message.reply_text(tx("pay",la), reply_markup=ReplyKeyboardMarkup([
        [tx("naqd",la), tx("qarz_btn",la)],
        [tx("back",la)],
    ], resize_keyboard=True))
    return TOLOV_METHOD

async def tolov_method(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        await update.message.reply_text(tx("sum",la), reply_markup=back_kb(la))
        return TOLOV_AMOUNT
    store = ctx.user_data["s"]
    amount = ctx.user_data["amount"]
    uid = update.effective_user.id
    method = "Naqd" if t == tx("naqd",la) else "Qarz"
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    sheet_append("Tolov",[now, str(uid), store, amount, method, ""])
    await update.message.reply_text(
        f"{tx('ok',la)}\n{store}\n{amount:,.0f} som - {method}",
        reply_markup=main_kb(la,adm(ctx))
    )
    return MAIN_MENU

async def daily(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    uid = str(update.effective_user.id)
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        sales = [r for r in sheet_get_all("Topshirish") if r.get("Sana","").startswith(today) and str(r.get("Dist_ID",""))==uid]
        pays  = [r for r in sheet_get_all("Tolov") if r.get("Sana","").startswith(today) and str(r.get("Dist_ID",""))==uid]
        ins   = [r for r in sheet_get_all("Qabul") if r.get("Sana","").startswith(today) and str(r.get("Dist_ID",""))==uid]
        ts = sum(float(r.get("Jami",0)) for r in sales)
        tc = sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Naqd")
        td = sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Qarz")
        ti = sum(float(r.get("Jami",0)) for r in ins)
        dc = len(set(r.get("Dokon","") for r in sales))
        if la=="uz":
            msg = f"Kunlik natija - {today}\n---\nQabul: {ti:,.0f} som\nSotuv: {ts:,.0f} som\nNaqd: {tc:,.0f} som\nQarz: {td:,.0f} som\nDokonlar: {dc}"
        else:
            msg = f"Итог дня - {today}\n---\nПолучено: {ti:,.0f} сум\nПродажи: {ts:,.0f} сум\nНаличные: {tc:,.0f} сум\nДолг: {td:,.0f} сум\nМагазинов: {dc}"
    except Exception as e:
        msg = f"Xatolik: {e}"
    await update.message.reply_text(msg)

async def stock(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    uid = str(update.effective_user.id)
    try:
        st = {}
        for r in sheet_get_all("Qabul"):
            if str(r.get("Dist_ID",""))==uid:
                k = r.get("Mahsulot","")
                st[k] = st.get(k,0) + float(r.get("Miqdor",0))
        for r in sheet_get_all("Topshirish"):
            if str(r.get("Dist_ID",""))==uid:
                k = r.get("Mahsulot","")
                st[k] = st.get(k,0) - float(r.get("Miqdor",0))
        lines = ["Ombor:" if la=="uz" else "Склад:","---"]
        for k,v in st.items():
            if v > 0:
                lines.append(f"{k}: {v:.1f}")
        if len(lines)==2:
            lines.append("Hammasi topshirilgan!" if la=="uz" else "Всё сдано!")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"Xatolik: {e}")

async def admin_h(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text(tx("no_admin",la))
        return MAIN_MENU
    if t == tx("back",la):
        await update.message.reply_text(tx("main",la), reply_markup=main_kb(la,True))
        return MAIN_MENU
    if t == tx("price_btn",la):
        await update.message.reply_text(tx("prod",la), reply_markup=prod_kb(la))
        return ADMIN_PRICE_PRODUCT
    if t == tx("add_store",la):
        await update.message.reply_text(tx("sname",la), reply_markup=back_kb(la))
        return ADMIN_ADD_STORE_NAME
    if t == tx("add_dist",la):
        await update.message.reply_text(tx("dname",la), reply_markup=back_kb(la))
        return ADMIN_ADD_DIST_NAME
    if t == tx("stats",la):
        await a_stats(update, ctx)
        return ADMIN_MENU
    return ADMIN_MENU

async def a_price_prod(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        return ADMIN_MENU
    p = find_prod(t, la)
    if not p:
        await update.message.reply_text(tx("prod",la), reply_markup=prod_kb(la))
        return ADMIN_PRICE_PRODUCT
    ctx.user_data["p"] = p
    price, _ = get_price(p["id"])
    await update.message.reply_text(f"{t}\nJoriy: {price:,.0f}\n\n{tx('new_price',la)}", reply_markup=back_kb(la))
    return ADMIN_PRICE_VALUE

async def a_price_val(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        await update.message.reply_text(tx("prod",la), reply_markup=prod_kb(la))
        return ADMIN_PRICE_PRODUCT
    try:
        price = float(t.replace(",",".").replace(" ",""))
    except Exception:
        await update.message.reply_text(tx("err",la))
        return ADMIN_PRICE_VALUE
    ctx.user_data["np"] = price
    await update.message.reply_text(tx("tannarx",la))
    return ADMIN_COST_VALUE

async def a_cost_val(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    try:
        cost = float(t.replace(",",".").replace(" ",""))
    except Exception:
        await update.message.reply_text(tx("err",la))
        return ADMIN_COST_VALUE
    p = ctx.user_data["p"]
    price = ctx.user_data["np"]
    set_price(p["id"], p[la], price, cost)
    await update.message.reply_text(f"Yangilandi!\n{p[la]}: {price:,.0f} / {cost:,.0f}", reply_markup=main_kb(la,True))
    return MAIN_MENU

async def a_store_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        return ADMIN_MENU
    ctx.user_data["ns"] = t
    await update.message.reply_text(tx("sdist",la))
    return ADMIN_ADD_STORE_DIST

async def a_store_dist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    store = ctx.user_data.get("ns","")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    cnt = len(sheet_get_all("Dokonlar")) + 1
    sheet_append("Dokonlar",[cnt, store, "", update.message.text, now])
    await update.message.reply_text(
        f"Dokon qoshildi: {store}" if la=="uz" else f"Магазин добавлен: {store}",
        reply_markup=main_kb(la,True)
    )
    return MAIN_MENU

async def a_dist_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    t = update.message.text
    if t == tx("back",la):
        return ADMIN_MENU
    ctx.user_data["nd"] = t
    await update.message.reply_text(tx("sdist",la))
    return ADMIN_ADD_DIST_ID

async def a_dist_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    name = ctx.user_data.get("nd","")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    sheet_append("Foydalanuvchilar",[update.message.text, name, "distributor", la, now])
    await update.message.reply_text(
        f"Distribyutor qoshildi: {name}" if la=="uz" else f"Добавлен: {name}",
        reply_markup=main_kb(la,True)
    )
    return MAIN_MENU

async def a_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    try:
        sales = sheet_get_all("Topshirish")
        pays  = sheet_get_all("Tolov")
        ins   = sheet_get_all("Qabul")
        stores= sheet_get_all("Dokonlar")
        ts = sum(float(r.get("Jami",0)) for r in sales)
        tc = sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Naqd")
        td = sum(float(r.get("Summa",0)) for r in pays if r.get("Usul")=="Qarz")
        ti = sum(float(r.get("Jami",0)) for r in ins)
        if la=="uz":
            msg = f"Umumiy statistika\n---\nQabul: {ti:,.0f} som\nSotuv: {ts:,.0f} som\nNaqd: {tc:,.0f} som\nQarz: {td:,.0f} som\nDokonlar: {len(stores)}"
        else:
            msg = f"Общая статистика\n---\nПолучено: {ti:,.0f} сум\nПродажи: {ts:,.0f} сум\nНаличные: {tc:,.0f} сум\nДолг: {td:,.0f} сум\nМагазинов: {len(stores)}"
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"Xatolik: {e}")

async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    la = lang(ctx)
    await update.message.reply_text(tx("main",la), reply_markup=main_kb(la,adm(ctx)))
    return MAIN_MENU

def main():
    if not BOT_TOKEN:
        print("BOT_TOKEN topilmadi!")
        return
    app = Application.builder().token(BOT_TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            LANG_SELECT:          [CallbackQueryHandler(lang_cb, pattern="^lang_"), CommandHandler("start",start)],
            MAIN_MENU:            [MessageHandler(filters.TEXT & ~filters.COMMAND, main_h)],
            QABUL_PRODUCT:        [MessageHandler(filters.TEXT & ~filters.COMMAND, qabul_prod)],
            QABUL_QTY:            [MessageHandler(filters.TEXT & ~filters.COMMAND, qabul_qty)],
            TOPSHIR_STORE:        [MessageHandler(filters.TEXT & ~filters.COMMAND, topshir_store)],
            TOPSHIR_PRODUCT:      [MessageHandler(filters.TEXT & ~filters.COMMAND, topshir_prod)],
            TOPSHIR_QTY:          [MessageHandler(filters.TEXT & ~filters.COMMAND, topshir_qty)],
            TOLOV_STORE:          [MessageHandler(filters.TEXT & ~filters.COMMAND, tolov_store)],
            TOLOV_AMOUNT:         [MessageHandler(filters.TEXT & ~filters.COMMAND, tolov_amount)],
            TOLOV_METHOD:         [MessageHandler(filters.TEXT & ~filters.COMMAND, tolov_method)],
            ADMIN_MENU:           [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_h)],
            ADMIN_PRICE_PRODUCT:  [MessageHandler(filters.TEXT & ~filters.COMMAND, a_price_prod)],
            ADMIN_PRICE_VALUE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, a_price_val)],
            ADMIN_COST_VALUE:     [MessageHandler(filters.TEXT & ~filters.COMMAND, a_cost_val)],
            ADMIN_ADD_STORE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, a_store_name)],
            ADMIN_ADD_STORE_DIST: [MessageHandler(filters.TEXT & ~filters.COMMAND, a_store_dist)],
            ADMIN_ADD_DIST_NAME:  [MessageHandler(filters.TEXT & ~filters.COMMAND, a_dist_name)],
            ADMIN_ADD_DIST_ID:    [MessageHandler(filters.TEXT & ~filters.COMMAND, a_dist_id)],
        },
        fallbacks=[CommandHandler("cancel", cancel), CommandHandler("start", start)],
        allow_reentry=True,
    )
    app.add_handler(conv)
    print("Bot ishga tushdi!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
