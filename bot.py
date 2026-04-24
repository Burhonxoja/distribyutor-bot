import os
import logging
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
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

# ─── SOZLAMALAR ───────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "YOUR_SPREADSHEET_ID")
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "123456789").split(",") if x]

# ─── MAHSULOTLAR ──────────────────────────────────────────────
PRODUCTS = [
    {"id": 1, "uz": "Tvorog",        "ru": "Творог",         "unit": "kg",   "price": 0, "cost": 0},
    {"id": 2, "uz": "Sut",           "ru": "Молоко",         "unit": "litr", "price": 0, "cost": 0},
    {"id": 3, "uz": "Qatiq",         "ru": "Катык",          "unit": "kg",   "price": 0, "cost": 0},
    {"id": 4, "uz": "Brinza",        "ru": "Брынза",         "unit": "kg",   "price": 0, "cost": 0},
    {"id": 5, "uz": "Qaymoq 0.4 kg", "ru": "Сливки 0.4 кг", "unit": "dona", "price": 0, "cost": 0},
    {"id": 6, "uz": "Qaymoq 0.2 kg", "ru": "Сливки 0.2 кг", "unit": "dona", "price": 0, "cost": 0},
    {"id": 7, "uz": "Suzma 0.5 kg",  "ru": "Сузьма 0.5 кг", "unit": "kg",   "price": 0, "cost": 0},
    {"id": 8, "uz": "Qurt",          "ru": "Курт",           "unit": "dona", "price": 0, "cost": 0},
    {"id": 9, "uz": "Tosh qurt",     "ru": "Каменный курт",  "unit": "dona", "price": 0, "cost": 0},
]

# ─── CONVERSATION STATES ──────────────────────────────────────
(
    LANG_SELECT,
    MAIN_MENU,
    QABUL_PRODUCT, QABUL_QTY,
    TOPSHIR_STORE, TOPSHIR_PRODUCT, TOPSHIR_QTY,
    TOLOV_STORE, TOLOV_AMOUNT, TOLOV_METHOD,
    ADMIN_MENU, ADMIN_PRICE_PRODUCT, ADMIN_PRICE_VALUE, ADMIN_COST_VALUE,
    ADMIN_ADD_STORE_NAME, ADMIN_ADD_STORE_DIST,
    ADMIN_ADD_DIST_NAME, ADMIN_ADD_DIST_ID,
) = range(18)

# ─── GOOGLE SHEETS ────────────────────────────────────────────
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
        logger.error(f"Google Sheets error: {e}")
        return None

def sheet_append(tab_name, row):
    try:
        wb = get_sheet()
        if wb is None:
            return False
        try:
            ws = wb.worksheet(tab_name)
        except:
            ws = wb.add_worksheet(tab_name, rows=1000, cols=20)
            if tab_name == "Qabul":
                ws.append_row(["Sana", "Distribyutor_ID", "Distribyutor", "Mahsulot", "Miqdor", "Birlik", "Narx", "Jami"])
            elif tab_name == "Topshirish":
                ws.append_row(["Sana", "Distribyutor_ID", "Do'kon", "Mahsulot", "Miqdor", "Birlik", "Narx", "Jami"])
            elif tab_name == "Tolov":
                ws.append_row(["Sana", "Distribyutor_ID", "Do'kon", "Summa", "Usul", "Izoh"])
            elif tab_name == "Foydalanuvchilar":
                ws.append_row(["Telegram_ID", "Ism", "Rol", "Til", "Qo'shilgan"])
            elif tab_name == "Do'konlar":
                ws.append_row(["ID", "Nomi", "Manzil", "Distribyutor_ID", "Qo'shilgan"])
            elif tab_name == "Narxlar":
                ws.append_row(["Mahsulot_ID", "Mahsulot", "Narx", "Tannarx", "O'zgartirilgan"])
        ws.append_row(row)
        return True
    except Exception as e:
        logger.error(f"sheet_append error: {e}")
        return False

def sheet_get_all(tab_name):
    try:
        wb = get_sheet()
        if wb is None:
            return []
        ws = wb.worksheet(tab_name)
        return ws.get_all_records()
    except Exception as e:
        logger.error(f"sheet_get_all error: {e}")
        return []

def get_product_price(product_id):
    try:
        records = sheet_get_all("Narxlar")
        for r in records:
            if int(r.get("Mahsulot_ID", 0)) == product_id:
                return float(r.get("Narx", 0)), float(r.get("Tannarx", 0))
    except:
        pass
    return 0, 0

def set_product_price(product_id, product_name, price, cost):
    try:
        wb = get_sheet()
        if wb is None:
            return False
        try:
            ws = wb.worksheet("Narxlar")
        except:
            ws = wb.add_worksheet("Narxlar", rows=100, cols=10)
            ws.append_row(["Mahsulot_ID", "Mahsulot", "Narx", "Tannarx", "O'zgartirilgan"])
        records = ws.get_all_records()
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        for i, r in enumerate(records):
            if int(r.get("Mahsulot_ID", 0)) == product_id:
                row_num = i + 2
                ws.update(f"A{row_num}:E{row_num}", [[product_id, product_name, price, cost, now]])
                return True
        ws.append_row([product_id, product_name, price, cost, now])
        return True
    except Exception as e:
        logger.error(f"set_product_price error: {e}")
        return False

def get_stores(dist_id=None):
    try:
        records = sheet_get_all("Do'konlar")
        if dist_id:
            return [r for r in records if str(r.get("Distribyutor_ID", "")) == str(dist_id)]
        return records
    except:
        return []

def get_debt(store_name):
    try:
        records = sheet_get_all("Tolov")
        sales = sheet_get_all("Topshirish")
        total_sold = sum(float(r.get("Jami", 0)) for r in sales if r.get("Do'kon") == store_name)
        total_paid = sum(float(r.get("Summa", 0)) for r in records if r.get("Do'kon") == store_name and r.get("Usul") == "Naqd")
        return max(0, total_sold - total_paid)
    except:
        return 0

# ─── MATNLAR (UZ / RU) ────────────────────────────────────────
def t(key, lang="uz"):
    texts = {
        "welcome": {
            "uz": "👋 Salom! Distribyutor botiga xush kelibsiz!\nTilni tanlang:",
            "ru": "👋 Привет! Добро пожаловать в бот дистрибьютора!\nВыберите язык:"
        },
        "main_menu": {
            "uz": "📋 Asosiy menyu:",
            "ru": "📋 Главное меню:"
        },
        "btn_qabul": {"uz": "📥 Zavoddan qabul qilish", "ru": "📥 Получить с завода"},
        "btn_buyurtmalar": {"uz": "📋 Buyurtmalar", "ru": "📋 Заказы"},
        "btn_topshir": {"uz": "⚖️ Mahsulot topshirish", "ru": "⚖️ Передать товар"},
        "btn_tolov": {"uz": "💳 To'lov", "ru": "💳 Оплата"},
        "btn_natija": {"uz": "📊 Kunlik natija", "ru": "📊 Итог дня"},
        "btn_ombor": {"uz": "📦 Ombor holati", "ru": "📦 Состояние склада"},
        "btn_admin": {"uz": "⚙️ Admin panel", "ru": "⚙️ Админ панель"},
        "choose_product": {"uz": "Qaysi mahsulotni oldingiz?", "ru": "Какой товар вы взяли?"},
        "enter_qty": {"uz": "Qancha miqdor? (raqam kiriting, masalan: 50)", "ru": "Сколько? (введите число, например: 50)"},
        "saved_ok": {"uz": "✅ Saqlandi!", "ru": "✅ Сохранено!"},
        "choose_store": {"uz": "Qaysi do'konga?", "ru": "В какой магазин?"},
        "debt_warning": {"uz": "⚠️ Bu do'konning qarzi bor: {amount} so'm", "ru": "⚠️ У этого магазина есть долг: {amount} сум"},
        "tolov_method": {"uz": "To'lov usuli?", "ru": "Способ оплаты?"},
        "btn_naqd": {"uz": "💵 Naqd", "ru": "💵 Наличные"},
        "btn_qarz": {"uz": "📝 Qarz (keyinroq)", "ru": "📝 Долг (позже)"},
        "cancel": {"uz": "❌ Bekor qilindi", "ru": "❌ Отменено"},
        "error_num": {"uz": "❗ Raqam kiriting!", "ru": "❗ Введите число!"},
        "no_stores": {"uz": "❗ Do'konlar topilmadi. Admin qo'shsin.", "ru": "❗ Магазины не найдены. Пусть добавит админ."},
        "admin_only": {"uz": "🚫 Siz admin emassiz!", "ru": "🚫 Вы не являетесь администратором!"},
        "admin_menu": {"uz": "⚙️ Admin paneli:", "ru": "⚙️ Админ панель:"},
        "btn_set_price": {"uz": "💰 Narx o'zgartirish", "ru": "💰 Изменить цены"},
        "btn_add_store": {"uz": "🏪 Do'kon qo'shish", "ru": "🏪 Добавить магазин"},
        "btn_add_dist": {"uz": "👤 Distribyutor qo'shish", "ru": "👤 Добавить дистрибьютора"},
        "btn_all_stats": {"uz": "📈 Umumiy statistika", "ru": "📈 Общая статистика"},
        "btn_back": {"uz": "🔙 Orqaga", "ru": "🔙 Назад"},
        "enter_store_name": {"uz": "Do'kon nomini kiriting:", "ru": "Введите название магазина:"},
        "enter_store_dist": {"uz": "Distribyutor Telegram ID sini kiriting:", "ru": "Введите Telegram ID дистрибьютора:"},
        "enter_dist_name": {"uz": "Distribyutor ismini kiriting:", "ru": "Введите имя дистрибьютора:"},
        "enter_dist_id": {"uz": "Distribyutor Telegram ID sini kiriting:", "ru": "Введите Telegram ID:"},
        "enter_price": {"uz": "Yangi narxni kiriting (so'm):", "ru": "Введите новую цену (сум):"},
        "enter_cost": {"uz": "Tannarxni kiriting (so'm):", "ru": "Введите себестоимость (сум):"},
        "price_updated": {"uz": "✅ Narx yangilandi!", "ru": "✅ Цена обновлена!"},
    }
    return texts.get(key, {}).get(lang, key)

# ─── YORDAMCHI FUNKSIYALAR ────────────────────────────────────
def get_lang(context):
    return context.user_data.get("lang", "uz")

def get_user_name(update):
    u = update.effective_user
    return u.full_name or u.username or str(u.id)

def main_menu_keyboard(lang, is_admin=False):
    buttons = [
        [t("btn_qabul", lang), t("btn_buyurtmalar", lang)],
        [t("btn_topshir", lang), t("btn_tolov", lang)],
        [t("btn_natija", lang), t("btn_ombor", lang)],
    ]
    if is_admin:
        buttons.append([t("btn_admin", lang)])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def product_keyboard(lang):
    buttons = []
    for i in range(0, len(PRODUCTS), 2):
        row = [PRODUCTS[i][lang]]
        if i + 1 < len(PRODUCTS):
            row.append(PRODUCTS[i + 1][lang])
        buttons.append(row)
    buttons.append([t("btn_back", lang)])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def find_product_by_name(name, lang):
    for p in PRODUCTS:
        if p[lang] == name:
            return p
    return None

def stores_keyboard(stores, lang):
    buttons = [[s.get("Nomi", s.get("Name", ""))] for s in stores]
    buttons.append([t("btn_back", lang)])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

# ─── HANDLERS ─────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🇺🇿 O'zbek", callback_data="lang_uz"),
         InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru")]
    ])
    await update.message.reply_text(
        "👋 Salom! / Привет!\nTilni tanlang / Выберите язык:",
        reply_markup=keyboard
    )
    return LANG_SELECT

async def lang_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    lang = query.data.replace("lang_", "")
    context.user_data["lang"] = lang
    uid = update.effective_user.id
    is_admin = uid in ADMIN_IDS
    context.user_data["is_admin"] = is_admin

    # Foydalanuvchini saqlash
    sheet_append("Foydalanuvchilar", [
        str(uid), get_user_name(update), "admin" if is_admin else "distributor",
        lang, datetime.now().strftime("%Y-%m-%d %H:%M")
    ])

    await query.edit_message_text(
        ("✅ O'zbek tili tanlandi!" if lang == "uz" else "✅ Выбран русский язык!") +
        "\n\n" + t("main_menu", lang)
    )
    await context.bot.send_message(
        chat_id=uid,
        text=t("main_menu", lang),
        reply_markup=main_menu_keyboard(lang, is_admin)
    )
    return MAIN_MENU

async def change_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🇺🇿 O'zbek", callback_data="lang_uz"),
         InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru")]
    ])
    await update.message.reply_text("Tilni tanlang / Выберите язык:", reply_markup=keyboard)
    return LANG_SELECT

async def main_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text
    uid = update.effective_user.id

    if text == t("btn_qabul", lang):
        await update.message.reply_text(t("choose_product", lang), reply_markup=product_keyboard(lang))
        context.user_data["qabul_items"] = []
        return QABUL_PRODUCT

    elif text == t("btn_topshir", lang):
        stores = get_stores(uid)
        if not stores:
            stores = get_stores()
        if not stores:
            await update.message.reply_text(t("no_stores", lang))
            return MAIN_MENU
        context.user_data["topshir_items"] = []
        await update.message.reply_text(t("choose_store", lang), reply_markup=stores_keyboard(stores, lang))
        context.user_data["stores"] = stores
        return TOPSHIR_STORE

    elif text == t("btn_tolov", lang):
        stores = get_stores(uid)
        if not stores:
            stores = get_stores()
        if not stores:
            await update.message.reply_text(t("no_stores", lang))
            return MAIN_MENU
        context.user_data["stores"] = stores
        await update.message.reply_text(t("choose_store", lang), reply_markup=stores_keyboard(stores, lang))
        return TOLOV_STORE

    elif text == t("btn_natija", lang):
        await show_daily_result(update, context)
        return MAIN_MENU

    elif text == t("btn_ombor", lang):
        await show_warehouse(update, context)
        return MAIN_MENU

    elif text == t("btn_buyurtmalar", lang):
        await show_orders(update, context)
        return MAIN_MENU

    elif text == t("btn_admin", lang) and context.user_data.get("is_admin"):
        await update.message.reply_text(
            t("admin_menu", lang),
            reply_markup=ReplyKeyboardMarkup([
                [t("btn_set_price", lang), t("btn_add_store", lang)],
                [t("btn_add_dist", lang), t("btn_all_stats", lang)],
                [t("btn_back", lang)]
            ], resize_keyboard=True)
        )
        return ADMIN_MENU

    else:
        await update.message.reply_text(t("main_menu", lang), reply_markup=main_menu_keyboard(lang, context.user_data.get("is_admin", False)))
        return MAIN_MENU

# ─── QABUL (ZAVODDAN OLISH) ───────────────────────────────────
async def qabul_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text

    if text == t("btn_back", lang):
        await update.message.reply_text(t("main_menu", lang), reply_markup=main_menu_keyboard(lang, context.user_data.get("is_admin", False)))
        return MAIN_MENU

    product = find_product_by_name(text, lang)
    if not product:
        await update.message.reply_text(t("choose_product", lang), reply_markup=product_keyboard(lang))
        return QABUL_PRODUCT

    context.user_data["selected_product"] = product
    await update.message.reply_text(
        f"{'Mahsulot' if lang=='uz' else 'Товар'}: {text}\n" + t("enter_qty", lang),
        reply_markup=ReplyKeyboardMarkup([[t("btn_back", lang)]], resize_keyboard=True)
    )
    return QABUL_QTY

async def qabul_qty(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text

    if text == t("btn_back", lang):
        await update.message.reply_text(t("choose_product", lang), reply_markup=product_keyboard(lang))
        return QABUL_PRODUCT

    try:
        qty = float(text.replace(",", "."))
    except:
        await update.message.reply_text(t("error_num", lang))
        return QABUL_QTY

    product = context.user_data["selected_product"]
    uid = update.effective_user.id
    name = get_user_name(update)
    price, cost = get_product_price(product["id"])
    total = qty * price
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    sheet_append("Qabul", [now, str(uid), name, product[lang], qty, product["unit"], price, total])

    msg = (
        f"✅ {'Saqlandi' if lang=='uz' else 'Сохранено'}!\n\n"
        f"📦 {product[lang]}: {qty} {product['unit']}\n"
        f"💰 {'Narx' if lang=='uz' else 'Цена'}: {price:,.0f} so'm\n"
        f"💵 {'Jami' if lang=='uz' else 'Итого'}: {total:,.0f} so'm\n\n"
        f"{'Yana qo\'shish uchun mahsulot tanlang' if lang=='uz' else 'Выберите ещё товар или вернитесь'}"
    )
    await update.message.reply_text(msg, reply_markup=product_keyboard(lang))
    return QABUL_PRODUCT

# ─── TOPSHIRISH (DO'KONGA BERISH) ─────────────────────────────
async def topshir_store(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text

    if text == t("btn_back", lang):
        await update.message.reply_text(t("main_menu", lang), reply_markup=main_menu_keyboard(lang, context.user_data.get("is_admin", False)))
        return MAIN_MENU

    stores = context.user_data.get("stores", [])
    store_names = [s.get("Nomi", "") for s in stores]
    if text not in store_names:
        await update.message.reply_text(t("choose_store", lang), reply_markup=stores_keyboard(stores, lang))
        return TOPSHIR_STORE

    context.user_data["selected_store"] = text
    debt = get_debt(text)
    if debt > 0:
        await update.message.reply_text(
            t("debt_warning", lang).format(amount=f"{debt:,.0f}") + "\n\n" + t("choose_product", lang),
            reply_markup=product_keyboard(lang)
        )
    else:
        await update.message.reply_text(t("choose_product", lang), reply_markup=product_keyboard(lang))
    return TOPSHIR_PRODUCT

async def topshir_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text

    if text == t("btn_back", lang):
        stores = context.user_data.get("stores", [])
        await update.message.reply_text(t("choose_store", lang), reply_markup=stores_keyboard(stores, lang))
        return TOPSHIR_STORE

    product = find_product_by_name(text, lang)
    if not product:
        await update.message.reply_text(t("choose_product", lang), reply_markup=product_keyboard(lang))
        return TOPSHIR_PRODUCT

    context.user_data["selected_product"] = product
    await update.message.reply_text(
        f"{'Mahsulot' if lang=='uz' else 'Товар'}: {text}\n" + t("enter_qty", lang),
        reply_markup=ReplyKeyboardMarkup([[t("btn_back", lang)]], resize_keyboard=True)
    )
    return TOPSHIR_QTY

async def topshir_qty(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text

    if text == t("btn_back", lang):
        await update.message.reply_text(t("choose_product", lang), reply_markup=product_keyboard(lang))
        return TOPSHIR_PRODUCT

    try:
        qty = float(text.replace(",", "."))
    except:
        await update.message.reply_text(t("error_num", lang))
        return TOPSHIR_QTY

    product = context.user_data["selected_product"]
    store = context.user_data["selected_store"]
    uid = update.effective_user.id
    name = get_user_name(update)
    price, cost = get_product_price(product["id"])
    total = qty * price
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    sheet_append("Topshirish", [now, str(uid), store, product[lang], qty, product["unit"], price, total])

    msg = (
        f"✅ {'Topshirildi' if lang=='uz' else 'Передано'}!\n\n"
        f"🏪 {'Do\'kon' if lang=='uz' else 'Магазин'}: {store}\n"
        f"📦 {product[lang]}: {qty} {product['unit']}\n"
        f"💰 {'Narx' if lang=='uz' else 'Цена'}: {price:,.0f} so'm\n"
        f"💵 {'Jami' if lang=='uz' else 'Итого'}: {total:,.0f} so'm\n\n"
        f"{'Yana mahsulot qo\'shish uchun tanlang yoki orqaga' if lang=='uz' else 'Выберите ещё или вернитесь'}"
    )
    await update.message.reply_text(msg, reply_markup=product_keyboard(lang))
    return TOPSHIR_PRODUCT

# ─── TO'LOV ───────────────────────────────────────────────────
async def tolov_store(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text

    if text == t("btn_back", lang):
        await update.message.reply_text(t("main_menu", lang), reply_markup=main_menu_keyboard(lang, context.user_data.get("is_admin", False)))
        return MAIN_MENU

    stores = context.user_data.get("stores", [])
    store_names = [s.get("Nomi", "") for s in stores]
    if text not in store_names:
        await update.message.reply_text(t("choose_store", lang), reply_markup=stores_keyboard(stores, lang))
        return TOLOV_STORE

    context.user_data["selected_store"] = text
    debt = get_debt(text)
    debt_info = f"\n⚠️ {'Qarz' if lang=='uz' else 'Долг'}: {debt:,.0f} so'm" if debt > 0 else ""
    await update.message.reply_text(
        f"🏪 {text}{debt_info}\n\n{'To\'lov summasini kiriting:' if lang=='uz' else 'Введите сумму оплаты:'}",
        reply_markup=ReplyKeyboardMarkup([[t("btn_back", lang)]], resize_keyboard=True)
    )
    return TOLOV_AMOUNT

async def tolov_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text

    if text == t("btn_back", lang):
        stores = context.user_data.get("stores", [])
        await update.message.reply_text(t("choose_store", lang), reply_markup=stores_keyboard(stores, lang))
        return TOLOV_STORE

    try:
        amount = float(text.replace(",", ".").replace(" ", ""))
    except:
        await update.message.reply_text(t("error_num", lang))
        return TOLOV_AMOUNT

    context.user_data["tolov_amount"] = amount
    await update.message.reply_text(
        t("tolov_method", lang),
        reply_markup=ReplyKeyboardMarkup(
            [[t("btn_naqd", lang), t("btn_qarz", lang)], [t("btn_back", lang)]],
            resize_keyboard=True
        )
    )
    return TOLOV_METHOD

async def tolov_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text

    if text == t("btn_back", lang):
        await update.message.reply_text(
            f"{'To\'lov summasini kiriting:' if lang=='uz' else 'Введите сумму:'}",
            reply_markup=ReplyKeyboardMarkup([[t("btn_back", lang)]], resize_keyboard=True)
        )
        return TOLOV_AMOUNT

    store = context.user_data["selected_store"]
    amount = context.user_data["tolov_amount"]
    uid = update.effective_user.id
    name = get_user_name(update)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    method = "Naqd" if text == t("btn_naqd", lang) else "Qarz"
    sheet_append("Tolov", [now, str(uid), store, amount, method, ""])

    msg = (
        f"✅ {'To\'lov saqlandi' if lang=='uz' else 'Оплата сохранена'}!\n\n"
        f"🏪 {store}\n"
        f"💵 {amount:,.0f} so'm\n"
        f"{'Usul' if lang=='uz' else 'Метод'}: {method}"
    )
    await update.message.reply_text(msg, reply_markup=main_menu_keyboard(lang, context.user_data.get("is_admin", False)))
    return MAIN_MENU

# ─── KUNLIK NATIJA ────────────────────────────────────────────
async def show_daily_result(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    uid = str(update.effective_user.id)
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        sales = sheet_get_all("Topshirish")
        payments = sheet_get_all("Tolov")
        intake = sheet_get_all("Qabul")

        today_sales = [r for r in sales if r.get("Sana", "").startswith(today) and str(r.get("Distribyutor_ID", "")) == uid]
        today_payments = [r for r in payments if r.get("Sana", "").startswith(today) and str(r.get("Distribyutor_ID", "")) == uid]
        today_intake = [r for r in intake if r.get("Sana", "").startswith(today) and str(r.get("Distribyutor_ID", "")) == uid]

        total_sales = sum(float(r.get("Jami", 0)) for r in today_sales)
        total_cash = sum(float(r.get("Summa", 0)) for r in today_payments if r.get("Usul") == "Naqd")
        total_debt = sum(float(r.get("Summa", 0)) for r in today_payments if r.get("Usul") == "Qarz")
        total_intake = sum(float(r.get("Jami", 0)) for r in today_intake)

        if lang == "uz":
            msg = (
                f"📊 Kunlik natija — {today}\n"
                f"{'─'*25}\n"
                f"📥 Zavoddan olindi: {total_intake:,.0f} so'm\n"
                f"💰 Sotuv: {total_sales:,.0f} so'm\n"
                f"💵 Naqd: {total_cash:,.0f} so'm\n"
                f"📝 Qarz: {total_debt:,.0f} so'm\n"
                f"{'─'*25}\n"
               f"🏪 Dokonlar soni: {len(set(r.get('Dokon', '') for r in today_sales))}"
            )
        else:
            msg = (
                f"📊 Итог дня — {today}\n"
                f"{'─'*25}\n"
                f"📥 Получено с завода: {total_intake:,.0f} сум\n"
                f"💰 Продажи: {total_sales:,.0f} сум\n"
                f"💵 Наличные: {total_cash:,.0f} сум\n"
                f"📝 Долг: {total_debt:,.0f} сум\n"
                f"{'─'*25}\n"
                f"🏪 Магазинов обслужено: {len(set(r.get(\"Do'kon\",\"\") for r in today_sales))}"
            )
    except Exception as e:
        msg = f"❗ Xatolik: {e}"

    await update.message.reply_text(msg)

async def show_warehouse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    uid = str(update.effective_user.id)
    try:
        intake = sheet_get_all("Qabul")
        sales = sheet_get_all("Topshirish")

        warehouse = {}
        for r in intake:
            if str(r.get("Distribyutor_ID", "")) == uid:
                key = r.get("Mahsulot", "")
                warehouse[key] = warehouse.get(key, 0) + float(r.get("Miqdor", 0))

        for r in sales:
            if str(r.get("Distribyutor_ID", "")) == uid:
                key = r.get("Mahsulot", "")
                warehouse[key] = warehouse.get(key, 0) - float(r.get("Miqdor", 0))

        if not warehouse:
            await update.message.reply_text("📦 Ombor bo'sh / Склад пуст")
            return

        lines = [f"📦 {'Ombor holati' if lang=='uz' else 'Состояние склада'}:\n{'─'*20}"]
        for product, qty in warehouse.items():
            if qty > 0:
                lines.append(f"• {product}: {qty:.1f}")
        if len(lines) == 1:
            lines.append("Hamma mahsulot topshirilgan ✅" if lang == "uz" else "Все товары сданы ✅")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❗ Xatolik: {e}")

async def show_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    await update.message.reply_text(
        "📋 " + ("Buyurtmalar moduli keyingi versiyada — admin do'konlarga buyurtma kiritadi." if lang == "uz"
                  else "Модуль заказов — в следующей версии. Админ вводит заказы по магазинам.")
    )

# ─── ADMIN PANEL ──────────────────────────────────────────────
async def admin_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text
    uid = update.effective_user.id

    if uid not in ADMIN_IDS:
        await update.message.reply_text(t("admin_only", lang))
        return MAIN_MENU

    if text == t("btn_back", lang):
        await update.message.reply_text(t("main_menu", lang), reply_markup=main_menu_keyboard(lang, True))
        return MAIN_MENU

    elif text == t("btn_set_price", lang):
        await update.message.reply_text(t("choose_product", lang), reply_markup=product_keyboard(lang))
        return ADMIN_PRICE_PRODUCT

    elif text == t("btn_add_store", lang):
        await update.message.reply_text(t("enter_store_name", lang), reply_markup=ReplyKeyboardMarkup([[t("btn_back", lang)]], resize_keyboard=True))
        return ADMIN_ADD_STORE_NAME

    elif text == t("btn_add_dist", lang):
        await update.message.reply_text(t("enter_dist_name", lang), reply_markup=ReplyKeyboardMarkup([[t("btn_back", lang)]], resize_keyboard=True))
        return ADMIN_ADD_DIST_NAME

    elif text == t("btn_all_stats", lang):
        await admin_stats(update, context)
        return ADMIN_MENU

    return ADMIN_MENU

async def admin_price_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text

    if text == t("btn_back", lang):
        await update.message.reply_text(t("admin_menu", lang), reply_markup=ReplyKeyboardMarkup([
            [t("btn_set_price", lang), t("btn_add_store", lang)],
            [t("btn_add_dist", lang), t("btn_all_stats", lang)],
            [t("btn_back", lang)]
        ], resize_keyboard=True))
        return ADMIN_MENU

    product = find_product_by_name(text, lang)
    if not product:
        await update.message.reply_text(t("choose_product", lang), reply_markup=product_keyboard(lang))
        return ADMIN_PRICE_PRODUCT

    context.user_data["selected_product"] = product
    current_price, current_cost = get_product_price(product["id"])
    await update.message.reply_text(
        f"{text}\n{'Joriy narx' if lang=='uz' else 'Текущая цена'}: {current_price:,.0f}\n\n" + t("enter_price", lang),
        reply_markup=ReplyKeyboardMarkup([[t("btn_back", lang)]], resize_keyboard=True)
    )
    return ADMIN_PRICE_VALUE

async def admin_price_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text

    if text == t("btn_back", lang):
        await update.message.reply_text(t("choose_product", lang), reply_markup=product_keyboard(lang))
        return ADMIN_PRICE_PRODUCT

    try:
        price = float(text.replace(",", ".").replace(" ", ""))
    except:
        await update.message.reply_text(t("error_num", lang))
        return ADMIN_PRICE_VALUE

    context.user_data["new_price"] = price
    await update.message.reply_text(t("enter_cost", lang))
    return ADMIN_COST_VALUE

async def admin_cost_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text
    try:
        cost = float(text.replace(",", ".").replace(" ", ""))
    except:
        await update.message.reply_text(t("error_num", lang))
        return ADMIN_COST_VALUE

    product = context.user_data["selected_product"]
    price = context.user_data["new_price"]
    set_product_price(product["id"], product[lang], price, cost)

    await update.message.reply_text(
        f"✅ {product[lang]}\n💰 {'Narx' if lang=='uz' else 'Цена'}: {price:,.0f}\n📊 {'Tannarx' if lang=='uz' else 'Себест.'}: {cost:,.0f}",
        reply_markup=main_menu_keyboard(lang, True)
    )
    return MAIN_MENU

async def admin_add_store_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text
    if text == t("btn_back", lang):
        await update.message.reply_text(t("admin_menu", lang))
        return ADMIN_MENU
    context.user_data["new_store_name"] = text
    await update.message.reply_text(t("enter_store_dist", lang))
    return ADMIN_ADD_STORE_DIST

async def admin_add_store_dist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text
    store_name = context.user_data.get("new_store_name", "")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    sheet_append("Do'konlar", [len(sheet_get_all("Do'konlar")) + 1, store_name, "", text, now])
    await update.message.reply_text(
        f"✅ {'Do\'kon qo\'shildi' if lang=='uz' else 'Магазин добавлен'}: {store_name}",
        reply_markup=main_menu_keyboard(lang, True)
    )
    return MAIN_MENU

async def admin_add_dist_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text
    if text == t("btn_back", lang):
        await update.message.reply_text(t("admin_menu", lang))
        return ADMIN_MENU
    context.user_data["new_dist_name"] = text
    await update.message.reply_text(t("enter_dist_id", lang))
    return ADMIN_ADD_DIST_ID

async def admin_add_dist_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    text = update.message.text
    dist_name = context.user_data.get("new_dist_name", "")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    sheet_append("Foydalanuvchilar", [text, dist_name, "distributor", lang, now])
    await update.message.reply_text(
        f"✅ {'Distribyutor qo\'shildi' if lang=='uz' else 'Дистрибьютор добавлен'}: {dist_name}",
        reply_markup=main_menu_keyboard(lang, True)
    )
    return MAIN_MENU

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    try:
        sales = sheet_get_all("Topshirish")
        payments = sheet_get_all("Tolov")
        intake = sheet_get_all("Qabul")
        stores = sheet_get_all("Do'konlar")

        total_sales = sum(float(r.get("Jami", 0)) for r in sales)
        total_cash = sum(float(r.get("Summa", 0)) for r in payments if r.get("Usul") == "Naqd")
        total_debt = sum(float(r.get("Summa", 0)) for r in payments if r.get("Usul") == "Qarz")
        total_intake = sum(float(r.get("Jami", 0)) for r in intake)

        if lang == "uz":
            msg = (
                f"📈 Umumiy statistika\n{'─'*25}\n"
                f"📥 Jami qabul: {total_intake:,.0f} so'm\n"
                f"💰 Jami sotuv: {total_sales:,.0f} so'm\n"
                f"💵 Naqd: {total_cash:,.0f} so'm\n"
                f"📝 Qarz: {total_debt:,.0f} so'm\n"
                f"🏪 Do'konlar: {len(stores)}"
            )
        else:
            msg = (
                f"📈 Общая статистика\n{'─'*25}\n"
                f"📥 Всего получено: {total_intake:,.0f} сум\n"
                f"💰 Всего продажи: {total_sales:,.0f} сум\n"
                f"💵 Наличные: {total_cash:,.0f} сум\n"
                f"📝 Долг: {total_debt:,.0f} сум\n"
                f"🏪 Магазинов: {len(stores)}"
            )
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"❗ Xatolik: {e}")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(context)
    await update.message.reply_text(t("cancel", lang), reply_markup=main_menu_keyboard(lang, context.user_data.get("is_admin", False)))
    return MAIN_MENU

# ─── MAIN ─────────────────────────────────────────────────────
def main():
    app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            LANG_SELECT: [
                CallbackQueryHandler(lang_selected, pattern="^lang_"),
                CommandHandler("lang", change_lang),
            ],
            MAIN_MENU: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu_handler),
            ],
            QABUL_PRODUCT: [MessageHandler(filters.TEXT & ~filters.COMMAND, qabul_product)],
            QABUL_QTY:     [MessageHandler(filters.TEXT & ~filters.COMMAND, qabul_qty)],
            TOPSHIR_STORE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, topshir_store)],
            TOPSHIR_PRODUCT: [MessageHandler(filters.TEXT & ~filters.COMMAND, topshir_product)],
            TOPSHIR_QTY:     [MessageHandler(filters.TEXT & ~filters.COMMAND, topshir_qty)],
            TOLOV_STORE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, tolov_store)],
            TOLOV_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, tolov_amount)],
            TOLOV_METHOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, tolov_method)],
            ADMIN_MENU:          [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_menu_handler)],
            ADMIN_PRICE_PRODUCT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_price_product)],
            ADMIN_PRICE_VALUE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_price_value)],
            ADMIN_COST_VALUE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_cost_value)],
            ADMIN_ADD_STORE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_store_name)],
            ADMIN_ADD_STORE_DIST: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_store_dist)],
            ADMIN_ADD_DIST_NAME:  [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_dist_name)],
            ADMIN_ADD_DIST_ID:    [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_dist_id)],
        },
        fallbacks=[CommandHandler("cancel", cancel), CommandHandler("start", start)],
        allow_reentry=True,
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("lang", change_lang))
    print("🤖 Bot ishga tushdi! / Бот запущен!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
