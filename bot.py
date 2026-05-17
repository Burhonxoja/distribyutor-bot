#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import logging
import re
from datetime import datetime, timedelta, date
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
)

# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
# ENVIRONMENT VARIABLES
# ═══════════════════════════════════════════════════════════
BOT_TOKEN = os.environ.get("BOT_TOKEN")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "")
CHANNEL_ID = os.environ.get("CHANNEL_ID")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")

if not all([BOT_TOKEN, SPREADSHEET_ID, GOOGLE_CREDS_JSON]):
    raise ValueError("Missing required environment variables")

ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(",") if x.strip()]

# ═══════════════════════════════════════════════════════════
# GOOGLE SHEETS CONNECTION
# ═══════════════════════════════════════════════════════════
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def get_sheets_client():
    """Initialize Google Sheets client"""
    try:
        creds_dict = json.loads(GOOGLE_CREDS_JSON)
        credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Sheets client: {e}")
        raise

gc = get_sheets_client()
sh = gc.open_by_key(SPREADSHEET_ID)

# ═══════════════════════════════════════════════════════════
# SHEETS HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def db_add(sheet_name: str, row_data: list) -> bool:
    """Add a single row to a sheet"""
    try:
        ws = sh.worksheet(sheet_name)
        ws.append_row(row_data, value_input_option='USER_ENTERED')
        logger.info(f"✅ Added row to {sheet_name}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to add row to {sheet_name}: {e}")
        return False

def db_get_all(sheet_name: str) -> list:
    """Get all records from a sheet"""
    try:
        ws = sh.worksheet(sheet_name)
        records = ws.get_all_records()
        return records
    except Exception as e:
        logger.error(f"❌ Failed to get records from {sheet_name}: {e}")
        return []

def db_update_row(sheet_name: str, row_num: int, col_num: int, value) -> bool:
    """Update a specific cell"""
    try:
        ws = sh.worksheet(sheet_name)
        ws.update_cell(row_num, col_num, value)
        return True
    except Exception as e:
        logger.error(f"❌ Failed to update {sheet_name}: {e}")
        return False

def generate_id(prefix: str) -> str:
    """Generate unique ID with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{prefix}_{timestamp}"

def clean_phone(phone: str) -> str:
    """Extract digits from phone number"""
    return re.sub(r'\D', '', phone)

def format_number(num) -> str:
    """Format number with thousand separators"""
    try:
        return f"{int(num):,}"
    except:
        return str(num)

# ═══════════════════════════════════════════════════════════
# PRICE MANAGEMENT FUNCTIONS
# ═══════════════════════════════════════════════════════════

def get_factory_price(mahsulot_id: str) -> int:
    """Get factory price for a product"""
    products = db_get_all("Mahsulotlar_Asosiy")
    for p in products:
        if p.get('ID') == mahsulot_id:
            return int(p.get('Zavod_Narxi', 0))
    return 0

def get_selling_price(dist_id: str, dokon_id: str, mahsulot_id: str) -> int:
    """
    Get selling price with priority:
    1. Shop-specific custom price
    2. Distributor's default price
    3. Admin's suggested price
    """
    # 1. Check shop-specific custom price
    custom_prices = db_get_all("Mahsulotlar_Maxsus_Narx")
    for cp in custom_prices:
        if (str(cp.get('Dist_ID')) == str(dist_id) and 
            str(cp.get('Dokon_ID')) == str(dokon_id) and 
            str(cp.get('Mahsulot_ID')) == str(mahsulot_id)):
            return int(cp.get('Sotish_Narxi', 0))
    
    # 2. Check distributor's default price
    dist_defaults = db_get_all("Mahsulotlar_Dist_Default")
    for dd in dist_defaults:
        if (str(dd.get('Dist_ID')) == str(dist_id) and 
            str(dd.get('Mahsulot_ID')) == str(mahsulot_id)):
            return int(dd.get('Sotish_Narxi', 0))
    
    # 3. Fallback to admin's suggested price
    products = db_get_all("Mahsulotlar_Asosiy")
    for p in products:
        if p.get('ID') == mahsulot_id:
            return int(p.get('Sotish_Narxi_Default', 0))
    
    return 0

def has_custom_price(dist_id: str, dokon_id: str, mahsulot_id: str) -> bool:
    """Check if shop has custom price for product"""
    custom_prices = db_get_all("Mahsulotlar_Maxsus_Narx")
    for cp in custom_prices:
        if (str(cp.get('Dist_ID')) == str(dist_id) and 
            str(cp.get('Dokon_ID')) == str(dokon_id) and 
            str(cp.get('Mahsulot_ID')) == str(mahsulot_id)):
            return True
    return False

# ═══════════════════════════════════════════════════════════
# CONVERSATION STATES
# ═══════════════════════════════════════════════════════════

# Shop Registration
S_DI_NM, S_DI_ADR, S_DI_MCHJ, S_DI_TEL1, S_DI_TEL2, S_DI_EG, S_DI_PHT, S_DI_LOC = range(8)

# Factory Receipt
S_QABUL_PRODUCT, S_QABUL_AMOUNT, S_QABUL_MORE = range(8, 11)

# Order
S_ORDER_SHOP, S_ORDER_PRODUCT, S_ORDER_AMOUNT, S_ORDER_MORE = range(11, 15)

# Delivery
S_DELIVERY_SHOP, S_DELIVERY_PRODUCT_INPUT, S_DELIVERY_VOZVRAT, S_DELIVERY_VOZVRAT_AMOUNT = range(15, 19)
S_DELIVERY_IZOH, S_DELIVERY_TOLOV, S_DELIVERY_NAQD, S_DELIVERY_TAROZI = range(19, 23)

# Price Management
S_PRICE_MENU, S_PRICE_SELECT_PRODUCT, S_PRICE_ENTER_PRICE = range(23, 26)
S_PRICE_CUSTOM_SELECT_SHOPS, S_PRICE_CUSTOM_ENTER_PRICE = range(26, 28)

# ═══════════════════════════════════════════════════════════
# START & MAIN MENU
# ═══════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    user_id = update.effective_user.id
    user_name = update.effective_user.full_name
    
    # Main menu buttons
    keyboard = [
        ["🏪 Do'konlarim", "➕ Do'kon qo'shish"],
        ["📦 Zavoddan qabul", "🚚 Tovar topshirish"],
        ["📋 Buyurtmalar", "💰 Narxlarim"],
        ["📊 Hisobotlar", "🏦 Ombor"]
    ]
    
    # Admin gets additional buttons
    if user_id in ADMIN_IDS:
        keyboard.append(["👨‍💼 Admin Panel"])
    
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    await update.message.reply_text(
        f"Assalomu alaykum, {user_name}!\n\n"
        "Alba Milk distribyutor botiga xush kelibsiz.",
        reply_markup=reply_markup
    )

async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle main menu selections"""
    text = update.message.text
    user_id = update.effective_user.id
    
    if text == "🏪 Do'konlarim":
        await show_my_shops(update, context)
    elif text == "➕ Do'kon qo'shish":
        return await dokon_start(update, context)
    elif text == "📦 Zavoddan qabul":
        return await qabul_start(update, context)
    elif text == "🚚 Tovar topshirish":
        return await topshirish_start(update, context)
    elif text == "📋 Buyurtmalar":
        return await buyurtmalar_start(update, context)
    elif text == "💰 Narxlarim":
        return await narxlar_menu(update, context)
    elif text == "📊 Hisobotlar":
        await hisobotlar_menu(update, context)
    elif text == "🏦 Ombor":
        await show_ombor(update, context)
    elif text == "👨‍💼 Admin Panel" and user_id in ADMIN_IDS:
        await admin_panel(update, context)
    
    return ConversationHandler.END

async def show_my_shops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show distributor's shops"""
    user_id = update.effective_user.id
    shops = db_get_all("Dokonlar")
    
    my_shops = [s for s in shops if str(s.get('Dist_ID')) == str(user_id)]
    
    if not my_shops:
        await update.message.reply_text("Sizda hali do'konlar yo'q.\n\n➕ Do'kon qo'shish tugmasini bosing.")
        return
    
    msg = "🏪 Mening do'konlarim:\n\n"
    for shop in my_shops:
        msg += f"📍 {shop.get('Nomi')}\n"
        msg += f"   {shop.get('Adres')}\n"
        msg += f"   📞 {shop.get('Tel1')}\n\n"
    
    await update.message.reply_text(msg)

# ═══════════════════════════════════════════════════════════
# SHOP REGISTRATION
# ═══════════════════════════════════════════════════════════

async def dokon_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start shop registration"""
    await update.message.reply_text(
        "Do'kon qo'shish\n\n"
        "Do'kon nomini kiriting:",
        reply_markup=ReplyKeyboardRemove()
    )
    return S_DI_NM

async def di_nm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get shop name"""
    context.user_data['dokon_nom'] = update.message.text
    await update.message.reply_text("📍 Manzilni kiriting:")
    return S_DI_ADR

async def di_adr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get shop address"""
    context.user_data['dokon_adres'] = update.message.text
    
    keyboard = [["O'tkazib yuborish"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    await update.message.reply_text(
        "🏢 MCHJ nomini kiriting (yoki o'tkazib yuboring):",
        reply_markup=reply_markup
    )
    return S_DI_MCHJ

async def di_mchj(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get MCHJ"""
    text = update.message.text
    context.user_data['dokon_mchj'] = "" if text == "O'tkazib yuborish" else text
    
    await update.message.reply_text(
        "📞 Tel 1 raqamini kiriting:\n\n"
        "Format: 998901234567\n"
        "Faqat raqamlar!",
        reply_markup=ReplyKeyboardRemove()
    )
    return S_DI_TEL1

async def di_tel1(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get Tel1 - NUMBERS ONLY, strict validation"""
    text = update.message.text
    phone = clean_phone(text)
    
    # Strict validation: must match 998XXXXXXXXX
    if not re.match(r'^998\d{9}$', phone):
        await update.message.reply_text(
            "❌ Faqat raqam kiriting (998901234567 formatida):\n\n"
            "Masalan: 998901234567"
        )
        return S_DI_TEL1
    
    context.user_data['dokon_tel1'] = phone
    
    keyboard = [["O'tkazib yuborish"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    await update.message.reply_text(
        "📞 Tel 2 raqamini kiriting (yoki o'tkazib yuboring):\n\n"
        "Format: 998901234567",
        reply_markup=reply_markup
    )
    return S_DI_TEL2

async def di_tel2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get Tel2 - NUMBERS ONLY or skip"""
    text = update.message.text
    
    if text == "O'tkazib yuborish":
        context.user_data['dokon_tel2'] = ""
    else:
        phone = clean_phone(text)
        if not re.match(r'^998\d{9}$', phone):
            keyboard = [["O'tkazib yuborish"]]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(
                "❌ Faqat raqam kiriting (998901234567 formatida) yoki o'tkazib yuboring:",
                reply_markup=reply_markup
            )
            return S_DI_TEL2
        context.user_data['dokon_tel2'] = phone
    
    await update.message.reply_text(
        "👤 Sotuvchi ismini kiriting:",
        reply_markup=ReplyKeyboardRemove()
    )
    return S_DI_EG

async def di_eg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get shop owner name"""
    context.user_data['dokon_sotuvchi'] = update.message.text
    await update.message.reply_text(
        "📸 Do'kon rasmini yuboring:\n\n"
        "❗️ MAJBURIY - rasm yuborish shart!"
    )
    return S_DI_PHT

async def di_pht(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get shop photo - MANDATORY"""
    if not update.message.photo:
        await update.message.reply_text(
            "❌ Iltimos rasm yuboring!\n\n"
            "Text emas, rasm kerak."
        )
        return S_DI_PHT
    
    # Get largest photo
    photo = update.message.photo[-1]
    context.user_data['dokon_photo'] = photo.file_id
    
    await update.message.reply_text(
        "📍 Do'kon lokatsiyasini yuboring:\n\n"
        "❗️ MAJBURIY - lokatsiya yuborish shart!"
    )
    return S_DI_LOC

async def di_loc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get shop location - MANDATORY, then save"""
    if not update.message.location:
        await update.message.reply_text(
            "❌ Iltimos lokatsiya yuboring!\n\n"
            "Text emas, lokatsiya kerak."
        )
        return S_DI_LOC
    
    loc = update.message.location
    user_id = update.effective_user.id
    user_name = update.effective_user.full_name
    
    # Generate shop ID
    dokon_id = generate_id("dk")
    
    # Prepare data
    nom = context.user_data.get('dokon_nom', '')
    adr = context.user_data.get('dokon_adres', '')
    mchj = context.user_data.get('dokon_mchj', '')
    tel1 = context.user_data.get('dokon_tel1', '')
    tel2 = context.user_data.get('dokon_tel2', '')
    sotuvchi = context.user_data.get('dokon_sotuvchi', '')
    photo_id = context.user_data.get('dokon_photo', '')
    lat = loc.latitude
    lng = loc.longitude
    sana = date.today().strftime("%Y-%m-%d")
    
    # Post to channel
    channel_msg_id = ""
    if CHANNEL_ID:
        try:
            caption = (
                f"✅ Do'kon qo'shildi!\n\n"
                f"📍 {nom}\n"
                f"🏢 {mchj}\n"
                f"📫 {adr}\n"
                f"📞 {tel1}\n"
                f"👤 {sotuvchi}\n"
                f"🚚 {user_name}"
            )
            
            # Send photo
            photo_msg = await context.bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=photo_id,
                caption=caption
            )
            channel_msg_id = str(photo_msg.message_id)
            
            # Send location
            await context.bot.send_location(
                chat_id=CHANNEL_ID,
                latitude=lat,
                longitude=lng
            )
        except Exception as e:
            logger.error(f"Failed to post to channel: {e}")
    
    # Save to Sheets
    row = [
        dokon_id, nom, adr, mchj, tel1, tel2, sotuvchi,
        str(user_id), user_name, lat, lng, channel_msg_id, sana
    ]
    
    if db_add("Dokonlar", row):
        await update.message.reply_text(
            f"✅ Do'kon muvaffaqiyatli qo'shildi!\n\n"
            f"📍 {nom}\n"
            f"📫 {adr}\n"
            f"📞 {tel1}",
            reply_markup=ReplyKeyboardMarkup([
                ["🏪 Do'konlarim", "➕ Do'kon qo'shish"],
                ["📦 Zavoddan qabul", "🚚 Tovar topshirish"]
            ], resize_keyboard=True)
        )
    else:
        await update.message.reply_text("❌ Xatolik yuz berdi. Qaytadan urinib ko'ring.")
    
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# FACTORY RECEIPT (ZAVODDAN QABUL) - Multi-product
# ═══════════════════════════════════════════════════════════

async def qabul_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start factory receipt"""
    context.user_data['qabul_items'] = []
    context.user_data['qabul_id'] = generate_id("qb")
    
    return await qabul_select_product(update, context)

async def qabul_select_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Select product for receipt"""
    products = db_get_all("Mahsulotlar_Asosiy")
    active_products = [p for p in products if p.get('Status') == 'active']
    
    if not active_products:
        await update.message.reply_text("❌ Mahsulotlar topilmadi.")
        return ConversationHandler.END
    
    # Group by product name
    grouped = {}
    for p in active_products:
        name = p.get('Nomi')
        if name not in grouped:
            grouped[name] = []
        grouped[name].append(p)
    
    # Create buttons
    buttons = []
    for name, variants in grouped.items():
        if len(variants) == 1:
            p = variants[0]
            turi = p.get('Turi', '-')
            label = f"{name}" if turi == '-' else f"{name} {turi}"
            buttons.append([InlineKeyboardButton(label, callback_data=f"qabul_prod_{p.get('ID')}")])
        else:
            for p in variants:
                turi = p.get('Turi', '-')
                label = f"{name} {turi}"
                buttons.append([InlineKeyboardButton(label, callback_data=f"qabul_prod_{p.get('ID')}")])
    
    buttons.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="qabul_cancel")])
    
    await update.message.reply_text(
        "📦 Zavoddan qabul qilish\n\n"
        "Mahsulotni tanlang:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_QABUL_PRODUCT

async def qabul_product_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle product selection"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "qabul_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    product_id = query.data.replace("qabul_prod_", "")
    products = db_get_all("Mahsulotlar_Asosiy")
    
    product = None
    for p in products:
        if p.get('ID') == product_id:
            product = p
            break
    
    if not product:
        await query.edit_message_text("❌ Mahsulot topilmadi")
        return ConversationHandler.END
    
    context.user_data['current_qabul_product'] = product
    
    birlik = product.get('Birlik', 'dona')
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    
    await query.edit_message_text(
        f"Mahsulot: {label}\n\n"
        f"Miqdorni kiriting ({birlik}):"
    )
    return S_QABUL_AMOUNT

async def qabul_amount_entered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle amount input"""
    try:
        amount = float(update.message.text)
        if amount <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri miqdor. Raqam kiriting:")
        return S_QABUL_AMOUNT
    
    product = context.user_data.get('current_qabul_product')
    zavod_narx = int(product.get('Zavod_Narxi', 0))
    jami = amount * zavod_narx
    
    # Add to items
    item = {
        'mahsulot_id': product.get('ID'),
        'nomi': product.get('Nomi'),
        'turi': product.get('Turi', '-'),
        'miqdor': amount,
        'birlik': product.get('Birlik'),
        'zavod_narx': zavod_narx,
        'jami': jami
    }
    
    if 'qabul_items' not in context.user_data:
        context.user_data['qabul_items'] = []
    context.user_data['qabul_items'].append(item)
    
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    
    buttons = [
        [InlineKeyboardButton("➕ Yana mahsulot qabul", callback_data="qabul_more")],
        [InlineKeyboardButton("✅ Yakunlash", callback_data="qabul_finish")]
    ]
    
    await update.message.reply_text(
        f"✅ Qabul qilindi!\n\n"
        f"{label}: {amount} {product.get('Birlik')}\n"
        f"Summa: {format_number(jami)} so'm\n\n"
        f"Omborga qo'shildi.",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_QABUL_MORE

async def qabul_more_or_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle more or finish"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "qabul_more":
        await query.message.delete()
        return await qabul_select_product(query, context)
    
    # Finish - save all items
    items = context.user_data.get('qabul_items', [])
    qabul_id = context.user_data.get('qabul_id')
    user_id = update.effective_user.id
    sana = date.today().strftime("%Y-%m-%d")
    
    total_sum = 0
    for item in items:
        row = [
            sana,
            str(user_id),
            item['nomi'],
            item['turi'],
            item['miqdor'],
            item['birlik'],
            item['zavod_narx'],
            item['jami'],
            'kutilmoqda',
            qabul_id
        ]
        db_add("Qabul", row)
        total_sum += item['jami']
        
        # Update Ombor
        update_ombor(user_id, item['mahsulot_id'], item['nomi'], item['turi'], item['miqdor'], item['birlik'], 'add')
    
    # Summary
    msg = "✅ Qabul yakunlandi!\n\n"
    for item in items:
        label = f"{item['nomi']}" if item['turi'] == '-' else f"{item['nomi']} {item['turi']}"
        msg += f"• {label}: {item['miqdor']} {item['birlik']}\n"
    msg += f"\n💰 Jami: {format_number(total_sum)} so'm"
    
    await query.edit_message_text(msg)
    context.user_data.clear()
    return ConversationHandler.END

def update_ombor(dist_id, mahsulot_id, nomi, turi, miqdor, birlik, operation='add'):
    """Update warehouse stock"""
    try:
        ombor = db_get_all("Ombor")
        found = False
        
        for idx, item in enumerate(ombor):
            if (str(item.get('Dist_ID')) == str(dist_id) and 
                item.get('Mahsulot') == nomi and 
                item.get('Turi') == turi):
                # Update existing
                found = True
                current = float(item.get('Miqdor', 0))
                if operation == 'add':
                    new_amount = current + float(miqdor)
                else:  # subtract
                    new_amount = current - float(miqdor)
                
                # Row number is idx + 2 (header + 1-indexed)
                db_update_row("Ombor", idx + 2, 4, new_amount)  # Column 4 is Miqdor
                break
        
        if not found:
            # Add new row
            row = [str(dist_id), nomi, turi, miqdor, birlik]
            db_add("Ombor", row)
    except Exception as e:
        logger.error(f"Failed to update Ombor: {e}")

# ═══════════════════════════════════════════════════════════
# ORDER (BUYURTMA) - Multi-product
# ═══════════════════════════════════════════════════════════

async def buyurtmalar_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start order or view orders"""
    buttons = [
        [InlineKeyboardButton("➕ Yangi buyurtma", callback_data="order_new")],
        [InlineKeyboardButton("📋 Buyurtmalarni ko'rish", callback_data="order_view")]
    ]
    
    await update.message.reply_text(
        "📋 Buyurtmalar\n\n"
        "Tanlov:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def order_new_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start new order - select shop"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    shops = db_get_all("Dokonlar")
    my_shops = [s for s in shops if str(s.get('Dist_ID')) == str(user_id)]
    
    if not my_shops:
        await query.edit_message_text("❌ Sizda do'konlar yo'q. Avval do'kon qo'shing.")
        return ConversationHandler.END
    
    buttons = []
    for shop in my_shops:
        buttons.append([InlineKeyboardButton(
            shop.get('Nomi'),
            callback_data=f"order_shop_{shop.get('ID')}"
        )])
    buttons.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="order_cancel")])
    
    context.user_data['order_items'] = []
    context.user_data['zakaz_id'] = generate_id("z")
    
    await query.edit_message_text(
        "Do'konni tanlang:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_ORDER_SHOP

async def order_shop_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle shop selection"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "order_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    shop_id = query.data.replace("order_shop_", "")
    shops = db_get_all("Dokonlar")
    
    shop = None
    for s in shops:
        if s.get('ID') == shop_id:
            shop = s
            break
    
    if not shop:
        await query.edit_message_text("❌ Do'kon topilmadi")
        return ConversationHandler.END
    
    context.user_data['order_shop'] = shop
    
    return await order_select_product(query, context)

async def order_select_product(update, context: ContextTypes.DEFAULT_TYPE):
    """Select product for order"""
    products = db_get_all("Mahsulotlar_Asosiy")
    active_products = [p for p in products if p.get('Status') == 'active']
    
    user_id = update.from_user.id if hasattr(update, 'from_user') else update.effective_user.id
    shop = context.user_data.get('order_shop')
    
    # Group products
    grouped = {}
    for p in active_products:
        name = p.get('Nomi')
        if name not in grouped:
            grouped[name] = []
        grouped[name].append(p)
    
    buttons = []
    for name, variants in grouped.items():
        if len(variants) == 1:
            p = variants[0]
            turi = p.get('Turi', '-')
            
            # Get price
            price = get_selling_price(user_id, shop.get('ID'), p.get('ID'))
            has_custom = has_custom_price(user_id, shop.get('ID'), p.get('ID'))
            
            label = f"{name}" if turi == '-' else f"{name} {turi}"
            label += f" — {format_number(price)}"
            if has_custom:
                label += " ⭐"
            
            buttons.append([InlineKeyboardButton(label, callback_data=f"order_prod_{p.get('ID')}")])
        else:
            for p in variants:
                turi = p.get('Turi', '-')
                price = get_selling_price(user_id, shop.get('ID'), p.get('ID'))
                has_custom = has_custom_price(user_id, shop.get('ID'), p.get('ID'))
                
                label = f"{name} {turi} — {format_number(price)}"
                if has_custom:
                    label += " ⭐"
                
                buttons.append([InlineKeyboardButton(label, callback_data=f"order_prod_{p.get('ID')}")])
    
    buttons.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="order_cancel")])
    
    if hasattr(update, 'edit_message_text'):
        await update.edit_message_text(
            f"🏪 Do'kon: {shop.get('Nomi')}\n\n"
            "Mahsulotni tanlang:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    else:
        await update.message.reply_text(
            f"🏪 Do'kon: {shop.get('Nomi')}\n\n"
            "Mahsulotni tanlang:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    
    return S_ORDER_PRODUCT

async def order_product_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle product selection"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "order_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    product_id = query.data.replace("order_prod_", "")
    products = db_get_all("Mahsulotlar_Asosiy")
    
    product = None
    for p in products:
        if p.get('ID') == product_id:
            product = p
            break
    
    if not product:
        await query.edit_message_text("❌ Mahsulot topilmadi")
        return ConversationHandler.END
    
    context.user_data['current_order_product'] = product
    
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    birlik = product.get('Birlik')
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    
    await query.edit_message_text(
        f"Mahsulot: {label}\n\n"
        f"Miqdorni kiriting ({birlik}):"
    )
    return S_ORDER_AMOUNT

async def order_amount_entered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle amount input"""
    try:
        amount = float(update.message.text)
        if amount <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri miqdor. Raqam kiriting:")
        return S_ORDER_AMOUNT
    
    user_id = update.effective_user.id
    shop = context.user_data.get('order_shop')
    product = context.user_data.get('current_order_product')
    
    # Get selling price
    price = get_selling_price(user_id, shop.get('ID'), product.get('ID'))
    jami = amount * price
    
    # Add to cart
    item = {
        'mahsulot_id': product.get('ID'),
        'nomi': product.get('Nomi'),
        'turi': product.get('Turi', '-'),
        'miqdor': amount,
        'birlik': product.get('Birlik'),
        'narx': price,
        'jami': jami
    }
    
    if 'order_items' not in context.user_data:
        context.user_data['order_items'] = []
    context.user_data['order_items'].append(item)
    
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    
    buttons = [
        [InlineKeyboardButton("➕ Yana mahsulot", callback_data="order_more")],
        [InlineKeyboardButton("📋 Zakazni tasdiqlash", callback_data="order_confirm")]
    ]
    
    await update.message.reply_text(
        f"✅ Savatga qo'shildi!\n\n"
        f"{label}: {amount} {product.get('Birlik')}\n"
        f"Narx: {format_number(price)} so'm\n"
        f"Jami: {format_number(jami)} so'm",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_ORDER_MORE

async def order_more_or_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle more products or confirm order"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "order_more":
        await query.message.delete()
        return await order_select_product(query, context)
    
    # Confirm - save order
    items = context.user_data.get('order_items', [])
    zakaz_id = context.user_data.get('zakaz_id')
    shop = context.user_data.get('order_shop')
    user_id = update.effective_user.id
    sana = date.today().strftime("%Y-%m-%d")
    
    total_sum = 0
    for item in items:
        row = [
            sana,
            str(user_id),
            shop.get('Nomi'),
            shop.get('ID'),
            item['nomi'],
            item['turi'],
            item['miqdor'],
            item['birlik'],
            item['narx'],
            item['jami'],
            '',  # Pay_Type
            0,   # Naqd
            0,   # Qarz
            'kutilmoqda',
            zakaz_id
        ]
        db_add("Buyurtmalar", row)
        total_sum += item['jami']
    
    # Summary
    msg = f"✅ Buyurtma tasdiqlandi!\n\n"
    msg += f"🏪 Do'kon: {shop.get('Nomi')}\n\n"
    for item in items:
        label = f"{item['nomi']}" if item['turi'] == '-' else f"{item['nomi']} {item['turi']}"
        msg += f"• {label}: {item['miqdor']} {item['birlik']}\n"
    msg += f"\n💰 Jami: {format_number(total_sum)} so'm"
    
    await query.edit_message_text(msg)
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# DELIVERY (TOPSHIRISH) - With reminders
# ═══════════════════════════════════════════════════════════

async def topshirish_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start delivery - select shop with active orders"""
    user_id = update.effective_user.id
    orders = db_get_all("Buyurtmalar")
    
    # Get shops with pending orders
    pending_shops = {}
    for order in orders:
        if str(order.get('Dist_ID')) == str(user_id) and order.get('Status') == 'kutilmoqda':
            shop_id = order.get('Dokon_ID')
            shop_name = order.get('Dokon')
            if shop_id not in pending_shops:
                pending_shops[shop_id] = shop_name
    
    if not pending_shops:
        await update.message.reply_text("📦 Topshirish uchun zakazlar yo'q.")
        return ConversationHandler.END
    
    buttons = []
    for shop_id, shop_name in pending_shops.items():
        buttons.append([InlineKeyboardButton(shop_name, callback_data=f"top_shop_{shop_id}")])
    buttons.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="top_cancel")])
    
    context.user_data['topshirish_products'] = []
    context.user_data['topshirish_current_index'] = 0
    
    await update.message.reply_text(
        "🚚 Tovar topshirish\n\n"
        "Do'konni tanlang:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_DELIVERY_SHOP

async def delivery_shop_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shop selected - get orders and start product-by-product input"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "top_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    shop_id = query.data.replace("top_shop_", "")
    user_id = update.effective_user.id
    
    # Get shop info
    shops = db_get_all("Dokonlar")
    shop = None
    for s in shops:
        if s.get('ID') == shop_id:
            shop = s
            break
    
    if not shop:
        await query.edit_message_text("❌ Do'kon topilmadi")
        return ConversationHandler.END
    
    # Get all ordered products for this shop
    orders = db_get_all("Buyurtmalar")
    products_ordered = []
    zakaz_id = None
    
    for order in orders:
        if (str(order.get('Dist_ID')) == str(user_id) and 
            order.get('Dokon_ID') == shop_id and 
            order.get('Status') == 'kutilmoqda'):
            
            if zakaz_id is None:
                zakaz_id = order.get('Zakaz_ID')
            
            products_ordered.append({
                'mahsulot': order.get('Mahsulot'),
                'turi': order.get('Turi'),
                'zakaz_miqdor': float(order.get('Miqdor', 0)),
                'birlik': order.get('Birlik'),
                'narx': int(order.get('Narx', 0))
            })
    
    if not products_ordered:
        await query.edit_message_text("❌ Zakazlar topilmadi")
        return ConversationHandler.END
    
    context.user_data['delivery_shop'] = shop
    context.user_data['delivery_products'] = products_ordered
    context.user_data['delivery_results'] = []
    context.user_data['delivery_index'] = 0
    context.user_data['zakaz_id'] = zakaz_id
    
    # Start first product
    return await ask_delivery_amount(query, context)

async def ask_delivery_amount(update, context: ContextTypes.DEFAULT_TYPE):
    """Ask delivery amount for current product"""
    idx = context.user_data.get('delivery_index', 0)
    products = context.user_data.get('delivery_products', [])
    
    if idx >= len(products):
        # All products done - move to next step
        return await ask_izoh(update, context)
    
    product = products[idx]
    label = f"{product['mahsulot']}" if product['turi'] == '-' else f"{product['mahsulot']} {product['turi']}"
    
    msg = (
        f"━━━━━━━━━━━━━━━━\n"
        f"Mahsulot: {label}\n"
        f"Zakaz: {product['zakaz_miqdor']} {product['birlik']}\n\n"
        f"Qancha topshirdingiz? ({product['birlik']})"
    )
    
    if hasattr(update, 'edit_message_text'):
        await update.edit_message_text(msg)
    else:
        await update.message.reply_text(msg)
    
    return S_DELIVERY_PRODUCT_INPUT

async def delivery_amount_entered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle delivery amount input"""
    try:
        amount = float(update.message.text)
        if amount < 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri miqdor. Raqam kiriting:")
        return S_DELIVERY_PRODUCT_INPUT
    
    context.user_data['current_delivery_amount'] = amount
    
    buttons = [
        [InlineKeyboardButton("Ha", callback_data="vozvrat_yes")],
        [InlineKeyboardButton("Yo'q", callback_data="vozvrat_no")]
    ]
    
    await update.message.reply_text(
        "Vozvrat bormi?",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_DELIVERY_VOZVRAT

async def delivery_vozvrat_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle vozvrat choice"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "vozvrat_no":
        context.user_data['current_vozvrat'] = 0
        return await save_current_product_and_next(query, context)
    
    # Ask vozvrat amount
    idx = context.user_data.get('delivery_index', 0)
    products = context.user_data.get('delivery_products', [])
    product = products[idx]
    
    await query.edit_message_text(
        f"Vozvrat miqdori? ({product['birlik']})"
    )
    return S_DELIVERY_VOZVRAT_AMOUNT

async def delivery_vozvrat_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle vozvrat amount"""
    try:
        vozvrat = float(update.message.text)
        if vozvrat < 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri miqdor. Raqam kiriting:")
        return S_DELIVERY_VOZVRAT_AMOUNT
    
    context.user_data['current_vozvrat'] = vozvrat
    return await save_current_product_and_next(update, context)

async def save_current_product_and_next(update, context: ContextTypes.DEFAULT_TYPE):
    """Save current product delivery data and move to next"""
    idx = context.user_data.get('delivery_index', 0)
    products = context.user_data.get('delivery_products', [])
    product = products[idx]
    
    delivery_amount = context.user_data.get('current_delivery_amount', 0)
    vozvrat = context.user_data.get('current_vozvrat', 0)
    
    # Save result
    result = {
        'mahsulot': product['mahsulot'],
        'turi': product['turi'],
        'zakaz_miqdor': product['zakaz_miqdor'],
        'topshirish_miqdor': delivery_amount,
        'vozvrat_miqdor': vozvrat,
        'birlik': product['birlik'],
        'narx': product['narx']
    }
    
    if 'delivery_results' not in context.user_data:
        context.user_data['delivery_results'] = []
    context.user_data['delivery_results'].append(result)
    
    # Move to next product
    context.user_data['delivery_index'] = idx + 1
    
    return await ask_delivery_amount(update, context)

async def ask_izoh(update, context: ContextTypes.DEFAULT_TYPE):
    """Ask for izoh after all products"""
    # Check if Qaymoq was delivered
    results = context.user_data.get('delivery_results', [])
    qaymoq_bor = any('Qaymoq' in r['mahsulot'] for r in results)
    
    context.user_data['qaymoq_bor'] = qaymoq_bor
    
    if qaymoq_bor:
        context.user_data['eslatma_kun'] = 5
        msg = "✅ Qaymoq topshirildi — 5 kunda eslatma o'rnatildi.\n\n"
    else:
        msg = ""
    
    buttons = [
        [InlineKeyboardButton("Ha", callback_data="izoh_yes")],
        [InlineKeyboardButton("Yo'q", callback_data="izoh_no")]
    ]
    
    msg += "Izoh qo'shasizmi?"
    
    if hasattr(update, 'edit_message_text'):
        await update.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
    
    return S_DELIVERY_IZOH

async def delivery_izoh_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle izoh choice"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "izoh_no":
        context.user_data['izoh'] = ""
        return await ask_tolov(query, context)
    
    await query.edit_message_text("Izohni kiriting:")
    return S_DELIVERY_IZOH

async def delivery_izoh_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle izoh text"""
    context.user_data['izoh'] = update.message.text
    
    # If no qaymoq, ask for eslatma kun
    if not context.user_data.get('qaymoq_bor'):
        await update.message.reply_text(
            "Necha kundan keyin eslatma? (1-30)\n\n"
            "Maksimal: 7 kun"
        )
        return S_DELIVERY_IZOH  # reuse state for eslatma_kun input
    
    return await ask_tolov(update, context)

async def delivery_eslatma_kun(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle eslatma kun input (if no qaymoq)"""
    try:
        kun = int(update.message.text)
        if kun < 1 or kun > 30:
            raise ValueError
    except:
        await update.message.reply_text("❌ 1 dan 30 gacha raqam kiriting:")
        return S_DELIVERY_IZOH
    
    if kun > 7:
        kun = 7
        await update.message.reply_text("✅ Eslatma 7 kunda o'rnatildi (maksimal muddat)")
    else:
        await update.message.reply_text(f"✅ Eslatma {kun} kunda o'rnatildi")
    
    context.user_data['eslatma_kun'] = kun
    return await ask_tolov(update, context)

async def ask_tolov(update, context: ContextTypes.DEFAULT_TYPE):
    """Ask payment method"""
    buttons = [
        [InlineKeyboardButton("💵 Naqd", callback_data="tolov_naqd")],
        [InlineKeyboardButton("📝 Realizatsiya", callback_data="tolov_real")]
    ]
    
    if hasattr(update, 'edit_message_text'):
        await update.edit_message_text(
            "To'lov usuli?",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    else:
        await update.message.reply_text(
            "To'lov usuli?",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    
    return S_DELIVERY_TOLOV

async def delivery_tolov_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle payment choice"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "tolov_real":
        context.user_data['naqd'] = 0
        return await check_tarozi_required(query, context)
    
    await query.edit_message_text("Naqd summasini kiriting:")
    return S_DELIVERY_NAQD

async def delivery_naqd_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle naqd amount"""
    try:
        naqd = int(update.message.text)
        if naqd < 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ To'g'ri summa kiriting:")
        return S_DELIVERY_NAQD
    
    context.user_data['naqd'] = naqd
    return await check_tarozi_required(update, context)

async def check_tarozi_required(update, context: ContextTypes.DEFAULT_TYPE):
    """Check if scale photo is required"""
    results = context.user_data.get('delivery_results', [])
    
    # Check if any product requires scale photo
    requires_tarozi = False
    for r in results:
        mahsulot = r['mahsulot']
        turi = r.get('turi', '-')
        
        if mahsulot in ['Tvorog', 'Suzma', 'Qaymoq'] and turi == '1kg':
            requires_tarozi = True
            break
        if mahsulot == 'Brinza':
            requires_tarozi = True
            break
    
    if requires_tarozi:
        if hasattr(update, 'edit_message_text'):
            await update.edit_message_text(
                "📸 Tarozi rasmini yuboring:\n\n"
                "❗️ MAJBURIY"
            )
        else:
            await update.message.reply_text(
                "📸 Tarozi rasmini yuboring:\n\n"
                "❗️ MAJBURIY"
            )
        return S_DELIVERY_TAROZI
    
    # No tarozi needed - save
    context.user_data['tarozi_rasm'] = ""
    return await save_delivery(update, context)

async def delivery_tarozi_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle tarozi photo"""
    if not update.message.photo:
        await update.message.reply_text("❌ Iltimos rasm yuboring!")
        return S_DELIVERY_TAROZI
    
    photo = update.message.photo[-1]
    context.user_data['tarozi_rasm'] = photo.file_id
    
    return await save_delivery(update, context)

async def save_delivery(update, context: ContextTypes.DEFAULT_TYPE):
    """Save all delivery data to Sheets"""
    user_id = update.effective_user.id if hasattr(update, 'effective_user') else update.from_user.id
    shop = context.user_data.get('delivery_shop')
    results = context.user_data.get('delivery_results', [])
    zakaz_id = context.user_data.get('zakaz_id')
    izoh = context.user_data.get('izoh', '')
    naqd = context.user_data.get('naqd', 0)
    eslatma_kun = context.user_data.get('eslatma_kun', 7)
    qaymoq_bor = context.user_data.get('qaymoq_bor', False)
    tarozi_rasm = context.user_data.get('tarozi_rasm', '')
    
    sana = date.today().strftime("%Y-%m-%d")
    eslatma_sana = (date.today() + timedelta(days=eslatma_kun)).strftime("%Y-%m-%d")
    
    total_qarz = 0
    
    for r in results:
        # Calculate qarz for this product
        jami = r['topshirish_miqdor'] * r['narx']
        qarz = jami - (naqd / len(results))  # Distribute naqd evenly
        total_qarz += qarz
        
        row = [
            sana,
            str(user_id),
            shop.get('Nomi'),
            shop.get('ID'),
            zakaz_id,
            r['mahsulot'],
            r['turi'],
            r['zakaz_miqdor'],
            r['topshirish_miqdor'],
            r['vozvrat_miqdor'],
            r['birlik'],
            naqd / len(results),  # Distribute evenly
            qarz,
            tarozi_rasm,
            eslatma_kun,
            eslatma_sana,
            'Ha' if qaymoq_bor else 'Yo\'q',
            izoh,
            'topshirildi'
        ]
        db_add("Topshirish", row)
        
        # Update Ombor
        products = db_get_all("Mahsulotlar_Asosiy")
        mahsulot_id = None
        for p in products:
            if p.get('Nomi') == r['mahsulot'] and p.get('Turi') == r['turi']:
                mahsulot_id = p.get('ID')
                break
        
        if mahsulot_id:
            update_ombor(user_id, mahsulot_id, r['mahsulot'], r['turi'], r['topshirish_miqdor'], r['birlik'], 'subtract')
            if r['vozvrat_miqdor'] > 0:
                update_ombor(user_id, mahsulot_id, r['mahsulot'], r['turi'], r['vozvrat_miqdor'], r['birlik'], 'add')
    
    # Update order status
    orders = db_get_all("Buyurtmalar")
    for idx, order in enumerate(orders):
        if order.get('Zakaz_ID') == zakaz_id:
            db_update_row("Buyurtmalar", idx + 2, 14, 'topshirildi')  # Status column
    
    # Summary message
    msg = f"✅ Topshirish yakunlandi!\n\n"
    msg += f"🏪 {shop.get('Nomi')}\n\n"
    for r in results:
        label = f"{r['mahsulot']}" if r['turi'] == '-' else f"{r['mahsulot']} {r['turi']}"
        msg += f"• {label}: {r['topshirish_miqdor']} {r['birlik']}\n"
        if r['vozvrat_miqdor'] > 0:
            msg += f"  ↩️ Vozvrat: {r['vozvrat_miqdor']} {r['birlik']}\n"
    
    msg += f"\n💵 Naqd: {format_number(naqd)}\n"
    msg += f"📝 Qarz: {format_number(total_qarz)}\n"
    msg += f"⏰ Eslatma: {eslatma_kun} kunda"
    
    if hasattr(update, 'edit_message_text'):
        await update.edit_message_text(msg)
    elif hasattr(update, 'message'):
        await update.message.reply_text(msg)
    
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# PRICE MANAGEMENT (NARXLARIM)
# ═══════════════════════════════════════════════════════════

async def narxlar_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Price management main menu"""
    buttons = [
        [InlineKeyboardButton("📋 Narxlarni ko'rish", callback_data="price_view")],
        [InlineKeyboardButton("✏️ Asosiy narx o'zgartirish", callback_data="price_default")],
        [InlineKeyboardButton("🏪 Do'kon uchun maxsus narx", callback_data="price_custom")],
        [InlineKeyboardButton("❌ Yopish", callback_data="price_close")]
    ]
    
    await update.message.reply_text(
        "💰 Narxlar boshqaruvi\n\n"
        "Tanlov:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_PRICE_MENU

async def price_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View my prices"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    products = db_get_all("Mahsulotlar_Asosiy")
    active_products = [p for p in products if p.get('Status') == 'active']
    
    dist_defaults = db_get_all("Mahsulotlar_Dist_Default")
    custom_prices = db_get_all("Mahsulotlar_Maxsus_Narx")
    
    msg = "💰 Mening narxlarim:\n\n"
    
    for p in active_products:
        name = p.get('Nomi')
        turi = p.get('Turi', '-')
        label = f"{name}" if turi == '-' else f"{name} {turi}"
        zavod = int(p.get('Zavod_Narxi', 0))
        
        # Get my default price
        my_default = None
        for dd in dist_defaults:
            if str(dd.get('Dist_ID')) == str(user_id) and dd.get('Mahsulot_ID') == p.get('ID'):
                my_default = int(dd.get('Sotish_Narxi', 0))
                break
        
        if my_default is None:
            my_default = int(p.get('Sotish_Narxi_Default', 0))
        
        msg += f"━━━━━━━━━━━━━━━━\n"
        msg += f"{label}\n"
        msg += f"  Zavod: {format_number(zavod)}\n"
        msg += f"  Mening narxi: {format_number(my_default)}\n"
        
        # Check custom prices
        customs = []
        for cp in custom_prices:
            if str(cp.get('Dist_ID')) == str(user_id) and cp.get('Mahsulot_ID') == p.get('ID'):
                customs.append(f"    • {cp.get('Dokon_Nomi')}: {format_number(int(cp.get('Sotish_Narxi', 0)))}")
        
        if customs:
            msg += "  Maxsus:\n" + "\n".join(customs) + "\n"
        else:
            msg += "  Maxsus: yo'q\n"
    
    await query.edit_message_text(msg)
    return ConversationHandler.END

async def price_default_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start setting default price"""
    query = update.callback_query
    await query.answer()
    
    products = db_get_all("Mahsulotlar_Asosiy")
    active_products = [p for p in products if p.get('Status') == 'active']
    
    buttons = []
    for p in active_products:
        name = p.get('Nomi')
        turi = p.get('Turi', '-')
        label = f"{name}" if turi == '-' else f"{name} {turi}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"price_def_{p.get('ID')}")])
    
    buttons.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="price_close")])
    
    await query.edit_message_text(
        "Mahsulotni tanlang:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_PRICE_SELECT_PRODUCT

async def price_default_product_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product selected for default price"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "price_close":
        await query.edit_message_text("❌ Bekor qilindi")
        return ConversationHandler.END
    
    product_id = query.data.replace("price_def_", "")
    products = db_get_all("Mahsulotlar_Asosiy")
    
    product = None
    for p in products:
        if p.get('ID') == product_id:
            product = p
            break
    
    if not product:
        await query.edit_message_text("❌ Mahsulot topilmadi")
        return ConversationHandler.END
    
    context.user_data['price_product'] = product
    
    user_id = update.effective_user.id
    zavod = int(product.get('Zavod_Narxi', 0))
    
    # Get current price
    dist_defaults = db_get_all("Mahsulotlar_Dist_Default")
    current = None
    for dd in dist_defaults:
        if str(dd.get('Dist_ID')) == str(user_id) and dd.get('Mahsulot_ID') == product_id:
            current = int(dd.get('Sotish_Narxi', 0))
            break
    
    if current is None:
        current = int(product.get('Sotish_Narxi_Default', 0))
    
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    
    await query.edit_message_text(
        f"Mahsulot: {label}\n\n"
        f"Zavod narxi: {format_number(zavod)} so'm\n"
        f"Joriy sotish narxi: {format_number(current)} so'm\n\n"
        f"Yangi sotish narxini kiriting:"
    )
    return S_PRICE_ENTER_PRICE

async def price_default_entered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle default price input"""
    try:
        price = int(update.message.text)
        if price <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ To'g'ri narx kiriting:")
        return S_PRICE_ENTER_PRICE
    
    product = context.user_data.get('price_product')
    zavod = int(product.get('Zavod_Narxi', 0))
    
    if price < zavod:
        await update.message.reply_text(
            f"❌ Sotish narxi zavod narxidan kam bo'lishi mumkin emas!\n\n"
            f"Zavod: {format_number(zavod)} so'm\n"
            f"Minimal sotish narxi: {format_number(zavod)} so'm\n\n"
            f"Yangi narxni kiriting:"
        )
        return S_PRICE_ENTER_PRICE
    
    user_id = update.effective_user.id
    product_id = product.get('ID')
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    
    # Check if default already exists
    dist_defaults = db_get_all("Mahsulotlar_Dist_Default")
    exists = False
    for idx, dd in enumerate(dist_defaults):
        if str(dd.get('Dist_ID')) == str(user_id) and dd.get('Mahsulot_ID') == product_id:
            # Update existing
            db_update_row("Mahsulotlar_Dist_Default", idx + 2, 5, price)  # Sotish_Narxi column
            exists = True
            break
    
    if not exists:
        # Add new
        sana = date.today().strftime("%Y-%m-%d")
        label = f"{name}" if turi == '-' else f"{name} {turi}"
        row = [str(user_id), product_id, name, turi, price, sana]
        db_add("Mahsulotlar_Dist_Default", row)
    
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    await update.message.reply_text(
        f"✅ Narx yangilandi!\n\n"
        f"{label}\n"
        f"Yangi narx: {format_number(price)} so'm\n\n"
        f"Barcha do'konlar uchun amal qiladi."
    )
    
    context.user_data.clear()
    return ConversationHandler.END

async def price_custom_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start setting custom price for shops"""
    query = update.callback_query
    await query.answer()
    
    products = db_get_all("Mahsulotlar_Asosiy")
    active_products = [p for p in products if p.get('Status') == 'active']
    
    buttons = []
    for p in active_products:
        name = p.get('Nomi')
        turi = p.get('Turi', '-')
        label = f"{name}" if turi == '-' else f"{name} {turi}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"price_cust_{p.get('ID')}")])
    
    buttons.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="price_close")])
    
    await query.edit_message_text(
        "Mahsulotni tanlang:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_PRICE_SELECT_PRODUCT

async def price_custom_product_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product selected - now select shops"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "price_close":
        await query.edit_message_text("❌ Bekor qilindi")
        return ConversationHandler.END
    
    product_id = query.data.replace("price_cust_", "")
    products = db_get_all("Mahsulotlar_Asosiy")
    
    product = None
    for p in products:
        if p.get('ID') == product_id:
            product = p
            break
    
    if not product:
        await query.edit_message_text("❌ Mahsulot topilmadi")
        return ConversationHandler.END
    
    context.user_data['price_product'] = product
    
    # Get my shops
    user_id = update.effective_user.id
    shops = db_get_all("Dokonlar")
    my_shops = [s for s in shops if str(s.get('Dist_ID')) == str(user_id)]
    
    if not my_shops:
        await query.edit_message_text("❌ Sizda do'konlar yo'q")
        return ConversationHandler.END
    
    msg = (
        f"Mahsulot: {product.get('Nomi')} {product.get('Turi', '')}\n\n"
        f"Qaysi do'konlar uchun maxsus narx?\n\n"
        f"Do'kon ID larini vergul bilan kiriting:\n"
    )
    
    for shop in my_shops:
        msg += f"{shop.get('ID')} - {shop.get('Nomi')}\n"
    
    await query.edit_message_text(msg)
    return S_PRICE_CUSTOM_SELECT_SHOPS

async def price_custom_shops_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shops selected - ask for price"""
    shop_ids = [x.strip() for x in update.message.text.split(',')]
    
    user_id = update.effective_user.id
    shops = db_get_all("Dokonlar")
    my_shops = [s for s in shops if str(s.get('Dist_ID')) == str(user_id)]
    
    # Validate shop IDs
    selected_shops = []
    for shop_id in shop_ids:
        for shop in my_shops:
            if shop.get('ID') == shop_id:
                selected_shops.append(shop)
                break
    
    if not selected_shops:
        await update.message.reply_text(
            "❌ Noto'g'ri ID. Qaytadan kiriting:"
        )
        return S_PRICE_CUSTOM_SELECT_SHOPS
    
    context.user_data['selected_shops'] = selected_shops
    
    product = context.user_data.get('price_product')
    zavod = int(product.get('Zavod_Narxi', 0))
    
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    
    msg = f"Mahsulot: {label}\n"
    msg += f"Zavod narxi: {format_number(zavod)} so'm\n\n"
    msg += f"Tanlangan do'konlar:\n"
    for shop in selected_shops:
        msg += f"• {shop.get('Nomi')}\n"
    msg += f"\nUshbu do'konlar uchun sotish narxini kiriting:"
    
    await update.message.reply_text(msg)
    return S_PRICE_CUSTOM_ENTER_PRICE

async def price_custom_entered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle custom price input"""
    try:
        price = int(update.message.text)
        if price <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ To'g'ri narx kiriting:")
        return S_PRICE_CUSTOM_ENTER_PRICE
    
    product = context.user_data.get('price_product')
    zavod = int(product.get('Zavod_Narxi', 0))
    
    if price < zavod:
        await update.message.reply_text(
            f"❌ Sotish narxi zavod narxidan kam bo'lishi mumkin emas!\n\n"
            f"Zavod: {format_number(zavod)} so'm\n\n"
            f"Yangi narxni kiriting:"
        )
        return S_PRICE_CUSTOM_ENTER_PRICE
    
    user_id = update.effective_user.id
    product_id = product.get('ID')
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    selected_shops = context.user_data.get('selected_shops', [])
    sana = date.today().strftime("%Y-%m-%d")
    
    # Save for each shop
    for shop in selected_shops:
        # Check if exists
        custom_prices = db_get_all("Mahsulotlar_Maxsus_Narx")
        exists = False
        
        for idx, cp in enumerate(custom_prices):
            if (str(cp.get('Dist_ID')) == str(user_id) and 
                cp.get('Dokon_ID') == shop.get('ID') and 
                cp.get('Mahsulot_ID') == product_id):
                # Update
                db_update_row("Mahsulotlar_Maxsus_Narx", idx + 2, 6, price)  # Sotish_Narxi column
                exists = True
                break
        
        if not exists:
            # Add new
            row = [
                str(user_id),
                shop.get('ID'),
                shop.get('Nomi'),
                product_id,
                name,
                turi,
                price,
                sana
            ]
            db_add("Mahsulotlar_Maxsus_Narx", row)
    
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    msg = f"✅ Maxsus narx o'rnatildi!\n\n{label}: {format_number(price)} so'm\n\nDo'konlar:\n"
    for shop in selected_shops:
        msg += f"• {shop.get('Nomi')}\n"
    
    await update.message.reply_text(msg)
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# REPORTS (HISOBOTLAR)
# ═══════════════════════════════════════════════════════════

async def hisobotlar_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reports menu"""
    buttons = [
        [InlineKeyboardButton("📊 Kunlik", callback_data="report_daily")],
        [InlineKeyboardButton("📅 7 kunlik", callback_data="report_7")],
        [InlineKeyboardButton("📅 15 kunlik", callback_data="report_15")],
        [InlineKeyboardButton("📅 30 kunlik", callback_data="report_30")]
    ]
    
    await update.message.reply_text(
        "📊 Hisobotlar\n\n"
        "Davr tanlang:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def report_generate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate report"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    today = date.today()
    
    if query.data == "report_daily":
        start_date = today
        end_date = today
        title = f"📊 Kunlik — {today.strftime('%Y-%m-%d')}"
    elif query.data == "report_7":
        start_date = today - timedelta(days=7)
        end_date = today
        title = f"📊 7 kunlik — {start_date.strftime('%d.%m')} - {end_date.strftime('%d.%m')}"
    elif query.data == "report_15":
        start_date = today - timedelta(days=15)
        end_date = today
        title = f"📊 15 kunlik — {start_date.strftime('%d.%m')} - {end_date.strftime('%d.%m')}"
    else:  # 30
        start_date = today - timedelta(days=30)
        end_date = today
        title = f"📊 30 kunlik — {start_date.strftime('%d.%m')} - {end_date.strftime('%d.%m')}"
    
    # Calculate metrics
    qabul_data = db_get_all("Qabul")
    topshirish_data = db_get_all("Topshirish")
    tolov_data = db_get_all("Tolov")
    
    zavod = 0
    for q in qabul_data:
        if str(q.get('Dist_ID')) == str(user_id):
            try:
                q_date = datetime.strptime(q.get('Sana'), '%Y-%m-%d').date()
                if start_date <= q_date <= end_date:
                    zavod += int(q.get('Jami', 0))
            except:
                pass
    
    sotuv = 0
    vozvrat_summa = 0
    naqd = 0
    qarz_period = 0
    dokonlar_set = set()
    
    products_lookup = {p.get('ID'): p for p in db_get_all("Mahsulotlar_Asosiy")}
    
    for t in topshirish_data:
        if str(t.get('Dist_ID')) == str(user_id):
            try:
                t_date = datetime.strptime(t.get('Sana'), '%Y-%m-%d').date()
                if start_date <= t_date <= end_date:
                    # Calculate sotuv
                    mahsulot = t.get('Mahsulot')
                    turi = t.get('Turi')
                    topshirish_miqdor = float(t.get('Topshirish_Miqdor', 0))
                    
                    # Get selling price
                    dokon_id = t.get('Dokon_ID')
                    
                    # Find product ID
                    product_id = None
                    for p_id, p in products_lookup.items():
                        if p.get('Nomi') == mahsulot and p.get('Turi') == turi:
                            product_id = p_id
                            break
                    
                    if product_id:
                        price = get_selling_price(user_id, dokon_id, product_id)
                        sotuv += topshirish_miqdor * price
                    
                    # Vozvrat at factory price
                    vozvrat_miqdor = float(t.get('Vozvrat_Miqdor', 0))
                    if vozvrat_miqdor > 0:
                        zavod_narx = get_factory_price(product_id) if product_id else 0
                        vozvrat_summa += vozvrat_miqdor * zavod_narx
                    
                    naqd += int(t.get('Naqd', 0))
                    qarz_period += int(t.get('Qarz', 0))
                    dokonlar_set.add(t.get('Dokon_ID'))
            except Exception as e:
                logger.error(f"Report calculation error: {e}")
    
    # Calculate jami_qarz (all time)
    jami_qarz = 0
    for t in topshirish_data:
        if str(t.get('Dist_ID')) == str(user_id):
            jami_qarz += int(t.get('Qarz', 0))
    
    for tl in tolov_data:
        if str(tl.get('Dist_ID')) == str(user_id):
            jami_qarz -= int(tl.get('Summa', 0))
    
    foyda = sotuv - zavod - vozvrat_summa
    dokonlar_count = len(dokonlar_set)
    
    msg = f"{title}\n"
    msg += f"━━━━━━━━━━━━━━━━\n"
    msg += f"📥 Zavod: {format_number(zavod)}\n"
    msg += f"🚚 Sotuv: {format_number(int(sotuv))}\n"
    msg += f"📦 Vozvrat: {format_number(int(vozvrat_summa))}\n"
    msg += f"💵 Naqd: {format_number(naqd)}\n"
    msg += f"📝 Qarz: {format_number(qarz_period)}\n"
    msg += f"💸 Jami qarz: {format_number(jami_qarz)}\n"
    msg += f"💰 Foyda: {format_number(int(foyda))}\n"
    msg += f"🏪 {dokonlar_count} do'kon"
    
    await query.edit_message_text(msg)

async def show_ombor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show warehouse stock"""
    user_id = update.effective_user.id
    ombor = db_get_all("Ombor")
    
    my_stock = [o for o in ombor if str(o.get('Dist_ID')) == str(user_id)]
    
    if not my_stock:
        await update.message.reply_text("📦 Ombor bo'sh")
        return
    
    msg = "📦 Mening omborim:\n\n"
    for item in my_stock:
        name = item.get('Mahsulot')
        turi = item.get('Turi', '-')
        miqdor = item.get('Miqdor')
        birlik = item.get('Birlik')
        
        label = f"{name}" if turi == '-' else f"{name} {turi}"
        msg += f"• {label}: {miqdor} {birlik}\n"
    
    await update.message.reply_text(msg)

# ═══════════════════════════════════════════════════════════
# ADMIN PANEL
# ═══════════════════════════════════════════════════════════

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin panel"""
    buttons = [
        [InlineKeyboardButton("➕ Mahsulot qo'shish", callback_data="admin_add_product")],
        [InlineKeyboardButton("📋 Barcha hisobotlar", callback_data="admin_reports")],
        [InlineKeyboardButton("❌ Yopish", callback_data="admin_close")]
    ]
    
    await update.message.reply_text(
        "👨‍💼 Admin Panel\n\n"
        "Tanlov:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ═══════════════════════════════════════════════════════════
# CONVERSATION HANDLERS
# ═══════════════════════════════════════════════════════════

def main():
    """Main function"""
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Shop registration handler
    shop_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^➕ Do'kon qo'shish$"), dokon_start)],
        states={
            S_DI_NM: [MessageHandler(filters.TEXT & ~filters.COMMAND, di_nm)],
            S_DI_ADR: [MessageHandler(filters.TEXT & ~filters.COMMAND, di_adr)],
            S_DI_MCHJ: [MessageHandler(filters.TEXT & ~filters.COMMAND, di_mchj)],
            S_DI_TEL1: [MessageHandler(filters.TEXT & ~filters.COMMAND, di_tel1)],
            S_DI_TEL2: [MessageHandler(filters.TEXT & ~filters.COMMAND, di_tel2)],
            S_DI_EG: [MessageHandler(filters.TEXT & ~filters.COMMAND, di_eg)],
            S_DI_PHT: [MessageHandler(filters.PHOTO | filters.TEXT, di_pht)],
            S_DI_LOC: [MessageHandler(filters.LOCATION | filters.TEXT, di_loc)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Factory receipt handler
    qabul_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^📦 Zavoddan qabul$"), qabul_start)],
        states={
            S_QABUL_PRODUCT: [CallbackQueryHandler(qabul_product_selected)],
            S_QABUL_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, qabul_amount_entered)],
            S_QABUL_MORE: [CallbackQueryHandler(qabul_more_or_finish)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Order handler
    order_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("^📋 Buyurtmalar$"), buyurtmalar_start),
            CallbackQueryHandler(order_new_start, pattern="^order_new$")
        ],
        states={
            S_ORDER_SHOP: [CallbackQueryHandler(order_shop_selected)],
            S_ORDER_PRODUCT: [CallbackQueryHandler(order_product_selected)],
            S_ORDER_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_amount_entered)],
            S_ORDER_MORE: [CallbackQueryHandler(order_more_or_confirm)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Delivery handler
    delivery_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^🚚 Tovar topshirish$"), topshirish_start)],
        states={
            S_DELIVERY_SHOP: [CallbackQueryHandler(delivery_shop_selected)],
            S_DELIVERY_PRODUCT_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_amount_entered)],
            S_DELIVERY_VOZVRAT: [CallbackQueryHandler(delivery_vozvrat_choice)],
            S_DELIVERY_VOZVRAT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_vozvrat_amount)],
            S_DELIVERY_IZOH: [
                CallbackQueryHandler(delivery_izoh_choice),
                MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_izoh_text)
            ],
            S_DELIVERY_TOLOV: [CallbackQueryHandler(delivery_tolov_choice)],
            S_DELIVERY_NAQD: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_naqd_amount)],
            S_DELIVERY_TAROZI: [MessageHandler(filters.PHOTO | filters.TEXT, delivery_tarozi_photo)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Price management handler
    price_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^💰 Narxlarim$"), narxlar_menu)],
        states={
            S_PRICE_MENU: [
                CallbackQueryHandler(price_view, pattern="^price_view$"),
                CallbackQueryHandler(price_default_start, pattern="^price_default$"),
                CallbackQueryHandler(price_custom_start, pattern="^price_custom$"),
            ],
            S_PRICE_SELECT_PRODUCT: [
                CallbackQueryHandler(price_default_product_selected, pattern="^price_def_"),
                CallbackQueryHandler(price_custom_product_selected, pattern="^price_cust_"),
                CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern="^price_close$"),
            ],
            S_PRICE_ENTER_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, price_default_entered)],
            S_PRICE_CUSTOM_SELECT_SHOPS: [MessageHandler(filters.TEXT & ~filters.COMMAND, price_custom_shops_selected)],
            S_PRICE_CUSTOM_ENTER_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, price_custom_entered)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(shop_conv)
    application.add_handler(qabul_conv)
    application.add_handler(order_conv)
    application.add_handler(delivery_conv)
    application.add_handler(price_conv)
    
    # Reports
    application.add_handler(MessageHandler(filters.Regex("^📊 Hisobotlar$"), hisobotlar_menu))
    application.add_handler(CallbackQueryHandler(report_generate, pattern="^report_"))
    
    # Ombor
    application.add_handler(MessageHandler(filters.Regex("^🏦 Ombor$"), show_ombor))
    
    # Admin
    application.add_handler(MessageHandler(filters.Regex("^👨‍💼 Admin Panel$"), admin_panel))
    
    # Main menu fallback
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu))
    
    # Start polling
    logger.info("Bot started!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
