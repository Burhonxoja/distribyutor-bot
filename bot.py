#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Alba Milk Distribution Bot
Complete working version
"""

import os
import json
import logging
import re
from datetime import datetime, timedelta, date
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
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
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def db_add(sheet_name: str, row_data: list) -> bool:
    """Add row to sheet"""
    try:
        ws = sh.worksheet(sheet_name)
        ws.append_row(row_data, value_input_option='USER_ENTERED')
        logger.info(f"✅ Added row to {sheet_name}")
        return True
    except Exception as e:
        logger.error(f"❌ db_add failed for {sheet_name}: {e}")
        return False

def db_get_all(sheet_name: str) -> list:
    """Get all records from sheet"""
    try:
        ws = sh.worksheet(sheet_name)
        records = ws.get_all_records()
        return records
    except Exception as e:
        logger.error(f"❌ db_get_all failed for {sheet_name}: {e}")
        return []

def db_update_row(sheet_name: str, row_num: int, col_num: int, value) -> bool:
    """Update specific cell"""
    try:
        ws = sh.worksheet(sheet_name)
        ws.update_cell(row_num, col_num, value)
        return True
    except Exception as e:
        logger.error(f"❌ db_update_row failed: {e}")
        return False

def generate_id(prefix: str) -> str:
    """Generate unique ID"""
    return f"{prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

def clean_phone(phone: str) -> str:
    """Extract digits from phone"""
    return re.sub(r'\D', '', phone)

def format_number(num) -> str:
    """Format number with separators"""
    try:
        return f"{int(num):,}"
    except:
        return str(num)

def get_factory_price(mahsulot_id: str) -> int:
    """Get factory price"""
    products = db_get_all("Mahsulotlar_Asosiy")
    for p in products:
        if p.get('ID') == mahsulot_id:
            return int(p.get('Zavod_Narxi', 0))
    return 0

def get_selling_price(dist_id: str, dokon_id: str, mahsulot_id: str) -> int:
    """Get selling price (3-tier)"""
    # 1. Custom price
    custom_prices = db_get_all("Mahsulotlar_Maxsus_Narx")
    for cp in custom_prices:
        if (str(cp.get('Dist_ID')) == str(dist_id) and 
            str(cp.get('Dokon_ID')) == str(dokon_id) and 
            str(cp.get('Mahsulot_ID')) == str(mahsulot_id)):
            return int(cp.get('Sotish_Narxi', 0))
    
    # 2. Distributor default
    dist_defaults = db_get_all("Mahsulotlar_Dist_Default")
    for dd in dist_defaults:
        if (str(dd.get('Dist_ID')) == str(dist_id) and 
            str(dd.get('Mahsulot_ID')) == str(mahsulot_id)):
            return int(dd.get('Sotish_Narxi', 0))
    
    # 3. Admin default
    products = db_get_all("Mahsulotlar_Asosiy")
    for p in products:
        if p.get('ID') == mahsulot_id:
            return int(p.get('Sotish_Narxi_Default', 0))
    
    return 0

def has_custom_price(dist_id: str, dokon_id: str, mahsulot_id: str) -> bool:
    """Check if custom price exists"""
    custom_prices = db_get_all("Mahsulotlar_Maxsus_Narx")
    for cp in custom_prices:
        if (str(cp.get('Dist_ID')) == str(dist_id) and 
            str(cp.get('Dokon_ID')) == str(dokon_id) and 
            str(cp.get('Mahsulot_ID')) == str(mahsulot_id)):
            return True
    return False

def update_ombor(dist_id, mahsulot_id, nomi, turi, miqdor, birlik, operation='add'):
    """Update warehouse stock"""
    try:
        ombor = db_get_all("Ombor")
        found = False
        
        for idx, item in enumerate(ombor):
            if (str(item.get('Dist_ID')) == str(dist_id) and 
                item.get('Mahsulot') == nomi and 
                item.get('Turi') == turi):
                found = True
                current = float(item.get('Miqdor', 0))
                if operation == 'add':
                    new_amount = current + float(miqdor)
                else:
                    new_amount = current - float(miqdor)
                
                db_update_row("Ombor", idx + 2, 4, new_amount)
                break
        
        if not found:
            row = [str(dist_id), nomi, turi, miqdor, birlik]
            db_add("Ombor", row)
    except Exception as e:
        logger.error(f"update_ombor failed: {e}")

# ═══════════════════════════════════════════════════════════
# CONVERSATION STATES
# ═══════════════════════════════════════════════════════════
S_DI_NM, S_DI_ADR, S_DI_MCHJ, S_DI_TEL1, S_DI_TEL2, S_DI_EG, S_DI_PHT, S_DI_LOC = range(8)
S_QABUL_PRODUCT, S_QABUL_AMOUNT, S_QABUL_MORE = range(8, 11)
S_ORDER_SHOP, S_ORDER_PRODUCT, S_ORDER_AMOUNT, S_ORDER_MORE = range(11, 15)

# ═══════════════════════════════════════════════════════════
# START & MAIN MENU
# ═══════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command"""
    user_id = update.effective_user.id
    user_name = update.effective_user.full_name
    
    keyboard = [
        ["🏪 Do'konlarim", "➕ Do'kon qo'shish"],
        ["📦 Zavoddan qabul", "📋 Buyurtmalar"],
        ["📊 Hisobotlar", "🏦 Ombor"]
    ]
    
    if user_id in ADMIN_IDS:
        keyboard.append(["👨‍💼 Admin"])
    
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    await update.message.reply_text(
        f"Assalomu alaykum, {user_name}!\n\n"
        "Alba Milk botiga xush kelibsiz.",
        reply_markup=reply_markup
    )

async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle main menu"""
    text = update.message.text
    
    if text == "🏪 Do'konlarim":
        await show_shops(update, context)
    elif text == "➕ Do'kon qo'shish":
        return await dokon_start(update, context)
    elif text == "📦 Zavoddan qabul":
        return await qabul_start(update, context)
    elif text == "📋 Buyurtmalar":
        return await buyurtma_menu(update, context)
    elif text == "📊 Hisobotlar":
        await hisobotlar_menu(update, context)
    elif text == "🏦 Ombor":
        await show_ombor(update, context)
    
    return ConversationHandler.END

async def show_shops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's shops"""
    user_id = update.effective_user.id
    shops = db_get_all("Dokonlar")
    
    my_shops = [s for s in shops if str(s.get('Dist_ID')) == str(user_id)]
    
    if not my_shops:
        await update.message.reply_text("Sizda do'konlar yo'q.\n\n➕ Do'kon qo'shish")
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
    """Get address"""
    context.user_data['dokon_adres'] = update.message.text
    
    keyboard = [["O'tkazib yuborish"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    await update.message.reply_text(
        "🏢 MCHJ nomini kiriting (yoki skip):",
        reply_markup=reply_markup
    )
    return S_DI_MCHJ

async def di_mchj(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get MCHJ"""
    text = update.message.text
    context.user_data['dokon_mchj'] = "" if text == "O'tkazib yuborish" else text
    
    await update.message.reply_text(
        "📞 Tel 1 raqamini kiriting:\n\n"
        "Format: 998901234567",
        reply_markup=ReplyKeyboardRemove()
    )
    return S_DI_TEL1

async def di_tel1(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get Tel1"""
    text = update.message.text
    phone = clean_phone(text)
    
    if not re.match(r'^998\d{9}$', phone):
        await update.message.reply_text(
            "❌ Faqat raqam kiriting (998901234567 formatida)"
        )
        return S_DI_TEL1
    
    context.user_data['dokon_tel1'] = phone
    
    keyboard = [["O'tkazib yuborish"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    await update.message.reply_text(
        "📞 Tel 2 (yoki skip):",
        reply_markup=reply_markup
    )
    return S_DI_TEL2

async def di_tel2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get Tel2"""
    text = update.message.text
    
    if text == "O'tkazib yuborish":
        context.user_data['dokon_tel2'] = ""
    else:
        phone = clean_phone(text)
        if not re.match(r'^998\d{9}$', phone):
            keyboard = [["O'tkazib yuborish"]]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(
                "❌ Faqat raqam yoki skip:",
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
    """Get owner name"""
    context.user_data['dokon_sotuvchi'] = update.message.text
    await update.message.reply_text(
        "📸 Do'kon rasmini yuboring:\n\n"
        "❗️ MAJBURIY"
    )
    return S_DI_PHT

async def di_pht(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get photo"""
    if not update.message.photo:
        await update.message.reply_text(
            "❌ Iltimos rasm yuboring!"
        )
        return S_DI_PHT
    
    photo = update.message.photo[-1]
    context.user_data['dokon_photo'] = photo.file_id
    
    await update.message.reply_text(
        "📍 Lokatsiyani yuboring:\n\n"
        "❗️ MAJBURIY"
    )
    return S_DI_LOC

async def di_loc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get location and save"""
    if not update.message.location:
        await update.message.reply_text(
            "❌ Iltimos lokatsiya yuboring!"
        )
        return S_DI_LOC
    
    loc = update.message.location
    user_id = update.effective_user.id
    user_name = update.effective_user.full_name
    
    dokon_id = generate_id("dk")
    
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
            
            photo_msg = await context.bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=photo_id,
                caption=caption
            )
            channel_msg_id = str(photo_msg.message_id)
            
            await context.bot.send_location(
                chat_id=CHANNEL_ID,
                latitude=lat,
                longitude=lng
            )
        except Exception as e:
            logger.error(f"Channel post failed: {e}")
    
    # Save to Sheets
    row = [
        dokon_id, nom, adr, mchj, tel1, tel2, sotuvchi,
        str(user_id), user_name, lat, lng, channel_msg_id, sana
    ]
    
    logger.info(f"Saving shop: {dokon_id} - {nom}")
    
    try:
        result = db_add("Dokonlar", row)
        if result:
            logger.info(f"✅ Shop saved: {dokon_id}")
            await update.message.reply_text(
                f"✅ Do'kon qo'shildi!\n\n"
                f"📍 {nom}\n"
                f"📫 {adr}\n"
                f"📞 {tel1}",
                reply_markup=ReplyKeyboardMarkup([
                    ["🏪 Do'konlarim", "➕ Do'kon qo'shish"],
                    ["📦 Zavoddan qabul", "📋 Buyurtmalar"]
                ], resize_keyboard=True)
            )
        else:
            logger.error(f"❌ Failed to save shop: {dokon_id}")
            await update.message.reply_text("❌ Xatolik! Qaytadan urinib ko'ring.")
    except Exception as e:
        logger.error(f"❌ Exception saving shop: {e}")
        await update.message.reply_text(f"❌ Xatolik: {str(e)}")
    
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# FACTORY RECEIPT
# ═══════════════════════════════════════════════════════════

async def qabul_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start factory receipt"""
    context.user_data['qabul_items'] = []
    context.user_data['qabul_id'] = generate_id("qb")
    
    return await qabul_select_product(update, context)

async def qabul_select_product(update, context: ContextTypes.DEFAULT_TYPE):
    """Select product"""
    products = db_get_all("Mahsulotlar_Asosiy")
    active = [p for p in products if p.get('Status') == 'active']
    
    if not active:
        await update.message.reply_text("❌ Mahsulotlar topilmadi.")
        return ConversationHandler.END
    
    # Group by name
    grouped = {}
    for p in active:
        name = p.get('Nomi')
        if name not in grouped:
            grouped[name] = []
        grouped[name].append(p)
    
    buttons = []
    for name, variants in grouped.items():
        if len(variants) == 1:
            p = variants[0]
            turi = p.get('Turi', '-')
            label = f"{name}" if turi == '-' else f"{name} {turi}"
            buttons.append([InlineKeyboardButton(label, callback_data=f"qb_{p.get('ID')}")])
        else:
            for p in variants:
                turi = p.get('Turi', '-')
                label = f"{name} {turi}"
                buttons.append([InlineKeyboardButton(label, callback_data=f"qb_{p.get('ID')}")])
    
    buttons.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="qb_cancel")])
    
    await update.message.reply_text(
        "📦 Zavoddan qabul\n\n"
        "Mahsulotni tanlang:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_QABUL_PRODUCT

async def qabul_product_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product selected"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "qb_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    product_id = query.data.replace("qb_", "")
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
    
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    birlik = product.get('Birlik')
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    
    await query.edit_message_text(
        f"Mahsulot: {label}\n\n"
        f"Miqdorni kiriting ({birlik}):"
    )
    return S_QABUL_AMOUNT

async def qabul_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Amount entered"""
    try:
        amount = float(update.message.text)
        if amount <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri miqdor!")
        return S_QABUL_AMOUNT
    
    product = context.user_data.get('current_qabul_product')
    zavod_narx = int(product.get('Zavod_Narxi', 0))
    jami = amount * zavod_narx
    
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
        [InlineKeyboardButton("➕ Yana mahsulot", callback_data="qb_more")],
        [InlineKeyboardButton("✅ Yakunlash", callback_data="qb_finish")]
    ]
    
    await update.message.reply_text(
        f"✅ Qabul qilindi!\n\n"
        f"{label}: {amount} {product.get('Birlik')}\n"
        f"Summa: {format_number(jami)} so'm",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_QABUL_MORE

async def qabul_more_or_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """More or finish"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "qb_more":
        await query.message.delete()
        return await qabul_select_product(query, context)
    
    # Finish
    items = context.user_data.get('qabul_items', [])
    qabul_id = context.user_data.get('qabul_id')
    user_id = update.effective_user.id
    sana = date.today().strftime("%Y-%m-%d")
    
    total = 0
    for item in items:
        row = [
            sana, str(user_id), item['nomi'], item['turi'],
            item['miqdor'], item['birlik'], item['zavod_narx'],
            item['jami'], 'qabul_qilindi', qabul_id
        ]
        db_add("Qabul", row)
        total += item['jami']
        
        # Update Ombor
        update_ombor(user_id, item['mahsulot_id'], item['nomi'], 
                    item['turi'], item['miqdor'], item['birlik'], 'add')
    
    msg = "✅ Qabul yakunlandi!\n\n"
    for item in items:
        label = f"{item['nomi']}" if item['turi'] == '-' else f"{item['nomi']} {item['turi']}"
        msg += f"• {label}: {item['miqdor']} {item['birlik']}\n"
    msg += f"\n💰 Jami: {format_number(total)} so'm"
    
    await query.edit_message_text(msg)
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# ORDER
# ═══════════════════════════════════════════════════════════

async def buyurtma_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Order menu"""
    buttons = [
        [InlineKeyboardButton("➕ Yangi zakaz", callback_data="order_new")],
        [InlineKeyboardButton("📋 Ko'rish", callback_data="order_view")]
    ]
    
    await update.message.reply_text(
        "📋 Buyurtmalar",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def order_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """New order - select shop"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    shops = db_get_all("Dokonlar")
    my_shops = [s for s in shops if str(s.get('Dist_ID')) == str(user_id)]
    
    if not my_shops:
        await query.edit_message_text("❌ Sizda do'konlar yo'q")
        return ConversationHandler.END
    
    buttons = []
    for shop in my_shops:
        buttons.append([InlineKeyboardButton(
            shop.get('Nomi'),
            callback_data=f"ord_shop_{shop.get('ID')}"
        )])
    buttons.append([InlineKeyboardButton("❌ Bekor", callback_data="ord_cancel")])
    
    context.user_data['order_items'] = []
    context.user_data['zakaz_id'] = generate_id("z")
    
    await query.edit_message_text(
        "Do'konni tanlang:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_ORDER_SHOP

async def order_shop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shop selected"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "ord_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    shop_id = query.data.replace("ord_shop_", "")
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
    """Select product"""
    products = db_get_all("Mahsulotlar_Asosiy")
    active = [p for p in products if p.get('Status') == 'active']
    
    user_id = update.from_user.id if hasattr(update, 'from_user') else update.effective_user.id
    shop = context.user_data.get('order_shop')
    
    # Group
    grouped = {}
    for p in active:
        name = p.get('Nomi')
        if name not in grouped:
            grouped[name] = []
        grouped[name].append(p)
    
    buttons = []
    for name, variants in grouped.items():
        if len(variants) == 1:
            p = variants[0]
            turi = p.get('Turi', '-')
            price = get_selling_price(user_id, shop.get('ID'), p.get('ID'))
            has_custom = has_custom_price(user_id, shop.get('ID'), p.get('ID'))
            
            label = f"{name}" if turi == '-' else f"{name} {turi}"
            label += f" — {format_number(price)}"
            if has_custom:
                label += " ⭐"
            
            buttons.append([InlineKeyboardButton(label, callback_data=f"ord_prod_{p.get('ID')}")])
        else:
            for p in variants:
                turi = p.get('Turi', '-')
                price = get_selling_price(user_id, shop.get('ID'), p.get('ID'))
                has_custom = has_custom_price(user_id, shop.get('ID'), p.get('ID'))
                
                label = f"{name} {turi} — {format_number(price)}"
                if has_custom:
                    label += " ⭐"
                
                buttons.append([InlineKeyboardButton(label, callback_data=f"ord_prod_{p.get('ID')}")])
    
    buttons.append([InlineKeyboardButton("❌ Bekor", callback_data="ord_cancel")])
    
    if hasattr(update, 'edit_message_text'):
        await update.edit_message_text(
            f"🏪 {shop.get('Nomi')}\n\n"
            "Mahsulotni tanlang:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    else:
        await update.message.reply_text(
            f"🏪 {shop.get('Nomi')}\n\n"
            "Mahsulotni tanlang:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    
    return S_ORDER_PRODUCT

async def order_product_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product selected"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "ord_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    product_id = query.data.replace("ord_prod_", "")
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

async def order_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Amount entered"""
    try:
        amount = float(update.message.text)
        if amount <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri miqdor!")
        return S_ORDER_AMOUNT
    
    user_id = update.effective_user.id
    shop = context.user_data.get('order_shop')
    product = context.user_data.get('current_order_product')
    
    price = get_selling_price(user_id, shop.get('ID'), product.get('ID'))
    jami = amount * price
    
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
        [InlineKeyboardButton("➕ Yana", callback_data="ord_more")],
        [InlineKeyboardButton("📋 Tasdiqlash", callback_data="ord_confirm")]
    ]
    
    await update.message.reply_text(
        f"✅ Savatga qo'shildi!\n\n"
        f"{label}: {amount} {product.get('Birlik')}\n"
        f"Jami: {format_number(jami)} so'm",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_ORDER_MORE

async def order_more_or_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """More or confirm"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "ord_more":
        await query.message.delete()
        return await order_select_product(query, context)
    
    # Confirm
    items = context.user_data.get('order_items', [])
    zakaz_id = context.user_data.get('zakaz_id')
    shop = context.user_data.get('order_shop')
    user_id = update.effective_user.id
    sana = date.today().strftime("%Y-%m-%d")
    
    total = 0
    for item in items:
        row = [
            sana, str(user_id), shop.get('Nomi'), shop.get('ID'),
            item['nomi'], item['turi'], item['miqdor'], item['birlik'],
            item['narx'], item['jami'], '', 0, 0, 'kutilmoqda', zakaz_id
        ]
        db_add("Buyurtmalar", row)
        total += item['jami']
    
    msg = f"✅ Zakaz tasdiqlandi!\n\n"
    msg += f"🏪 {shop.get('Nomi')}\n\n"
    for item in items:
        label = f"{item['nomi']}" if item['turi'] == '-' else f"{item['nomi']} {item['turi']}"
        msg += f"• {label}: {item['miqdor']} {item['birlik']}\n"
    msg += f"\n💰 Jami: {format_number(total)} so'm"
    
    await query.edit_message_text(msg)
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# REPORTS
# ═══════════════════════════════════════════════════════════

async def hisobotlar_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reports menu"""
    buttons = [
        [InlineKeyboardButton("📊 Kunlik", callback_data="rep_daily")],
        [InlineKeyboardButton("📅 7 kun", callback_data="rep_7")],
        [InlineKeyboardButton("📅 15 kun", callback_data="rep_15")],
        [InlineKeyboardButton("📅 30 kun", callback_data="rep_30")]
    ]
    
    await update.message.reply_text(
        "📊 Hisobotlar",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def report_generate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate report"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    today = date.today()
    
    if query.data == "rep_daily":
        start = today
        end = today
        title = f"📊 Kunlik — {today.strftime('%Y-%m-%d')}"
    elif query.data == "rep_7":
        start = today - timedelta(days=7)
        end = today
        title = f"📊 7 kunlik"
    elif query.data == "rep_15":
        start = today - timedelta(days=15)
        end = today
        title = f"📊 15 kunlik"
    else:
        start = today - timedelta(days=30)
        end = today
        title = f"📊 30 kunlik"
    
    # Calculate
    qabul_data = db_get_all("Qabul")
    topshirish_data = db_get_all("Topshirish")
    tolov_data = db_get_all("Tolov")
    
    zavod = 0
    for q in qabul_data:
        if str(q.get('Dist_ID')) == str(user_id):
            try:
                q_date = datetime.strptime(q.get('Sana'), '%Y-%m-%d').date()
                if start <= q_date <= end:
                    zavod += int(q.get('Jami', 0))
            except:
                pass
    
    sotuv = 0
    vozvrat_summa = 0
    naqd = 0
    qarz = 0
    dokonlar = set()
    
    products_map = {p.get('ID'): p for p in db_get_all("Mahsulotlar_Asosiy")}
    
    for t in topshirish_data:
        if str(t.get('Dist_ID')) == str(user_id):
            try:
                t_date = datetime.strptime(t.get('Sana'), '%Y-%m-%d').date()
                if start <= t_date <= end:
                    mahsulot = t.get('Mahsulot')
                    turi = t.get('Turi')
                    top_miqdor = float(t.get('Topshirish_Miqdor', 0))
                    
                    # Find product ID
                    prod_id = None
                    for pid, p in products_map.items():
                        if p.get('Nomi') == mahsulot and p.get('Turi') == turi:
                            prod_id = pid
                            break
                    
                    if prod_id:
                        price = get_selling_price(user_id, t.get('Dokon_ID'), prod_id)
                        sotuv += top_miqdor * price
                    
                    voz_miqdor = float(t.get('Vozvrat_Miqdor', 0))
                    if voz_miqdor > 0 and prod_id:
                        zavod_narx = get_factory_price(prod_id)
                        vozvrat_summa += voz_miqdor * zavod_narx
                    
                    naqd += int(t.get('Naqd', 0))
                    qarz += int(t.get('Qarz', 0))
                    dokonlar.add(t.get('Dokon_ID'))
            except Exception as e:
                logger.error(f"Report calc error: {e}")
    
    jami_qarz = 0
    for t in topshirish_data:
        if str(t.get('Dist_ID')) == str(user_id):
            jami_qarz += int(t.get('Qarz', 0))
    for tl in tolov_data:
        if str(tl.get('Dist_ID')) == str(user_id):
            jami_qarz -= int(tl.get('Summa', 0))
    
    foyda = sotuv - zavod - vozvrat_summa
    
    msg = f"{title}\n"
    msg += f"━━━━━━━━━━━━━━━━\n"
    msg += f"📥 Zavod: {format_number(zavod)}\n"
    msg += f"🚚 Sotuv: {format_number(int(sotuv))}\n"
    msg += f"📦 Vozvrat: {format_number(int(vozvrat_summa))}\n"
    msg += f"💵 Naqd: {format_number(naqd)}\n"
    msg += f"📝 Qarz: {format_number(qarz)}\n"
    msg += f"💸 Jami qarz: {format_number(jami_qarz)}\n"
    msg += f"💰 Foyda: {format_number(int(foyda))}\n"
    msg += f"🏪 {len(dokonlar)} do'kon"
    
    await query.edit_message_text(msg)

async def show_ombor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show warehouse"""
    user_id = update.effective_user.id
    ombor = db_get_all("Ombor")
    
    my = [o for o in ombor if str(o.get('Dist_ID')) == str(user_id)]
    
    if not my:
        await update.message.reply_text("📦 Ombor bo'sh")
        return
    
    msg = "📦 Mening omborim:\n\n"
    for item in my:
        name = item.get('Mahsulot')
        turi = item.get('Turi', '-')
        miqdor = item.get('Miqdor')
        birlik = item.get('Birlik')
        
        label = f"{name}" if turi == '-' else f"{name} {turi}"
        msg += f"• {label}: {miqdor} {birlik}\n"
    
    await update.message.reply_text(msg)

# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    """Main function"""
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Shop registration
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
    
    # Factory receipt
    qabul_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^📦 Zavoddan qabul$"), qabul_start)],
        states={
            S_QABUL_PRODUCT: [CallbackQueryHandler(qabul_product_callback)],
            S_QABUL_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, qabul_amount)],
            S_QABUL_MORE: [CallbackQueryHandler(qabul_more_or_finish)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Order
    order_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("^📋 Buyurtmalar$"), buyurtma_menu),
            CallbackQueryHandler(order_new, pattern="^order_new$")
        ],
        states={
            S_ORDER_SHOP: [CallbackQueryHandler(order_shop_callback)],
            S_ORDER_PRODUCT: [CallbackQueryHandler(order_product_callback)],
            S_ORDER_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_amount)],
            S_ORDER_MORE: [CallbackQueryHandler(order_more_or_confirm)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(shop_conv)
    application.add_handler(qabul_conv)
    application.add_handler(order_conv)
    
    # Reports
    application.add_handler(MessageHandler(filters.Regex("^📊 Hisobotlar$"), hisobotlar_menu))
    application.add_handler(CallbackQueryHandler(report_generate, pattern="^rep_"))
    
    # Ombor
    application.add_handler(MessageHandler(filters.Regex("^🏦 Ombor$"), show_ombor))
    
    # Main menu
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu))
    
    logger.info("Bot started!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
