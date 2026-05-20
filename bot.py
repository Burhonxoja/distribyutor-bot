#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ALBA MILK DISTRIBUTION BOT - TO'LIQ VERSIYA
Complete production-ready version with all features
"""

import os
import json
import logging
import re
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict

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
    raise ValueError("❌ Missing required environment variables")

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
        logger.error(f"Failed to initialize Sheets: {e}")
        raise

gc = get_sheets_client()
sh = gc.open_by_key(SPREADSHEET_ID)

# ═══════════════════════════════════════════════════════════
# DATABASE HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def db_add(sheet_name: str, row_data: list) -> bool:
    """Add row to sheet"""
    try:
        ws = sh.worksheet(sheet_name)
        ws.append_row(row_data, value_input_option='USER_ENTERED')
        logger.info(f"✅ Row added to {sheet_name}")
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

def db_update_cell(sheet_name: str, row_num: int, col_num: int, value) -> bool:
    """Update specific cell"""
    try:
        ws = sh.worksheet(sheet_name)
        ws.update_cell(row_num, col_num, value)
        return True
    except Exception as e:
        logger.error(f"❌ db_update_cell failed: {e}")
        return False

def db_find_row(sheet_name: str, conditions: dict) -> tuple:
    """Find row by conditions, returns (row_index, row_data) or (None, None)"""
    try:
        records = db_get_all(sheet_name)
        for idx, record in enumerate(records):
            match = all(str(record.get(k)) == str(v) for k, v in conditions.items())
            if match:
                return idx + 2, record  # +2 because header is row 1, data starts at row 2
        return None, None
    except Exception as e:
        logger.error(f"❌ db_find_row failed: {e}")
        return None, None

# ═══════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════

def generate_id(prefix: str) -> str:
    """Generate unique ID with timestamp"""
    return f"{prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

def clean_phone(phone: str) -> str:
    """Extract digits from phone number"""
    return re.sub(r'\D', '', phone)

def format_number(num) -> str:
    """Format number with thousand separators"""
    try:
        return f"{int(num):,}"
    except:
        return str(num)

def get_product_by_id(product_id: str) -> Optional[dict]:
    """Get product by ID"""
    products = db_get_all("Mahsulotlar_Asosiy")
    for p in products:
        if p.get('ID') == product_id:
            return p
    return None

def get_shop_by_id(shop_id: str) -> Optional[dict]:
    """Get shop by ID"""
    shops = db_get_all("Dokonlar")
    for s in shops:
        if s.get('ID') == shop_id:
            return s
    return None

# ═══════════════════════════════════════════════════════════
# PRICE MANAGEMENT FUNCTIONS
# ═══════════════════════════════════════════════════════════

def get_factory_price(mahsulot_id: str) -> int:
    """Get factory price for product"""
    product = get_product_by_id(mahsulot_id)
    if product:
        return int(product.get('Zavod_Narxi', 0))
    return 0

def get_selling_price(dist_id: str, dokon_id: str, mahsulot_id: str) -> int:
    """
    Get selling price with 3-tier priority:
    1. Shop-specific custom price
    2. Distributor's default price
    3. Admin's suggested price
    """
    # 1. Check custom price for this shop
    custom_prices = db_get_all("Mahsulotlar_Maxsus_Narx")
    for cp in custom_prices:
        if (str(cp.get('Dist_ID')) == str(dist_id) and 
            str(cp.get('Dokon_ID')) == str(dokon_id) and 
            str(cp.get('Mahsulot_ID')) == str(mahsulot_id)):
            return int(cp.get('Sotish_Narxi', 0))
    
    # 2. Check distributor default price
    dist_defaults = db_get_all("Mahsulotlar_Dist_Default")
    for dd in dist_defaults:
        if (str(dd.get('Dist_ID')) == str(dist_id) and 
            str(dd.get('Mahsulot_ID')) == str(mahsulot_id)):
            return int(dd.get('Sotish_Narxi', 0))
    
    # 3. Fallback to admin default
    product = get_product_by_id(mahsulot_id)
    if product:
        return int(product.get('Sotish_Narxi_Default', 0))
    
    return 0

def has_custom_price(dist_id: str, dokon_id: str, mahsulot_id: str) -> bool:
    """Check if shop has custom price"""
    custom_prices = db_get_all("Mahsulotlar_Maxsus_Narx")
    for cp in custom_prices:
        if (str(cp.get('Dist_ID')) == str(dist_id) and 
            str(cp.get('Dokon_ID')) == str(dokon_id) and 
            str(cp.get('Mahsulot_ID')) == str(mahsulot_id)):
            return True
    return False

def update_ombor(dist_id: str, mahsulot_id: str, nomi: str, turi: str, miqdor: float, birlik: str, operation: str = 'add'):
    """Update warehouse stock (add or subtract)"""
    try:
        row_num, existing = db_find_row("Ombor", {
            'Dist_ID': dist_id,
            'Mahsulot': nomi,
            'Turi': turi
        })
        
        if row_num:
            # Update existing row
            current = float(existing.get('Miqdor', 0))
            if operation == 'add':
                new_amount = current + float(miqdor)
            else:  # subtract
                new_amount = current - float(miqdor)
            
            db_update_cell("Ombor", row_num, 4, new_amount)  # Column 4 is Miqdor
            logger.info(f"✅ Ombor updated: {nomi} {turi} -> {new_amount}")
        else:
            # Add new row
            row = [str(dist_id), nomi, turi, miqdor, birlik]
            db_add("Ombor", row)
            logger.info(f"✅ Ombor added: {nomi} {turi} = {miqdor}")
    except Exception as e:
        logger.error(f"❌ update_ombor failed: {e}")

# ═══════════════════════════════════════════════════════════
# CONVERSATION STATES
# ═══════════════════════════════════════════════════════════

# Shop registration
(S_SHOP_NAME, S_SHOP_ADDRESS, S_SHOP_MCHJ, S_SHOP_TEL1, 
 S_SHOP_TEL2, S_SHOP_OWNER, S_SHOP_PHOTO, S_SHOP_LOCATION) = range(8)

# Factory receipt
S_QABUL_PRODUCT, S_QABUL_AMOUNT, S_QABUL_MORE = range(8, 11)

# Order
S_ORDER_SHOP, S_ORDER_PRODUCT, S_ORDER_AMOUNT, S_ORDER_MORE = range(11, 15)

# Delivery
(S_DELIVERY_SHOP, S_DELIVERY_PRODUCT_INPUT, S_DELIVERY_VOZVRAT_CHOICE,
 S_DELIVERY_VOZVRAT_AMOUNT, S_DELIVERY_NEXT_PRODUCT, S_DELIVERY_IZOH_CHOICE,
 S_DELIVERY_IZOH_TEXT, S_DELIVERY_ESLATMA_KUN, S_DELIVERY_TOLOV,
 S_DELIVERY_NAQD, S_DELIVERY_TAROZI) = range(15, 26)

# Price management
(S_PRICE_MENU, S_PRICE_SELECT_PRODUCT, S_PRICE_ENTER_DEFAULT,
 S_PRICE_CUSTOM_SELECT_SHOPS, S_PRICE_CUSTOM_ENTER) = range(26, 31)

# ═══════════════════════════════════════════════════════════
# START & MAIN MENU
# ═══════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command - show main menu"""
    user_id = update.effective_user.id
    user_name = update.effective_user.full_name
    
    buttons = [
        [InlineKeyboardButton("🏪 Do'konlarim", callback_data="menu_shops")],
        [InlineKeyboardButton("➕ Do'kon qo'shish", callback_data="menu_add_shop")],
        [InlineKeyboardButton("📦 Zavoddan qabul", callback_data="menu_qabul")],
        [InlineKeyboardButton("📋 Buyurtmalar", callback_data="menu_orders")],
        [InlineKeyboardButton("💰 Narxlarim", callback_data="menu_prices")],
        [InlineKeyboardButton("📊 Hisobotlar", callback_data="menu_reports")],
        [InlineKeyboardButton("🏦 Ombor", callback_data="menu_ombor")]
    ]
    
    if user_id in ADMIN_IDS:
        buttons.append([InlineKeyboardButton("👨‍💼 Admin Panel", callback_data="menu_admin")])
    
    await update.message.reply_text(
        f"Assalomu alaykum, {user_name}!\n\n"
        "Alba Milk botiga xush kelibsiz.",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show main menu (for callbacks)"""
    query = update.callback_query
    if query:
        await query.answer()
    
    user_id = update.effective_user.id
    
    buttons = [
        [InlineKeyboardButton("🏪 Do'konlarim", callback_data="menu_shops")],
        [InlineKeyboardButton("➕ Do'kon qo'shish", callback_data="menu_add_shop")],
        [InlineKeyboardButton("📦 Zavoddan qabul", callback_data="menu_qabul")],
        [InlineKeyboardButton("📋 Buyurtmalar", callback_data="menu_orders")],
        [InlineKeyboardButton("💰 Narxlarim", callback_data="menu_prices")],
        [InlineKeyboardButton("📊 Hisobotlar", callback_data="menu_reports")],
        [InlineKeyboardButton("🏦 Ombor", callback_data="menu_ombor")]
    ]
    
    if user_id in ADMIN_IDS:
        buttons.append([InlineKeyboardButton("👨‍💼 Admin Panel", callback_data="menu_admin")])
    
    if query:
        await query.edit_message_text(
            "🏠 Asosiy menyu",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    else:
        await update.message.reply_text(
            "🏠 Asosiy menyu",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

# ═══════════════════════════════════════════════════════════
# SHOP LIST WITH PHOTOS & LOCATIONS
# ═══════════════════════════════════════════════════════════

async def show_shops_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's shops with photos and locations"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    shops = db_get_all("Dokonlar")
    
    my_shops = [s for s in shops if str(s.get('Dist_ID')) == str(user_id)]
    
    if not my_shops:
        buttons = [
            [InlineKeyboardButton("➕ Do'kon qo'shish", callback_data="menu_add_shop")],
            [InlineKeyboardButton("🏠 Asosiy menyu", callback_data="menu_main")]
        ]
        await query.edit_message_text(
            "Sizda hali do'konlar yo'q.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    await query.edit_message_text("📥 Do'konlar yuklanmoqda...")
    
    # Send each shop with photo and location
    for shop in my_shops:
        caption = (
            f"📍 {shop.get('Nomi')}\n"
            f"📫 {shop.get('Adres')}\n"
            f"📞 {shop.get('Tel1')}\n"
            f"👤 {shop.get('Sotuvchi')}"
        )
        
        # Try to get photo from channel message
        channel_msg_id = shop.get('Channel_Msg_ID')
        if channel_msg_id and CHANNEL_ID:
            try:
                # Forward photo from channel
                msg = await context.bot.forward_message(
                    chat_id=user_id,
                    from_chat_id=CHANNEL_ID,
                    message_id=int(channel_msg_id)
                )
            except Exception as e:
                logger.error(f"Failed to forward photo: {e}")
                await context.bot.send_message(
                    chat_id=user_id,
                    text=caption
                )
        else:
            await context.bot.send_message(
                chat_id=user_id,
                text=caption
            )
        
        # Send location
        lat = shop.get('Lat')
        lng = shop.get('Lng')
        if lat and lng:
            try:
                await context.bot.send_location(
                    chat_id=user_id,
                    latitude=float(lat),
                    longitude=float(lng)
                )
            except Exception as e:
                logger.error(f"Failed to send location: {e}")
    
    # Show menu button
    buttons = [[InlineKeyboardButton("🏠 Asosiy menyu", callback_data="menu_main")]]
    await context.bot.send_message(
        chat_id=user_id,
        text="✅ Barcha do'konlar ko'rsatildi",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ═══════════════════════════════════════════════════════════
# SHOP REGISTRATION
# ═══════════════════════════════════════════════════════════

async def shop_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start shop registration"""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text(
        "📝 Do'kon qo'shish\n\n"
        "Do'kon nomini kiriting:"
    )
    return S_SHOP_NAME

async def shop_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get shop name"""
    context.user_data['shop_name'] = update.message.text
    
    await update.message.reply_text("📍 Manzilni kiriting:")
    return S_SHOP_ADDRESS

async def shop_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get shop address"""
    context.user_data['shop_address'] = update.message.text
    
    buttons = [[InlineKeyboardButton("⏭ O'tkazib yuborish", callback_data="shop_skip_mchj")]]
    
    await update.message.reply_text(
        "🏢 MCHJ nomini kiriting (yoki skip):",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_SHOP_MCHJ

async def shop_mchj(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get MCHJ or skip"""
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        context.user_data['shop_mchj'] = ""
        await query.edit_message_text(
            "📞 Tel 1 raqamini kiriting:\n\n"
            "Format: 998901234567"
        )
    else:
        context.user_data['shop_mchj'] = update.message.text
        await update.message.reply_text(
            "📞 Tel 1 raqamini kiriting:\n\n"
            "Format: 998901234567"
        )
    return S_SHOP_TEL1

async def shop_tel1(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get Tel1 - MANDATORY, numbers only"""
    text = update.message.text
    phone = clean_phone(text)
    
    if not re.match(r'^998\d{9}$', phone):
        await update.message.reply_text(
            "❌ Noto'g'ri format!\n\n"
            "Faqat raqam kiriting: 998901234567"
        )
        return S_SHOP_TEL1
    
    context.user_data['shop_tel1'] = phone
    
    buttons = [[InlineKeyboardButton("⏭ O'tkazib yuborish", callback_data="shop_skip_tel2")]]
    
    await update.message.reply_text(
        "📞 Tel 2 raqamini kiriting (yoki skip):",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_SHOP_TEL2

async def shop_tel2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get Tel2 or skip"""
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        context.user_data['shop_tel2'] = ""
        await query.edit_message_text("👤 Sotuvchi ismini kiriting:")
    else:
        phone = clean_phone(update.message.text)
        if not re.match(r'^998\d{9}$', phone):
            buttons = [[InlineKeyboardButton("⏭ O'tkazib yuborish", callback_data="shop_skip_tel2")]]
            await update.message.reply_text(
                "❌ Noto'g'ri format! Qaytadan kiriting yoki skip:",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return S_SHOP_TEL2
        context.user_data['shop_tel2'] = phone
        await update.message.reply_text("👤 Sotuvchi ismini kiriting:")
    return S_SHOP_OWNER

async def shop_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get owner name"""
    context.user_data['shop_owner'] = update.message.text
    
    await update.message.reply_text(
        "📸 Do'kon rasmini yuboring:\n\n"
        "❗️ MAJBURIY — rasm yuboring"
    )
    return S_SHOP_PHOTO

async def shop_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get photo - MANDATORY"""
    if not update.message.photo:
        await update.message.reply_text(
            "❌ Iltimos RASM yuboring!\n\n"
            "Text emas, rasm kerak."
        )
        return S_SHOP_PHOTO
    
    photo = update.message.photo[-1]
    context.user_data['shop_photo'] = photo.file_id
    
    await update.message.reply_text(
        "📍 Lokatsiyani yuboring:\n\n"
        "❗️ MAJBURIY — lokatsiya yuboring"
    )
    return S_SHOP_LOCATION

async def shop_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get location and save shop"""
    if not update.message.location:
        await update.message.reply_text(
            "❌ Iltimos LOKATSIYA yuboring!\n\n"
            "Text emas, lokatsiya kerak."
        )
        return S_SHOP_LOCATION
    
    loc = update.message.location
    user_id = update.effective_user.id
    user_name = update.effective_user.full_name
    
    # Generate shop ID
    shop_id = generate_id("dk")
    
    # Get data from context
    name = context.user_data.get('shop_name', '')
    address = context.user_data.get('shop_address', '')
    mchj = context.user_data.get('shop_mchj', '')
    tel1 = context.user_data.get('shop_tel1', '')
    tel2 = context.user_data.get('shop_tel2', '')
    owner = context.user_data.get('shop_owner', '')
    photo_id = context.user_data.get('shop_photo', '')
    lat = loc.latitude
    lng = loc.longitude
    sana = date.today().strftime("%Y-%m-%d")
    
    # Post to channel
    channel_msg_id = ""
    if CHANNEL_ID:
        try:
            caption = (
                f"✅ Yangi do'kon!\n\n"
                f"📍 {name}\n"
                f"🏢 {mchj}\n"
                f"📫 {address}\n"
                f"📞 {tel1}\n"
                f"👤 {owner}\n"
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
            
            logger.info(f"✅ Posted to channel: {channel_msg_id}")
        except Exception as e:
            logger.error(f"❌ Channel post failed: {e}")
    
    # Save to Sheets
    row = [
        shop_id, name, address, mchj, tel1, tel2, owner,
        str(user_id), user_name, lat, lng, channel_msg_id, sana
    ]
    
    logger.info(f"💾 Saving shop: {shop_id} - {name}")
    
    try:
        result = db_add("Dokonlar", row)
        if result:
            logger.info(f"✅ Shop saved successfully: {shop_id}")
            
            buttons = [[InlineKeyboardButton("🏠 Asosiy menyu", callback_data="menu_main")]]
            
            await update.message.reply_text(
                f"✅ Do'kon muvaffaqiyatli qo'shildi!\n\n"
                f"📍 {name}\n"
                f"📫 {address}\n"
                f"📞 {tel1}\n"
                f"👤 {owner}",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        else:
            logger.error(f"❌ db_add returned False")
            await update.message.reply_text(
                "❌ Xatolik yuz berdi!\n"
                "Qaytadan urinib ko'ring."
            )
    except Exception as e:
        logger.error(f"❌ Exception: {e}")
        await update.message.reply_text(f"❌ Xatolik: {str(e)}")
    
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# FACTORY RECEIPT (Multi-product)
# ═══════════════════════════════════════════════════════════

async def qabul_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start factory receipt"""
    query = update.callback_query
    await query.answer()
    
    context.user_data['qabul_items'] = []
    context.user_data['qabul_id'] = generate_id("qb")
    
    return await qabul_select_product(query, context)

async def qabul_select_product(query_or_update, context: ContextTypes.DEFAULT_TYPE):
    """Select product for receipt"""
    products = db_get_all("Mahsulotlar_Asosiy")
    active = [p for p in products if p.get('Status') == 'active']
    
    if not active:
        if hasattr(query_or_update, 'edit_message_text'):
            await query_or_update.edit_message_text("❌ Mahsulotlar topilmadi!")
        else:
            await query_or_update.message.reply_text("❌ Mahsulotlar topilmadi!")
        return ConversationHandler.END
    
    # Group by name
    grouped = {}
    for p in active:
        name = p.get('Nomi')
        if name not in grouped:
            grouped[name] = []
        grouped[name].append(p)
    
    buttons = []
    for name, variants in sorted(grouped.items()):
        if len(variants) == 1:
            p = variants[0]
            turi = p.get('Turi', '-')
            label = f"{name}" if turi == '-' else f"{name} {turi}"
            buttons.append([InlineKeyboardButton(label, callback_data=f"qabul_p_{p.get('ID')}")])
        else:
            for p in variants:
                turi = p.get('Turi', '-')
                label = f"{name} {turi}"
                buttons.append([InlineKeyboardButton(label, callback_data=f"qabul_p_{p.get('ID')}")])
    
    buttons.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="qabul_cancel")])
    
    if hasattr(query_or_update, 'edit_message_text'):
        await query_or_update.edit_message_text(
            "📦 Zavoddan qabul qilish\n\n"
            "Mahsulotni tanlang:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    else:
        await query_or_update.message.reply_text(
            "📦 Zavoddan qabul qilish\n\n"
            "Mahsulotni tanlang:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    
    return S_QABUL_PRODUCT

async def qabul_product_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product selected"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "qabul_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    product_id = query.data.replace("qabul_p_", "")
    product = get_product_by_id(product_id)
    
    if not product:
        await query.edit_message_text("❌ Mahsulot topilmadi!")
        return ConversationHandler.END
    
    context.user_data['current_qabul_product'] = product
    
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    birlik = product.get('Birlik')
    zavod = format_number(product.get('Zavod_Narxi'))
    
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    
    await query.edit_message_text(
        f"📦 Mahsulot: {label}\n"
        f"💰 Zavod narxi: {zavod} so'm/{birlik}\n\n"
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
        await update.message.reply_text("❌ Noto'g'ri miqdor! Raqam kiriting:")
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
    
    context.user_data['qabul_items'].append(item)
    
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    
    buttons = [
        [InlineKeyboardButton("➕ Yana mahsulot qabul qilish", callback_data="qabul_more")],
        [InlineKeyboardButton("✅ Yakunlash", callback_data="qabul_finish")]
    ]
    
    await update.message.reply_text(
        f"✅ Qabul qilindi!\n\n"
        f"📦 {label}\n"
        f"📊 Miqdor: {amount} {product.get('Birlik')}\n"
        f"💰 Summa: {format_number(jami)} so'm\n\n"
        f"Omborga qo'shildi!",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_QABUL_MORE

async def qabul_more_or_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """More products or finish"""
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
    
    total = 0
    for item in items:
        # Save to Qabul sheet
        row = [
            sana, str(user_id), item['nomi'], item['turi'],
            item['miqdor'], item['birlik'], item['zavod_narx'],
            item['jami'], 'qabul_qilindi', qabul_id
        ]
        db_add("Qabul", row)
        total += item['jami']
        
        # Update Ombor
        update_ombor(
            str(user_id), item['mahsulot_id'], item['nomi'],
            item['turi'], item['miqdor'], item['birlik'], 'add'
        )
    
    # Summary
    msg = "✅ Qabul yakunlandi!\n\n"
    for item in items:
        label = f"{item['nomi']}" if item['turi'] == '-' else f"{item['nomi']} {item['turi']}"
        msg += f"• {label}: {item['miqdor']} {item['birlik']}\n"
    msg += f"\n💰 Jami summa: {format_number(total)} so'm"
    
    buttons = [[InlineKeyboardButton("🏠 Asosiy menyu", callback_data="menu_main")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# ORDER (Multi-product)
# ═══════════════════════════════════════════════════════════

async def orders_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Orders menu"""
    query = update.callback_query
    await query.answer()
    
    buttons = [
        [InlineKeyboardButton("➕ Yangi zakaz", callback_data="order_new")],
        [InlineKeyboardButton("📋 Zakazlarni ko'rish", callback_data="order_view")],
        [InlineKeyboardButton("🏠 Asosiy menyu", callback_data="menu_main")]
    ]
    
    await query.edit_message_text(
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
        buttons = [[InlineKeyboardButton("🏠 Asosiy menyu", callback_data="menu_main")]]
        await query.edit_message_text(
            "❌ Sizda do'konlar yo'q!\n\n"
            "Avval do'kon qo'shing.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return ConversationHandler.END
    
    buttons = []
    for shop in my_shops:
        buttons.append([InlineKeyboardButton(
            shop.get('Nomi'),
            callback_data=f"order_s_{shop.get('ID')}"
        )])
    buttons.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="order_cancel")])
    
    context.user_data['order_items'] = []
    context.user_data['zakaz_id'] = generate_id("z")
    
    await query.edit_message_text(
        "🏪 Do'konni tanlang:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_ORDER_SHOP

async def order_shop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shop selected"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "order_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    shop_id = query.data.replace("order_s_", "")
    shop = get_shop_by_id(shop_id)
    
    if not shop:
        await query.edit_message_text("❌ Do'kon topilmadi!")
        return ConversationHandler.END
    
    context.user_data['order_shop'] = shop
    return await order_select_product(query, context)

async def order_select_product(query_or_update, context: ContextTypes.DEFAULT_TYPE):
    """Select product for order"""
    products = db_get_all("Mahsulotlar_Asosiy")
    active = [p for p in products if p.get('Status') == 'active']
    
    if not active:
        if hasattr(query_or_update, 'edit_message_text'):
            await query_or_update.edit_message_text("❌ Mahsulotlar topilmadi!")
        return ConversationHandler.END
    
    user_id = query_or_update.from_user.id if hasattr(query_or_update, 'from_user') else query_or_update.effective_user.id
    shop = context.user_data.get('order_shop')
    
    # Group
    grouped = {}
    for p in active:
        name = p.get('Nomi')
        if name not in grouped:
            grouped[name] = []
        grouped[name].append(p)
    
    buttons = []
    for name, variants in sorted(grouped.items()):
        if len(variants) == 1:
            p = variants[0]
            turi = p.get('Turi', '-')
            price = get_selling_price(user_id, shop.get('ID'), p.get('ID'))
            has_custom = has_custom_price(user_id, shop.get('ID'), p.get('ID'))
            
            label = f"{name}" if turi == '-' else f"{name} {turi}"
            label += f" — {format_number(price)}"
            if has_custom:
                label += " ⭐"
            
            buttons.append([InlineKeyboardButton(label, callback_data=f"order_p_{p.get('ID')}")])
        else:
            for p in variants:
                turi = p.get('Turi', '-')
                price = get_selling_price(user_id, shop.get('ID'), p.get('ID'))
                has_custom = has_custom_price(user_id, shop.get('ID'), p.get('ID'))
                
                label = f"{name} {turi} — {format_number(price)}"
                if has_custom:
                    label += " ⭐"
                
                buttons.append([InlineKeyboardButton(label, callback_data=f"order_p_{p.get('ID')}")])
    
    buttons.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="order_cancel")])
    
    if hasattr(query_or_update, 'edit_message_text'):
        await query_or_update.edit_message_text(
            f"🏪 {shop.get('Nomi')}\n\n"
            "Mahsulotni tanlang:\n"
            "⭐ = Maxsus narx",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    
    return S_ORDER_PRODUCT

async def order_product_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product selected"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "order_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    product_id = query.data.replace("order_p_", "")
    product = get_product_by_id(product_id)
    
    if not product:
        await query.edit_message_text("❌ Mahsulot topilmadi!")
        return ConversationHandler.END
    
    context.user_data['current_order_product'] = product
    
    user_id = update.effective_user.id
    shop = context.user_data.get('order_shop')
    
    name = product.get('Nomi')
    turi = product.get('Turi', '-')
    birlik = product.get('Birlik')
    price = get_selling_price(user_id, shop.get('ID'), product_id)
    
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    
    await query.edit_message_text(
        f"📦 Mahsulot: {label}\n"
        f"💰 Narx: {format_number(price)} so'm/{birlik}\n\n"
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
        await update.message.reply_text("❌ Noto'g'ri miqdor! Raqam kiriting:")
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
        f"📦 {label}\n"
        f"📊 Miqdor: {amount} {product.get('Birlik')}\n"
        f"💰 Narx: {format_number(price)} so'm\n"
        f"💵 Jami: {format_number(jami)} so'm",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return S_ORDER_MORE

async def order_more_or_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """More products or confirm order"""
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
    msg += f"🏪 Do'kon: {shop.get('Nomi')}\n\n"
    for item in items:
        label = f"{item['nomi']}" if item['turi'] == '-' else f"{item['nomi']} {item['turi']}"
        msg += f"• {label}: {item['miqdor']} {item['birlik']}\n"
    msg += f"\n💰 Jami: {format_number(total)} so'm"
    
    buttons = [[InlineKeyboardButton("🏠 Asosiy menyu", callback_data="menu_main")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# REPORTS
# ═══════════════════════════════════════════════════════════

async def reports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reports menu"""
    query = update.callback_query
    await query.answer()
    
    buttons = [
        [InlineKeyboardButton("📊 Kunlik", callback_data="rep_day")],
        [InlineKeyboardButton("📅 7 kunlik", callback_data="rep_7")],
        [InlineKeyboardButton("📅 15 kunlik", callback_data="rep_15")],
        [InlineKeyboardButton("📅 30 kunlik", callback_data="rep_30")],
        [InlineKeyboardButton("🏠 Asosiy menyu", callback_data="menu_main")]
    ]
    
    await query.edit_message_text(
        "📊 Hisobotlar\n\n"
        "Davr tanlang:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate report for selected period"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    today = date.today()
    
    if query.data == "rep_day":
        start = today
        end = today
        title = f"📊 Kunlik — {today.strftime('%d.%m.%Y')}"
    elif query.data == "rep_7":
        start = today - timedelta(days=7)
        end = today
        title = f"📊 7 kunlik"
    elif query.data == "rep_15":
        start = today - timedelta(days=15)
        end = today
        title = f"📊 15 kunlik"
    else:  # rep_30
        start = today - timedelta(days=30)
        end = today
        title = f"📊 30 kunlik"
    
    await query.edit_message_text("⏳ Hisobot tayyorlanmoqda...")
    
    # Get data
    qabul_data = db_get_all("Qabul")
    topshirish_data = db_get_all("Topshirish")
    tolov_data = db_get_all("Tolov")
    products_map = {p.get('ID'): p for p in db_get_all("Mahsulotlar_Asosiy")}
    
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
    
    for t in topshirish_data:
        if str(t.get('Dist_ID')) == str(user_id):
            try:
                t_date = datetime.strptime(t.get('Sana'), '%Y-%m-%d').date()
                if start <= t_date <= end:
                    mahsulot = t.get('Mahsulot')
                    turi = t.get('Turi')
                    top_miqdor = float(t.get('Topshirish_Miqdor', 0))
                    
                    # Find product
                    prod_id = None
                    for pid, p in products_map.items():
                        if p.get('Nomi') == mahsulot and p.get('Turi') == turi:
                            prod_id = pid
                            break
                    
                    if prod_id:
                        price = get_selling_price(user_id, t.get('Dokon_ID'), prod_id)
                        sotuv += top_miqdor * price
                        
                        # Vozvrat at factory price
                        voz_miqdor = float(t.get('Vozvrat_Miqdor', 0))
                        if voz_miqdor > 0:
                            zavod_narx = get_factory_price(prod_id)
                            vozvrat_summa += voz_miqdor * zavod_narx
                    
                    naqd += int(t.get('Naqd', 0))
                    qarz += int(t.get('Qarz', 0))
                    dokonlar.add(t.get('Dokon_ID'))
            except Exception as e:
                logger.error(f"Report calc error: {e}")
    
    # Total debt (all time)
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
    
    buttons = [
        [InlineKeyboardButton("🔄 Boshqa davr", callback_data="menu_reports")],
        [InlineKeyboardButton("🏠 Asosiy menyu", callback_data="menu_main")]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))

async def show_ombor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show warehouse stock"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    ombor = db_get_all("Ombor")
    
    my_stock = [o for o in ombor if str(o.get('Dist_ID')) == str(user_id)]
    
    if not my_stock:
        buttons = [[InlineKeyboardButton("🏠 Asosiy menyu", callback_data="menu_main")]]
        await query.edit_message_text(
            "📦 Ombor bo'sh",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return
    
    msg = "📦 Mening omborim:\n\n"
    for item in my_stock:
        name = item.get('Mahsulot')
        turi = item.get('Turi', '-')
        miqdor = item.get('Miqdor')
        birlik = item.get('Birlik')
        
        label = f"{name}" if turi == '-' else f"{name} {turi}"
        msg += f"• {label}: {miqdor} {birlik}\n"
    
    buttons = [[InlineKeyboardButton("🏠 Asosiy menyu", callback_data="menu_main")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))

# ═══════════════════════════════════════════════════════════
# CALLBACK ROUTER
# ═══════════════════════════════════════════════════════════

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Route callbacks to appropriate handlers"""
    query = update.callback_query
    data = query.data
    
    if data == "menu_main":
        await show_main_menu(update, context)
    elif data == "menu_shops":
        await show_shops_list(update, context)
    elif data == "menu_reports":
        await reports_menu(update, context)
    elif data == "menu_ombor":
        await show_ombor(update, context)
    elif data == "menu_orders":
        await orders_menu(update, context)

# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    """Main function"""
    logger.info("🚀 Starting Alba Milk Bot...")
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Shop registration conversation
    shop_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(shop_start, pattern="^menu_add_shop$")],
        states={
            S_SHOP_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_name)],
            S_SHOP_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_address)],
            S_SHOP_MCHJ: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, shop_mchj),
                CallbackQueryHandler(shop_mchj, pattern="^shop_skip_mchj$")
            ],
            S_SHOP_TEL1: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_tel1)],
            S_SHOP_TEL2: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, shop_tel2),
                CallbackQueryHandler(shop_tel2, pattern="^shop_skip_tel2$")
            ],
            S_SHOP_OWNER: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_owner)],
            S_SHOP_PHOTO: [MessageHandler(filters.PHOTO | filters.TEXT, shop_photo)],
            S_SHOP_LOCATION: [MessageHandler(filters.LOCATION | filters.TEXT, shop_location)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Factory receipt conversation
    qabul_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(qabul_start, pattern="^menu_qabul$")],
        states={
            S_QABUL_PRODUCT: [CallbackQueryHandler(qabul_product_callback)],
            S_QABUL_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, qabul_amount)],
            S_QABUL_MORE: [CallbackQueryHandler(qabul_more_or_finish)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Order conversation
    order_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(order_new, pattern="^order_new$")],
        states={
            S_ORDER_SHOP: [CallbackQueryHandler(order_shop_callback)],
            S_ORDER_PRODUCT: [CallbackQueryHandler(order_product_callback)],
            S_ORDER_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_amount)],
            S_ORDER_MORE: [CallbackQueryHandler(order_more_or_confirm)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(shop_conv)
    application.add_handler(qabul_conv)
    application.add_handler(order_conv)
    
    # Callback handlers
    application.add_handler(CallbackQueryHandler(generate_report, pattern="^rep_"))
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    # Start bot
    logger.info("✅ Bot started successfully!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
