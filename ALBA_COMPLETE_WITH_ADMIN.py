#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════
ALBA MILK DISTRIBUTION BOT - YAKUNIY TO'LIQ VERSIYA
FINAL COMPLETE PRODUCTION VERSION
Version: 3.0 FINAL
Lines: 2500+
═══════════════════════════════════════════════════════════════════════

✅ BARCHA FUNKSIYALAR:
  🏪 Do'kon qo'shish (rasm + lokatsiya)
  📦 Zavoddan qabul (ko'p mahsulotli)
  📋 Buyurtma (ko'p mahsulotli)
  🚚 Tovar topshirish (har bir mahsulot uchun input + vozvrat)
  💰 Narx boshqaruvi (asosiy + maxsus)
  📊 Hisobotlar (kunlik, 7, 15, 30 kun)
  🏦 Ombor
  👨‍💼 Admin panel
  🔔 Eslatmalar (5 kun Qaymoq, boshqalar 1-30)
  📸 Tarozi rasmi (1kg mahsulotlar)
  💵 To'lov (naqd/realizatsiya)
  
✅ 2 USTUN INLINE KNOPKALAR
✅ RASM + LOKATSIYA DO'KONLAR RO'YXATI
✅ 3 DARAJALI NARX TIZIMI

═══════════════════════════════════════════════════════════════════════
"""

import os
import json
import logging
import re
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Tuple

import gspread
from google.oauth2.service_account import Credentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
    raise ValueError("❌ Missing environment variables!")

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
        logger.info("✅ Sheets client initialized")
        return client
    except Exception as e:
        logger.error(f"❌ Sheets init failed: {e}")
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
        logger.info(f"✅ Got {len(records)} records from {sheet_name}")
        return records
    except Exception as e:
        logger.error(f"❌ db_get_all failed for {sheet_name}: {e}")
        return []

def db_update_cell(sheet_name: str, row_num: int, col_num: int, value) -> bool:
    """Update specific cell"""
    try:
        ws = sh.worksheet(sheet_name)
        ws.update_cell(row_num, col_num, value)
        logger.info(f"✅ Updated {sheet_name} R{row_num}C{col_num} = {value}")
        return True
    except Exception as e:
        logger.error(f"❌ db_update_cell failed: {e}")
        return False

def db_find_row(sheet_name: str, conditions: dict) -> Tuple[Optional[int], Optional[dict]]:
    """
    Find row matching conditions
    Returns: (row_number, row_data) or (None, None)
    row_number is 1-indexed (includes header at row 1)
    """
    try:
        records = db_get_all(sheet_name)
        for idx, record in enumerate(records):
            match = all(str(record.get(k, '')).strip() == str(v).strip() 
                       for k, v in conditions.items())
            if match:
                return idx + 2, record  # +2: header=1, data starts at 2
        return None, None
    except Exception as e:
        logger.error(f"❌ db_find_row failed for {sheet_name}: {e}")
        return None, None

def db_update_row(sheet_name: str, conditions: dict, updates: dict) -> bool:
    """Update row matching conditions"""
    try:
        row_num, existing = db_find_row(sheet_name, conditions)
        if not row_num:
            return False
        
        # Get headers
        ws = sh.worksheet(sheet_name)
        headers = ws.row_values(1)
        
        # Update each field
        for key, value in updates.items():
            if key in headers:
                col_num = headers.index(key) + 1
                db_update_cell(sheet_name, row_num, col_num, value)
        
        return True
    except Exception as e:
        logger.error(f"❌ db_update_row failed: {e}")
        return False

# ═══════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════

def generate_id(prefix: str) -> str:
    """Generate unique ID with timestamp"""
    return f"{prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

def clean_phone(phone: str) -> str:
    """Extract only digits from phone number"""
    return re.sub(r'\D', '', phone)

def format_number(num) -> str:
    """Format number with thousand separators"""
    try:
        return f"{int(float(num)):,}"
    except:
        return str(num)

def make_2col_buttons(items: list, prefix: str, id_key='ID', label_key='Nomi') -> list:
    """Create 2-column inline keyboard layout"""
    buttons = []
    row = []
    
    for item in items:
        btn = InlineKeyboardButton(
            item.get(label_key, '?'),
            callback_data=f"{prefix}{item.get(id_key, '')}"
        )
        row.append(btn)
        
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    return buttons

def get_product_by_id(product_id: str) -> Optional[dict]:
    """Get product by ID from Mahsulotlar_Asosiy"""
    products = db_get_all("Mahsulotlar_Asosiy")
    for p in products:
        if str(p.get('ID', '')).strip() == str(product_id).strip():
            return p
    return None

def get_shop_by_id(shop_id: str) -> Optional[dict]:
    """Get shop by ID from Dokonlar"""
    shops = db_get_all("Dokonlar")
    for s in shops:
        if str(s.get('ID', '')).strip() == str(shop_id).strip():
            return s
    return None

# ═══════════════════════════════════════════════════════════
# PRICE MANAGEMENT FUNCTIONS
# ═══════════════════════════════════════════════════════════

def get_factory_price(mahsulot_id: str) -> int:
    """Get factory price (Zavod_Narxi) for product"""
    product = get_product_by_id(mahsulot_id)
    if product:
        try:
            return int(product.get('Zavod_Narxi', 0))
        except:
            return 0
    return 0

def get_selling_price(dist_id: str, dokon_id: str, mahsulot_id: str) -> int:
    """
    Get selling price with 3-tier priority:
    1. Shop-specific custom price (Mahsulotlar_Maxsus_Narx)
    2. Distributor's default price (Mahsulotlar_Dist_Default)
    3. Admin's suggested price (Mahsulotlar_Asosiy.Sotish_Narxi_Default)
    """
    # Priority 1: Custom price for this specific shop
    custom_prices = db_get_all("Mahsulotlar_Maxsus_Narx")
    for cp in custom_prices:
        if (str(cp.get('Dist_ID', '')).strip() == str(dist_id).strip() and 
            str(cp.get('Dokon_ID', '')).strip() == str(dokon_id).strip() and 
            str(cp.get('Mahsulot_ID', '')).strip() == str(mahsulot_id).strip()):
            try:
                return int(cp.get('Sotish_Narxi', 0))
            except:
                pass
    
    # Priority 2: Distributor's default price
    dist_defaults = db_get_all("Mahsulotlar_Dist_Default")
    for dd in dist_defaults:
        if (str(dd.get('Dist_ID', '')).strip() == str(dist_id).strip() and 
            str(dd.get('Mahsulot_ID', '')).strip() == str(mahsulot_id).strip()):
            try:
                return int(dd.get('Sotish_Narxi', 0))
            except:
                pass
    
    # Priority 3: Admin's suggested price (fallback)
    product = get_product_by_id(mahsulot_id)
    if product:
        try:
            return int(product.get('Sotish_Narxi_Default', 0))
        except:
            return 0
    
    return 0

def has_custom_price(dist_id: str, dokon_id: str, mahsulot_id: str) -> bool:
    """Check if shop has custom price set"""
    custom_prices = db_get_all("Mahsulotlar_Maxsus_Narx")
    for cp in custom_prices:
        if (str(cp.get('Dist_ID', '')).strip() == str(dist_id).strip() and 
            str(cp.get('Dokon_ID', '')).strip() == str(dokon_id).strip() and 
            str(cp.get('Mahsulot_ID', '')).strip() == str(mahsulot_id).strip()):
            return True
    return False

def update_ombor(dist_id: str, mahsulot_id: str, nomi: str, turi: str, 
                 miqdor: float, birlik: str, operation: str = 'add'):
    """
    Update warehouse stock
    operation: 'add' to increase, 'subtract' to decrease
    """
    try:
        row_num, existing = db_find_row("Ombor", {
            'Dist_ID': str(dist_id),
            'Mahsulot': nomi,
            'Turi': turi
        })
        
        if row_num:
            # Update existing stock
            current = float(existing.get('Miqdor', 0))
            if operation == 'add':
                new_amount = current + float(miqdor)
            else:  # subtract
                new_amount = current - float(miqdor)
                if new_amount < 0:
                    new_amount = 0
            
            db_update_cell("Ombor", row_num, 4, new_amount)  # Column 4 is Miqdor
            logger.info(f"✅ Ombor updated: {nomi} {turi} = {new_amount}")
        else:
            # Create new stock entry
            row = [str(dist_id), nomi, turi, miqdor, birlik]
            db_add("Ombor", row)
            logger.info(f"✅ Ombor created: {nomi} {turi} = {miqdor}")
    except Exception as e:
        logger.error(f"❌ update_ombor failed: {e}")

# ═══════════════════════════════════════════════════════════
# CONVERSATION STATES
# ═══════════════════════════════════════════════════════════

# Shop registration
(S_SHOP_NAME, S_SHOP_ADDR, S_SHOP_MCHJ, S_SHOP_TEL1,
 S_SHOP_TEL2, S_SHOP_OWNER, S_SHOP_PHOTO, S_SHOP_LOC) = range(100, 108)

# Factory receipt
S_QAB_PROD, S_QAB_AMT, S_QAB_MORE = range(200, 203)

# Order
S_ORD_SHOP, S_ORD_PROD, S_ORD_AMT, S_ORD_MORE = range(300, 304)

# Delivery
(S_DEL_SHOP, S_DEL_INPUT, S_DEL_VOZ_YN, S_DEL_VOZ_AMT,
 S_DEL_IZOH_YN, S_DEL_IZOH_TXT, S_DEL_ESLATMA,
 S_DEL_TOLOV, S_DEL_NAQD, S_DEL_TAROZI) = range(400, 410)

# Price management
(S_PRC_MENU, S_PRC_SEL_PROD, S_PRC_ENT_DEF,
 S_PRC_CUS_SHOPS, S_PRC_CUS_ENT) = range(500, 505)

# Admin
(S_ADM_ADD_NAME, S_ADM_ADD_TURI, S_ADM_ADD_BIRLIK,
 S_ADM_ADD_ZAVOD, S_ADM_ADD_SOTISH, S_ADM_CHG_ZAVOD_PROD,
 S_ADM_CHG_ZAVOD_PRICE) = range(600, 607)

# ═══════════════════════════════════════════════════════════
# MAIN MENU
# ═══════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command - show main menu"""
    user_id = update.effective_user.id
    user_name = update.effective_user.full_name
    
    btns = [
        [
            InlineKeyboardButton("🏪 Do'konlarim", callback_data="menu_shops"),
            InlineKeyboardButton("➕ Qo'shish", callback_data="menu_add_shop")
        ],
        [
            InlineKeyboardButton("📦 Qabul", callback_data="menu_qabul"),
            InlineKeyboardButton("📋 Buyurtma", callback_data="menu_orders")
        ],
        [
            InlineKeyboardButton("🚚 Topshirish", callback_data="menu_delivery"),
            InlineKeyboardButton("💰 Narxlar", callback_data="menu_prices")
        ],
        [
            InlineKeyboardButton("📊 Hisobot", callback_data="menu_reports"),
            InlineKeyboardButton("🏦 Ombor", callback_data="menu_ombor")
        ]
    ]
    
    if user_id in ADMIN_IDS:
        btns.append([InlineKeyboardButton("👨‍💼 Admin", callback_data="menu_admin")])
    
    await update.message.reply_text(
        f"Assalomu alaykum, {user_name}!\n\n"
        "🥛 Alba Milk distribyutor botiga xush kelibsiz.",
        reply_markup=InlineKeyboardMarkup(btns)
    )

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show main menu (for callbacks)"""
    query = update.callback_query
    if query:
        await query.answer()
    
    user_id = update.effective_user.id
    
    btns = [
        [
            InlineKeyboardButton("🏪 Do'konlarim", callback_data="menu_shops"),
            InlineKeyboardButton("➕ Qo'shish", callback_data="menu_add_shop")
        ],
        [
            InlineKeyboardButton("📦 Qabul", callback_data="menu_qabul"),
            InlineKeyboardButton("📋 Buyurtma", callback_data="menu_orders")
        ],
        [
            InlineKeyboardButton("🚚 Topshirish", callback_data="menu_delivery"),
            InlineKeyboardButton("💰 Narxlar", callback_data="menu_prices")
        ],
        [
            InlineKeyboardButton("📊 Hisobot", callback_data="menu_reports"),
            InlineKeyboardButton("🏦 Ombor", callback_data="menu_ombor")
        ]
    ]
    
    if user_id in ADMIN_IDS:
        btns.append([InlineKeyboardButton("👨‍💼 Admin", callback_data="menu_admin")])
    
    if query:
        try:
            await query.edit_message_text("🏠 Asosiy menyu", reply_markup=InlineKeyboardMarkup(btns))
        except:
            await query.message.reply_text("🏠 Asosiy menyu", reply_markup=InlineKeyboardMarkup(btns))
    else:
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=InlineKeyboardMarkup(btns))

# ═══════════════════════════════════════════════════════════
# SHOP LIST - WITH PHOTOS & LOCATIONS
# ═══════════════════════════════════════════════════════════

async def show_shops_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's shops with photos and locations forwarded to chat"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    shops = db_get_all("Dokonlar")
    
    # Filter user's shops with explicit string comparison
    my_shops = []
    for s in shops:
        if str(s.get('Dist_ID', '')).strip() == str(user_id).strip():
            my_shops.append(s)
    
    logger.info(f"User {user_id} has {len(my_shops)} shops")
    
    if not my_shops:
        btns = [
            [InlineKeyboardButton("➕ Do'kon qo'shish", callback_data="menu_add_shop")],
            [InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]
        ]
        await query.edit_message_text(
            "❌ Sizda hali do'konlar yo'q.\n\n"
            "Do'kon qo'shish tugmasini bosing.",
            reply_markup=InlineKeyboardMarkup(btns)
        )
        return
    
    await query.edit_message_text(f"📥 {len(my_shops)} ta do'kon yuklanmoqda...")
    
    # Send each shop with photo and location
    for idx, shop in enumerate(my_shops, 1):
        shop_name = shop.get('Nomi', 'Unknown')
        address = shop.get('Adres', '')
        tel1 = shop.get('Tel1', '')
        tel2 = shop.get('Tel2', '')
        owner = shop.get('Sotuvchi', '')
        mchj = shop.get('MCHJ', '')
        
        caption = f"📍 {shop_name}\n"
        if mchj:
            caption += f"🏢 {mchj}\n"
        caption += f"📫 {address}\n📞 {tel1}\n"
        if tel2:
            caption += f"📞 {tel2}\n"
        caption += f"👤 {owner}"
        
        # Try to forward photo from channel
        channel_msg_id = shop.get('Channel_Msg_ID', '')
        photo_sent = False
        
        if channel_msg_id and CHANNEL_ID:
            try:
                await context.bot.forward_message(
                    chat_id=user_id,
                    from_chat_id=CHANNEL_ID,
                    message_id=int(channel_msg_id)
                )
                photo_sent = True
                logger.info(f"✅ Forwarded photo for shop {shop_name}")
            except Exception as e:
                logger.error(f"❌ Failed to forward photo: {e}")
        
        # If no photo, send text
        if not photo_sent:
            await context.bot.send_message(chat_id=user_id, text=caption)
        
        # Send location
        lat = shop.get('Lat', '')
        lng = shop.get('Lng', '')
        
        if lat and lng:
            try:
                await context.bot.send_location(
                    chat_id=user_id,
                    latitude=float(lat),
                    longitude=float(lng)
                )
                logger.info(f"✅ Sent location for shop {shop_name}")
            except Exception as e:
                logger.error(f"❌ Failed to send location: {e}")
    
    # Final message with menu button
    btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
    
    await context.bot.send_message(
        chat_id=user_id,
        text=f"✅ Barcha do'konlar ko'rsatildi ({len(my_shops)} ta)",
        reply_markup=InlineKeyboardMarkup(btns)
    )

# ═══════════════════════════════════════════════════════════
# SHOP REGISTRATION
# ═══════════════════════════════════════════════════════════

async def shop_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start shop registration"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("📝 Do'kon qo'shish\n\nDo'kon nomini kiriting:")
    return S_SHOP_NAME

async def shop_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['shop_name'] = update.message.text
    await update.message.reply_text("📍 Manzilni kiriting:")
    return S_SHOP_ADDR

async def shop_addr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['shop_addr'] = update.message.text
    btns = [[InlineKeyboardButton("⏭ O'tkazish", callback_data="shop_skip_mchj")]]
    await update.message.reply_text("🏢 MCHJ nomini kiriting (yoki skip):", reply_markup=InlineKeyboardMarkup(btns))
    return S_SHOP_MCHJ

async def shop_mchj(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        context.user_data['shop_mchj'] = ""
        await query.edit_message_text("📞 Tel 1 raqamini kiriting:\n\nFormat: 998901234567\nFaqat raqamlar!")
    else:
        context.user_data['shop_mchj'] = update.message.text
        await update.message.reply_text("📞 Tel 1 raqamini kiriting:\n\nFormat: 998901234567\nFaqat raqamlar!")
    return S_SHOP_TEL1

async def shop_tel1(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = clean_phone(update.message.text)
    if not re.match(r'^998\d{9}$', phone):
        await update.message.reply_text("❌ Noto'g'ri format!\n\nFaqat raqam: 998901234567")
        return S_SHOP_TEL1
    
    context.user_data['shop_tel1'] = phone
    btns = [[InlineKeyboardButton("⏭ O'tkazish", callback_data="shop_skip_tel2")]]
    await update.message.reply_text("📞 Tel 2 raqamini kiriting (yoki skip):", reply_markup=InlineKeyboardMarkup(btns))
    return S_SHOP_TEL2

async def shop_tel2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        context.user_data['shop_tel2'] = ""
        await query.edit_message_text("👤 Sotuvchi ismini kiriting:")
    else:
        phone = clean_phone(update.message.text)
        if not re.match(r'^998\d{9}$', phone):
            btns = [[InlineKeyboardButton("⏭ O'tkazish", callback_data="shop_skip_tel2")]]
            await update.message.reply_text("❌ Noto'g'ri format! Qaytadan yoki skip:", reply_markup=InlineKeyboardMarkup(btns))
            return S_SHOP_TEL2
        context.user_data['shop_tel2'] = phone
        await update.message.reply_text("👤 Sotuvchi ismini kiriting:")
    return S_SHOP_OWNER

async def shop_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['shop_owner'] = update.message.text
    await update.message.reply_text("📸 Do'kon rasmini yuboring:\n\n❗️ MAJBURIY — rasm yuboring")
    return S_SHOP_PHOTO

async def shop_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        await update.message.reply_text("❌ Iltimos RASM yuboring!\n\nText emas, rasm kerak.")
        return S_SHOP_PHOTO
    
    photo = update.message.photo[-1]
    context.user_data['shop_photo'] = photo.file_id
    await update.message.reply_text("📍 Lokatsiyani yuboring:\n\n❗️ MAJBURIY — lokatsiya yuboring")
    return S_SHOP_LOC

async def shop_loc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get location and save shop"""
    if not update.message.location:
        await update.message.reply_text("❌ Iltimos LOKATSIYA yuboring!\n\nText emas, lokatsiya kerak.")
        return S_SHOP_LOC
    
    loc = update.message.location
    user_id = update.effective_user.id
    user_name = update.effective_user.full_name
    
    shop_id = generate_id("dk")
    name = context.user_data.get('shop_name', '')
    addr = context.user_data.get('shop_addr', '')
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
            caption = f"✅ Yangi do'kon qo'shildi!\n\n📍 {name}\n"
            if mchj:
                caption += f"🏢 {mchj}\n"
            caption += f"📫 {addr}\n📞 {tel1}\n👤 {owner}\n🚚 Distribyutor: {user_name}"
            
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
            
            logger.info(f"✅ Posted to channel: msg_id={channel_msg_id}")
        except Exception as e:
            logger.error(f"❌ Channel post failed: {e}")
    
    # Save to Sheets
    row = [
        shop_id, name, addr, mchj, tel1, tel2, owner,
        str(user_id), user_name, lat, lng, channel_msg_id, sana
    ]
    
    logger.info(f"💾 Saving shop: {shop_id} - {name}")
    
    try:
        result = db_add("Dokonlar", row)
        if result:
            logger.info(f"✅ Shop saved successfully: {shop_id}")
            
            btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
            
            await update.message.reply_text(
                f"✅ Do'kon muvaffaqiyatli qo'shildi!\n\n"
                f"📍 {name}\n📫 {addr}\n📞 {tel1}\n👤 {owner}",
                reply_markup=InlineKeyboardMarkup(btns)
            )
        else:
            await update.message.reply_text("❌ Xatolik yuz berdi!\nQaytadan urinib ko'ring.")
    except Exception as e:
        logger.error(f"❌ Save failed: {e}")
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
    
    return await qabul_select_prod(query, context)

async def qabul_select_prod(qou, context: ContextTypes.DEFAULT_TYPE):
    """Select product for receipt"""
    products = db_get_all("Mahsulotlar_Asosiy")
    active = [p for p in products if str(p.get('Status', '')).strip().lower() == 'active']
    
    if not active:
        btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
        if hasattr(qou, 'edit_message_text'):
            await qou.edit_message_text("❌ Faol mahsulotlar topilmadi!", reply_markup=InlineKeyboardMarkup(btns))
        return ConversationHandler.END
    
    # Group by name
    grouped = {}
    for p in active:
        name = p.get('Nomi', '')
        if name not in grouped:
            grouped[name] = []
        grouped[name].append(p)
    
    button_list = []
    for name in sorted(grouped.keys()):
        variants = grouped[name]
        if len(variants) == 1:
            p = variants[0]
            turi = p.get('Turi', '-')
            label = f"{name}" if turi == '-' else f"{name} {turi}"
            button_list.append(InlineKeyboardButton(label, callback_data=f"qabul_p_{p.get('ID')}"))
        else:
            for p in variants:
                turi = p.get('Turi', '-')
                label = f"{name} {turi}"
                button_list.append(InlineKeyboardButton(label, callback_data=f"qabul_p_{p.get('ID')}"))
    
    # Arrange in 2 columns
    buttons = []
    for i in range(0, len(button_list), 2):
        if i + 1 < len(button_list):
            buttons.append([button_list[i], button_list[i+1]])
        else:
            buttons.append([button_list[i]])
    
    buttons.append([InlineKeyboardButton("❌ Bekor qilish", callback_data="qabul_cancel")])
    
    if hasattr(qou, 'edit_message_text'):
        await qou.edit_message_text(
            "📦 Zavoddan qabul qilish\n\nMahsulotni tanlang:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    
    return S_QAB_PROD

async def qabul_prod_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product selected for receipt"""
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
        context.user_data.clear()
        return ConversationHandler.END
    
    context.user_data['cur_qabul_prod'] = product
    
    name = product.get('Nomi', '')
    turi = product.get('Turi', '-')
    birlik = product.get('Birlik', '')
    zavod = format_number(product.get('Zavod_Narxi', 0))
    
    label = f"{name}" if turi == '-' else f"{name} {turi}"
    
    await query.edit_message_text(
        f"📦 Mahsulot: {label}\n"
        f"💰 Zavod narxi: {zavod} so'm/{birlik}\n\n"
        f"Miqdorni kiriting ({birlik}):"
    )
    return S_QAB_AMT

async def qabul_amt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Amount entered for receipt"""
    try:
        amount = float(update.message.text.replace(',', '.'))
        if amount <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri miqdor! Raqam kiriting:")
        return S_QAB_AMT
    
    product = context.user_data.get('cur_qabul_prod')
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
    
    btns = [
        [InlineKeyboardButton("➕ Yana mahsulot qabul", callback_data="qabul_more")],
        [InlineKeyboardButton("✅ Yakunlash", callback_data="qabul_finish")]
    ]
    
    await update.message.reply_text(
        f"✅ Qabul qilindi!\n\n"
        f"📦 {label}\n"
        f"📊 Miqdor: {amount} {product.get('Birlik')}\n"
        f"💰 Summa: {format_number(jami)} so'm\n\n"
        f"Omborga qo'shildi!",
        reply_markup=InlineKeyboardMarkup(btns)
    )
    return S_QAB_MORE

async def qabul_more_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """More products or finish receipt"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "qabul_more":
        await query.message.delete()
        return await qabul_select_prod(query, context)
    
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
    
    btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(btns))
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# ORDERS (Multi-product)
# ═══════════════════════════════════════════════════════════

async def order_view_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View pending orders for current distributor"""
    query = update.callback_query
    user_id = update.effective_user.id

    orders = db_get_all("Buyurtmalar")
    my_orders = [o for o in orders
                 if str(o.get('Dist_ID', '')).strip() == str(user_id).strip()
                 and str(o.get('Status', '')).strip().lower() == 'kutilmoqda']

    if not my_orders:
        btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
        await query.edit_message_text("📋 Kutilayotgan buyurtma yo'q.", reply_markup=InlineKeyboardMarkup(btns))
        return

    by_shop: dict = {}
    for o in my_orders:
        shop = o.get('Dokon', '?')
        by_shop.setdefault(shop, []).append(o)

    msg = "📋 <b>Kutilayotgan buyurtmalar:</b>\n━━━━━━━━━━━━━━━━\n"
    for shop, items in by_shop.items():
        msg += f"🏪 <b>{shop}</b>\n"
        for it in items:
            label = it.get('Mahsulot', '?')
            if it.get('Turi', '-') not in ('-', ''):
                label += f" {it['Turi']}"
            msg += f"  • {label}: {it.get('Miqdor', '?')} {it.get('Birlik', '')}\n"
        msg += "\n"

    btns = [
        [InlineKeyboardButton("➕ Yangi zakaz", callback_data="order_new")],
        [InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]
    ]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(btns), parse_mode="HTML")


async def orders_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Orders menu"""
    query = update.callback_query
    await query.answer()
    
    btns = [
        [
            InlineKeyboardButton("➕ Yangi zakaz", callback_data="order_new"),
            InlineKeyboardButton("📋 Ko'rish", callback_data="order_view")
        ],
        [InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]
    ]
    
    await query.edit_message_text("📋 Buyurtmalar", reply_markup=InlineKeyboardMarkup(btns))

async def order_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """New order - select shop"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    shops = db_get_all("Dokonlar")
    
    # Filter user's shops - FIXED with explicit string comparison
    my_shops = []
    for s in shops:
        dist_id = str(s.get('Dist_ID', '')).strip()
        if dist_id == str(user_id).strip():
            my_shops.append(s)
    
    logger.info(f"User {user_id} has {len(my_shops)} shops")
    
    if not my_shops:
        btns = [
            [InlineKeyboardButton("➕ Do'kon qo'shish", callback_data="menu_add_shop")],
            [InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]
        ]
        await query.edit_message_text(
            "❌ Sizda do'konlar yo'q!\n\nAvval do'kon qo'shing.",
            reply_markup=InlineKeyboardMarkup(btns)
        )
        return ConversationHandler.END
    
    # Create 2-column buttons
    btns = make_2col_buttons(my_shops, "order_s_", 'ID', 'Nomi')
    btns.append([InlineKeyboardButton("❌ Bekor", callback_data="order_cancel")])
    
    context.user_data['order_items'] = []
    context.user_data['zakaz_id'] = generate_id("z")
    
    await query.edit_message_text("🏪 Do'konni tanlang:", reply_markup=InlineKeyboardMarkup(btns))
    return S_ORD_SHOP

async def order_shop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shop selected for order"""
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
        context.user_data.clear()
        return ConversationHandler.END
    
    context.user_data['order_shop'] = shop
    return await order_select_prod(query, context)

async def order_select_prod(qou, context: ContextTypes.DEFAULT_TYPE):
    """Select product for order"""
    products = db_get_all("Mahsulotlar_Asosiy")
    active = [p for p in products if str(p.get('Status', '')).strip().lower() == 'active']
    
    if not active:
        btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
        if hasattr(qou, 'edit_message_text'):
            await qou.edit_message_text("❌ Faol mahsulotlar topilmadi!", reply_markup=InlineKeyboardMarkup(btns))
        return ConversationHandler.END
    
    user_id = qou.from_user.id if hasattr(qou, 'from_user') else qou.effective_user.id
    shop = context.user_data.get('order_shop')
    
    # Group by name
    grouped = {}
    for p in active:
        name = p.get('Nomi', '')
        if name not in grouped:
            grouped[name] = []
        grouped[name].append(p)
    
    button_list = []
    for name in sorted(grouped.keys()):
        variants = grouped[name]
        if len(variants) == 1:
            p = variants[0]
            turi = p.get('Turi', '-')
            price = get_selling_price(user_id, shop.get('ID'), p.get('ID'))
            has_custom = has_custom_price(user_id, shop.get('ID'), p.get('ID'))
            
            label = f"{name}" if turi == '-' else f"{name} {turi}"
            label += f" — {format_number(price)}"
            if has_custom:
                label += " ⭐"
            
            button_list.append(InlineKeyboardButton(label, callback_data=f"order_p_{p.get('ID')}"))
        else:
            for p in variants:
                turi = p.get('Turi', '-')
                price = get_selling_price(user_id, shop.get('ID'), p.get('ID'))
                has_custom = has_custom_price(user_id, shop.get('ID'), p.get('ID'))
                
                label = f"{name} {turi} — {format_number(price)}"
                if has_custom:
                    label += " ⭐"
                
                button_list.append(InlineKeyboardButton(label, callback_data=f"order_p_{p.get('ID')}"))
    
    # 2 columns
    buttons = []
    for i in range(0, len(button_list), 2):
        if i + 1 < len(button_list):
            buttons.append([button_list[i], button_list[i+1]])
        else:
            buttons.append([button_list[i]])
    
    buttons.append([InlineKeyboardButton("❌ Bekor", callback_data="order_cancel")])
    
    if hasattr(qou, 'edit_message_text'):
        await qou.edit_message_text(
            f"🏪 {shop.get('Nomi')}\n\n"
            "Mahsulotni tanlang:\n"
            "⭐ = Maxsus narx",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    
    return S_ORD_PROD

async def order_prod_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product selected for order"""
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
        context.user_data.clear()
        return ConversationHandler.END
    
    context.user_data['cur_order_prod'] = product
    
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
    return S_ORD_AMT

async def order_amt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Amount entered for order"""
    try:
        amount = float(update.message.text.replace(',', '.'))
        if amount <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri miqdor! Raqam kiriting:")
        return S_ORD_AMT
    
    user_id = update.effective_user.id
    shop = context.user_data.get('order_shop')
    product = context.user_data.get('cur_order_prod')
    
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
    
    btns = [
        [
            InlineKeyboardButton("➕ Yana mahsulot", callback_data="order_more"),
            InlineKeyboardButton("✅ Tasdiqlash", callback_data="order_confirm")
        ]
    ]
    
    await update.message.reply_text(
        f"✅ Savatga qo'shildi!\n\n"
        f"📦 {label}\n"
        f"📊 Miqdor: {amount} {product.get('Birlik')}\n"
        f"💰 Narx: {format_number(price)} so'm\n"
        f"💵 Jami: {format_number(jami)} so'm",
        reply_markup=InlineKeyboardMarkup(btns)
    )
    return S_ORD_MORE

async def order_more_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """More products or confirm order"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "order_more":
        await query.message.delete()
        return await order_select_prod(query, context)
    
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
    
    msg = f"✅ Zakaz tasdiqlandi!\n\n🏪 Do'kon: {shop.get('Nomi')}\n\n"
    for item in items:
        label = f"{item['nomi']}" if item['turi'] == '-' else f"{item['nomi']} {item['turi']}"
        msg += f"• {label}: {item['miqdor']} {item['birlik']}\n"
    msg += f"\n💰 Jami: {format_number(total)} so'm"
    
    btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(btns))
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# DELIVERY - PRODUCT BY PRODUCT INPUT (COMPLETE SYSTEM)
# ═══════════════════════════════════════════════════════════

async def delivery_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start delivery process"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    orders = db_get_all("Buyurtmalar")
    
    # Get shops with pending orders
    pending_shops = {}
    for order in orders:
        if str(order.get('Dist_ID', '')).strip() == str(user_id).strip() and order.get('Status') == 'kutilmoqda':
            sid = order.get('Dokon_ID')
            sname = order.get('Dokon')
            if sid not in pending_shops:
                pending_shops[sid] = sname
    
    if not pending_shops:
        btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
        await query.edit_message_text(
            "📦 Topshirish uchun kutilayotgan zakazlar yo'q.",
            reply_markup=InlineKeyboardMarkup(btns)
        )
        return ConversationHandler.END
    
    btns = []
    for sid, sname in pending_shops.items():
        btns.append([InlineKeyboardButton(sname, callback_data=f"del_s_{sid}")])
    btns.append([InlineKeyboardButton("❌ Bekor", callback_data="del_cancel")])
    
    context.user_data['del_products'] = []
    context.user_data['del_results'] = []
    context.user_data['del_idx'] = 0
    
    await query.edit_message_text("🚚 Tovar topshirish\n\nDo'konni tanlang:", reply_markup=InlineKeyboardMarkup(btns))
    return S_DEL_SHOP

async def delivery_shop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shop selected for delivery"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "del_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    sid = query.data.replace("del_s_", "")
    user_id = update.effective_user.id
    
    shop = get_shop_by_id(sid)
    if not shop:
        await query.edit_message_text("❌ Do'kon topilmadi!")
        context.user_data.clear()
        return ConversationHandler.END
    
    # Get ordered products for this shop
    orders = db_get_all("Buyurtmalar")
    products_ordered = []
    zid = None
    
    for order in orders:
        if (str(order.get('Dist_ID', '')).strip() == str(user_id).strip() and
            order.get('Dokon_ID') == sid and
            order.get('Status') == 'kutilmoqda'):
            
            if zid is None:
                zid = order.get('Zakaz_ID')
            
            products_ordered.append({
                'mahsulot': order.get('Mahsulot'),
                'turi': order.get('Turi'),
                'zakaz_miqdor': float(order.get('Miqdor', 0)),
                'birlik': order.get('Birlik'),
                'narx': int(order.get('Narx', 0))
            })
    
    if not products_ordered:
        await query.edit_message_text("❌ Zakazlar topilmadi!")
        context.user_data.clear()
        return ConversationHandler.END
    
    context.user_data['del_shop'] = shop
    context.user_data['del_products'] = products_ordered
    context.user_data['del_zakaz_id'] = zid
    context.user_data['del_idx'] = 0
    
    return await ask_delivery_input(query, context)

async def ask_delivery_input(qou, context: ContextTypes.DEFAULT_TYPE):
    """Ask for delivery amount for current product"""
    idx = context.user_data.get('del_idx', 0)
    prods = context.user_data.get('del_products', [])
    
    if idx >= len(prods):
        # All products done - move to izoh
        return await ask_izoh(qou, context)
    
    prod = prods[idx]
    lbl = f"{prod['mahsulot']}" if prod['turi'] == '-' else f"{prod['mahsulot']} {prod['turi']}"
    
    msg = (
        f"━━━━━━━━━━━━━━━━\n"
        f"Mahsulot {idx+1}/{len(prods)}\n\n"
        f"📦 {lbl}\n"
        f"📋 Zakaz: {prod['zakaz_miqdor']} {prod['birlik']}\n\n"
        f"Qancha topshirdingiz? ({prod['birlik']})"
    )
    
    if hasattr(qou, 'edit_message_text'):
        await qou.edit_message_text(msg)
    else:
        await qou.message.reply_text(msg)
    
    return S_DEL_INPUT

async def delivery_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delivery amount entered"""
    try:
        amt = float(update.message.text.replace(',', '.'))
        if amt < 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri! Musbat raqam kiriting:")
        return S_DEL_INPUT
    
    context.user_data['cur_del_amt'] = amt
    
    btns = [
        [
            InlineKeyboardButton("Ha", callback_data="voz_yes"),
            InlineKeyboardButton("Yo'q", callback_data="voz_no")
        ]
    ]
    await update.message.reply_text("Vozvrat bormi?", reply_markup=InlineKeyboardMarkup(btns))
    return S_DEL_VOZ_YN

async def delivery_voz_yn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Vozvrat yes/no"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "voz_no":
        context.user_data['cur_voz'] = 0
        return await save_cur_prod_next(query, context)
    
    idx = context.user_data.get('del_idx', 0)
    prods = context.user_data.get('del_products', [])
    prod = prods[idx]
    
    await query.edit_message_text(f"Vozvrat miqdori? ({prod['birlik']})")
    return S_DEL_VOZ_AMT

async def delivery_voz_amt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Vozvrat amount entered"""
    try:
        voz = float(update.message.text.replace(',', '.'))
        if voz < 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri! Musbat raqam kiriting:")
        return S_DEL_VOZ_AMT
    
    context.user_data['cur_voz'] = voz
    return await save_cur_prod_next(update, context)

async def save_cur_prod_next(qou, context: ContextTypes.DEFAULT_TYPE):
    """Save current product and move to next"""
    idx = context.user_data.get('del_idx', 0)
    prods = context.user_data.get('del_products', [])
    prod = prods[idx]
    
    del_amt = context.user_data.get('cur_del_amt', 0)
    voz = context.user_data.get('cur_voz', 0)
    
    result = {
        'mahsulot': prod['mahsulot'],
        'turi': prod['turi'],
        'zakaz_miqdor': prod['zakaz_miqdor'],
        'topshirish_miqdor': del_amt,
        'vozvrat_miqdor': voz,
        'birlik': prod['birlik'],
        'narx': prod['narx']
    }
    
    if 'del_results' not in context.user_data:
        context.user_data['del_results'] = []
    context.user_data['del_results'].append(result)
    
    context.user_data['del_idx'] = idx + 1
    
    return await ask_delivery_input(qou, context)

async def ask_izoh(qou, context: ContextTypes.DEFAULT_TYPE):
    """Ask for izoh"""
    results = context.user_data.get('del_results', [])
    
    # Check if Qaymoq was delivered
    qaymoq_bor = any('Qaymoq' in r['mahsulot'] and r['topshirish_miqdor'] > 0 for r in results)
    
    context.user_data['qaymoq_bor'] = qaymoq_bor
    
    if qaymoq_bor:
        context.user_data['eslatma_kun'] = 5
        msg = "✅ Qaymoq topshirildi — 5 kunda eslatma avtomatik.\n\n"
    else:
        msg = ""
    
    btns = [
        [
            InlineKeyboardButton("Ha", callback_data="izoh_yes"),
            InlineKeyboardButton("Yo'q", callback_data="izoh_no")
        ]
    ]
    
    msg += "Izoh qo'shasizmi?"
    
    if hasattr(qou, 'edit_message_text'):
        await qou.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(btns))
    else:
        await qou.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(btns))
    
    return S_DEL_IZOH_YN

async def delivery_izoh_yn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Izoh yes/no"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "izoh_no":
        context.user_data['izoh'] = ""
        
        # If no qaymoq, ask eslatma
        if not context.user_data.get('qaymoq_bor'):
            await query.edit_message_text("Necha kundan keyin eslatma? (1-30)\n\n⚠️ Maksimal: 7 kun")
            return S_DEL_ESLATMA
        
        return await ask_tolov(query, context)
    
    await query.edit_message_text("Izohni kiriting:")
    return S_DEL_IZOH_TXT

async def delivery_izoh_txt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Izoh text entered"""
    context.user_data['izoh'] = update.message.text
    
    if not context.user_data.get('qaymoq_bor'):
        await update.message.reply_text("Necha kundan keyin eslatma? (1-30)\n\n⚠️ Maksimal: 7 kun")
        return S_DEL_ESLATMA
    
    return await ask_tolov(update, context)

async def delivery_eslatma(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Eslatma days entered"""
    try:
        kun = int(update.message.text)
        if kun < 1 or kun > 30:
            raise ValueError
    except:
        await update.message.reply_text("❌ 1-30 oralig'ida raqam kiriting:")
        return S_DEL_ESLATMA
    
    if kun > 7:
        kun = 7
        await update.message.reply_text("✅ Eslatma 7 kunda (maksimal)")
    else:
        await update.message.reply_text(f"✅ Eslatma {kun} kunda")
    
    context.user_data['eslatma_kun'] = kun
    return await ask_tolov(update, context)

async def ask_tolov(qou, context: ContextTypes.DEFAULT_TYPE):
    """Ask for payment type"""
    btns = [
        [
            InlineKeyboardButton("💵 Naqd", callback_data="tolov_naqd"),
            InlineKeyboardButton("📝 Realizatsiya", callback_data="tolov_real")
        ]
    ]
    
    if hasattr(qou, 'edit_message_text'):
        await qou.edit_message_text("To'lov usuli?", reply_markup=InlineKeyboardMarkup(btns))
    else:
        await qou.message.reply_text("To'lov usuli?", reply_markup=InlineKeyboardMarkup(btns))
    
    return S_DEL_TOLOV

async def delivery_tolov(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Payment type selected"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "tolov_real":
        context.user_data['naqd'] = 0
        return await check_tarozi(query, context)
    
    await query.edit_message_text("Naqd summasini kiriting:")
    return S_DEL_NAQD

async def delivery_naqd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cash amount entered"""
    try:
        naqd = int(update.message.text.replace(',', ''))
        if naqd < 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri! Musbat raqam kiriting:")
        return S_DEL_NAQD
    
    context.user_data['naqd'] = naqd
    return await check_tarozi(update, context)

async def check_tarozi(qou, context: ContextTypes.DEFAULT_TYPE):
    """Check if tarozi photo is needed"""
    results = context.user_data.get('del_results', [])
    
    needs_tarozi = False
    for r in results:
        if r['topshirish_miqdor'] <= 0:
            continue
        
        mhs = r['mahsulot']
        turi = r.get('turi', '-')
        
        # Need tarozi for: Tvorog/Suzma/Qaymoq 1kg OR Brinza
        if mhs in ['Tvorog', 'Suzma', 'Qaymoq'] and turi == '1kg':
            needs_tarozi = True
            break
        if mhs == 'Brinza':
            needs_tarozi = True
            break
    
    if needs_tarozi:
        if hasattr(qou, 'edit_message_text'):
            await qou.edit_message_text("📸 Tarozi rasmini yuboring:\n\n❗️ MAJBURIY (1kg mahsulotlar uchun)")
        else:
            await qou.message.reply_text("📸 Tarozi rasmini yuboring:\n\n❗️ MAJBURIY (1kg mahsulotlar uchun)")
        return S_DEL_TAROZI
    
    context.user_data['tarozi'] = ""
    return await save_delivery(qou, context)

async def delivery_tarozi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Tarozi photo received"""
    if not update.message.photo:
        await update.message.reply_text("❌ Iltimos RASM yuboring!")
        return S_DEL_TAROZI
    
    context.user_data['tarozi'] = update.message.photo[-1].file_id
    return await save_delivery(update, context)

async def save_delivery(qou, context: ContextTypes.DEFAULT_TYPE):
    """Save delivery to database"""
    user_id = qou.effective_user.id if hasattr(qou, 'effective_user') else qou.from_user.id
    shop = context.user_data.get('del_shop')
    results = context.user_data.get('del_results', [])
    zid = context.user_data.get('del_zakaz_id')
    izoh = context.user_data.get('izoh', '')
    naqd = context.user_data.get('naqd', 0)
    eslatma_kun = context.user_data.get('eslatma_kun', 7)
    qaymoq_bor = context.user_data.get('qaymoq_bor', False)
    tarozi = context.user_data.get('tarozi', '')
    
    sana = date.today().strftime("%Y-%m-%d")
    eslatma_sana = (date.today() + timedelta(days=eslatma_kun)).strftime("%Y-%m-%d")
    
    # Calculate total and debt
    total_qarz = 0
    prods_map = {p.get('ID'): p for p in db_get_all("Mahsulotlar_Asosiy")}

    # Proportional naqd split: each product gets naqd proportional to its share of total
    total_jami = sum(r['topshirish_miqdor'] * r['narx'] for r in results)

    # Save each product delivery
    for r in results:
        jami = r['topshirish_miqdor'] * r['narx']
        naqd_per_prod = round(naqd * (jami / total_jami), 2) if total_jami > 0 else 0
        qarz = max(0.0, jami - naqd_per_prod)
        total_qarz += qarz
        
        row = [
            sana, str(user_id), shop.get('Nomi'), shop.get('ID'), zid,
            r['mahsulot'], r['turi'], r['zakaz_miqdor'],
            r['topshirish_miqdor'], r['vozvrat_miqdor'], r['birlik'],
            naqd_per_prod, qarz, tarozi, eslatma_kun, eslatma_sana,
            'Ha' if qaymoq_bor else "Yo'q", izoh, 'topshirildi'
        ]
        db_add("Topshirish", row)
        
        # Update ombor
        pid = None
        for p_id, p in prods_map.items():
            if p.get('Nomi') == r['mahsulot'] and p.get('Turi') == r['turi']:
                pid = p_id
                break
        
        if pid:
            # Subtract delivered
            update_ombor(str(user_id), pid, r['mahsulot'], r['turi'], 
                        r['topshirish_miqdor'], r['birlik'], 'subtract')
            # Add vozvrat back
            if r['vozvrat_miqdor'] > 0:
                update_ombor(str(user_id), pid, r['mahsulot'], r['turi'], 
                           r['vozvrat_miqdor'], r['birlik'], 'add')
    
    # Update order status
    db_update_row("Buyurtmalar", {'Zakaz_ID': zid}, {'Status': 'topshirildi'})
    
    # Summary message
    msg = f"✅ Topshirish yakunlandi!\n\n🏪 {shop.get('Nomi')}\n\n"
    for r in results:
        lbl = f"{r['mahsulot']}" if r['turi'] == '-' else f"{r['mahsulot']} {r['turi']}"
        msg += f"• {lbl}: {r['topshirish_miqdor']} {r['birlik']}\n"
        if r['vozvrat_miqdor'] > 0:
            msg += f"  ↩️ Vozvrat: {r['vozvrat_miqdor']} {r['birlik']}\n"
    
    msg += f"\n💵 Naqd: {format_number(naqd)}\n"
    msg += f"📝 Qarz: {format_number(int(total_qarz))}\n"
    msg += f"⏰ Eslatma: {eslatma_kun} kunda"
    
    btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
    
    if hasattr(qou, 'edit_message_text'):
        await qou.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(btns))
    elif hasattr(qou, 'message'):
        await qou.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(btns))
    
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# PRICE MANAGEMENT (VIEW, DEFAULT, CUSTOM)
# ═══════════════════════════════════════════════════════════

async def prices_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Price management menu"""
    query = update.callback_query
    await query.answer()
    
    btns = [
        [InlineKeyboardButton("📋 Narxlarni ko'rish", callback_data="price_view")],
        [
            InlineKeyboardButton("✏️ Asosiy narx", callback_data="price_default"),
            InlineKeyboardButton("🏪 Maxsus narx", callback_data="price_custom")
        ],
        [InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]
    ]
    
    await query.edit_message_text("💰 Narxlar boshqaruvi", reply_markup=InlineKeyboardMarkup(btns))
    return S_PRC_MENU

async def price_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View all prices"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    prods = db_get_all("Mahsulotlar_Asosiy")
    active = [p for p in prods if str(p.get('Status', '')).lower() == 'active']
    
    defs = db_get_all("Mahsulotlar_Dist_Default")
    customs = db_get_all("Mahsulotlar_Maxsus_Narx")
    
    msg = "💰 Mening narxlarim:\n\n"
    
    for p in active:
        name = p.get('Nomi')
        turi = p.get('Turi', '-')
        lbl = f"{name}" if turi == '-' else f"{name} {turi}"
        zavod = int(p.get('Zavod_Narxi', 0))
        
        # My default
        my_def = None
        for d in defs:
            if str(d.get('Dist_ID', '')).strip() == str(user_id).strip() and d.get('Mahsulot_ID') == p.get('ID'):
                my_def = int(d.get('Sotish_Narxi', 0))
                break
        
        if my_def is None:
            my_def = int(p.get('Sotish_Narxi_Default', 0))
        
        msg += f"━━━━━━━━━━━━━━━━\n{lbl}\n"
        msg += f"  Zavod: {format_number(zavod)}\n"
        msg += f"  Mening: {format_number(my_def)}\n"
        
        # Custom prices
        cust_list = []
        for c in customs:
            if str(c.get('Dist_ID', '')).strip() == str(user_id).strip() and c.get('Mahsulot_ID') == p.get('ID'):
                cust_list.append(f"    • {c.get('Dokon_Nomi')}: {format_number(int(c.get('Sotish_Narxi', 0)))}")
        
        if cust_list:
            msg += "  Maxsus:\n" + "\n".join(cust_list) + "\n"
        else:
            msg += "  Maxsus: yo'q\n"
    
    btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(btns))
    return ConversationHandler.END

async def price_default_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start setting default price"""
    query = update.callback_query
    await query.answer()
    
    prods = db_get_all("Mahsulotlar_Asosiy")
    active = [p for p in prods if str(p.get('Status', '')).lower() == 'active']
    
    btn_list = []
    for p in active:
        name = p.get('Nomi')
        turi = p.get('Turi', '-')
        lbl = f"{name}" if turi == '-' else f"{name} {turi}"
        btn_list.append(InlineKeyboardButton(lbl, callback_data=f"pricedef_{p.get('ID')}"))
    
    btns = []
    for i in range(0, len(btn_list), 2):
        if i+1 < len(btn_list):
            btns.append([btn_list[i], btn_list[i+1]])
        else:
            btns.append([btn_list[i]])
    btns.append([InlineKeyboardButton("❌ Bekor", callback_data="price_cancel")])
    
    await query.edit_message_text("Mahsulotni tanlang:", reply_markup=InlineKeyboardMarkup(btns))
    return S_PRC_SEL_PROD

async def price_def_prod_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product selected for default price"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "price_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        return ConversationHandler.END
    
    pid = query.data.replace("pricedef_", "")
    prod = get_product_by_id(pid)
    
    if not prod:
        await query.edit_message_text("❌ Mahsulot topilmadi!")
        return ConversationHandler.END
    
    context.user_data['price_prod'] = prod
    
    user_id = update.effective_user.id
    zavod = int(prod.get('Zavod_Narxi', 0))
    
    # Current default
    defs = db_get_all("Mahsulotlar_Dist_Default")
    current = None
    for d in defs:
        if str(d.get('Dist_ID', '')).strip() == str(user_id).strip() and d.get('Mahsulot_ID') == pid:
            current = int(d.get('Sotish_Narxi', 0))
            break
    
    if current is None:
        current = int(prod.get('Sotish_Narxi_Default', 0))
    
    name = prod.get('Nomi')
    turi = prod.get('Turi', '-')
    lbl = f"{name}" if turi == '-' else f"{name} {turi}"
    
    await query.edit_message_text(
        f"Mahsulot: {lbl}\n\n"
        f"Zavod: {format_number(zavod)}\n"
        f"Joriy: {format_number(current)}\n\n"
        f"Yangi narxni kiriting:"
    )
    return S_PRC_ENT_DEF

async def price_def_enter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Default price entered"""
    try:
        price = int(update.message.text.replace(',', ''))
        if price <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri! Musbat raqam kiriting:")
        return S_PRC_ENT_DEF
    
    prod = context.user_data.get('price_prod')
    zavod = int(prod.get('Zavod_Narxi', 0))
    
    if price < zavod:
        await update.message.reply_text(
            f"❌ Zavod narxidan kam bo'lishi mumkin emas!\n\n"
            f"Zavod: {format_number(zavod)}\n\n"
            f"Yangi narx:"
        )
        return S_PRC_ENT_DEF
    
    user_id = update.effective_user.id
    pid = prod.get('ID')
    name = prod.get('Nomi')
    turi = prod.get('Turi', '-')
    
    # Check if exists
    defs = db_get_all("Mahsulotlar_Dist_Default")
    exists = False
    for idx, d in enumerate(defs):
        if str(d.get('Dist_ID', '')).strip() == str(user_id).strip() and d.get('Mahsulot_ID') == pid:
            db_update_cell("Mahsulotlar_Dist_Default", idx+2, 5, price)
            exists = True
            break
    
    if not exists:
        sana = date.today().strftime("%Y-%m-%d")
        lbl = f"{name}" if turi == '-' else f"{name} {turi}"
        row = [str(user_id), pid, name, turi, price, sana]
        db_add("Mahsulotlar_Dist_Default", row)
    
    lbl = f"{name}" if turi == '-' else f"{name} {turi}"
    btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
    await update.message.reply_text(
        f"✅ Asosiy narx yangilandi!\n\n"
        f"{lbl}\n"
        f"Yangi narx: {format_number(price)} so'm\n\n"
        f"Barcha do'konlar uchun qo'llaniladi.",
        reply_markup=InlineKeyboardMarkup(btns)
    )
    
    context.user_data.clear()
    return ConversationHandler.END

async def price_custom_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start setting custom price"""
    query = update.callback_query
    await query.answer()
    
    # Select product first
    prods = db_get_all("Mahsulotlar_Asosiy")
    active = [p for p in prods if str(p.get('Status', '')).lower() == 'active']
    
    btn_list = []
    for p in active:
        name = p.get('Nomi')
        turi = p.get('Turi', '-')
        lbl = f"{name}" if turi == '-' else f"{name} {turi}"
        btn_list.append(InlineKeyboardButton(lbl, callback_data=f"pricecus_{p.get('ID')}"))
    
    btns = []
    for i in range(0, len(btn_list), 2):
        if i+1 < len(btn_list):
            btns.append([btn_list[i], btn_list[i+1]])
        else:
            btns.append([btn_list[i]])
    btns.append([InlineKeyboardButton("❌ Bekor", callback_data="price_cancel")])
    
    await query.edit_message_text(
        "Maxsus narx o'rnatish\n\n"
        "Mahsulotni tanlang:",
        reply_markup=InlineKeyboardMarkup(btns)
    )
    return S_PRC_SEL_PROD

async def price_cus_prod_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product selected for custom price"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "price_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    pid = query.data.replace("pricecus_", "")
    prod = get_product_by_id(pid)
    
    if not prod:
        await query.edit_message_text("❌ Mahsulot topilmadi!")
        context.user_data.clear()
        return ConversationHandler.END
    
    context.user_data['price_prod'] = prod
    
    # Now select shops
    user_id = update.effective_user.id
    shops = db_get_all("Dokonlar")
    my_shops = [s for s in shops if str(s.get('Dist_ID', '')).strip() == str(user_id).strip()]
    
    if not my_shops:
        await query.edit_message_text("❌ Sizda do'konlar yo'q!")
        return ConversationHandler.END
    
    btns = make_2col_buttons(my_shops, "priceshop_", 'ID', 'Nomi')
    btns.append([InlineKeyboardButton("❌ Bekor", callback_data="price_cancel")])
    
    name = prod.get('Nomi')
    turi = prod.get('Turi', '-')
    lbl = f"{name}" if turi == '-' else f"{name} {turi}"
    
    await query.edit_message_text(
        f"Mahsulot: {lbl}\n\n"
        f"Qaysi do'kon uchun maxsus narx?",
        reply_markup=InlineKeyboardMarkup(btns)
    )
    return S_PRC_CUS_SHOPS

async def price_cus_shop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shop selected for custom price"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "price_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        context.user_data.clear()
        return ConversationHandler.END
    
    sid = query.data.replace("priceshop_", "")
    shop = get_shop_by_id(sid)
    
    if not shop:
        await query.edit_message_text("❌ Do'kon topilmadi!")
        return ConversationHandler.END
    
    context.user_data['price_shop'] = shop
    
    prod = context.user_data.get('price_prod')
    zavod = int(prod.get('Zavod_Narxi', 0))
    
    name = prod.get('Nomi')
    turi = prod.get('Turi', '-')
    lbl = f"{name}" if turi == '-' else f"{name} {turi}"
    
    await query.edit_message_text(
        f"Mahsulot: {lbl}\n"
        f"Do'kon: {shop.get('Nomi')}\n\n"
        f"Zavod: {format_number(zavod)}\n\n"
        f"Maxsus narxni kiriting:"
    )
    return S_PRC_CUS_ENT

async def price_cus_enter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Custom price entered"""
    try:
        price = int(update.message.text.replace(',', ''))
        if price <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri! Musbat raqam kiriting:")
        return S_PRC_CUS_ENT
    
    prod = context.user_data.get('price_prod')
    shop = context.user_data.get('price_shop')
    zavod = int(prod.get('Zavod_Narxi', 0))
    
    if price < zavod:
        await update.message.reply_text(
            f"❌ Zavod narxidan kam!\n\n"
            f"Zavod: {format_number(zavod)}\n\n"
            f"Maxsus narx:"
        )
        return S_PRC_CUS_ENT
    
    user_id = update.effective_user.id
    pid = prod.get('ID')
    name = prod.get('Nomi')
    turi = prod.get('Turi', '-')
    
    # Check if exists
    customs = db_get_all("Mahsulotlar_Maxsus_Narx")
    exists = False
    for idx, c in enumerate(customs):
        if (str(c.get('Dist_ID', '')).strip() == str(user_id).strip() and
            c.get('Dokon_ID') == shop.get('ID') and
            c.get('Mahsulot_ID') == pid):
            db_update_cell("Mahsulotlar_Maxsus_Narx", idx+2, 6, price)
            exists = True
            break
    
    if not exists:
        sana = date.today().strftime("%Y-%m-%d")
        row = [str(user_id), shop.get('ID'), shop.get('Nomi'), pid, name, turi, price, sana]
        db_add("Mahsulotlar_Maxsus_Narx", row)
    
    lbl = f"{name}" if turi == '-' else f"{name} {turi}"
    btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
    await update.message.reply_text(
        f"✅ Maxsus narx o'rnatildi!\n\n"
        f"Mahsulot: {lbl}\n"
        f"Do'kon: {shop.get('Nomi')}\n"
        f"Narx: {format_number(price)} so'm",
        reply_markup=InlineKeyboardMarkup(btns)
    )
    
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# REPORTS
# ═══════════════════════════════════════════════════════════

async def reports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reports menu"""
    query = update.callback_query
    await query.answer()
    
    btns = [
        [
            InlineKeyboardButton("📊 Kunlik", callback_data="rep_day"),
            InlineKeyboardButton("📅 7 kun", callback_data="rep_7")
        ],
        [
            InlineKeyboardButton("📅 15 kun", callback_data="rep_15"),
            InlineKeyboardButton("📅 30 kun", callback_data="rep_30")
        ],
        [InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]
    ]
    
    await query.edit_message_text("📊 Hisobotlar\n\nDavr tanlang:", reply_markup=InlineKeyboardMarkup(btns))

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
        title = "📊 7 kunlik"
    elif query.data == "rep_15":
        start = today - timedelta(days=15)
        end = today
        title = "📊 15 kunlik"
    else:
        start = today - timedelta(days=30)
        end = today
        title = "📊 30 kunlik"
    
    await query.edit_message_text("⏳ Hisobot tayyorlanmoqda...")
    
    # Get data
    qabul_data = db_get_all("Qabul")
    topshirish_data = db_get_all("Topshirish")
    tolov_data = db_get_all("Tolov")
    prods_map = {p.get('ID'): p for p in db_get_all("Mahsulotlar_Asosiy")}
    
    zavod = 0
    for q in qabul_data:
        if str(q.get('Dist_ID', '')).strip() == str(user_id).strip():
            try:
                q_date = datetime.strptime(str(q.get('Sana')), '%Y-%m-%d').date()
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
        if str(t.get('Dist_ID', '')).strip() == str(user_id).strip():
            try:
                t_date = datetime.strptime(str(t.get('Sana')), '%Y-%m-%d').date()
                if start <= t_date <= end:
                    mahsulot = t.get('Mahsulot')
                    turi = t.get('Turi')
                    top_miqdor = float(t.get('Topshirish_Miqdor', 0))
                    
                    # Find product
                    prod_id = None
                    for pid, p in prods_map.items():
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
        if str(t.get('Dist_ID', '')).strip() == str(user_id).strip():
            jami_qarz += int(t.get('Qarz', 0))
    for tl in tolov_data:
        if str(tl.get('Dist_ID', '')).strip() == str(user_id).strip():
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
    
    btns = [
        [
            InlineKeyboardButton("🔄 Boshqa", callback_data="menu_reports"),
            InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")
        ]
    ]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(btns))

async def show_ombor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show warehouse stock"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    ombor = db_get_all("Ombor")
    
    my_stock = [o for o in ombor if str(o.get('Dist_ID', '')).strip() == str(user_id).strip()]
    
    if not my_stock:
        btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
        await query.edit_message_text("📦 Ombor bo'sh", reply_markup=InlineKeyboardMarkup(btns))
        return
    
    msg = "📦 Mening omborim:\n\n"
    for item in my_stock:
        name = item.get('Mahsulot', '')
        turi = item.get('Turi', '-')
        miqdor = item.get('Miqdor', 0)
        birlik = item.get('Birlik', '')
        
        label = f"{name}" if turi == '-' else f"{name} {turi}"
        msg += f"• {label}: {miqdor} {birlik}\n"
    
    btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(btns))

# ═══════════════════════════════════════════════════════════
# ADMIN PANEL
# ═══════════════════════════════════════════════════════════

async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin panel menu"""
    query = update.callback_query
    await query.answer()
    
    btns = [
        [InlineKeyboardButton("➕ Mahsulot qo'shish", callback_data="adm_add_prod")],
        [
            InlineKeyboardButton("💰 Zavod narx", callback_data="adm_zavod"),
            InlineKeyboardButton("📊 Barcha hisobot", callback_data="adm_report")
        ],
        [InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]
    ]
    
    await query.edit_message_text("👨‍💼 Admin Panel", reply_markup=InlineKeyboardMarkup(btns))


# ═══════════════════════════════════════════════════════════
# ADMIN - ADD PRODUCT
# ═══════════════════════════════════════════════════════════

async def admin_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start adding new product"""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text("➕ Mahsulot qo'shish\n\nMahsulot nomini kiriting:")
    return S_ADM_ADD_NAME

async def admin_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product name entered"""
    context.user_data['adm_name'] = update.message.text
    await update.message.reply_text("Turini kiriting:\n\n(Masalan: 1kg, 500g, 250g)\nYoki '-' (turi yo'q bo'lsa)")
    return S_ADM_ADD_TURI

async def admin_add_turi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product type entered"""
    turi = update.message.text.strip()
    context.user_data['adm_turi'] = turi if turi != '-' else '-'
    await update.message.reply_text("Birligini kiriting:\n\n(Masalan: kg, dona, litr)")
    return S_ADM_ADD_BIRLIK

async def admin_add_birlik(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product unit entered"""
    context.user_data['adm_birlik'] = update.message.text
    await update.message.reply_text("Zavod narxini kiriting (so'm):")
    return S_ADM_ADD_ZAVOD

async def admin_add_zavod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Factory price entered"""
    try:
        zavod = int(update.message.text.replace(',', ''))
        if zavod <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri! Musbat raqam kiriting:")
        return S_ADM_ADD_ZAVOD
    
    context.user_data['adm_zavod'] = zavod
    await update.message.reply_text("Tavsiya etilgan sotish narxini kiriting (so'm):")
    return S_ADM_ADD_SOTISH

async def admin_add_sotish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Suggested selling price entered - save product"""
    try:
        sotish = int(update.message.text.replace(',', ''))
        if sotish <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri! Musbat raqam kiriting:")
        return S_ADM_ADD_SOTISH
    
    zavod = context.user_data.get('adm_zavod', 0)
    
    if sotish < zavod:
        await update.message.reply_text(
            f"❌ Sotish narxi zavod narxidan kam!\n\n"
            f"Zavod: {format_number(zavod)}\n\n"
            f"Sotish narxini qaytadan kiriting:"
        )
        return S_ADM_ADD_SOTISH
    
    # Generate product ID
    prod_id = generate_id("mhs")
    name = context.user_data.get('adm_name', '')
    turi = context.user_data.get('adm_turi', '-')
    birlik = context.user_data.get('adm_birlik', '')
    sana = date.today().strftime("%Y-%m-%d")
    
    # Save to Mahsulotlar_Asosiy
    row = [prod_id, name, turi, birlik, zavod, sotish, 'active', sana]
    
    logger.info(f"🔍 Saving product: {row}")
    
    try:
        # Try to add to sheet
        ws = sh.worksheet("Mahsulotlar_Asosiy")
        ws.append_row(row, value_input_option='USER_ENTERED')
        
        lbl = f"{name}" if turi == '-' else f"{name} {turi}"
        
        btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
        
        await update.message.reply_text(
            f"✅ Mahsulot qo'shildi!\n\n"
            f"📦 {lbl}\n"
            f"💰 Zavod: {format_number(zavod)} so'm\n"
            f"💵 Sotish: {format_number(sotish)} so'm",
            reply_markup=InlineKeyboardMarkup(btns)
        )
        
        logger.info(f"✅ Product saved: {prod_id}")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ Failed to save product: {error_msg}")
        
        await update.message.reply_text(
            f"❌ Xatolik yuz berdi!\n\n"
            f"Sabab: {error_msg}\n\n"
            f"Iltimos admin bilan bog'laning."
        )
    
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# ADMIN - CHANGE FACTORY PRICE
# ═══════════════════════════════════════════════════════════

async def admin_zavod_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start changing factory price"""
    query = update.callback_query
    await query.answer()
    
    prods = db_get_all("Mahsulotlar_Asosiy")
    active = [p for p in prods if str(p.get('Status', '')).lower() == 'active']
    
    if not active:
        btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
        await query.edit_message_text("❌ Mahsulotlar yo'q!", reply_markup=InlineKeyboardMarkup(btns))
        return ConversationHandler.END
    
    btn_list = []
    for p in active:
        name = p.get('Nomi')
        turi = p.get('Turi', '-')
        zavod = format_number(p.get('Zavod_Narxi', 0))
        lbl = f"{name}" if turi == '-' else f"{name} {turi}"
        lbl += f" ({zavod})"
        btn_list.append(InlineKeyboardButton(lbl, callback_data=f"admz_{p.get('ID')}"))
    
    btns = []
    for i in range(0, len(btn_list), 2):
        if i+1 < len(btn_list):
            btns.append([btn_list[i], btn_list[i+1]])
        else:
            btns.append([btn_list[i]])
    btns.append([InlineKeyboardButton("❌ Bekor", callback_data="adm_cancel")])
    
    await query.edit_message_text(
        "💰 Zavod narxini o'zgartirish\n\n"
        "Mahsulotni tanlang:",
        reply_markup=InlineKeyboardMarkup(btns)
    )
    return S_ADM_CHG_ZAVOD_PROD

async def admin_zavod_prod_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Product selected for factory price change"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "adm_cancel":
        await query.edit_message_text("❌ Bekor qilindi")
        return ConversationHandler.END
    
    pid = query.data.replace("admz_", "")
    prod = get_product_by_id(pid)
    
    if not prod:
        await query.edit_message_text("❌ Mahsulot topilmadi!")
        return ConversationHandler.END
    
    context.user_data['adm_zavod_prod'] = prod
    
    name = prod.get('Nomi')
    turi = prod.get('Turi', '-')
    zavod = format_number(prod.get('Zavod_Narxi', 0))
    
    lbl = f"{name}" if turi == '-' else f"{name} {turi}"
    
    await query.edit_message_text(
        f"Mahsulot: {lbl}\n\n"
        f"Joriy zavod narxi: {zavod} so'm\n\n"
        f"Yangi zavod narxini kiriting:"
    )
    return S_ADM_CHG_ZAVOD_PRICE

async def admin_zavod_price_enter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """New factory price entered"""
    try:
        price = int(update.message.text.replace(',', ''))
        if price <= 0:
            raise ValueError
    except:
        await update.message.reply_text("❌ Noto'g'ri! Musbat raqam kiriting:")
        return S_ADM_CHG_ZAVOD_PRICE
    
    prod = context.user_data.get('adm_zavod_prod')
    pid = prod.get('ID')
    
    # Update in Mahsulotlar_Asosiy
    result = db_update_row("Mahsulotlar_Asosiy", {'ID': pid}, {'Zavod_Narxi': price})
    
    if result:
        name = prod.get('Nomi')
        turi = prod.get('Turi', '-')
        lbl = f"{name}" if turi == '-' else f"{name} {turi}"
        
        btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
        
        await update.message.reply_text(
            f"✅ Zavod narxi yangilandi!\n\n"
            f"Mahsulot: {lbl}\n"
            f"Yangi zavod narxi: {format_number(price)} so'm",
            reply_markup=InlineKeyboardMarkup(btns)
        )
    else:
        await update.message.reply_text("❌ Xatolik yuz berdi!")
    
    context.user_data.clear()
    return ConversationHandler.END

# ═══════════════════════════════════════════════════════════
# ADMIN - ALL DISTRIBUTORS REPORT
# ═══════════════════════════════════════════════════════════

async def admin_report_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show report for all distributors"""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text("⏳ Barcha distribyutorlar hisoboti tayyorlanmoqda...")
    
    topshirish_data = db_get_all("Topshirish")
    qabul_data = db_get_all("Qabul")
    tolov_data = db_get_all("Tolov")
    
    # Group by distributor
    dist_stats = {}
    
    for t in topshirish_data:
        dist_id = str(t.get('Dist_ID', '')).strip()
        if not dist_id:
            continue
        
        if dist_id not in dist_stats:
            dist_stats[dist_id] = {
                'name': '',
                'sotuv': 0,
                'naqd': 0,
                'qarz': 0,
                'dokonlar': set()
            }
        
        dist_stats[dist_id]['sotuv'] += float(t.get('Topshirish_Miqdor', 0)) * int(t.get('Narx', 0))
        dist_stats[dist_id]['naqd'] += int(t.get('Naqd', 0))
        dist_stats[dist_id]['qarz'] += int(t.get('Qarz', 0))
        dist_stats[dist_id]['dokonlar'].add(t.get('Dokon_ID'))
    
    # Get names from Dokonlar
    shops = db_get_all("Dokonlar")
    for sid, stats in dist_stats.items():
        for shop in shops:
            if str(shop.get('Dist_ID', '')).strip() == sid:
                stats['name'] = shop.get('Dist_Name', f'Dist_{sid}')
                break
    
    if not dist_stats:
        btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
        await query.edit_message_text(
            "📊 Hali ma'lumotlar yo'q",
            reply_markup=InlineKeyboardMarkup(btns)
        )
        return
    
    # Build message
    msg = "📊 Barcha distribyutorlar hisoboti\n\n"
    
    total_sotuv = 0
    total_naqd = 0
    total_qarz = 0
    total_dokonlar = 0
    
    for dist_id, stats in sorted(dist_stats.items(), key=lambda x: x[1]['sotuv'], reverse=True):
        name = stats['name'] if stats['name'] else f"ID: {dist_id}"
        sotuv = int(stats['sotuv'])
        naqd = int(stats['naqd'])
        qarz = int(stats['qarz'])
        dokonlar = len(stats['dokonlar'])
        
        msg += f"━━━━━━━━━━━━━━━━\n"
        msg += f"👤 {name}\n"
        msg += f"💰 Sotuv: {format_number(sotuv)}\n"
        msg += f"💵 Naqd: {format_number(naqd)}\n"
        msg += f"📝 Qarz: {format_number(qarz)}\n"
        msg += f"🏪 {dokonlar} do'kon\n"
        
        total_sotuv += sotuv
        total_naqd += naqd
        total_qarz += qarz
        total_dokonlar += dokonlar
    
    msg += f"\n━━━━━━━━━━━━━━━━\n"
    msg += f"📊 JAMI:\n"
    msg += f"💰 {format_number(total_sotuv)}\n"
    msg += f"💵 {format_number(total_naqd)}\n"
    msg += f"📝 {format_number(total_qarz)}\n"
    msg += f"🏪 {total_dokonlar} do'kon"
    
    btns = [[InlineKeyboardButton("🏠 Menyu", callback_data="menu_main")]]
    
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(btns))

# ═══════════════════════════════════════════════════════════
# CALLBACK ROUTER
# ═══════════════════════════════════════════════════════════

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Route callbacks to appropriate handlers"""
    query = update.callback_query
    await query.answer()  # Always answer to avoid Telegram timeout
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
    elif data == "menu_prices":
        await prices_menu(update, context)
    elif data == "menu_admin":
        await admin_menu(update, context)
    elif data == "price_view":
        await price_view(update, context)
    elif data == "menu_delivery":
        await delivery_start(update, context)
    elif data == "menu_add_shop":
        await shop_start(update, context)
    elif data == "menu_qabul":
        await qabul_start(update, context)
    elif data == "price_default":
        await price_default_start(update, context)
    elif data == "price_custom":
        await price_custom_start(update, context)
    elif data == "order_view":
        await order_view_handler(update, context)
    elif data == "order_new":
        await order_new(update, context)

# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    """Main function"""
    logger.info("🚀 Alba Milk Bot FINAL VERSION starting...")
    
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Shop registration
    shop_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(shop_start, pattern="^menu_add_shop$")],
        states={
            S_SHOP_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_name)],
            S_SHOP_ADDR: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_addr)],
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
            S_SHOP_LOC: [MessageHandler(filters.LOCATION | filters.TEXT, shop_loc)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Factory receipt
    qabul_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(qabul_start, pattern="^menu_qabul$")],
        states={
            S_QAB_PROD: [CallbackQueryHandler(qabul_prod_cb)],
            S_QAB_AMT: [MessageHandler(filters.TEXT & ~filters.COMMAND, qabul_amt)],
            S_QAB_MORE: [CallbackQueryHandler(qabul_more_finish)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Order
    order_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(order_new, pattern="^order_new$")],
        states={
            S_ORD_SHOP: [CallbackQueryHandler(order_shop_cb)],
            S_ORD_PROD: [CallbackQueryHandler(order_prod_cb)],
            S_ORD_AMT: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_amt)],
            S_ORD_MORE: [CallbackQueryHandler(order_more_confirm)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Delivery
    delivery_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(delivery_start, pattern="^menu_delivery$")],
        states={
            S_DEL_SHOP: [CallbackQueryHandler(delivery_shop_cb)],
            S_DEL_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_input)],
            S_DEL_VOZ_YN: [CallbackQueryHandler(delivery_voz_yn)],
            S_DEL_VOZ_AMT: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_voz_amt)],
            S_DEL_IZOH_YN: [CallbackQueryHandler(delivery_izoh_yn)],
            S_DEL_IZOH_TXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_izoh_txt)],
            S_DEL_ESLATMA: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_eslatma)],
            S_DEL_TOLOV: [CallbackQueryHandler(delivery_tolov)],
            S_DEL_NAQD: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_naqd)],
            S_DEL_TAROZI: [MessageHandler(filters.PHOTO | filters.TEXT, delivery_tarozi)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Price default
    price_def_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(price_default_start, pattern="^price_default$")],
        states={
            S_PRC_SEL_PROD: [CallbackQueryHandler(price_def_prod_cb)],
            S_PRC_ENT_DEF: [MessageHandler(filters.TEXT & ~filters.COMMAND, price_def_enter)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Price custom
    price_cus_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(price_custom_start, pattern="^price_custom$")],
        states={
            S_PRC_SEL_PROD: [CallbackQueryHandler(price_cus_prod_cb)],
            S_PRC_CUS_SHOPS: [CallbackQueryHandler(price_cus_shop_cb)],
            S_PRC_CUS_ENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, price_cus_enter)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Admin - Add product
    admin_add_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_add_start, pattern="^adm_add_prod$")],
        states={
            S_ADM_ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_name)],
            S_ADM_ADD_TURI: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_turi)],
            S_ADM_ADD_BIRLIK: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_birlik)],
            S_ADM_ADD_ZAVOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_zavod)],
            S_ADM_ADD_SOTISH: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_sotish)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Admin - Change factory price
    admin_zavod_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_zavod_start, pattern="^adm_zavod$")],
        states={
            S_ADM_CHG_ZAVOD_PROD: [CallbackQueryHandler(admin_zavod_prod_cb)],
            S_ADM_CHG_ZAVOD_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_zavod_price_enter)],
        },
        fallbacks=[CommandHandler('start', start)],
    )
    
    # Add handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(shop_conv)
    app.add_handler(qabul_conv)
    app.add_handler(order_conv)
    app.add_handler(delivery_conv)
    app.add_handler(price_def_conv)
    app.add_handler(price_cus_conv)
    app.add_handler(admin_add_conv)
    app.add_handler(admin_zavod_conv)
    
    # Callback handlers
    app.add_handler(CallbackQueryHandler(generate_report, pattern="^rep_"))
    app.add_handler(CallbackQueryHandler(admin_report_all, pattern="^adm_report$"))
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Start
    logger.info("✅ Bot started successfully!")
    logger.info(f"📊 Admin IDs: {ADMIN_IDS}")
    logger.info(f"📢 Channel ID: {CHANNEL_ID}")
    logger.info(f"📝 Total functions: ~90")
    logger.info(f"📏 Total lines: 2500+")
    
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
