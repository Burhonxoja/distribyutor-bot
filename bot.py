"""Alba Milk Distribyutor Bot v5.0"""
import os, logging, json, re, base64, random
from datetime import datetime, timedelta, time as dtime
import gspread
from google.oauth2.service_account import Credentials
import google.auth.transport.requests
from telegram import (Update, InlineKeyboardMarkup, InlineKeyboardButton,
                      ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove)
from telegram.ext import (Application, CommandHandler, MessageHandler,
                          CallbackQueryHandler, ConversationHandler,
                          filters, ContextTypes)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── ENV ───────────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
CREDS_JSON  = os.environ.get("GOOGLE_CREDS_JSON", "")
SHEET_ID    = os.environ.get("SPREADSHEET_ID", "")
ADMIN_IDS   = [int(x) for x in os.environ.get("ADMIN_IDS","0").split(",") if x.strip()]
CHANNEL_ID  = os.environ.get("CHANNEL_ID", "")

# ── STATES ────────────────────────────────────────────────────────────────────
(S_LANG, S_REG_NAME, S_REG_FNAME, S_REG_PHONE, S_REG_PASS,
 S_WAIT, S_MAIN,
 S_ZAV_QTY,
 S_TOP_STORE, S_TOP_PROD, S_TOP_PHOTO, S_TOP_PAY, S_TOP_NAQD,
 S_VOZ_PROD, S_VOZ_QTY,
 S_DI_NAME, S_DI_ADDR, S_DI_MCHJ, S_DI_TEL1, S_DI_TEL2, S_DI_EGA, S_DI_PHOTO, S_DI_LOC,
 S_NARX_VAL, S_NARX_COST, S_NARX_D_VAL, S_NARX_D_COST,
 S_ZAK_FROM_QTY, S_DK_EDIT_VAL,
 S_ADM_PROD_UZ, S_ADM_PROD_RU, S_ADM_PROD_UNIT,
 S_ADM_NARX_VAL, S_ADM_NARX_COST,
 S_ADM_DK_NAME, S_ADM_DK_ADDR, S_ADM_DK_DIST,
 S_ADM_DIST_NAME, S_ADM_DIST_TG,
 S_ADM_BC, S_TOLOV_SUMMA) = range(41)

# ── SHEETS ────────────────────────────────────────────────────────────────────
HEADERS = {
    "Foydalanuvchilar": ["TG_ID","Ism","Familiya","Telefon","Rol","Til","Status","Short_ID","Sana"],
    "Mahsulotlar":      ["ID","Nomi_UZ","Nomi_RU","Birlik","Faol","Sana"],
    "Dokonlar":         ["ID","Nomi","Adres","MCHJ","Tel1","Tel2","Ega","Dist_ID","Dist_Ism","Lat","Lng","Channel_Msg_ID","Sana"],
    "Narxlar":          ["Mahsulot_ID","Mahsulot","Narx","Tannarx","Dist_ID","Dokon_ID","Sana"],
    "Mahsulotlar_Dist_Default": ["Dist_ID","Mahsulot_ID","Mahsulot","Turi","Sotish_Narxi","Sana"],
    "Mahsulotlar_Maxsus_Narx":  ["Dist_ID","Dokon_ID","Dokon_Nomi","Mahsulot_ID","Mahsulot","Turi","Sotish_Narxi","Sana"],
    "Qabul":            ["Sana","Dist_ID","Dist_Ism","Mahsulot","Miqdor","Birlik","Narx","Jami","Status","Qabul_ID"],
    "Topshirish":       ["Sana","Dist_ID","Dokon","Dokon_ID","Mahsulot","Miqdor","Birlik","Narx","Jami","Pay_Type","Naqd","Qarz","Status","Top_ID"],
    "Tolov":            ["Sana","Dist_ID","Dokon","Dokon_ID","Summa","Status","Tolov_ID"],
    "Buyurtmalar":      ["Sana","Dokon_ID","Dokon","Dist_ID","Mahsulot","Miqdor","Status","Zakaz_ID"],
    "Vozvrat":          ["Sana","Dist_ID","Dokon","Dokon_ID","Mahsulot","Miqdor","Birlik","Narx","Jami","Status","Voz_ID"],
    "Sozlamalar":       ["Kalit","Qiymat","Sana"],
}

def _sheet():
    if not CREDS_JSON: return None
    try:
        creds = Credentials.from_service_account_info(
            json.loads(CREDS_JSON),
            scopes=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(SHEET_ID)
    except Exception as e: logger.error(f"Sheet: {e}"); return None

def ws(name):
    wb = _sheet()
    if not wb: return None
    try: return wb.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        w = wb.add_worksheet(name, 3000, 25)
        if name in HEADERS: w.append_row(HEADERS[name])
        return w
    except Exception as e: logger.error(f"ws {name}: {e}"); return None

def db_get(tab):
    try: w=ws(tab); return w.get_all_records() if w else []
    except Exception as e: logger.error(f"db_get {tab}: {e}"); return []

def db_add(tab, row):
    try: w=ws(tab); w and w.append_row([str(x) for x in row])
    except Exception as e: logger.error(f"db_add {tab}: {e}")

def db_set(tab, sc, sv, uc, uv):
    try:
        w=ws(tab);
        if not w: return
        h=w.row_values(1)
        if uc not in h: return
        for i,r in enumerate(w.get_all_records()):
            if str(r.get(sc,"")).strip()==str(sv).strip():
                w.update_cell(i+2,h.index(uc)+1,str(uv)); return
    except Exception as e: logger.error(f"db_set {tab}: {e}")

def db_del(tab, sc, sv):
    try:
        w=ws(tab)
        if not w: return
        for i,r in enumerate(w.get_all_records()):
            if str(r.get(sc,"")).strip()==str(sv).strip():
                w.delete_rows(i+2); return
    except Exception as e: logger.error(f"db_del {tab}: {e}")

now_s  = lambda: datetime.now().strftime("%Y-%m-%d %H:%M")
today_s= lambda: datetime.now().strftime("%Y-%m-%d")
mk_id  = lambda p="": p+datetime.now().strftime("%m%d%H%M%S")+str(random.randint(10,99))
mk_sid = lambda: str(random.randint(100000,999999))

# ── MAHSULOTLAR ───────────────────────────────────────────────────────────────
DEF_PRODS = [
    {"id":1,"uz":"Tvorog","ru":"Tvorog","unit":"kg"},
    {"id":2,"uz":"Sut","ru":"Sut","unit":"litr"},
    {"id":3,"uz":"Qatiq","ru":"Qatiq","unit":"kg"},
    {"id":4,"uz":"Brinza","ru":"Brinza","unit":"kg"},
    {"id":5,"uz":"Qaymoq 0.4","ru":"Qaymoq 0.4","unit":"dona"},
    {"id":6,"uz":"Qaymoq 0.2","ru":"Qaymoq 0.2","unit":"dona"},
    {"id":7,"uz":"Suzma 0.5","ru":"Suzma 0.5","unit":"kg"},
    {"id":8,"uz":"Qurt","ru":"Qurt","unit":"dona"},
    {"id":9,"uz":"Tosh qurt","ru":"Tosh qurt","unit":"dona"},
]

def get_prods():
    try:
        r=db_get("Mahsulotlar")
        if r: return [{"id":int(x.get("ID",0)),"uz":x.get("Nomi_UZ",""),"ru":x.get("Nomi_RU",""),"unit":x.get("Birlik","kg")} for x in r if str(x.get("Faol","1"))=="1"]
    except: pass
    return DEF_PRODS

is_brinza = lambda n: "brinza" in str(n).lower()

def fmtq(qty, unit, name="", top=False):
    """Brinza: top=True→kg, False→dona. Dona→int. Kg/litr→ortiqcha nol yo'q"""
    def fk(v,u):
        if v==int(v): return f"{int(v)} {u}"
        return f"{str(round(v,3)).rstrip('0').rstrip('.')} {u}"
    if is_brinza(name): return fk(float(qty),"kg") if top else f"{int(round(qty))} dona"
    if unit=="dona": return f"{int(round(qty))} dona"
    if unit=="kg":   return fk(float(qty),"kg")
    if unit=="litr": return fk(float(qty),"litr")
    v=float(qty); return f"{int(v)} {unit}" if v==int(v) else f"{round(v,3)} {unit}"

# ── FOYDALANUVCHI ─────────────────────────────────────────────────────────────
def get_user(uid):
    for r in db_get("Foydalanuvchilar"):
        if str(r.get("TG_ID","")).strip()==str(uid).strip(): return r
    return None

def approved(uid):
    if int(uid) in ADMIN_IDS: return True
    u=get_user(uid); return u and str(u.get("Status","")).lower() in ["tasdiqlangan","1"]

def get_sid(uid):
    if int(uid) in ADMIN_IDS: return "ADMIN"
    u=get_user(uid); return u.get("Short_ID","?") if u else "?"

def la(ctx): return ctx.user_data.get("lang","uz")

# ── NARX ──────────────────────────────────────────────────────────────────────
def get_price(pid, did=None, dokon_id=None):
    recs=db_get("Narxlar")
    def match(r,d,k): return str(r.get("Mahsulot_ID",""))==str(pid) and str(r.get("Dist_ID",""))==str(d or "") and str(r.get("Dokon_ID",""))==str(k or "")
    if did and dokon_id:
        for r in recs:
            if match(r,did,dokon_id): return float(r.get("Narx",0) or 0), float(r.get("Tannarx",0) or 0)
    if did:
        for r in recs:
            if match(r,did,""): return float(r.get("Narx",0) or 0), float(r.get("Tannarx",0) or 0)
    for r in recs:
        if match(r,"",""): return float(r.get("Narx",0) or 0), float(r.get("Tannarx",0) or 0)
    return 0.0, 0.0

def set_price(pid, pname, price, cost, did="", dokon_id=""):
    w=ws("Narxlar")
    if not w: return
    for i,r in enumerate(w.get_all_records()):
        if str(r.get("Mahsulot_ID",""))==str(pid) and str(r.get("Dist_ID",""))==str(did) and str(r.get("Dokon_ID",""))==str(dokon_id):
            w.update(f"A{i+2}:G{i+2}",[[str(pid),pname,str(price),str(cost),str(did),str(dokon_id),now_s()]]); break
    else:
        w.append_row([str(pid),pname,str(price),str(cost),str(did),str(dokon_id),now_s()])
    # Log to audit sheets
    if did and not dokon_id:
        _log_dist_default(did, pid, pname, price)
    elif did and dokon_id:
        _log_maxsus_narx(did, dokon_id, pid, pname, price)

def _log_dist_default(did, pid, pname, price):
    """Log distributor default price — keep latest per (Dist_ID, Mahsulot_ID)"""
    w=ws("Mahsulotlar_Dist_Default")
    if not w: return
    for i,r in enumerate(w.get_all_records()):
        if str(r.get("Dist_ID",""))==str(did) and str(r.get("Mahsulot_ID",""))==str(pid):
            w.update(f"A{i+2}:F{i+2}",[[str(did),str(pid),pname,"default",str(price),now_s()]]); return
    w.append_row([str(did),str(pid),pname,"default",str(price),now_s()])

def _log_maxsus_narx(did, dokon_id, pid, pname, price):
    """Log shop-specific price — keep latest per (Dist_ID, Dokon_ID, Mahsulot_ID)"""
    w=ws("Mahsulotlar_Maxsus_Narx")
    if not w: return
    stores=db_get("Dokonlar")
    dokon_nomi=next((s.get("Nomi","") for s in stores if str(s.get("ID",""))==str(dokon_id)),"")
    for i,r in enumerate(w.get_all_records()):
        if str(r.get("Dist_ID",""))==str(did) and str(r.get("Dokon_ID",""))==str(dokon_id) and str(r.get("Mahsulot_ID",""))==str(pid):
            w.update(f"A{i+2}:H{i+2}",[[str(did),str(dokon_id),dokon_nomi,str(pid),pname,"maxsus",str(price),now_s()]]); return
    w.append_row([str(did),str(dokon_id),dokon_nomi,str(pid),pname,"maxsus",str(price),now_s()])

# ── DO'KON & QARZ ────────────────────────────────────────────────────────────
def get_stores(did=None):
    r=db_get("Dokonlar")
    return [x for x in r if str(x.get("Dist_ID","")).strip()==str(did).strip()] if did else r

def get_debt(dokon_id):
    """Qarz = Topshirish.Qarz - To'lov.Summa - Vozvrat.Jami (hammasi tasdiqlangan)"""
    dk=str(dokon_id)
    tops  = db_get("Topshirish")
    tolovs= db_get("Tolov")
    vozs  = db_get("Vozvrat")
    qarz  = sum(float(r.get("Qarz",0) or 0) for r in tops   if str(r.get("Dokon_ID",""))==dk and r.get("Status","")=="tasdiqlangan")
    paid  = sum(float(r.get("Summa",0) or 0) for r in tolovs if str(r.get("Dokon_ID",""))==dk and r.get("Status","")=="tasdiqlangan")
    vozret= sum(float(r.get("Jami",0) or 0) for r in vozs    if str(r.get("Dokon_ID",""))==dk and r.get("Status","")=="tasdiqlangan")
    return max(0.0, qarz - paid - vozret)

def get_ombor(uid):
    """Qabul(tasdiqlangan+kutilmoqda) - Topshirish(tasdiqlangan) + Vozvrat(tasdiqlangan)"""
    uid=str(uid); st={}
    for r in db_get("Qabul"):
        if str(r.get("Dist_ID",""))==uid and r.get("Status","") in ("tasdiqlangan","kutilmoqda"):
            k=r.get("Mahsulot",""); st[k]=st.get(k,0)+float(r.get("Miqdor",0) or 0)
    for r in db_get("Topshirish"):
        if str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan":
            k=r.get("Mahsulot",""); st[k]=st.get(k,0)-float(r.get("Miqdor",0) or 0)
    for r in db_get("Vozvrat"):
        if str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan":
            k=r.get("Mahsulot",""); st[k]=st.get(k,0)+float(r.get("Miqdor",0) or 0)
    return {k:v for k,v in st.items() if v>0.001}

def parse_num(t): 
    try: return float(str(t).strip().replace(" ","").replace(",","."))
    except: return 0.0

def parse_w(t):
    v=parse_num(t); return round(v/1000,3) if v>=100 else round(v,3)

def parse_money(t):
    t=str(t).strip().replace(" ","")
    if re.match(r'^\d+[.,]\d{3}$',t): t=re.sub(r'[.,]','',t)
    else: t=t.replace(",",".")
    try: return float(t)
    except: return 0.0

def clean_phone(t): return re.sub(r'[^\d+]','',str(t).strip())

# ── OCR ───────────────────────────────────────────────────────────────────────
async def ocr(img_bytes):
    try:
        import httpx
        creds=Credentials.from_service_account_info(json.loads(CREDS_JSON),scopes=["https://www.googleapis.com/auth/cloud-vision"])
        creds.refresh(google.auth.transport.requests.Request())
        b64=base64.b64encode(img_bytes).decode()
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.post("https://vision.googleapis.com/v1/images:annotate",
                headers={"Authorization":f"Bearer {creds.token}"},
                json={"requests":[{"image":{"content":b64},"features":[{"type":"TEXT_DETECTION"}]}]})
            return r.json()["responses"][0].get("fullTextAnnotation",{}).get("text","").strip()
    except Exception as e: logger.error(f"OCR: {e}"); return ""

def parse_scale(txt):
    nums=re.findall(r'\d+',str(txt))
    if not nums: return 0.0
    v=int(nums[0]); return round(v/1000,3) if v>=100 else float(v)

# ── SOZLAMALAR ────────────────────────────────────────────────────────────────
def get_set(k,d="1"):
    for r in db_get("Sozlamalar"):
        if r.get("Kalit","")==k: return str(r.get("Qiymat",d))
    return d

def put_set(k,v):
    w=ws("Sozlamalar")
    if not w: return
    for i,r in enumerate(w.get_all_records()):
        if r.get("Kalit","")==k: w.update_cell(i+2,2,str(v)); w.update_cell(i+2,3,now_s()); return
    w.append_row([k,str(v),now_s()])

# ── MOTIVATSIYA ───────────────────────────────────────────────────────────────
MOTIV=[
    "🔥 Tovar qabul qilindi! Endi do'konlarga chiqish vaqti — har bir soat pul!",
    "💪 Ajoyib! Omboringiz to'ldi. Tezroq tarqatsangiz, tezroq foyda!",
    "🚀 Professional distribyutor: oldi — darhol yetkazdi. Siz shundasiz!",
    "⚡ Har kg tovar — bu daromad! Do'konlar sizni kutmoqda, yo'lga chiqing!",
    "🏆 Eng yaxshi distribyutor eng tez ishlaydi. Bugun rekord qo'ying!",
    "📈 Ko'proq do'kon = ko'proq sotuv = ko'proq foyda. Formula oddiy!",
    "💡 Aqlli distribyutor: bugun oldi — bugun tarqatdi. Vaqt yo'qotmang!",
    "🎯 Maqsad: bugun barchani aylanib chiqish. Do'konlar kutmoqda!",
    "🦁 Kuchli distribyutor omborni to'ldiradi, undan ham tez bo'shatadi!",
    "🌟 Har bir yetkazib berish — bu foyda va obro'. Davom eting!",
    "🔑 Muvaffaqiyat = doimiy harakat. Omboringiz to'ldi, ish boshlandi!",
    "📦 Tovar qabul = boshlang'ich. Yetkazib berish = asosiy ish. Olg'a!",
    "💰 Har bir yetkazish — bu sizning cho'ntagingizga pul. Ko'proq harakat!",
    "🏅 Bugun qancha do'kon aylansangiz — shuncha foyda. Maksimalga chiqing!",
    "🎖️ Eng tez distribyutor = eng ko'p mijoz. Raqobatdan oldinda boring!",
    "⚙️ Sistem: qabul → yetkazish → pul. Davr aylanmoqda, to'xtatmang!",
    "🌍 Yangi do'konlar, yangi buyurtmalar — ularni qo'lga kiriting bugun!",
    "🚀 Bugungi tovar = ertangi foyda. Kechiktirmang — harakatga!",
    "💥 Energiya baland, tovar qo'lda — natija faqat sizga bog'liq!",
    "🎯 Tez qabul + tez yetkazish = ko'p daromad. Bu sizning formulangiz!",
    "🏃 Vaqt = pul! Tovar keldi, endi do'konlarga olib boring!",
    "🌱 Har yaxshi xizmat — bu doimiy mijoz. Bugun sifatli ishlang!",
    "💎 Sifat + tezlik = obro'. Obro' = ko'proq zakaz. Ishlang!",
    "🔥 Omboringiz to'ldi! Bu mas'uliyat — do'konlar kutmoqda!",
    "📊 Har yetkazib berish statistikangizni yaxshilaydi. Davom eting!",
    "⭐ O'z vaqtida, sifatli — bu sizning standartingiz. Saqlab boring!",
    "🎪 Bozor sizniki — egallab oling! Bugun tovaringizni tarqating!",
    "💡 Bir kunda ko'proq do'kon = ko'proq sotish = ko'proq pul!",
    "🌊 Tovar oqimi to'xtamassin! Qabul-yetkazish ritmini saqlang!",
    "🦅 Baland ko'tariling! Har qabul — yangi cho'qqiga qadam!",
    "🔑 Bugungi mehnat — ertangi muvaffaqiyat. Boshlang!",
    "⚡ Tezkor distribyutor! Tovar keldi — harakatga!",
    "🎯 Maqsad: ombordan hamma tovarni do'konlarga yetkazish. Yo'lga!",
    "🏆 Champion distribyutor: oladi va tezda tarqatadi. Siz champion!",
    "💰 Har soat pul! Tovar keldi, vaqt yo'qotmang!",
    "🌟 Ombor — kapital. Kapital aylanishi = foyda. To'xtatmang!",
    "🔥 Kuch, tezlik, tovar — faqat harakat qoldi!",
    "📦 50% bajarildi (qabul). Qolgan 50% — yetkazish. Oldinga!",
    "💪 Har kg yetkazilgan — bu mehnatning meyvasi. Ko'proq!",
    "🚀 Distribyutor raketa kabi: tez va to'g'ri nishonga. Siz shundasiz!",
    "🌍 Katta distribyutorlar katta harakat qiladi. Bugun katta ish!",
    "⚙️ Siz biznes mashinasining eng muhim qismisiz!",
    "🎖️ Har muvaffaqiyatli yetkazish reputatsiyangizni mustahkamlaydi!",
    "💡 Strategiya: ko'proq do'kon → ko'proq zakaz → ko'proq foyda!",
    "🏅 Professional! Qabul — bu siz. Yetkazish — ham siz!",
    "🔥 Ombor boy — do'konlarga xabar bering, ular kutmoqda!",
    "⭐ Samaradorlik: kam vaqtda ko'p do'konga yetkazish. Bu sizning kuchingiz!",
    "🌱 Yaxshi xizmat = yangi imkoniyat. Bugun yaxshi ish!",
    "🎯 Distribyutor shiori: oldim — tarqatdim — topdim!",
    "💰 Bugun qabul = ertangi foyda. Tez harakat = ko'p foyda!",
]
get_motiv=lambda: random.choice(MOTIV)

# ── KEYBOARD BUILDERS ─────────────────────────────────────────────────────────
def ikr(*rows):
    return InlineKeyboardMarkup([[InlineKeyboardButton(b[0],callback_data=b[1]) for b in row] for row in rows])

def ik1(*btns):
    return InlineKeyboardMarkup([[InlineKeyboardButton(b[0],callback_data=b[1])] for b in btns])

def back(cb="m:main"): return ik1(("🔙 Orqaga",cb))

def main_kb(uid):
    rows=[
        [("📥 Zavod qabul","m:zav"),("📋 Buyurtmalar","m:buy")],
        [("🚚 Topshirish","m:top"),("💵 To'lov","m:tol")],
        [("📊 Kunlik","m:nat"),("📦 Ombor","m:omb")],
        [("🗺 Marshrut","m:mar"),("📈 Hisobot","m:his")],
        [("🏪 Do'konlarim","m:dok"),("💰 Narxlarim","m:nar")],
    ]
    if int(uid) in ADMIN_IDS: rows.append([("⚙️ Admin","m:adm")])
    rows.append([("🔄 /start","m:start")])
    return InlineKeyboardMarkup([[InlineKeyboardButton(b[0],callback_data=b[1]) for b in row] for row in rows])

def prod_kb(prods, la_, pfx, back_cb="m:main"):
    rows=[]
    for i in range(0,len(prods),2):
        row=[(prods[i][la_],f"{pfx}:{prods[i]['id']}")]
        if i+1<len(prods): row.append((prods[i+1][la_],f"{pfx}:{prods[i+1]['id']}"))
        rows.append(row)
    rows.append([("🔙 Orqaga",back_cb)])
    return InlineKeyboardMarkup([[InlineKeyboardButton(b[0],callback_data=b[1]) for b in r] for r in rows])

def store_kb(stores, pfx, back_cb="m:main"):
    rows=[[( s.get("Nomi",""),f"{pfx}:{s.get('ID','')}")] for s in stores]
    rows.append([("🔙 Orqaga",back_cb)])
    return InlineKeyboardMarkup([[InlineKeyboardButton(b[0],callback_data=b[1]) for b in r] for r in rows])

def phone_kb(): return ReplyKeyboardMarkup([[KeyboardButton("📱 Telefon yuborish",request_contact=True)]],resize_keyboard=True)
def loc_kb():   return ReplyKeyboardMarkup([[KeyboardButton("📍 Lokatsiya yuborish",request_location=True)],["⏭ O'tkazib yuborish"]],resize_keyboard=True)

async def send_main(upd_or_uid, ctx, edit=False):
    uid=upd_or_uid.effective_user.id if hasattr(upd_or_uid,'effective_user') else upd_or_uid
    sid=get_sid(uid); l=ctx.user_data.get("lang","uz")
    txt=f"📋 <b>Asosiy menyu</b>\n🔑 ID: <b>{sid}</b>"
    kb=main_kb(uid)
    if edit and hasattr(upd_or_uid,'callback_query') and upd_or_uid.callback_query:
        try: await upd_or_uid.callback_query.edit_message_text(txt,reply_markup=kb,parse_mode="HTML"); return
        except: pass
    await ctx.bot.send_message(uid,txt,reply_markup=kb,parse_mode="HTML")

async def ans(q,t=""): 
    try: await q.answer(t)
    except: pass

async def esend(upd,ctx,txt,kb=None):
    if upd.callback_query:
        await ans(upd.callback_query)
        try: await upd.callback_query.edit_message_text(txt,reply_markup=kb,parse_mode="HTML"); return
        except: pass
    elif upd.message: await upd.message.reply_text(txt,reply_markup=kb,parse_mode="HTML")


# ── START & RO'YXAT ───────────────────────────────────────────────────────────
async def start(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    uid=upd.effective_user.id
    await upd.message.reply_text("...",reply_markup=ReplyKeyboardRemove())
    u=get_user(uid)
    if uid in ADMIN_IDS:
        ctx.user_data.setdefault("lang","uz")
        await send_main(upd,ctx); return S_MAIN
    if u:
        ctx.user_data["lang"]=u.get("Til","uz")
        st=str(u.get("Status","")).lower()
        if st in ["tasdiqlangan","1"]: await send_main(upd,ctx); return S_MAIN
        if st=="rad_etildi":
            await upd.message.reply_text("❌ Rad etildi. Ism kiriting:"); return S_REG_NAME
        await upd.message.reply_text("⏳ Tasdiqlanmagan.",reply_markup=ik1(("📤 Qayta yuborish","resend"))); return S_WAIT
    await upd.message.reply_text("Tilni tanlang:",reply_markup=ikr([("🇺🇿 O'zbek","lang:uz"),("🇷🇺 Русский","lang:ru")])); return S_LANG

async def lang_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q)
    ctx.user_data["lang"]=q.data.split(":")[1]
    await q.edit_message_text("Ismingizni kiriting:"); return S_REG_NAME

async def reg_name(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    ctx.user_data["rn"]=upd.message.text.strip()
    await upd.message.reply_text("Familiya:"); return S_REG_FNAME

async def reg_fname(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    ctx.user_data["rf"]=upd.message.text.strip()
    await upd.message.reply_text("📱 Telefon:",reply_markup=phone_kb()); return S_REG_PHONE

async def reg_phone(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    l=la(ctx)
    phone=upd.message.contact.phone_number if upd.message.contact else clean_phone(upd.message.text)
    if len(phone.replace("+",""))<7:
        await upd.message.reply_text("❌ Noto'g'ri!",reply_markup=phone_kb()); return S_REG_PHONE
    ctx.user_data["rph"]=phone
    await upd.message.reply_text("📷 Passport (yoki o'tkazib yuborish):",
        reply_markup=ik1(("⏭ O'tkazib yuborish","skip_pass")),
        reply_markup_override=ReplyKeyboardRemove()); return S_REG_PASS

async def reg_pass(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    uid=upd.effective_user.id; l=la(ctx)
    if upd.callback_query: await ans(upd.callback_query); passport="otkazildi"; photo=None
    else:
        passport="rasm_bor" if upd.message.photo else (upd.message.text or "otkazildi")
        photo=upd.message.photo[-1].file_id if upd.message.photo else None
    name=ctx.user_data.get("rn",""); fname=ctx.user_data.get("rf",""); phone=ctx.user_data.get("rph","")
    sid=mk_sid(); full=f"{name} {fname}".strip()
    db_del("Foydalanuvchilar","TG_ID",str(uid))
    db_add("Foydalanuvchilar",[str(uid),name,fname,phone,"distributor",l,"kutilmoqda",sid,now_s()])
    for adm in ADMIN_IDS:
        try:
            await ctx.bot.send_message(adm,f"👤 <b>YANGI DISTRIBYUTOR</b>\n{full} | {phone}\nTG: {uid} | ID: <b>{sid}</b>\n✅ /approve_{uid}\n❌ /reject_{uid}",parse_mode="HTML")
            if photo: await ctx.bot.send_photo(adm,photo,caption=f"Passport: {full}")
        except: pass
    await ctx.bot.send_message(uid,f"✅ Ro'yxatdan o'tdingiz!\n🔑 ID: <b>{sid}</b>\n\nAdmin tasdiqlashini kuting...",parse_mode="HTML",reply_markup=ik1(("📤 Qayta yuborish","resend")))
    return S_WAIT

async def wait_h(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    uid=upd.effective_user.id; l=la(ctx)
    if upd.callback_query:
        await ans(upd.callback_query)
        if upd.callback_query.data=="resend":
            u=get_user(uid)
            if u:
                for adm in ADMIN_IDS:
                    try: await ctx.bot.send_message(adm,f"👤 QAYTA: {u.get('Ism','')} | ID:{u.get('Short_ID','')}\n✅ /approve_{uid}\n❌ /reject_{uid}")
                    except: pass
            await upd.callback_query.edit_message_text("✅ Yuborildi.",reply_markup=ik1(("📤 Yana","resend"))); return S_WAIT
    if approved(uid): await send_main(upd,ctx); return S_MAIN
    return S_WAIT

async def approve_cmd(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m=re.search(r'/approve_(\d+)',upd.message.text or "")
    if not m: return
    t=m.group(1); db_set("Foydalanuvchilar","TG_ID",t,"Status","tasdiqlangan")
    await upd.message.reply_text(f"✅ {t}")
    try:
        u=get_user(t); sid=u.get("Short_ID","?") if u else "?"
        await ctx.bot.send_message(int(t),f"✅ Tasdiqlandi! ID: <b>{sid}</b>\n/start bosing.",parse_mode="HTML")
    except: pass

async def reject_cmd(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m=re.search(r'/reject_(\d+)',upd.message.text or "")
    if not m: return
    t=m.group(1); db_set("Foydalanuvchilar","TG_ID",t,"Status","rad_etildi")
    await upd.message.reply_text(f"❌ {t}")
    try: await ctx.bot.send_message(int(t),"❌ Hisobingiz rad etildi.")
    except: pass

# ── ASOSIY MENYU ──────────────────────────────────────────────────────────────
async def main_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q); uid=upd.effective_user.id
    if not approved(uid): return S_WAIT
    d=q.data
    if d=="m:start": await send_main(upd,ctx,edit=True); return S_MAIN
    if d=="m:main":  await send_main(upd,ctx,edit=True); return S_MAIN
    if d=="m:zav":   return await zav_start(upd,ctx)
    if d=="m:top":   return await top_start(upd,ctx)
    if d=="m:buy":   return await buy_show(upd,ctx)
    if d=="m:tol":   return await tol_show(upd,ctx)
    if d=="m:nat":   return await nat_show(upd,ctx)
    if d=="m:omb":   return await omb_show(upd,ctx)
    if d=="m:mar":   return await mar_show(upd,ctx)
    if d=="m:his":   return await his_menu(upd,ctx)
    if d=="m:dok":   return await dok_show(upd,ctx)
    if d=="m:nar":   return await nar_start(upd,ctx)
    if d=="m:adm":   return await adm_menu(upd,ctx)
    return S_MAIN

# ── ZAVOD QABUL ───────────────────────────────────────────────────────────────
async def zav_start(upd,ctx):
    l=la(ctx); prods=get_prods()
    await esend(upd,ctx,"📥 <b>Zavod qabul</b>\nMahsulotni tanlang:",prod_kb(prods,l,"zav")); return S_MAIN

async def zav_prod_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q); l=la(ctx); uid=str(upd.effective_user.id)
    pid=int(q.data.split(":")[1]); p=next((x for x in get_prods() if x["id"]==pid),None)
    if not p: return S_MAIN
    ctx.user_data["zp"]=p; price,_=get_price(pid,uid)
    if not price: price,_=get_price(pid)
    ctx.user_data["zprice"]=price
    lbl="dona (butun son)" if is_brinza(p[l]) else p["unit"]
    await q.edit_message_text(f"📥 <b>{p[l]}</b>\n💰 Narx: {price:,.0f}/{p['unit']}\n\nMiqdor kiriting ({lbl}):",reply_markup=back("m:zav"),parse_mode="HTML")
    return S_ZAV_QTY

async def zav_qty(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    l=la(ctx); uid=str(upd.effective_user.id); p=ctx.user_data.get("zp",{})
    price=ctx.user_data.get("zprice",0)
    if is_brinza(p.get(l,"")):
        try: qty=int(float(upd.message.text.strip().replace(",",".")))
        except: qty=0
        if qty<=0: await upd.message.reply_text("Butun son kiriting (dona):"); return S_ZAV_QTY
    else:
        qty=parse_w(upd.message.text)
        if qty<=0: await upd.message.reply_text("❌ Noto'g'ri:"); return S_ZAV_QTY
    jami=qty*price; qid=make_id=mk_id("Q")
    u=get_user(uid); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else uid
    sid=u.get("Short_ID","?") if u else "ADMIN"
    db_add("Qabul",[now_s(),uid,dn,p[l],qty,p["unit"],price,jami,"kutilmoqda",qid])
    for adm in ADMIN_IDS:
        try: await ctx.bot.send_message(adm,f"⏳ <b>ZAVOD SO'ROVI</b>\n{dn} (ID:{sid})\n{p[l]}: {fmtq(qty,p['unit'],p[l],False)}\nJami: {jami:,.0f}\n✅ /zok_{qid}\n❌ /zrad_{qid}",parse_mode="HTML")
        except: pass
    await upd.message.reply_text("⏳ So'rov yuborildi. Kuting...",reply_markup=main_kb(uid))
    return S_MAIN

async def zok_cmd(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m=re.search(r'/zok_(\w+)',upd.message.text or "")
    if not m: return
    qid=m.group(1); db_set("Qabul","Qabul_ID",qid,"Status","tasdiqlangan")
    await upd.message.reply_text(f"✅ {qid}")
    for r in db_get("Qabul"):
        if r.get("Qabul_ID","")==qid:
            try:
                did=str(r.get("Dist_ID",""))
                await ctx.bot.send_message(int(did),"✅ <b>Tovar tasdiqlandi!</b> Omboringiz yangilandi.",parse_mode="HTML")
                await ctx.bot.send_message(int(did),get_motiv())
            except: pass
            break

async def zrad_cmd(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    if upd.effective_user.id not in ADMIN_IDS: return
    m=re.search(r'/zrad_(\w+)',upd.message.text or "")
    if not m: return
    qid=m.group(1); db_set("Qabul","Qabul_ID",qid,"Status","rad_etildi")
    await upd.message.reply_text(f"❌ {qid}")
    for r in db_get("Qabul"):
        if r.get("Qabul_ID","")==qid:
            try: await ctx.bot.send_message(int(r.get("Dist_ID",0)),"❌ Zavod so'rovi rad etildi.")
            except: pass
            break


# ── TOPSHIRISH ────────────────────────────────────────────────────────────────
async def top_start(upd,ctx):
    l=la(ctx); uid=str(upd.effective_user.id)
    stores=get_stores(uid)
    if not stores: await esend(upd,ctx,"⚠️ Do'konlar yo'q.",back()); return S_MAIN
    if not get_ombor(uid): await esend(upd,ctx,"❌ Ombor bo'sh! Avval zavoddan qabul qiling.",back()); return S_MAIN
    ctx.user_data["stores"]=stores
    await esend(upd,ctx,"🚚 <b>Topshirish</b>\nDo'konni tanlang:",store_kb(stores,"top_s")); return S_MAIN

async def top_s_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q); l=la(ctx); uid=str(upd.effective_user.id)
    sid=q.data.split(":")[1]
    stores=ctx.user_data.get("stores",get_stores(uid))
    s=next((x for x in stores if str(x.get("ID",""))==sid),None)
    if not s: return S_MAIN
    ctx.user_data["ts"]=s
    orders=[r for r in db_get("Buyurtmalar") if str(r.get("Dokon_ID",""))==sid and r.get("Status","")=="Yangi"]
    ctx.user_data["torders"]=orders
    debt=get_debt(sid)
    ombor=get_ombor(uid); prods=get_prods()
    omborli=[p for p in prods if ombor.get(p["uz"],ombor.get(p["ru"],0))>0.001]
    if not omborli: await q.edit_message_text("❌ Ombor bo'sh!",reply_markup=back()); return S_MAIN
    msg=f"🏪 <b>{s.get('Nomi','')}</b>\n"
    if debt>0: msg+=f"💸 Qarz: {debt:,.0f}\n"
    if orders:
        msg+="\n📋 <b>Aktiv zakazlar:</b>\n"
        for r in orders: msg+=f"  • {r.get('Mahsulot','')}: {fmtq(float(r.get('Miqdor',0) or 0),'dona',r.get('Mahsulot',''),False)}\n"
    msg+="\nMahsulotni tanlang:"
    rows=[]
    for i in range(0,len(omborli),2):
        p=omborli[i]; ov=ombor.get(p["uz"],ombor.get(p["ru"],0))
        row=[(f"{p[l]} ({fmtq(ov,p['unit'],p[l],False)})",f"top_p:{p['id']}")]
        if i+1<len(omborli):
            p2=omborli[i+1]; ov2=ombor.get(p2["uz"],ombor.get(p2["ru"],0))
            row.append((f"{p2[l]} ({fmtq(ov2,p2['unit'],p2[l],False)})",f"top_p:{p2['id']}"))
        rows.append(row)
    rows.append([("🔙 Orqaga","m:top")]); 
    await q.edit_message_text(msg,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(b[0],callback_data=b[1]) for b in r] for r in rows]),parse_mode="HTML")
    return S_MAIN

async def top_p_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q); l=la(ctx)
    pid=int(q.data.split(":")[1]); p=next((x for x in get_prods() if x["id"]==pid),None)
    if not p: return S_MAIN
    ctx.user_data["tp"]=p; s=ctx.user_data.get("ts",{})
    brinza=is_brinza(p[l])
    hint="Tarozi rasmini yuboring (MAJBURIY) yoki og'irlikni kg da yozing:" if brinza else "Miqdorni kiriting:"
    await q.edit_message_text(f"🚚 <b>{p[l]}</b>\n\n{hint}",reply_markup=back(f"top_s:{s.get('ID','')}"),parse_mode="HTML")
    return S_TOP_PHOTO

async def top_photo(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    l=la(ctx); uid=str(upd.effective_user.id); p=ctx.user_data.get("tp",{})
    brinza=is_brinza(p.get(l,""))
    if upd.message.photo:
        await upd.message.reply_text("⏳ O'qilmoqda...")
        file=await ctx.bot.get_file(upd.message.photo[-1].file_id)
        img=bytes(await file.download_as_bytearray())
        raw=await ocr(img); w=parse_scale(raw)
        ctx.user_data["scale_photo"]=upd.message.photo[-1].file_id
        if w>0:
            ctx.user_data["_ocr"]=w
            await upd.message.reply_text(f"📸 O'qildi: {fmtq(w,'kg',p[l],True)}\nTo'g'rimi?",
                reply_markup=ikr([("✅ Ha",f"ocr_yes"),("✏️ Boshqa","ocr_no")])); return S_TOP_PHOTO
        await upd.message.reply_text("📷 Rasm saqlandi. Og'irlikni kg da kiriting:"); return S_TOP_PHOTO
    t=upd.message.text or ""
    if brinza and "scale_photo" not in ctx.user_data and "_ocr" not in ctx.user_data:
        await upd.message.reply_text("🧀 Brinza uchun tarozi rasmi MAJBURIY!"); return S_TOP_PHOTO
    qty=parse_w(t)
    if qty<=0: await upd.message.reply_text("❌ Noto'g'ri:"); return S_TOP_PHOTO
    ctx.user_data["tqty"]=qty
    s=ctx.user_data.get("ts",{})
    await upd.message.reply_text(
        f"💳 To'lov usuli?\n{p[l]}: {fmtq(qty,p['unit'],p[l],True)}",
        reply_markup=ikr([("💵 Naqd","pay:naqd")],[("📝 Realizatsiya","pay:real")]))
    return S_TOP_PAY

async def top_ocr_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q); l=la(ctx); p=ctx.user_data.get("tp",{})
    if q.data=="ocr_yes":
        qty=ctx.user_data.pop("_ocr",0); ctx.user_data["tqty"]=qty
        s=ctx.user_data.get("ts",{})
        await q.edit_message_text(f"💳 To'lov usuli?\n{p[l]}: {fmtq(qty,p['unit'],p[l],True)}",
            reply_markup=ikr([("💵 Naqd","pay:naqd")],[("📝 Realizatsiya","pay:real")]))
        return S_TOP_PAY
    ctx.user_data.pop("_ocr",None)
    await q.edit_message_text("Og'irlikni kg da kiriting:"); return S_TOP_PHOTO

async def top_pay_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q); l=la(ctx)
    d=q.data
    if d=="pay:real":
        ctx.user_data["pay_type"]="realizatsiya"; ctx.user_data["tnaqd"]=0.0
        await q.edit_message_text("📦 Vozvrat bormi?",reply_markup=ikr([("✅ Ha","voz:yes")],[("❌ Yo'q","voz:no")])); return S_TOP_PAY
    if d=="pay:naqd":
        ctx.user_data["pay_type"]="naqd"
        await q.edit_message_text("💵 Naqd summani kiriting (0 = to'liq qarz):"); return S_TOP_NAQD
    if d=="voz:no":
        ctx.user_data["vjami"]=0.0; return await save_top(upd,ctx)
    if d=="voz:yes":
        s=ctx.user_data.get("ts",{}); sid=str(s.get("ID",""))
        tops=[r for r in db_get("Topshirish") if str(r.get("Dokon_ID",""))==sid and r.get("Status","")=="tasdiqlangan"]
        pset={r.get("Mahsulot",""):1 for r in tops}
        if not pset: ctx.user_data["vjami"]=0.0; return await save_top(upd,ctx)
        ctx.user_data["vprods"]=list(pset.keys())
        rows=[[(pn,f"voz_p:{pn}")] for pn in pset]+[[("🔙 Orqaga","voz:no")]]
        await q.edit_message_text("Qaysi mahsulot qaytarildi?",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(b[0],callback_data=b[1]) for b in r] for r in rows]))
        return S_VOZ_PROD
    return S_TOP_PAY

async def top_naqd(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    summa=parse_money(upd.message.text)
    if summa<0: await upd.message.reply_text("❌ Noto'g'ri:"); return S_TOP_NAQD
    ctx.user_data["tnaqd"]=summa
    await upd.message.reply_text("📦 Vozvrat bormi?",reply_markup=ikr([("✅ Ha","voz:yes")],[("❌ Yo'q","voz:no")])); return S_TOP_PAY

async def voz_prod_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q); l=la(ctx)
    pn=q.data.split(":",1)[1]; ctx.user_data["vprod"]=pn
    brinza=is_brinza(pn)
    await q.edit_message_text(f"<b>{pn}</b>\n{'Nechta? (dona)' if brinza else 'Necha kg?'}",parse_mode="HTML"); return S_VOZ_QTY

async def voz_qty(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    l=la(ctx); uid=str(upd.effective_user.id)
    pn=ctx.user_data.get("vprod",""); s=ctx.user_data.get("ts",{}); sid=str(s.get("ID",""))
    brinza=is_brinza(pn)
    if brinza:
        try: qty=int(float(upd.message.text.strip().replace(",",".")))
        except: qty=0
        if qty<=0: await upd.message.reply_text("Butun son:"); return S_VOZ_QTY
        bir="dona"
    else:
        qty=parse_w(upd.message.text)
        if qty<=0: await upd.message.reply_text("❌:"); return S_VOZ_QTY
        bir="kg"
    p_obj=next((p for p in get_prods() if p["uz"]==pn or p["ru"]==pn),None)
    pid=p_obj["id"] if p_obj else 0
    price,_=get_price(pid,uid,sid)
    if not price: price,_=get_price(pid,uid)
    if not price: price,_=get_price(pid)
    jami=qty*price; vid=mk_id("VOZ")
    db_add("Vozvrat",[now_s(),uid,s.get("Nomi",""),sid,pn,qty,bir,price,jami,"tasdiqlangan",vid])
    ctx.user_data["vjami"]=jami
    await upd.message.reply_text(f"✅ Vozvrat: {pn} {fmtq(qty,bir,pn,bir=='kg')}\n💰 {jami:,.0f}")
    return await save_top(upd,ctx)

async def save_top(upd,ctx):
    l=la(ctx); uid=str(upd.effective_user.id)
    p=ctx.user_data.get("tp",{}); s=ctx.user_data.get("ts",{}); sid=str(s.get("ID",""))
    qty=ctx.user_data.get("tqty",0); naqd=ctx.user_data.get("tnaqd",0.0)
    vjami=ctx.user_data.get("vjami",0.0); pay_type=ctx.user_data.get("pay_type","naqd")
    pid=p.get("id",0)
    price,_=get_price(pid,uid,sid)
    if not price: price,_=get_price(pid,uid)
    if not price: price,_=get_price(pid)
    jami=qty*price; effective_naqd=min(naqd+vjami,jami); qarz=max(0.0,jami-effective_naqd)

    # Zakaz bilan taqqoslash (brinza bundan mustasno)
    orders=ctx.user_data.get("torders",[])
    if not is_brinza(p.get(l,"")):
        zakaz_qty=sum(float(r.get("Miqdor",0) or 0) for r in orders if r.get("Mahsulot","")==p.get(l,""))
        if zakaz_qty>0 and qty>zakaz_qty:
            diff=qty-zakaz_qty; ctx.user_data["_pending_top"]=True
            if upd.callback_query:
                await upd.callback_query.edit_message_text(
                    f"🎉 Ajoyib! Ko'proq sotmoqdasiz!\n\nZakaz: {fmtq(zakaz_qty,p['unit'],p[l],True)}\nQo'shimcha: {fmtq(diff,p['unit'],p[l],True)}\n\nAdashmadinmi?",
                    reply_markup=ikr([("✅ Ha, davom et","top_ok")],[("❌ Yo'q, qaytaman","top_back")]))
            else:
                await upd.message.reply_text(
                    f"🎉 Ajoyib! Ko'proq sotmoqdasiz!\n\nZakaz: {fmtq(zakaz_qty,p['unit'],p[l],True)}\nQo'shimcha: {fmtq(diff,p['unit'],p[l],True)}\n\nAdashmadinmi?",
                    reply_markup=ikr([("✅ Ha, davom et","top_ok")],[("❌ Yo'q, qaytaman","top_back")]))
            return S_TOP_PAY

    tid=mk_id("T"); u=get_user(uid); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else uid
    db_add("Topshirish",[now_s(),uid,s.get("Nomi",""),sid,p.get(l,""),qty,p.get("unit",""),price,jami,pay_type,effective_naqd,qarz,"tasdiqlangan",tid])
    qty_str=fmtq(qty,p.get("unit",""),p.get(l,""),True)
    msg=f"✅ <b>Mol topshirildi!</b>\n🏪 {s.get('Nomi','')}\n📦 {p.get(l,'')}: {qty_str}\n💰 Jami: {jami:,.0f}\n💵 Naqd: {effective_naqd:,.0f}\n📝 Qarz: {qarz:,.0f}"
    if vjami>0: msg+=f"\n↩️ Vozvrat: -{vjami:,.0f}"
    for k in ["tp","ts","tqty","tnaqd","vjami","pay_type","torders","vprods","vprod","scale_photo","_ocr","_pending_top"]: ctx.user_data.pop(k,None)
    if upd.callback_query:
        try: await upd.callback_query.edit_message_text(msg,parse_mode="HTML")
        except: pass
    else: await upd.message.reply_text(msg,parse_mode="HTML")
    await send_main(upd,ctx); return S_MAIN

async def top_confirm_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q)
    if q.data=="top_ok": ctx.user_data.pop("_pending_top",None); return await save_top(upd,ctx)
    await q.edit_message_text("Orqaga. Miqdorni qaytadan kiriting."); ctx.user_data.pop("_pending_top",None)
    return await top_p_cb_restart(upd,ctx)

async def top_p_cb_restart(upd,ctx):
    p=ctx.user_data.get("tp",{}); l=la(ctx); s=ctx.user_data.get("ts",{})
    brinza=is_brinza(p.get(l,""))
    hint="Tarozi rasmi yoki kg kiriting:" if brinza else "Miqdorni kiriting:"
    await upd.callback_query.edit_message_text(f"🚚 <b>{p.get(l,'')}</b>\n\n{hint}",reply_markup=back(f"top_s:{s.get('ID','')}"),parse_mode="HTML")
    return S_TOP_PHOTO

async def tok_cmd(upd,ctx):
    m=re.search(r'/tok_(\w+)',upd.message.text or "")
    if m: db_set("Topshirish","Top_ID",m.group(1),"Status","tasdiqlangan"); await upd.message.reply_text("✅")

async def trad_cmd(upd,ctx):
    m=re.search(r'/trad_(\w+)',upd.message.text or "")
    if m: db_set("Topshirish","Top_ID",m.group(1),"Status","rad_etildi"); await upd.message.reply_text("❌")


# ── BUYURTMALAR ───────────────────────────────────────────────────────────────
async def buy_show(upd,ctx):
    uid=str(upd.effective_user.id)
    my_store_ids={str(s.get("ID","")) for s in get_stores(uid)}
    orders=[r for r in db_get("Buyurtmalar") if (str(r.get("Dist_ID",""))==uid or str(r.get("Dokon_ID","")) in my_store_ids) and r.get("Status","")=="Yangi"]
    if not orders: await esend(upd,ctx,"📋 Yangi buyurtma yo'q.",back()); return S_MAIN
    by_dk={}; jami={}
    for r in orders:
        by_dk.setdefault(r.get("Dokon","?"),[]).append(r)
        pn=r.get("Mahsulot",""); jami[pn]=jami.get(pn,0)+float(r.get("Miqdor",0) or 0)
    lines=["📋 <b>Buyurtmalar</b>","━━━━━━━━━━━━━━━━"]
    for dk,recs in by_dk.items():
        lines.append(f"🏪 <b>{dk}:</b>")
        for r in recs: lines.append(f"  • {r.get('Mahsulot','')}: {fmtq(float(r.get('Miqdor',0) or 0),'dona',r.get('Mahsulot',''),False)}")
    lines+=["━━━━━━━━━━━━━━━━","📊 <b>Jami:</b>"]
    for pn,q in jami.items(): lines.append(f"  • {pn}: {fmtq(q,'dona',pn,False)}")
    await esend(upd,ctx,"\n".join(lines),back()); return S_MAIN

# ── TO'LOV ────────────────────────────────────────────────────────────────────
async def tol_show(upd,ctx):
    uid=str(upd.effective_user.id); stores=get_stores(uid)
    qarzdor=[(s,get_debt(str(s.get("ID","")))) for s in stores]; qarzdor=[(s,d) for s,d in qarzdor if d>0]
    if not qarzdor: await esend(upd,ctx,"✅ Qarz yo'q!",back()); return S_MAIN
    total=sum(d for _,d in qarzdor)
    lines=["💸 <b>Qarzdorlar:</b>","━━━━━━━━━━━━━━━━"]
    rows=[]
    for s,d in qarzdor:
        sid=str(s.get("ID","")); lines.append(f"• {s.get('Nomi','')}: {d:,.0f}")
        rows.append([(f"💵 {s.get('Nomi','')} — to'lov",f"tol_s:{sid}")])
    lines.append(f"━━━━━━━━━━━━━━━━\nJami: {total:,.0f}"); rows.append([("🔙 Orqaga","m:main")])
    await esend(upd,ctx,"\n".join(lines),InlineKeyboardMarkup([[InlineKeyboardButton(b[0],callback_data=b[1]) for b in r] for r in rows])); return S_MAIN

async def tol_store_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q)
    sid=q.data.split(":")[1]; uid=str(upd.effective_user.id)
    s=next((x for x in get_stores(uid) if str(x.get("ID",""))==sid),None)
    if not s: return S_MAIN
    debt=get_debt(sid); ctx.user_data["tol_s"]=s; ctx.user_data["tol_sid"]=sid; ctx.user_data["tol_debt"]=debt
    await q.edit_message_text(
        f"💵 <b>{s.get('Nomi','')}</b>\n💸 Qarz: {debt:,.0f}\n\nSummani kiriting:",
        reply_markup=ik1((f"✅ To'liq ({debt:,.0f})",f"tol_full:{sid}"),("🔙 Orqaga","m:tol")),parse_mode="HTML")
    return S_TOLOV_SUMMA

async def tol_full_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q); uid=str(upd.effective_user.id)
    s=ctx.user_data.get("tol_s",{}); sid=ctx.user_data.get("tol_sid",""); debt=ctx.user_data.get("tol_debt",0)
    db_add("Tolov",[now_s(),uid,s.get("Nomi",""),sid,debt,"tasdiqlangan",mk_id("TOL")])
    await q.edit_message_text(f"✅ To'lov qabul!\n🏪 {s.get('Nomi','')}\n💰 {debt:,.0f}\n✅ Qarz yopildi!",reply_markup=back("m:tol"),parse_mode="HTML")
    return S_MAIN

async def tol_summa(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    uid=str(upd.effective_user.id); summa=parse_money(upd.message.text)
    s=ctx.user_data.get("tol_s",{}); sid=ctx.user_data.get("tol_sid",""); debt=ctx.user_data.get("tol_debt",0)
    if summa<=0: await upd.message.reply_text("❌ Noto'g'ri:"); return S_TOLOV_SUMMA
    if summa>debt: await upd.message.reply_text(f"⚠️ Qarzdan ({debt:,.0f}) ko'p! Qaytadan:"); return S_TOLOV_SUMMA
    db_add("Tolov",[now_s(),uid,s.get("Nomi",""),sid,summa,"tasdiqlangan",mk_id("TOL")])
    qoldi=max(0.0,debt-summa)
    msg=f"✅ To'lov!\n🏪 {s.get('Nomi','')}\n💰 {summa:,.0f}\n"
    msg+=f"💸 Qoldi: {qoldi:,.0f}" if qoldi>0 else "✅ Qarz yopildi!"
    await upd.message.reply_text(msg,reply_markup=main_kb(upd.effective_user.id),parse_mode="HTML"); return S_MAIN

async def vok_cmd(upd,ctx):
    m=re.search(r'/vok_(\w+)',upd.message.text or "")
    if m: db_set("Tolov","Tolov_ID",m.group(1),"Status","tasdiqlangan"); await upd.message.reply_text("✅")
async def vrad_cmd(upd,ctx):
    m=re.search(r'/vrad_(\w+)',upd.message.text or "")
    if m: db_set("Tolov","Tolov_ID",m.group(1),"Status","rad_etildi"); await upd.message.reply_text("❌")

# ── KUNLIK, OMBOR, MARSHRUT, HISOBOT ─────────────────────────────────────────
async def nat_show(upd,ctx):
    uid=str(upd.effective_user.id); td=today_s()
    tops=[r for r in db_get("Topshirish") if str(r.get("Sana","")).startswith(td) and str(r.get("Dist_ID",""))==uid]
    ins=[r for r in db_get("Qabul") if str(r.get("Sana","")).startswith(td) and str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan"]
    ts=sum(float(r.get("Jami",0) or 0) for r in tops); tn=sum(float(r.get("Naqd",0) or 0) for r in tops)
    tq=sum(float(r.get("Qarz",0) or 0) for r in tops); ti=sum(float(r.get("Jami",0) or 0) for r in ins)
    jq=sum(get_debt(str(s.get("ID",""))) for s in get_stores(uid)); dc=len({r.get("Dokon","") for r in tops})
    msg=(f"📊 <b>Kunlik — {td}</b>\n━━━━━━━━━━━━━━━━\n"
         f"📥 Zavod: {ti:,.0f}\n🚚 Sotuv: {ts:,.0f}\n💵 Naqd: {tn:,.0f}\n"
         f"📝 Qarz: {tq:,.0f}\n💸 Jami qarz: {jq:,.0f}\n💰 Foyda: {ts-ti:,.0f}\n🏪 {dc} do'kon")
    await esend(upd,ctx,msg,back()); return S_MAIN

async def omb_show(upd,ctx):
    uid=str(upd.effective_user.id); st=get_ombor(uid); prods=get_prods()
    pmap={p["uz"].lower():p for p in prods}; pmap.update({p["ru"].lower():p for p in prods})
    lines=["📦 <b>Ombor:</b>","━━━━━━━━━━━━━━━━"]; ts=0; ttn=0
    for k,v in st.items():
        if v<=0.001: continue
        p=pmap.get(k.lower()); unit=p["unit"] if p else "kg"; pid=p["id"] if p else 0
        narx,tn=get_price(pid,uid)
        if not narx: narx,tn=get_price(pid)
        sv=v*narx; tv=v*tn; ts+=sv; ttn+=tv
        nstr=f"{narx:,.0f}" if narx else "?"
        lines.append(f"• <b>{k}</b>: {fmtq(v,unit,k,False)}\n  💰 {nstr} → {sv:,.0f} | Foyda: {sv-tv:,.0f}")
    if len(lines)==2: lines.append("Ombor bo'sh!")
    else: lines+=["━━━━━━━━━━━━━━━━",f"📊 Sotuv: {ts:,.0f} | Foyda: {ts-ttn:,.0f}"]
    await esend(upd,ctx,"\n".join(lines),back()); return S_MAIN

async def mar_show(upd,ctx):
    uid=str(upd.effective_user.id); stores=get_stores(uid)
    if not stores: await esend(upd,ctx,"⚠️ Do'konlar yo'q.",back()); return S_MAIN
    lines=["🗺 <b>Marshrut:</b>","━━━━━━━━━━━━━━━━"]
    for i,s in enumerate(stores,1):
        d=get_debt(str(s.get("ID",""))); ds=f" ⚠️{d:,.0f}" if d>0 else ""
        lines.append(f"{i}. {s.get('Nomi','')}{ds}\n   📞 {s.get('Tel1','-')}")
    await esend(upd,ctx,"\n".join(lines),back()); return S_MAIN

async def his_menu(upd,ctx):
    await esend(upd,ctx,"📈 <b>Hisobot</b>",ikr([("7 kun","his:7"),("30 kun","his:30")],[("🔙 Orqaga","m:main")])); return S_MAIN

async def his_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=upd.callback_query; await ans(q); uid=str(upd.effective_user.id)
    days=int(q.data.split(":")[1]); fd=(datetime.now()-timedelta(days=days)).strftime("%Y-%m-%d")
    tops=[r for r in db_get("Topshirish") if str(r.get("Sana",""))>=fd and str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan"]
    ins=[r for r in db_get("Qabul") if str(r.get("Sana",""))>=fd and str(r.get("Dist_ID",""))==uid and r.get("Status","")=="tasdiqlangan"]
    vozs=[r for r in db_get("Vozvrat") if str(r.get("Sana",""))>=fd and str(r.get("Dist_ID",""))==uid]
    ts=sum(float(r.get("Jami",0) or 0) for r in tops); tn=sum(float(r.get("Naqd",0) or 0) for r in tops)
    tq=sum(float(r.get("Qarz",0) or 0) for r in tops); ti=sum(float(r.get("Jami",0) or 0) for r in ins)
    vs=sum(float(r.get("Jami",0) or 0) for r in vozs); jq=sum(get_debt(str(s.get("ID",""))) for s in get_stores(uid))
    msg=(f"📈 <b>{days} kunlik hisobot</b>\n━━━━━━━━━━━━━━━━\n"
         f"📥 Qabul: {ti:,.0f}\n🚚 Sotuv: {ts:,.0f}\n💵 Naqd: {tn:,.0f}\n"
         f"📝 Qarz(davr): {tq:,.0f}\n↩️ Vozvrat: {vs:,.0f}\n💸 Jami qarz: {jq:,.0f}\n💰 Foyda: {ts-ti:,.0f}")
    await q.edit_message_text(msg,reply_markup=back("m:his"),parse_mode="HTML"); return S_MAIN


# ── DO'KONLAR (lokatsiya bilan) ────────────────────────────────────────────────
async def dok_show(upd,ctx):
    uid=str(upd.effective_user.id); stores=get_stores(uid)
    if upd.callback_query:
        try: await upd.callback_query.edit_message_text("🏪 <b>Do'konlarim</b>",parse_mode="HTML")
        except: pass
    if not stores:
        await ctx.bot.send_message(uid,"Do'konlar yo'q.",
            reply_markup=ik1(("➕ Do'kon qo'shish","dok:add"),("🔙 Orqaga","m:main"))); return S_MAIN
    for s in stores:
        did=str(s.get("ID","")); debt=get_debt(did)
        debt_str=f"\n💸 Qarz: {debt:,.0f}" if debt>0 else ""
        card=(f"🏪 <b>{s.get('Nomi','')}</b>{debt_str}\n━━━━━━━━━━━━━━━━\n"
              f"📍 {s.get('Adres','—')}\n🏢 {s.get('MCHJ','—') or '—'}\n"
              f"📞 {s.get('Tel1','—')}\n📞 {s.get('Tel2','—') or '—'}\n"
              f"👤 {s.get('Ega','—') or '—'}")
        kb=ikr([(f"📋 Zakaz","dzak:"+did),(f"✏️ Tahrirlash","dedit:"+did)])
        # Kanaldan forward
        ch_mid=str(s.get("Channel_Msg_ID","")).strip()
        if CHANNEL_ID and ch_mid and ch_mid not in ("","0"):
            try:
                await ctx.bot.forward_message(chat_id=uid,from_chat_id=int(CHANNEL_ID),message_id=int(ch_mid))
                await ctx.bot.send_message(uid,f"☝️ {s.get('Nomi','')}:",reply_markup=kb)
            except Exception as e:
                logger.error(f"Forward: {e}")
                await ctx.bot.send_message(uid,card,reply_markup=kb,parse_mode="HTML")
        else:
            await ctx.bot.send_message(uid,card,reply_markup=kb,parse_mode="HTML")
    await ctx.bot.send_message(uid,"Yangi do'kon:",reply_markup=ik1(("➕ Qo'shish","dok:add"),("🔙 Orqaga","m:main")))
    return S_MAIN

async def dok_add_cb(upd,ctx):
    q=upd.callback_query; await ans(q)
    await q.edit_message_text("🏪 <b>Yangi do'kon</b>\n\nDo'kon nomini kiriting:",parse_mode="HTML"); return S_DI_NAME

async def di_name(upd,ctx):
    ctx.user_data["dn"]=upd.message.text.strip()
    await upd.message.reply_text("📍 Manzil:"); return S_DI_ADDR

async def di_addr(upd,ctx):
    ctx.user_data["da"]=upd.message.text.strip()
    await upd.message.reply_text("🏢 MCHJ (yoki o'tkazish):",reply_markup=ik1(("⏭","di_skip:mchj"))); return S_DI_MCHJ

async def di_mchj(upd,ctx):
    if upd.callback_query: await ans(upd.callback_query); ctx.user_data["dm"]=""
    else: ctx.user_data["dm"]=upd.message.text.strip()
    await ctx.bot.send_message(upd.effective_user.id,"📞 Tel 1:",reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📱 Yuborish",request_contact=True)]],resize_keyboard=True))
    return S_DI_TEL1

async def di_tel1(upd,ctx):
    phone=upd.message.contact.phone_number if upd.message.contact else clean_phone(upd.message.text)
    if len(phone.replace("+",""))<7:
        await upd.message.reply_text("❌ Noto'g'ri!",reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📱 Yuborish",request_contact=True)]],resize_keyboard=True)); return S_DI_TEL1
    ctx.user_data["dt1"]=phone
    await upd.message.reply_text("📞 Tel 2 (yoki o'tkazish):",
        reply_markup=ReplyKeyboardRemove())
    await ctx.bot.send_message(upd.effective_user.id,"📞 Tel 2:",reply_markup=ik1(("⏭","di_skip:tel2"))); return S_DI_TEL2

async def di_tel2(upd,ctx):
    if upd.callback_query: await ans(upd.callback_query); ctx.user_data["dt2"]=""
    else: ctx.user_data["dt2"]=clean_phone(upd.message.text)
    await ctx.bot.send_message(upd.effective_user.id,"👤 Do'kon egasi ismi:"); return S_DI_EGA

async def di_ega(upd,ctx):
    ctx.user_data["de"]=upd.message.text.strip()
    await upd.message.reply_text("📸 Do'kon rasmi (MAJBURIY):"); return S_DI_PHOTO

async def di_photo(upd,ctx):
    if not upd.message.photo:
        await upd.message.reply_text("❗ Rasm yuboring!"); return S_DI_PHOTO
    ctx.user_data["dphoto"]=upd.message.photo[-1].file_id
    # Avval ReplyKeyboard ni o'chir, keyin lokatsiya tugmasi bilan yangi xabar
    await upd.message.reply_text(
        "✅ Rasm qabul!\n\n📍 Lokatsiyani yuboring yoki o'tkazib yuboring:",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("📍 Lokatsiya yuborish", request_location=True)],
             ["⏭ O'tkazib yuborish"]],
            resize_keyboard=True, one_time_keyboard=True))
    return S_DI_LOC

async def di_loc(upd,ctx):
    uid=upd.effective_user.id; lat=""; lng=""
    if upd.message.location:
        lat=str(upd.message.location.latitude)
        lng=str(upd.message.location.longitude)
    elif upd.message.text:
        t = upd.message.text
        if "tkazib" in t or "Пропустить" in t or "⏭" in t:
            pass  # lokatsiyasiz davom
        else:
            # Boshqa matn keldi — qayta so'rash
            await upd.message.reply_text(
                "📍 Quyidagi tugmani bosib lokatsiya yuboring yoki o'tkazib yuboring:",
                reply_markup=ReplyKeyboardMarkup(
                    [[KeyboardButton("📍 Lokatsiya yuborish", request_location=True)],
                     ["⏭ O'tkazib yuborish"]],
                    resize_keyboard=True))
            return S_DI_LOC

    name=ctx.user_data.get("dn",""); addr=ctx.user_data.get("da","")
    mchj=ctx.user_data.get("dm",""); tel1=ctx.user_data.get("dt1","")
    tel2=ctx.user_data.get("dt2",""); ega=ctx.user_data.get("de","")
    photo=ctx.user_data.get("dphoto","")
    u=get_user(uid); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else str(uid)
    cnt=len(db_get("Dokonlar"))+1
    lat_txt = f"\n📍 {lat}, {lng}" if lat and lng else ""
    card=(f"🏪 <b>{name}</b>\n━━━━━━━━━━━━━━━━\n"
          f"📍 {addr}\n🏢 {mchj or '—'}\n📞 {tel1}\n📞 {tel2 or '—'}\n👤 {ega or '—'}\n🚚 {dn}{lat_txt}")

    # Kanalga yuborish va message_id saqlash
    ch_mid=""
    channel_int = int(CHANNEL_ID) if CHANNEL_ID else 0
    if channel_int and photo:
        try:
            msg = await ctx.bot.send_photo(channel_int, photo,
                caption=f"🏪 DO'KON MA'LUMOTI\n\n{card}", parse_mode="HTML")
            ch_mid = str(msg.message_id)
            logger.info(f"Kanal: {name} | msg_id={ch_mid}")
        except Exception as e:
            logger.error(f"Kanal xato ({CHANNEL_ID}): {e}")
    elif channel_int and not photo:
        logger.warning(f"Kanal: rasm yo'q, {name}")
    elif not channel_int:
        logger.warning("CHANNEL_ID o'rnatilmagan yoki noto'g'ri")

    db_add("Dokonlar",[str(cnt),name,addr,mchj,tel1,tel2,ega,str(uid),dn,lat,lng,ch_mid,now_s()])

    # Adminga
    for adm in ADMIN_IDS:
        try:
            if photo: await ctx.bot.send_photo(adm,photo,caption=card,parse_mode="HTML")
            else: await ctx.bot.send_message(adm,card,parse_mode="HTML")
        except: pass

    await upd.message.reply_text("...",reply_markup=ReplyKeyboardRemove())
    if photo:
        await upd.message.reply_photo(photo,caption=f"✅ Do'kon qo'shildi!\n\n{card}",parse_mode="HTML")
    else:
        await upd.message.reply_text(f"✅ Do'kon qo'shildi!\n\n{card}",parse_mode="HTML")
    if lat and lng:
        await ctx.bot.send_location(uid,float(lat),float(lng))
    for k in ["dn","da","dm","dt1","dt2","de","dphoto"]: ctx.user_data.pop(k,None)
    await send_main(upd,ctx); return S_MAIN

# Do'kon zakaz qo'shish
async def dok_zak_cb(upd,ctx):
    q=upd.callback_query; await ans(q); l=la(ctx); uid=str(upd.effective_user.id)
    sid=q.data.split(":")[1]
    s=next((x for x in get_stores(uid) if str(x.get("ID",""))==sid),None)
    if not s: return S_MAIN
    ctx.user_data["zak_s"]=s; prods=get_prods()
    await q.edit_message_text(f"📋 {s.get('Nomi','')} — zakaz\nMahsulot:",reply_markup=prod_kb(prods,l,"zprod","m:dok")); return S_MAIN

async def zprod_cb(upd,ctx):
    q=upd.callback_query; await ans(q); l=la(ctx)
    pid=int(q.data.split(":")[1]); p=next((x for x in get_prods() if x["id"]==pid),None)
    if not p: return S_MAIN
    ctx.user_data["zak_p"]=p; brinza=is_brinza(p[l])
    await q.edit_message_text(f"📦 <b>{p[l]}</b>\n{'Nechta? (dona)' if brinza else 'Miqdor:'}",parse_mode="HTML"); return S_ZAK_FROM_QTY

async def zak_from_qty(upd,ctx):
    l=la(ctx); uid=str(upd.effective_user.id)
    p=ctx.user_data.get("zak_p",{}); s=ctx.user_data.get("zak_s",{})
    pn=p.get(l,""); brinza=is_brinza(pn)
    if brinza:
        try: qty=int(float(upd.message.text.strip().replace(",",".")))
        except: qty=0
        if qty<=0: await upd.message.reply_text("Butun son:"); return S_ZAK_FROM_QTY
        bir="dona"
    else:
        qty=parse_w(upd.message.text)
        if qty<=0: await upd.message.reply_text("❌:"); return S_ZAK_FROM_QTY
        bir=p.get("unit","kg")
    sid=str(s.get("ID","")); sname=s.get("Nomi","")
    dist_id=str(s.get("Dist_ID","")) or uid
    db_add("Buyurtmalar",[now_s(),sid,sname,dist_id,pn,qty,"Yangi",mk_id("Z")])
    await upd.message.reply_text(f"✅ Zakaz: {sname}\n{pn}: {fmtq(qty,bir,pn,bir=='kg')}",reply_markup=main_kb(uid))
    return S_MAIN

# Do'kon tahrirlash
async def dok_edit_cb(upd,ctx):
    q=upd.callback_query; await ans(q)
    sid=q.data.split(":")[1]
    stores=db_get("Dokonlar"); s=next((x for x in stores if str(x.get("ID",""))==sid),None)
    if not s: return S_MAIN
    ctx.user_data["es_id"]=sid; ctx.user_data["es"]=dict(s)
    fields={"1":"Nomi","2":"Adres","3":"MCHJ","4":"Tel1","5":"Tel2","6":"Ega"}
    lines=[f"✏️ <b>{s.get('Nomi','')} — tahrirlash</b>\n"]
    for k,v in fields.items(): lines.append(f"{k}. {v}: <i>{s.get(v,'—') or '—'}</i>")
    lines.append("\nRaqam bosing:")
    await q.edit_message_text("\n".join(lines),
        reply_markup=ikr([("1","ef:1"),("2","ef:2"),("3","ef:3")],[("4","ef:4"),("5","ef:5"),("6","ef:6")],[("🔙","m:dok")]),
        parse_mode="HTML"); return S_MAIN

async def dok_ef_cb(upd,ctx):
    q=upd.callback_query; await ans(q)
    num=q.data.split(":")[1]; fields={"1":"Nomi","2":"Adres","3":"MCHJ","4":"Tel1","5":"Tel2","6":"Ega"}
    f=fields.get(num,"Nomi"); ctx.user_data["ef"]=f
    s=ctx.user_data.get("es",{})
    await q.edit_message_text(f"✏️ <b>{f}</b>\nJoriy: <i>{s.get(f,'—') or '—'}</i>\n\nYangi qiymat:",parse_mode="HTML",reply_markup=back("m:dok"))
    return S_DK_EDIT_VAL

async def dk_edit_val(upd,ctx):
    f=ctx.user_data.get("ef",""); sid=ctx.user_data.get("es_id",""); v=upd.message.text.strip()
    try:
        w=ws("Dokonlar")
        if w:
            h=w.row_values(1)
            if f in h:
                for i,r in enumerate(w.get_all_records()):
                    if str(r.get("ID",""))==sid: w.update_cell(i+2,h.index(f)+1,v); break
    except Exception as e: logger.error(f"dk_edit: {e}")
    ctx.user_data.get("es",{})[f]=v
    await upd.message.reply_text(f"✅ {f}: <b>{v}</b>",parse_mode="HTML",reply_markup=main_kb(upd.effective_user.id))
    return S_MAIN


# ── NARXLAR ───────────────────────────────────────────────────────────────────
async def nar_start(upd,ctx):
    l=la(ctx); prods=get_prods()
    await esend(upd,ctx,"💰 <b>Narxlarim</b>\nMahsulot:",prod_kb(prods,l,"nar")); return S_MAIN

async def nar_prod_cb(upd,ctx):
    q=upd.callback_query; await ans(q); l=la(ctx); uid=str(upd.effective_user.id)
    pid=int(q.data.split(":")[1]); p=next((x for x in get_prods() if x["id"]==pid),None)
    if not p: return S_MAIN
    ctx.user_data["np"]=p; ctx.user_data["nd_id"]=""
    narx,tn=get_price(pid,uid)
    if not narx: narx,tn=get_price(pid)
    stores=get_stores(uid)
    await q.edit_message_text(f"💰 <b>{p[l]}</b>\nJoriy: {narx:,.0f} / {tn:,.0f}\n\nQaysi narx?",
        reply_markup=ikr([("🔵 Barcha","nt:all")]+([("🟡 Bitta do'kon","nt:one")] if stores else [])+[("🔙","m:nar")]),
        parse_mode="HTML"); return S_MAIN

async def nar_type_cb(upd,ctx):
    q=upd.callback_query; await ans(q); uid=str(upd.effective_user.id)
    t=q.data.split(":")[1]
    if t=="all": ctx.user_data["nd_id"]=""; await q.edit_message_text("💰 Narx kiriting:"); return S_NARX_VAL
    stores=get_stores(uid)
    await q.edit_message_text("Do'konni tanlang:",reply_markup=store_kb(stores,"nar_d","m:nar")); return S_MAIN

async def nar_d_cb(upd,ctx):
    q=upd.callback_query; await ans(q)
    ctx.user_data["nd_id"]=q.data.split(":")[1]; await q.edit_message_text("💰 Narx:"); return S_NARX_D_VAL

async def nar_val(upd,ctx):
    price=parse_money(upd.message.text)
    if price<=0: await upd.message.reply_text("❌:"); return S_NARX_VAL
    ctx.user_data["nval"]=price; await upd.message.reply_text("📉 Tannarx:"); return S_NARX_COST

async def nar_d_val(upd,ctx):
    price=parse_money(upd.message.text)
    if price<=0: await upd.message.reply_text("❌:"); return S_NARX_D_VAL
    ctx.user_data["nval"]=price; await upd.message.reply_text("📉 Tannarx:"); return S_NARX_D_COST

async def nar_cost(upd,ctx):
    l=la(ctx); uid=str(upd.effective_user.id)
    cost=parse_money(upd.message.text); p=ctx.user_data.get("np",{}); price=ctx.user_data.get("nval",0)
    did_store=ctx.user_data.get("nd_id","")
    set_price(p.get("id",0),p.get(l,""),price,cost,uid,did_store)
    await upd.message.reply_text(f"✅ {p.get(l,'')}: {price:,.0f} / {cost:,.0f}",reply_markup=main_kb(uid)); return S_MAIN

async def nar_d_cost(upd,ctx): return await nar_cost(upd,ctx)

# ── ADMIN PANEL ───────────────────────────────────────────────────────────────
async def adm_menu(upd,ctx):
    uid=upd.effective_user.id
    if uid not in ADMIN_IDS: await esend(upd,ctx,"🚫 Ruxsat yo'q!"); return S_MAIN
    namoz_stat="✅ Yoq" if get_set("namoz","1")=="1" else "❌ O'chiq"
    await esend(upd,ctx,"⚙️ <b>Admin</b>",
        ikr([("➕ Mahsulot","adm:prod")],[("💰 Narxlar","adm:price")],
            [("🏪 Do'kon qo'sh","adm:dk")],[("🚚 Distribyutor","adm:dist")],
            [("📊 Statistika","adm:stats"),("💸 Qarzdorlar","adm:debt")],
            [("🏪 Do'konlar","adm:dks"),("🚚 Distlar","adm:dists")],
            [("📦 Zavod","adm:zav")],[("📢 Xabar","adm:bc")],
            [(f"🕌 Namoz: {namoz_stat}","adm:namoz")],[("🔙 Orqaga","m:main")])); return S_MAIN

async def adm_cb(upd,ctx):
    q=upd.callback_query; await ans(q); uid=upd.effective_user.id; l=la(ctx)
    if uid not in ADMIN_IDS: return S_MAIN
    d=q.data.split(":")[1]
    if d=="namoz":
        nv="0" if get_set("namoz","1")=="1" else "1"
        put_set("namoz",nv); await q.answer("✅ Yangilandi!"); return await adm_menu(upd,ctx)
    if d=="prod": await q.edit_message_text("Nom (UZ):",reply_markup=back("m:adm")); return S_ADM_PROD_UZ
    if d=="price":
        await q.edit_message_text("Mahsulot:",reply_markup=prod_kb(get_prods(),l,"adm_narx","m:adm")); return S_MAIN
    if d=="dk": ctx.user_data["adm_dk"]=True; await q.edit_message_text("Do'kon nomi:"); return S_ADM_DK_NAME
    if d=="dist": await q.edit_message_text("Distribyutor ismi:"); return S_ADM_DIST_NAME
    if d=="stats": return await adm_stats(upd,ctx)
    if d=="debt":  return await adm_debt(upd,ctx)
    if d=="dks":   return await adm_dks(upd,ctx)
    if d=="dists": return await adm_dists(upd,ctx)
    if d=="zav":   return await adm_zav(upd,ctx)
    if d=="bc": await q.edit_message_text("Xabar matni:"); return S_ADM_BC
    return S_MAIN

async def adm_narx_p_cb(upd,ctx):
    q=upd.callback_query; await ans(q); l=la(ctx)
    pid=int(q.data.split(":")[1]); p=next((x for x in get_prods() if x["id"]==pid),None)
    if not p: return S_MAIN
    ctx.user_data["np"]=p; ctx.user_data["nd_id"]=""
    narx,tn=get_price(pid)
    await q.edit_message_text(f"💰 <b>{p[l]}</b>\nJoriy: {narx:,.0f}/{tn:,.0f}\n\nNarx:",parse_mode="HTML"); return S_ADM_NARX_VAL

async def adm_narx_val(upd,ctx):
    price=parse_money(upd.message.text)
    if price<=0: await upd.message.reply_text("❌:"); return S_ADM_NARX_VAL
    ctx.user_data["nval"]=price; await upd.message.reply_text("Tannarx:"); return S_ADM_NARX_COST

async def adm_narx_cost(upd,ctx):
    l=la(ctx); cost=parse_money(upd.message.text); p=ctx.user_data.get("np",{}); price=ctx.user_data.get("nval",0)
    set_price(p.get("id",0),p.get(l,""),price,cost)
    await upd.message.reply_text(f"✅ {p.get(l,'')}: {price:,.0f}/{cost:,.0f}",reply_markup=main_kb(upd.effective_user.id)); return S_MAIN

async def adm_prod_uz(upd,ctx): ctx.user_data["m_uz"]=upd.message.text.strip(); await upd.message.reply_text("Nom (RU):"); return S_ADM_PROD_RU
async def adm_prod_ru(upd,ctx):
    ctx.user_data["m_ru"]=upd.message.text.strip()
    await upd.message.reply_text("Birlik:",reply_markup=ikr([("kg","unit:kg"),("litr","unit:litr")],[("dona","unit:dona"),("g","unit:g")])); return S_ADM_PROD_UNIT
async def adm_unit_cb(upd,ctx):
    q=upd.callback_query; await ans(q); l=la(ctx)
    unit=q.data.split(":")[1]; uz=ctx.user_data.get("m_uz",""); ru=ctx.user_data.get("m_ru","")
    recs=db_get("Mahsulotlar"); nid=max([int(r.get("ID",0)) for r in recs],default=0)+1
    db_add("Mahsulotlar",[str(nid),uz,ru,unit,"1",now_s()])
    await q.edit_message_text(f"✅ {uz}",reply_markup=back("m:adm")); return S_MAIN

async def adm_dk_name(upd,ctx): ctx.user_data["adk_n"]=upd.message.text.strip(); await upd.message.reply_text("Manzil:"); return S_ADM_DK_ADDR
async def adm_dk_addr(upd,ctx): ctx.user_data["adk_a"]=upd.message.text.strip(); await upd.message.reply_text("Dist TG_ID:"); return S_ADM_DK_DIST
async def adm_dk_dist(upd,ctx):
    l=la(ctx); did=upd.message.text.strip()
    u=get_user(did); dn=f"{u.get('Ism','')} {u.get('Familiya','')}".strip() if u else str(did)
    n=ctx.user_data.get("adk_n",""); a=ctx.user_data.get("adk_a","")
    cnt=len(db_get("Dokonlar"))+1
    db_add("Dokonlar",[str(cnt),n,a,"","","","",did,dn,"","","",now_s()])
    await upd.message.reply_text(f"✅ {n}",reply_markup=main_kb(upd.effective_user.id)); return S_MAIN

async def adm_dist_name(upd,ctx): ctx.user_data["adn"]=upd.message.text.strip(); await upd.message.reply_text("TG_ID:"); return S_ADM_DIST_TG
async def adm_dist_tg(upd,ctx):
    name=ctx.user_data.get("adn",""); tg=upd.message.text.strip(); sid=mk_sid()
    db_add("Foydalanuvchilar",[tg,name,"","","distributor","uz","tasdiqlangan",sid,now_s()])
    await upd.message.reply_text(f"✅ {name} | ID:{sid}",reply_markup=main_kb(upd.effective_user.id)); return S_MAIN

async def adm_bc(upd,ctx):
    t=upd.message.text; users=db_get("Foydalanuvchilar"); ok=0; fail=0
    for u in users:
        try: await ctx.bot.send_message(int(u.get("TG_ID",0)),t); ok+=1
        except: fail+=1
    await upd.message.reply_text(f"✅ {ok} | ❌ {fail}",reply_markup=main_kb(upd.effective_user.id)); return S_MAIN

async def adm_stats(upd,ctx):
    q=upd.callback_query
    tops=db_get("Topshirish"); ins=db_get("Qabul"); stores=db_get("Dokonlar"); users=db_get("Foydalanuvchilar")
    ts=sum(float(r.get("Jami",0) or 0) for r in tops if r.get("Status","")=="tasdiqlangan")
    ti=sum(float(r.get("Jami",0) or 0) for r in ins if r.get("Status","")=="tasdiqlangan")
    jq=sum(get_debt(str(s.get("ID",""))) for s in stores)
    dists=len([u for u in users if u.get("Rol","")=="distributor"])
    zakazlar=len([r for r in db_get("Buyurtmalar") if r.get("Status","")=="Yangi"])
    msg=(f"📊 <b>Statistika</b>\n━━━━━━━━━━━━━━━━\n📥 {ti:,.0f}\n🚚 {ts:,.0f}\n"
         f"💸 Qarz: {jq:,.0f}\n💰 Foyda: {ts-ti:,.0f}\n🏪 {len(stores)}\n🚚 {dists}\n📋 {zakazlar}")
    await q.edit_message_text(msg,reply_markup=back("m:adm"),parse_mode="HTML"); return S_MAIN

async def adm_debt(upd,ctx):
    q=upd.callback_query; stores=db_get("Dokonlar"); lines=["💸 <b>Qarzdorlar:</b>","━━━━━━━━━━━━━━━━"]; total=0
    for s in stores:
        d=get_debt(str(s.get("ID",""))); 
        if d>0: lines.append(f"• {s.get('Nomi','')}: {d:,.0f}\n  🚚 {s.get('Dist_Ism','')}"); total+=d
    if len(lines)==2: lines.append("Qarz yo'q!")
    else: lines.append(f"━━━━━━━━━━━━━━━━\nJami: {total:,.0f}")
    await q.edit_message_text("\n".join(lines),reply_markup=back("m:adm"),parse_mode="HTML"); return S_MAIN

async def adm_dks(upd,ctx):
    q=upd.callback_query; stores=db_get("Dokonlar")
    if not stores: await q.edit_message_text("Yo'q.",reply_markup=back("m:adm")); return S_MAIN
    lines=[f"🏪 <b>{len(stores)} ta do'kon</b>","━━━━━━━━━━━━━━━━"]
    for s in stores:
        d=get_debt(str(s.get("ID",""))); ds=f" | {d:,.0f}" if d>0 else ""
        lines.append(f"• {s.get('Nomi','')}{ds}\n  📍 {s.get('Adres','-')}\n  📞 {s.get('Tel1','-')}\n  🚚 {s.get('Dist_Ism','')}")
    text="\n".join(lines)
    for i in range(0,len(text),3500): await ctx.bot.send_message(upd.effective_user.id,text[i:i+3500],parse_mode="HTML")
    await ctx.bot.send_message(upd.effective_user.id,"⬆️",reply_markup=back("m:adm")); return S_MAIN

async def adm_dists(upd,ctx):
    q=upd.callback_query; dists=[u for u in db_get("Foydalanuvchilar") if u.get("Rol","")=="distributor"]
    if not dists: await q.edit_message_text("Yo'q.",reply_markup=back("m:adm")); return S_MAIN
    lines=[f"🚚 <b>{len(dists)} ta distribyutor</b>","━━━━━━━━━━━━━━━━"]
    for u in dists:
        did=u.get("TG_ID",""); stores=get_stores(did)
        jq=sum(get_debt(str(s.get("ID",""))) for s in stores)
        lines.append(f"• {u.get('Ism','')} {u.get('Familiya','')} (ID:{u.get('Short_ID','')})\n  📞 {u.get('Telefon','')}\n  🏪 {len(stores)} | 💸 {jq:,.0f}")
    text="\n".join(lines)
    for i in range(0,len(text),3500): await ctx.bot.send_message(upd.effective_user.id,text[i:i+3500],parse_mode="HTML")
    await ctx.bot.send_message(upd.effective_user.id,"⬆️",reply_markup=back("m:adm")); return S_MAIN

async def adm_zav(upd,ctx):
    q=upd.callback_query; recs=[r for r in db_get("Qabul") if r.get("Status","")=="kutilmoqda"]
    if not recs: await q.edit_message_text("Kutilmoqda yo'q.",reply_markup=back("m:adm")); return S_MAIN
    lines=[f"📦 <b>{len(recs)} ta kutilmoqda</b>","━━━━━━━━━━━━━━━━"]
    for r in recs:
        qid=r.get("Qabul_ID","")
        lines.append(f"• {r.get('Dist_Ism','')} | {r.get('Mahsulot','')} {r.get('Miqdor','')}\n  Jami: {float(r.get('Jami',0) or 0):,.0f}\n  ✅ /zok_{qid} | ❌ /zrad_{qid}")
    text="\n".join(lines)
    for i in range(0,len(text),3500): await ctx.bot.send_message(upd.effective_user.id,text[i:i+3500],parse_mode="HTML")
    await ctx.bot.send_message(upd.effective_user.id,"⬆️",reply_markup=back("m:adm")); return S_MAIN


# ── SCHEDULER ─────────────────────────────────────────────────────────────────
async def job_qarz(ctx:ContextTypes.DEFAULT_TYPE):
    for u in db_get("Foydalanuvchilar"):
        if u.get("Rol","")!="distributor" or not approved(u.get("TG_ID","")): continue
        uid=str(u.get("TG_ID",""))
        debts=[(s.get("Nomi",""),get_debt(str(s.get("ID","")))) for s in get_stores(uid)]
        debts=[(n,d) for n,d in debts if d>0]
        if not debts: continue
        try:
            lines=["💸 <b>Bugungi qarzlar:</b>","━━━━━━━━━━━━━━━━"]
            for n,d in debts: lines.append(f"• {n}: {d:,.0f}")
            await ctx.bot.send_message(int(uid),"\n".join(lines),parse_mode="HTML")
        except: pass

async def job_zakaz(ctx:ContextTypes.DEFAULT_TYPE):
    kecha=(datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
    tops=db_get("Topshirish"); dist_map={}
    for r in tops:
        if not str(r.get("Sana","")).startswith(kecha) or r.get("Status","")!="tasdiqlangan": continue
        did=str(r.get("Dist_ID",""))
        if not did: continue
        key=f"{did}:{r.get('Dokon_ID','')}"; dist_map.setdefault(did,{})[key]=(r.get("Dokon",""),str(r.get("Dokon_ID","")))
    for did,dokonlar in dist_map.items():
        try:
            stores_all=db_get("Dokonlar"); lines=["📋 <b>KECHA MOL BERILGAN:</b>\n(Zakaz olish kerakmi?)\n","━━━━━━━━━━━━━━━━"]
            for _,(dkn,dkid) in dokonlar.items():
                s=next((x for x in stores_all if str(x.get("ID",""))==dkid),None)
                tel=s.get("Tel1","") if s else ""; tel2=s.get("Tel2","") if s else ""
                tel_str=f"📞 {tel}"; tel_str+=(f" / {tel2}" if tel2 else "")
                lines.append(f"• <b>{dkn}</b>\n  {tel_str}")
            lines.append("\nUlar bilan bog'laning!")
            await ctx.bot.send_message(int(did),"\n".join(lines),parse_mode="HTML")
        except Exception as e: logger.error(f"job_zakaz: {e}")

async def job_24h(ctx:ContextTypes.DEFAULT_TYPE):
    now=datetime.now(); tops=db_get("Topshirish")
    for r in tops:
        if r.get("Status","")!="tasdiqlangan": continue
        try: tt=datetime.strptime(r.get("Sana",""),"%Y-%m-%d %H:%M")
        except: continue
        hours=(now-tt).total_seconds()/3600
        if not (23.5<=hours<=24.5): continue
        did=str(r.get("Dist_ID",""))
        if not did: continue
        try:
            dkid=str(r.get("Dokon_ID","")); stores=db_get("Dokonlar")
            s=next((x for x in stores if str(x.get("ID",""))==dkid),None)
            tel=s.get("Tel1","") if s else ""
            await ctx.bot.send_message(int(did),
                f"📋 <b>24 SOAT ESLATMASI</b>\n🏪 {r.get('Dokon','')}\n📞 {tel}\n📦 {r.get('Mahsulot','')}: {r.get('Miqdor','')}\n\nYangi zakaz bormi? Bog'laning!",
                parse_mode="HTML")
        except Exception as e: logger.error(f"job_24h: {e}")

async def job_5kun(ctx:ContextTypes.DEFAULT_TYPE):
    now=datetime.now(); besh=(now-timedelta(days=5)).strftime("%Y-%m-%d"); olti=(now-timedelta(days=6)).strftime("%Y-%m-%d")
    tops=db_get("Topshirish"); buyurtmalar=db_get("Buyurtmalar")
    for top in tops:
        if top.get("Status","")!="tasdiqlangan": continue
        sana=str(top.get("Sana",""))[:10]
        if not (olti<=sana<=besh): continue
        did=str(top.get("Dist_ID","")); dkid=str(top.get("Dokon_ID","")); mahsulot=top.get("Mahsulot","")
        if not did: continue
        yangi=any(b for b in buyurtmalar if str(b.get("Dokon_ID",""))==dkid and b.get("Mahsulot","")==mahsulot and str(b.get("Sana",""))[:10]>=besh)
        if yangi: continue
        try:
            stores=db_get("Dokonlar"); s=next((x for x in stores if str(x.get("ID",""))==dkid),None)
            tel=s.get("Tel1","") if s else ""
            qty=float(top.get("Miqdor",0) or 0); unit=top.get("Birlik","")
            await ctx.bot.send_message(int(did),
                f"🔄 <b>TOVAR ALMASHTIRISH</b>\n🏪 {top.get('Dokon','')}\n📞 {tel}\n📦 {mahsulot}: {fmtq(qty,unit,mahsulot,False)}\n📅 {sana}\n\n⚠️ 5 kun o'tdi, yangi zakaz yo'q!\nTovarni almashtirish vaqti!",
                parse_mode="HTML")
        except Exception as e: logger.error(f"job_5kun: {e}")

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN: print("BOT_TOKEN yo'q!"); return
    app=Application.builder().token(BOT_TOKEN).build()
    app.job_queue.run_daily(job_qarz,  time=dtime(9,0))
    app.job_queue.run_daily(job_zakaz, time=dtime(20,0))
    app.job_queue.run_daily(job_5kun,  time=dtime(10,0))
    app.job_queue.run_repeating(job_24h, interval=3600, first=60)

    # /start komandani Telegram da ko'rsatish (klaviaturada / bossalar chiqadi)
    async def post_init(application):
        from telegram import BotCommand
        try:
            await application.bot.set_my_commands([
                BotCommand("start", "Botni ishga tushirish"),
            ])
            logger.info("Commands set OK")
        except Exception as e:
            logger.error(f"set_my_commands: {e}")
    app.post_init = post_init

    txt=filters.TEXT&~filters.COMMAND
    ptxt=(filters.PHOTO|filters.TEXT)&~filters.COMMAND
    ctxt=(filters.CONTACT|filters.TEXT)&~filters.COMMAND
    lctxt=(filters.LOCATION|filters.TEXT)&~filters.COMMAND

    conv=ConversationHandler(
        entry_points=[CommandHandler("start",start)],
        states={
            S_LANG:         [CallbackQueryHandler(lang_cb,pattern="^lang:")],
            S_REG_NAME:     [MessageHandler(txt,reg_name)],
            S_REG_FNAME:    [MessageHandler(txt,reg_fname)],
            S_REG_PHONE:    [MessageHandler(ctxt,reg_phone)],
            S_REG_PASS:     [MessageHandler(ptxt,reg_pass),CallbackQueryHandler(reg_pass,pattern="^skip_pass$")],
            S_WAIT:         [MessageHandler(txt,wait_h),CallbackQueryHandler(wait_h,pattern="^resend$")],
            S_MAIN: [
                CallbackQueryHandler(main_cb,      pattern="^m:"),
                CallbackQueryHandler(zav_prod_cb,  pattern="^zav:"),
                CallbackQueryHandler(top_s_cb,     pattern="^top_s:"),
                CallbackQueryHandler(top_p_cb,     pattern="^top_p:"),
                CallbackQueryHandler(top_ocr_cb,   pattern="^ocr_"),
                CallbackQueryHandler(top_pay_cb,   pattern="^(pay:|voz:)"),
                CallbackQueryHandler(voz_prod_cb,  pattern="^voz_p:"),
                CallbackQueryHandler(top_confirm_cb,pattern="^top_(ok|back)$"),
                CallbackQueryHandler(tol_store_cb, pattern="^tol_s:"),
                CallbackQueryHandler(tol_full_cb,  pattern="^tol_full:"),
                CallbackQueryHandler(his_cb,       pattern="^his:"),
                CallbackQueryHandler(dok_add_cb,   pattern="^dok:add$"),
                CallbackQueryHandler(dok_zak_cb,   pattern="^dzak:"),
                CallbackQueryHandler(zprod_cb,     pattern="^zprod:"),
                CallbackQueryHandler(dok_edit_cb,  pattern="^dedit:"),
                CallbackQueryHandler(dok_ef_cb,    pattern="^ef:"),
                CallbackQueryHandler(nar_prod_cb,  pattern="^nar:"),
                CallbackQueryHandler(nar_type_cb,  pattern="^nt:"),
                CallbackQueryHandler(nar_d_cb,     pattern="^nar_d:"),
                CallbackQueryHandler(adm_cb,       pattern="^adm:"),
                CallbackQueryHandler(adm_narx_p_cb,pattern="^adm_narx:"),
                CallbackQueryHandler(adm_unit_cb,  pattern="^unit:"),
                MessageHandler(txt,lambda u,c: S_MAIN),
            ],
            S_ZAV_QTY:      [MessageHandler(txt,zav_qty)],
            S_TOP_PHOTO:    [MessageHandler(ptxt,top_photo),CallbackQueryHandler(top_ocr_cb,pattern="^ocr_")],
            S_TOP_PAY:      [CallbackQueryHandler(top_pay_cb,pattern="^(pay:|voz:|voz_p:)"),CallbackQueryHandler(voz_prod_cb,pattern="^voz_p:"),CallbackQueryHandler(top_confirm_cb,pattern="^top_(ok|back)$"),MessageHandler(txt,top_naqd)],
            S_TOP_NAQD:     [MessageHandler(txt,top_naqd)],
            S_VOZ_PROD:     [CallbackQueryHandler(voz_prod_cb,pattern="^voz_p:")],
            S_VOZ_QTY:      [MessageHandler(txt,voz_qty)],
            S_DI_NAME:      [MessageHandler(txt,di_name)],
            S_DI_ADDR:      [MessageHandler(txt,di_addr)],
            S_DI_MCHJ:      [MessageHandler(txt,di_mchj),CallbackQueryHandler(di_mchj,pattern="^di_skip:mchj$")],
            S_DI_TEL1:      [MessageHandler(ctxt,di_tel1)],
            S_DI_TEL2:      [MessageHandler(ctxt,di_tel2),CallbackQueryHandler(di_tel2,pattern="^di_skip:tel2$")],
            S_DI_EGA:       [MessageHandler(txt,di_ega)],
            S_DI_PHOTO:     [MessageHandler(ptxt,di_photo)],
            S_DI_LOC: [
                MessageHandler(filters.LOCATION, di_loc),
                MessageHandler(filters.TEXT & ~filters.COMMAND, di_loc),
            ],
            S_NARX_VAL:     [MessageHandler(txt,nar_val)],
            S_NARX_COST:    [MessageHandler(txt,nar_cost)],
            S_NARX_D_VAL:   [MessageHandler(txt,nar_d_val)],
            S_NARX_D_COST:  [MessageHandler(txt,nar_d_cost)],
            S_ZAK_FROM_QTY: [MessageHandler(txt,zak_from_qty)],
            S_DK_EDIT_VAL:  [MessageHandler(txt,dk_edit_val)],
            S_ADM_PROD_UZ:  [MessageHandler(txt,adm_prod_uz)],
            S_ADM_PROD_RU:  [MessageHandler(txt,adm_prod_ru)],
            S_ADM_PROD_UNIT:[CallbackQueryHandler(adm_unit_cb,pattern="^unit:")],
            S_ADM_NARX_VAL: [MessageHandler(txt,adm_narx_val)],
            S_ADM_NARX_COST:[MessageHandler(txt,adm_narx_cost)],
            S_ADM_DK_NAME:  [MessageHandler(txt,adm_dk_name)],
            S_ADM_DK_ADDR:  [MessageHandler(txt,adm_dk_addr)],
            S_ADM_DK_DIST:  [MessageHandler(txt,adm_dk_dist)],
            S_ADM_DIST_NAME:[MessageHandler(txt,adm_dist_name)],
            S_ADM_DIST_TG:  [MessageHandler(txt,adm_dist_tg)],
            S_ADM_BC:       [MessageHandler(txt,adm_bc)],
            S_TOLOV_SUMMA:  [MessageHandler(txt,tol_summa),CallbackQueryHandler(tol_full_cb,pattern="^tol_full:"),CallbackQueryHandler(tol_show,pattern="^m:tol$")],
        },
        fallbacks=[CommandHandler("start",start),CommandHandler("cancel",start)],
        allow_reentry=True,
    )
    for pat,h in [
        (r'^/approve_\d+$',approve_cmd),(r'^/reject_\d+$',reject_cmd),
        (r'^/zok_\w+$',zok_cmd),(r'^/zrad_\w+$',zrad_cmd),
        (r'^/tok_\w+$',tok_cmd),(r'^/trad_\w+$',trad_cmd),
        (r'^/vok_\w+$',vok_cmd),(r'^/vrad_\w+$',vrad_cmd),
    ]: app.add_handler(MessageHandler(filters.Regex(pat),h))
    app.add_handler(conv)
    print("Alba Milk Bot v5.0 ✅")
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__":
    main()
