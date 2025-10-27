import io, os, traceback, mimetypes
from datetime import datetime
from fractions import Fraction
from typing import Optional

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from openpyxl import load_workbook, Workbook
from PIL import Image, ImageDraw, ImageFont

# ========= إعدادات عامة =========
ENABLE_EXIF   = True
STAMP_ON_SAVE = True
IMAGES_PREFIX = "images/"           # المسار داخل البكت
EXCEL_KEY     = "TaskLog.xlsx"      # اسم ملف الإكسل داخل البكت
BASE_URL      = os.environ.get("BASE_URL")  # اختياري: https://field-uploads.onrender.com

# ========= Backblaze B2 =========
from b2sdk.v2 import (
    InMemoryAccountInfo, B2Api, UploadSourceBytes, UploadSourceLocalFile,
    DownloadDestBytes, DownloadDestLocalFile, FileVersionInfo,
)

_B2 = None
_BUCKET = None

def b2_connect():
    """Connect once to B2 using env vars."""
    global _B2, _BUCKET
    if _B2 and _BUCKET:
        return _B2, _BUCKET

    key_id   = os.environ.get("B2_KEY_ID")
    app_key  = os.environ.get("B2_APP_KEY")
    bucket   = os.environ.get("B2_BUCKET")

    if not (key_id and app_key and bucket):
        raise RuntimeError(
            "Missing B2 env vars. Please set B2_KEY_ID, B2_APP_KEY, B2_BUCKET."
        )
    info = InMemoryAccountInfo()
    _B2 = B2Api(info)
    _B2.authorize_account("production", key_id, app_key)
    _BUCKET = _B2.get_bucket_by_name(bucket)
    return _B2, _BUCKET

def b2_upload_local(local_path: str, remote_key: str) -> FileVersionInfo:
    _, bucket = b2_connect()
    src = UploadSourceLocalFile(local_path)
    return bucket.upload(src, remote_key)

def b2_upload_bytes(content: bytes, remote_key: str, content_type: Optional[str] = None) -> FileVersionInfo:
    _, bucket = b2_connect()
    if content_type is None:
        content_type = "application/octet-stream"
    src = UploadSourceBytes(content, file_name=remote_key, content_type=content_type)
    return bucket.upload(src, remote_key, content_type=content_type)

def b2_download_bytes(remote_key: str) -> bytes:
    _, bucket = b2_connect()
    dest = DownloadDestBytes()
    bucket.download_file_by_name(remote_key, dest)
    return dest.get_bytes_written()

def b2_download_to(local_path: str, remote_key: str) -> bool:
    _, bucket = b2_connect()
    try:
        dest = DownloadDestLocalFile(local_path)
        bucket.download_file_by_name(remote_key, dest)
        return True
    except Exception:
        return False

def b2_exists(remote_key: str) -> bool:
    _, bucket = b2_connect()
    try:
        # List by prefix; if we get anything exact, it exists
        for f in bucket.ls(prefix=remote_key, show_versions=False):
            if f[0].file_name == remote_key:
                return True
        return False
    except Exception:
        return False

def b2_list_images(limit: int = 200):
    _, bucket = b2_connect()
    out = []
    for info, _ in bucket.ls(prefix=IMAGES_PREFIX, show_versions=False):
        if info.file_name.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
            out.append(info.file_name)
            if len(out) >= limit:
                break
    # أحدث الملفات في النهاية عادة، فلنرتّب تنازليًا بالاسم (يحوي timestamp لدينا)
    return sorted(out, reverse=True)

# ========= أدوات الصورة / EXIF =========
try:
    import piexif
    PEX_OK = True
except Exception as e:
    print("[WARN] piexif not available:", e)
    PEX_OK = False

def _rat(x: float):
    from fractions import Fraction as _F
    fr = _F(x).limit_denominator()
    return (fr.numerator, fr.denominator)

def _deg_to_dms_rationals(deg_float: float):
    deg = int(abs(deg_float))
    minutes_float = (abs(deg_float) - deg) * 60
    minutes = int(minutes_float)
    seconds = (minutes_float - minutes) * 60
    return (_rat(deg), _rat(minutes), _rat(round(seconds, 2)))

def write_gps_exif_jpeg_inplace(local_jpeg_path: str, lat_s: str, lon_s: str) -> bool:
    if not (ENABLE_EXIF and PEX_OK):
        return False
    try:
        lat = float(lat_s); lon = float(lon_s)
    except Exception:
        print("[INFO] Skip EXIF: invalid lat/lon", lat_s, lon_s)
        return False
    lat_ref = "N" if lat >= 0 else "S"
    lon_ref = "E" if lon >= 0 else "W"
    lat_dms = _deg_to_dms_rationals(lat)
    lon_dms = _deg_to_dms_rationals(lon)
    try:
        try:
            exif_dict = piexif.load(local_jpeg_path)
        except Exception:
            exif_dict = {"0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None}
        # طهّر أي IFD قد يسبب خطأ
        if not isinstance(exif_dict.get("0th"), dict): exif_dict["0th"] = {}
        if not isinstance(exif_dict.get("1st"), dict): exif_dict["1st"] = {}
        exif_dict["Exif"] = {}
        if "GPS" not in exif_dict: exif_dict["GPS"] = {}

        gps = exif_dict["GPS"]
        gps[piexif.GPSIFD.GPSVersionID]    = (2, 3, 0, 0)
        gps[piexif.GPSIFD.GPSLatitudeRef]  = lat_ref.encode("ascii")
        gps[piexif.GPSIFD.GPSLatitude]     = lat_dms
        gps[piexif.GPSIFD.GPSLongitudeRef] = lon_ref.encode("ascii")
        gps[piexif.GPSIFD.GPSLongitude]    = lon_dms

        exif_bytes = piexif.dump(exif_dict)
        with Image.open(local_jpeg_path) as im:
            if im.mode != "RGB":
                im = im.convert("RGB")
            im.save(local_jpeg_path, "JPEG", quality=95, exif=exif_bytes)
        return True
    except Exception as e:
        print("[WARN] EXIF write failed:", e)
        return False

def ensure_jpeg(local_path: str) -> str:
    base, ext = os.path.splitext(local_path.lower())
    if ext in [".jpg", ".jpeg"]:
        return local_path
    out = base + ".jpg"
    with Image.open(local_path) as im:
        if im.mode != "RGB":
            im = im.convert("RGB")
        im.save(out, "JPEG", quality=95)
    try:
        os.remove(local_path)
    except Exception:
        pass
    return out

def stamp_text_on_image(local_path: str, lines, margin=16, line_spacing=10, scale=4) -> bool:
    """يطبع النص مباشرة على الصورة (بدون خلفية) بخط كبير بسيط."""
    try:
        with Image.open(local_path).convert("RGB") as im:
            draw = ImageDraw.Draw(im)
            # خط افتراضي (لاتيني/أرقام) — لا يعتمد على ملفات النظام
            font = ImageFont.load_default()
            # قياسات الأسطر تقريبية بالـ textbbox
            W, H = im.size
            y_accum = 0
            heights = []
            for line in lines:
                bbox = draw.textbbox((0, 0), line, font=font)
                h = (bbox[3] - bbox[1]) * scale
                heights.append(h)
                y_accum += h + line_spacing * scale
            y_start = max(margin, H - margin - y_accum)
            x = margin
            y = y_start
            for i, line in enumerate(lines):
                # تكبير الخط عن طريق رسمه عدة مرات بإزاحات بسيطة (حيلة بسيطة)
                for dx, dy in ((0,0),(1,0),(0,1),(1,1)):
                    draw.text((x+dx, y+dy), line, fill=(0,0,0), font=font)
                draw.text((x, y), line, fill=(255,255,255), font=font)
                y += heights[i] + line_spacing * scale
            im.save(local_path, "JPEG", quality=95)
        return True
    except Exception as e:
        print("[WARN] stamping failed:", e)
        return False

# ========= Excel in B2 =========
def ensure_excel_exists_locally(tmp_excel: str) -> None:
    """ينزّل TaskLog.xlsx من B2 إن وُجد، وإلا ينشئ ملفًا جديدًا."""
    if b2_download_to(tmp_excel, EXCEL_KEY):
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "TaskLog"
    ws.append(["رقم المهمة", "رقم المحطة", "ملاحظات", "اسم الصورة",
               "Latitude", "Longitude", "Timestamp", "Image URL"])
    wb.save(tmp_excel)

def append_record_to_excel_and_upload(task_id, station_id, note, img_name, lat, lon, img_url):
    tmp_excel = "/tmp/TaskLog.xlsx"
    ensure_excel_exists_locally(tmp_excel)
    wb = load_workbook(tmp_excel)
    ws = wb["TaskLog"]
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws.append([task_id, station_id, note, img_name, lat, lon, ts, img_url])
    # Hyperlink في العمود الثامن
    link_cell = ws.cell(row=ws.max_row, column=8)
    link_cell.hyperlink = img_url
    link_cell.style = "Hyperlink"
    wb.save(tmp_excel)
    b2_upload_local(tmp_excel, EXCEL_KEY)

# ========= Flask =========
app = Flask(__name__)
CORS(app)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50MB

@app.get("/ping")
def ping():
    try:
        b2_connect()
        ok = True
    except Exception as e:
        ok = False
    return jsonify({"ok": ok}), 200 if ok else 500

@app.get("/")
def root():
    return "<h3>FieldUploads server ✅</h3><p>Upload to POST /upload</p><p><a href='/gallery'>Gallery</a></p>"

@app.get("/images/<path:fname>")
def get_image_proxy(fname: str):
    """يبثّ الصورة مباشرة من B2 (لا حاجة إلى جعل البكت Public)."""
    if not fname.startswith(IMAGES_PREFIX):
        key = IMAGES_PREFIX + fname
    else:
        key = fname
    try:
        data = b2_download_bytes(key)
        mime = mimetypes.guess_type(fname)[0] or "image/jpeg"
        return send_file(io.BytesIO(data), mimetype=mime)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 404

@app.get("/gallery")
def gallery():
    items = b2_list_images(limit=120)
    def card(key):
        name = key.split("/", 1)[-1]
        return f"<div style='margin:8px'><img src='/images/{name}' style='max-width:240px;display:block'><small>{name}</small></div>"
    body = "".join(card(k) for k in items)
    return f"<html><body style='font-family:sans-serif;padding:12px'><h3>Gallery</h3><div style='display:flex;flex-wrap:wrap'>{body}</div></body></html>"

@app.post("/upload")
def upload():
    try:
        if "image" not in request.files:
            return jsonify({"ok": False, "error": "no image part"}), 400

        image = request.files["image"]
        task_id    = (request.form.get("task_id")    or "").strip()
        station_id = (request.form.get("station_id") or "").strip()
        note       = (request.form.get("note")       or "").strip()
        latitude   = (request.form.get("latitude")   or "").strip()
        longitude  = (request.form.get("longitude")  or "").strip()

        if not task_id:
            return jsonify({"ok": False, "error": "task_id required"}), 400
        if image.filename == "":
            return jsonify({"ok": False, "error": "empty filename"}), 400

        # حفظ مؤقت محلي
        tmp_path = f"/tmp/{task_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{image.filename}"
        image.save(tmp_path)

        # تأكد من JPG + EXIF + ختم
        final_path = ensure_jpeg(tmp_path)
        exif_ok = write_gps_exif_jpeg_inplace(final_path, latitude, longitude)
        stamp_ok = False
        if STAMP_ON_SAVE:
            ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fname_only = os.path.basename(final_path)
            stamp_ok = stamp_text_on_image(
                final_path,
                [
                    f"Task: {task_id}  |  Station: {station_id}",
                    f"Lat: {latitude}  |  Lon: {longitude}",
                    f"Time: {ts_str}",
                    f"File: {fname_only}",
                ],
            )

        # رفع إلى B2
        fname_only = os.path.basename(final_path)
        b2_key = IMAGES_PREFIX + fname_only
        b2_upload_local(final_path, b2_key)

        # رابط الصورة عبر خادمنا (بروكسي)
        base = (BASE_URL or request.host_url).rstrip("/")
        img_url = f"{base}/images/{fname_only}"

        # حدّث السجل وارفعه إلى B2
        append_record_to_excel_and_upload(task_id, station_id, note, fname_only, latitude, longitude, img_url)

        # تنظيف الملف المؤقت
        try:
            os.remove(final_path)
        except Exception:
            pass

        return jsonify({
            "ok": True,
            "saved": fname_only,
            "url": img_url,
            "exif_gps_written": bool(exif_ok and ENABLE_EXIF and PEX_OK),
            "stamped": stamp_ok
        }), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
