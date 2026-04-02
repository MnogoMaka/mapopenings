#!/usr/bin/env python3
"""
Визуализатор разрытий и камер.
Фотографии подгружаются напрямую из Yandex Cloud Object Storage.
Зависимости: pip install dash pandas folium requests flask
Запуск: python app.py  ->  http://127.0.0.1:8050/
"""

import json
import os
import time
import threading
import requests
import xml.etree.ElementTree as ET
import folium
from folium.plugins import MarkerCluster
import pandas as pd
import dash
from dash import dcc, html, Input, Output, State, dash_table
from flask import Response
from dotenv import load_dotenv
load_dotenv()
# ══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ══════════════════════════════════════════════════════════════
RESULT_JSON  = "result.json"
MAP_CENTER   = [55.75, 37.62]
MAP_ZOOM     = 10

# Yandex Cloud — OAuth токен для загрузки фото
# Получить: https://oauth.yandex.ru/authorize?response_type=token&client_id=1a6990aa636648e9b2ef855fa7bec2fb
OAUTH_TOKEN  = os.getenv('OAUTH_TOKEN')

BUCKET       = "kube-cxm-cni"
BASE_PREFIX  = "pvc-a6daa919-5f6c-4bca-8838-7d3f103e5fae/cctv/day"
STORAGE_URL  = "https://storage.yandexcloud.net"
IAM_URL      = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
IMAGE_EXTS   = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
# ══════════════════════════════════════════════════════════════

ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
os.makedirs(ASSETS_DIR, exist_ok=True)
MAP_FILE = os.path.join(ASSETS_DIR, "map.html")

# ── IAM-токен (обновляется автоматически каждые 10 часов) ─────
S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"

class IamToken:
    def __init__(self):
        self._token    = None
        self._lock     = threading.Lock()
        self._expires  = 0

    def get(self) -> str | None:
        if OAUTH_TOKEN == "ВАШ_OAUTH_ТОКЕН":
            return None   # фото отключены — токен не задан
        with self._lock:
            if time.time() < self._expires:
                return self._token
            try:
                resp = requests.post(
                    IAM_URL,
                    json={"yandexPassportOauthToken": OAUTH_TOKEN},
                    timeout=10,
                )
                resp.raise_for_status()
                self._token   = resp.json()["iamToken"]
                self._expires = time.time() + 36000  # 10 часов
                print("  ✅ IAM-токен обновлён", flush=True)
            except Exception as e:
                print(f"  ⚠️  IAM-токен: {e}", flush=True)
                self._token = None
            return self._token

iam = IamToken()

def yandex_session() -> requests.Session | None:
    token = iam.get()
    if not token:
        return None
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}"})
    return s

def find_image_key(session: requests.Session, shortname: str) -> str | None:
    """Ищет первый файл-изображение в папке shortname в бакете."""
    prefix = f"{BASE_PREFIX}/{shortname}/"
    try:
        resp = session.get(
            f"{STORAGE_URL}/{BUCKET}",
            params={"prefix": prefix, "list-type": "2"},
            timeout=10,
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        for node in root.findall(f".//{{{S3_NS}}}Key"):
            k = node.text or ""
            if os.path.splitext(k)[-1].lower() in IMAGE_EXTS:
                return k
    except Exception:
        pass
    return None

# ── Загрузка данных ────────────────────────────────────────────
print("Загрузка данных...", flush=True)
with open(RESULT_JSON, encoding="utf-8") as f:
    DATA = json.load(f)
print(f"Загружено {len(DATA)} ордеров", flush=True)

ALL_TYPES = sorted({
    cam.get("type", "")
    for rec in DATA
    for cam in rec.get("cameras", [])
    if cam.get("type")
})

def parse_date(v):
    try:
        return pd.to_datetime(str(v)).date()
    except Exception:
        return None

all_dates_start = [d for d in (parse_date(r.get("date_start")) for r in DATA) if d]
all_dates_end   = [d for d in (parse_date(r.get("date_end"))   for r in DATA) if d]
MIN_DATE = str(min(all_dates_start)) if all_dates_start else "2020-01-01"
MAX_DATE = str(max(all_dates_end))   if all_dates_end   else "2030-12-31"

folium.Map(location=MAP_CENTER, zoom_start=MAP_ZOOM).save(MAP_FILE)

PHOTOS_ENABLED = OAUTH_TOKEN != "ВАШ_OAUTH_ТОКЕН"
print(f"Фото из Яндекса: {'✅ включены' if PHOTOS_ENABLED else '⚠️  токен не задан'}", flush=True)
print("Сервер запускается...", flush=True)

COLORS = {"header": "#1a237e", "panel": "#f0f2f5", "border": "#dce0e8", "accent": "#1565c0"}

app = dash.Dash(__name__, title="Разрытия & Камеры")

# ── Flask-маршрут: проксирует фото из Яндекса ─────────────────
@app.server.route("/photo/<path:shortname>")
def get_photo(shortname):
    session = yandex_session()
    if session is None:
        return Response("OAuth токен не задан", status=503)

    key = find_image_key(session, shortname)
    if not key:
        return Response("Фото не найдено", status=404)

    try:
        r = session.get(f"{STORAGE_URL}/{BUCKET}/{key}", stream=True, timeout=30)
        r.raise_for_status()
        ext = os.path.splitext(key)[-1].lower()
        mime = {
            ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
            ".png": "image/png",  ".webp": "image/webp",
        }.get(ext, "image/jpeg")
        return Response(r.content, content_type=mime)
    except Exception as e:
        return Response(str(e), status=500)

# ── Layout ─────────────────────────────────────────────────────
app.layout = html.Div([

    html.Div([
        html.H2("📍 Разрытия и камеры наблюдения",
                style={"margin": "0", "color": "#fff", "fontSize": "20px"}),
        html.Span(
            f"Загружено {len(DATA):,} ордеров | Даты: {MIN_DATE} — {MAX_DATE} | "
            f"Фото: {'🟢 подключены' if PHOTOS_ENABLED else '🔴 токен не задан'}",
            style={"color": "#90caf9", "fontSize": "13px"},
        ),
    ], style={"background": COLORS["header"], "padding": "14px 24px",
              "display": "flex", "alignItems": "center", "gap": "24px"}),

    html.Div([
        html.Div([
            html.Label("📅 Дата начала:", style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
            html.Div([
                html.Span("От:", style={"fontSize": "12px", "marginRight": "4px"}),
                dcc.Input(id="date-from", type="text", value=MIN_DATE,
                          style={"width": "120px", "marginRight": "8px", "fontSize": "13px", "padding": "4px"}),
            ], style={"marginBottom": "6px"}),
            html.Div([
                html.Span("До:", style={"fontSize": "12px", "marginRight": "4px"}),
                dcc.Input(id="date-to", type="text", value=MAX_DATE,
                          style={"width": "120px", "fontSize": "13px", "padding": "4px"}),
            ]),
        ], style={"flex": "1.5", "minWidth": "180px"}),

        html.Div([
            html.Label("📷 Типы камер:", style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
            dcc.Checklist(
                id="camtype-filter",
                options=[{"label": f"  {t}", "value": t} for t in ALL_TYPES],
                value=ALL_TYPES,
                labelStyle={"display": "block", "marginBottom": "4px", "fontSize": "13px"},
            ),
        ], style={"flex": "2", "minWidth": "200px"}),

        html.Div([
            html.Label("🔎 Разрытия:", style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
            dcc.RadioItems(
                id="dig-filter",
                options=[
                    {"label": "  Все",               "value": "all"},
                    {"label": "  Только с камерами", "value": "with"},
                    {"label": "  Только без камер",  "value": "without"},
                ],
                value="all",
                labelStyle={"display": "block", "marginBottom": "4px", "fontSize": "13px"},
            ),
        ], style={"flex": "1.5", "minWidth": "160px"}),

        html.Div([
            html.Label("🗂 Слои:", style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
            dcc.Checklist(
                id="layer-toggle",
                options=[
                    {"label": "  🟠 Разрытия",      "value": "digs"},
                    {"label": "  🔵 Камеры",         "value": "cameras"},
                    {"label": "  🟢 Зоны радиуса",  "value": "circles"},
                    {"label": "  ➖ Линии связи",    "value": "lines"},
                ],
                value=["digs", "cameras", "circles", "lines"],
                labelStyle={"display": "block", "marginBottom": "4px", "fontSize": "13px"},
            ),
        ], style={"flex": "1", "minWidth": "160px"}),

        html.Div([
            html.Label("📖 Легенда:", style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
            html.Div("🟠 Разрытие (есть камеры)", style={"fontSize": "12px", "marginBottom": "3px"}),
            html.Div("⚫ Разрытие (нет камер)",   style={"fontSize": "12px", "marginBottom": "3px"}),
            html.Div("🔵 Камера в зоне",           style={"fontSize": "12px", "marginBottom": "3px"}),
            html.Div("🔘 Камера вне зоны",         style={"fontSize": "12px", "marginBottom": "3px"}),
            html.Div("➖ Линия связи",             style={"fontSize": "12px", "color": "#1565c0"}),
        ], style={"flex": "1.5", "minWidth": "160px"}),

        html.Div([
            html.Button(
                "🗺 Построить карту", id="build-btn", n_clicks=0,
                style={
                    "background": COLORS["accent"], "color": "#fff",
                    "border": "none", "borderRadius": "6px",
                    "padding": "10px 20px", "fontSize": "14px",
                    "cursor": "pointer", "fontWeight": "bold",
                    "width": "100%", "marginBottom": "12px",
                }
            ),
            html.Div(id="stats-bar", style={"fontSize": "13px", "lineHeight": "2"}),
        ], style={"flex": "1.5", "minWidth": "180px"}),

    ], style={
        "display": "flex", "gap": "24px", "flexWrap": "wrap",
        "padding": "16px 24px", "background": COLORS["panel"],
        "borderBottom": f"1px solid {COLORS['border']}", "alignItems": "flex-start",
    }),

    html.Div(id="map-status", style={
        "padding": "6px 24px", "fontSize": "13px", "color": "#555",
        "background": "#fff", "borderBottom": f"1px solid {COLORS['border']}",
    }),

    html.Div(
        html.Iframe(id="map-frame", src="/assets/map.html",
                    style={"width": "100%", "height": "100%", "border": "none"}),
        style={"height": "58vh", "padding": "8px 24px"}
    ),

    html.Div([
        html.H4("Пары ордер — камера", style={"margin": "0 0 10px", "color": COLORS["header"]}),
        html.Div(id="info-table"),
    ], style={"padding": "16px 24px 32px"}),

], style={"fontFamily": "Segoe UI, Arial, sans-serif", "background": "#fafafa", "minHeight": "100vh"})


def cam_popup_html(cam: dict, is_near: bool) -> str:
    shortname = cam.get("shortname", "")
    icon = "🔵" if is_near else "🔘"

    if PHOTOS_ENABLED and shortname:
        url = "/photo/" + shortname
        photo_block = """
<a href="{url}" target="_blank" style="display:block;text-decoration:none;margin-top:10px">
  <div style="width:280px;height:180px;background:#e0e0e0;border-radius:8px 8px 0 0;
              border:2px solid #1565c0;display:flex;align-items:center;
              justify-content:center;overflow:hidden;position:relative">
    <span style="color:#aaa;font-size:12px;position:absolute">&#9200; Загрузка...</span>
    <img data-src="{url}" width="280" height="180"
         style="object-fit:cover;width:280px;height:180px;display:block;position:relative;z-index:1"
         onerror="this.style.display=&apos;none&apos;;this.previousSibling.textContent=&apos;Фото не найдено&apos;">
  </div>
  <div style="background:#1565c0;color:#fff;text-align:center;padding:8px;
              border-radius:0 0 8px 8px;font-size:13px;font-weight:bold">
    🔍 Открыть полный размер
  </div>
</a>""".format(url=url)
    else:
        photo_block = (
            "<div style='margin-top:8px;padding:10px;background:#f5f5f5;"
            "border-radius:8px;color:#aaa;text-align:center;font-size:12px'>"
            "📷 Фото не подключены</div>"
        )

    info = (
        "<div style='min-width:280px;font-size:13px'>"
        "<b>" + icon + " Камера</b>"
        "<hr style='margin:4px 0'>"
        "<b>Shortname:</b> <span style='color:#1565c0;font-weight:bold'>"
        + (shortname or "—") + "</span><br>"
        "<b>ID:</b> " + str(cam.get("camera_id", "")) + "<br>"
        "<b>Тип:</b> " + str(cam.get("type", "")) + "<br>"
        "<b>Адрес:</b> " + str(cam.get("address", "")) + "<br>"
        "<b>Модель:</b> " + str(cam.get("model", "")) + "<br>"
        "<b>Статус:</b> " + str(cam.get("status", ""))
        + photo_block
        + "</div>"
    )
    return info

def build_and_save_map(records, layers):
    m = folium.Map(location=MAP_CENTER, zoom_start=MAP_ZOOM, tiles="OpenStreetMap")
    cam_ids_near = {cam.get("camera_id") for rec in records for cam in rec["_visible_cams"]}

    if "circles" in layers:
        cg = folium.FeatureGroup(name="🟢 Зоны радиуса", show=True)
        for rec in records:
            if rec["_visible_cams"]:
                folium.Circle(
                    location=[rec["centroid_lat"], rec["centroid_lng"]],
                    radius=rec.get("search_radius_m", 500),
                    color="#2e7d32", fill=True, fill_color="#2e7d32",
                    fill_opacity=0.08, weight=1.5,
                ).add_to(cg)
        cg.add_to(m)

    if "lines" in layers:
        lg = folium.FeatureGroup(name="➖ Линии связи", show=True)
        for rec in records:
            for cam in rec["_visible_cams"]:
                sn = cam.get("shortname") or cam.get("camera_id", "")
                folium.PolyLine(
                    locations=[[rec["centroid_lat"], rec["centroid_lng"]],
                               [cam["lat"], cam["lng"]]],
                    color="#1565c0", weight=1.2, opacity=0.4,
                    tooltip=f"Ордер {rec.get('order_number','')} ↔ {sn} | {round(cam.get('distance_m',0))} м",
                ).add_to(lg)
        lg.add_to(m)

    if "digs" in layers:
        dc = MarkerCluster(name="🟠 Разрытия",
                           options={"maxClusterRadius": 40, "showCoverageOnHover": False})
        for rec in records:
            has = bool(rec["_visible_cams"])
            r   = rec.get("search_radius_m", "?")
            folium.Marker(
                location=[rec["centroid_lat"], rec["centroid_lng"]],
                popup=folium.Popup(
                    f"<div style='min-width:240px;font-size:13px'>"
                    f"<b>🟠 Разрытие</b><hr style='margin:4px 0'>"
                    f"<b>Ордер:</b> {rec.get('order_number','')}<br>"
                    f"<b>Работы:</b> {rec.get('work_types','')}<br>"
                    f"<b>Заказчик:</b> {rec.get('contractor','')}<br>"
                    f"<b>Период:</b> {rec.get('date_start','')} — {rec.get('date_end','')}<br>"
                    f"<b>Камер в радиусе {r}м:</b> "
                    f"<span style='color:{'#c62828' if has else '#555'}'>{len(rec['_visible_cams'])}</span>"
                    f"</div>",
                    max_width=300,
                ),
                tooltip=f"Ордер {rec.get('order_number','')} | {len(rec['_visible_cams'])} камер",
                icon=folium.Icon(
                    color="orange" if has else "gray",
                    icon="exclamation-sign" if has else "minus-sign",
                    prefix="glyphicon",
                ),
            ).add_to(dc)
        dc.add_to(m)

    if "cameras" in layers:
        cam_fg = folium.FeatureGroup(name="🔵 Камеры", show=True)
        all_cams = {}
        for rec in records:
            for cam in rec.get("cameras", []):
                all_cams[cam["camera_id"]] = cam

        for cid, cam in all_cams.items():
            is_near = cid in cam_ids_near
            color   = "#1565c0" if is_near else "#546e7a"
            radius  = 6 if is_near else 4
            sn      = cam.get("shortname") or cid
            folium.CircleMarker(
                location=[cam["lat"], cam["lng"]],
                radius=radius,
                color=color, fill=True, fill_color=color,
                fill_opacity=0.9, weight=1.5,
                popup=folium.Popup(cam_popup_html(cam, is_near), max_width=300),
                tooltip=f"📷 {sn} | {cam.get('type','')}",
            ).add_to(cam_fg)
        cam_fg.add_to(m)


    # Lazy-load: фото грузится только при открытии попапа
    lazy_script = """
    <script>
    (function() {
        function init() {
            var found = false;
            for (var key in window) {
                try {
                    var obj = window[key];
                    if (obj && typeof obj.on === 'function' && obj._container) {
                        obj.on('popupopen', function(e) {
                            e.popup._contentNode
                             .querySelectorAll('img[data-src]')
                             .forEach(function(img) {
                                 img.src = img.getAttribute('data-src');
                                 img.removeAttribute('data-src');
                             });
                        });
                        found = true;
                    }
                } catch(err) {}
            }
            if (!found) { setTimeout(init, 300); }
        }
        setTimeout(init, 400);
    })();
    </script>
    """
    from branca.element import Element
    m.get_root().html.add_child(Element(lazy_script))
    folium.LayerControl(collapsed=False).add_to(m)
    m.save(MAP_FILE)


@app.callback(
    Output("map-frame",  "src"),
    Output("map-status", "children"),
    Output("info-table", "children"),
    Output("stats-bar",  "children"),
    Input("build-btn",      "n_clicks"),
    State("camtype-filter", "value"),
    State("dig-filter",     "value"),
    State("layer-toggle",   "value"),
    State("date-from",      "value"),
    State("date-to",        "value"),
    prevent_initial_call=True,
)
def update(n_clicks, cam_types, dig_filter, layers, date_from, date_to):
    cam_types = set(cam_types or [])
    layers    = layers or []
    try:
        df = pd.to_datetime(date_from).date() if date_from else None
        dt = pd.to_datetime(date_to).date()   if date_to   else None
    except Exception:
        df = dt = None

    records = []
    for rec in DATA:
        if df or dt:
            d = parse_date(rec.get("date_start"))
            if d:
                if df and d < df: continue
                if dt and d > dt: continue
        visible_cams = [c for c in rec.get("cameras", []) if c.get("type") in cam_types]
        if dig_filter == "with"    and not visible_cams: continue
        if dig_filter == "without" and visible_cams:     continue
        records.append({**rec, "_visible_cams": visible_cams})

    print(f"⏳ Строю карту: {len(records)} разрытий...", flush=True)
    build_and_save_map(records, layers)
    src = f"/assets/map.html?v={int(time.time())}"

    TABLE_LIMIT = 5000
    rows = []
    for rec in records:
        for cam in rec["_visible_cams"]:
            if len(rows) >= TABLE_LIMIT: break
            rows.append({
                "Ордер":         rec.get("order_number", ""),
                "Дата начала":   rec.get("date_start", ""),
                "Дата конца":    rec.get("date_end", ""),
                "Shortname":     cam.get("shortname", ""),
                "Камера ID":     cam.get("camera_id", ""),
                "Тип камеры":    cam.get("type", ""),
                "Адрес камеры":  cam.get("address", ""),
                "Расстояние, м": cam.get("distance_m", ""),
            })

    table_el = dash_table.DataTable(
        data=rows,
        columns=[{"name": c, "id": c} for c in rows[0].keys()] if rows else [],
        page_size=20, sort_action="native", filter_action="native",
        style_table={"overflowX": "auto"},
        style_cell={"fontSize": "13px", "padding": "6px 10px", "textAlign": "left"},
        style_header={"background": COLORS["header"], "color": "#fff", "fontWeight": "bold"},
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#f5f7ff"},
            {"if": {"column_id": "Shortname"}, "fontWeight": "bold", "color": "#1565c0"},
        ],
        style_filter={"backgroundColor": "#e8eaf6"},
    ) if rows else html.P("Нет совпадений.", style={"color": "#888", "fontStyle": "italic"})

    with_cams = sum(1 for r in records if r["_visible_cams"])
    cam_total = len({c.get("camera_id") for r in records for c in r["_visible_cams"]})
    lines_n   = sum(len(r["_visible_cams"]) for r in records)

    stats = html.Div([
        html.Div([html.B(f"{len(DATA):,}"),    "  всего ордеров"]),
        html.Div([html.B(f"{len(records):,}"), "  по фильтру"]),
        html.Div([html.B(str(with_cams), style={"color": "#c62828"}), "  с камерами"]),
        html.Div([html.B(str(cam_total), style={"color": COLORS["accent"]}), "  уникальных камер"]),
        html.Div([html.B(f"{lines_n:,}"), "  линий связи"]),
    ])

    status = f"✅ Карта построена: {len(records)} разрытий, {cam_total} камер"
    print(status, flush=True)
    return src, status, table_el, stats


if __name__ == "__main__":
    print(f"\n✅ Данные загружены: {len(DATA):,} ордеров")
    print(f"   Диапазон дат: {MIN_DATE} — {MAX_DATE}")
    print(f"   Фото из Яндекса: {'включены' if PHOTOS_ENABLED else 'выключены (задайте OAUTH_TOKEN)'}")
    print("✅ Открыть: http://127.0.0.1:8050/\n")
    app.run(debug=False, port=8050)
