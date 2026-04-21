#!/usr/bin/env python3
"""
Визуализатор разрытий и камер.
Данные загружаются из PostgreSQL (схема penovation_ii).
Фотографии подгружаются из Yandex Cloud Object Storage.
Зависимости: pip install -r requirements.txt
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
from dash import dcc, html, Input, Output, State, dash_table, no_update, ctx
from flask import Response, send_file
from dotenv import load_dotenv

import db_utils

load_dotenv()

# ══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ══════════════════════════════════════════════════════════════
RESULT_JSON = "result.json"
MAP_CENTER = [55.75, 37.62]
MAP_ZOOM = 10

OAUTH_TOKEN = os.getenv("OAUTH_TOKEN", "")
BUCKET = "kube-cxm-cni"
BASE_PREFIX = "pvc-a6daa919-5f6c-4bca-8838-7d3f103e5fae/cctv/day"
STORAGE_URL = "https://storage.yandexcloud.net"
IAM_URL = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"

ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
os.makedirs(ASSETS_DIR, exist_ok=True)
MAP_FILE = os.path.join(ASSETS_DIR, "map.html")
SCREENSHOTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "screenshots")

COLORS = {
    "header": "#1a237e",
    "panel": "#f0f2f5",
    "border": "#dce0e8",
    "accent": "#1565c0",
}

# ══════════════════════════════════════════════════════════════
#  IAM-токен
# ══════════════════════════════════════════════════════════════

class IamToken:
    def __init__(self):
        self._token = None
        self._lock = threading.Lock()
        self._expires = 0

    def get(self):
        if not OAUTH_TOKEN:
            return None
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
                self._token = resp.json()["iamToken"]
                self._expires = time.time() + 36000
                print("  IAM-токен обновлён", flush=True)
            except Exception as e:
                print(f"  IAM-токен ошибка: {e}", flush=True)
                self._token = None
            return self._token

iam = IamToken()


def yandex_session():
    token = iam.get()
    if not token:
        return None
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}"})
    return s


def _s3_prefixes_for(shortname):
    """Вернуть варианты S3-префиксов: без пробела и с ведущим пробелом."""
    return [
        f"{BASE_PREFIX}/{shortname}/",
        f"{BASE_PREFIX}/ {shortname}/",
    ]


def find_image_key(session, shortname):
    for prefix in _s3_prefixes_for(shortname):
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


def list_all_image_keys(session, shortname):
    all_keys = []
    for prefix in _s3_prefixes_for(shortname):
        try:
            resp = session.get(
                f"{STORAGE_URL}/{BUCKET}",
                params={"prefix": prefix, "list-type": "2"},
                timeout=15,
            )
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            for node in root.findall(f".//{{{S3_NS}}}Key"):
                k = node.text
                if k and os.path.splitext(k)[-1].lower() in IMAGE_EXTS:
                    all_keys.append(k)
        except Exception:
            pass
    return all_keys


import re as _re

DATE_RE = _re.compile(r"\d{4}-\d{2}-\d{2}")



def discover_storage_dates(session, sample_shortnames=None, max_keys=5000):
    """Обнаружить даты-подпапки, сканируя S3 напрямую (без привязки к shortname).

    Папки камер в хранилище могут содержать ведущий пробел в имени,
    поэтому вместо поиска по shortname мы листаем все ключи под BASE_PREFIX
    и вытаскиваем даты regex-ом.
    """
    dates = set()
    has_root_photos = False
    prefix = f"{BASE_PREFIX}/"
    continuation_token = None
    total_scanned = 0

    while total_scanned < max_keys:
        params = {"prefix": prefix, "list-type": "2", "max-keys": "1000"}
        if continuation_token:
            params["continuation-token"] = continuation_token
        try:
            resp = session.get(
                f"{STORAGE_URL}/{BUCKET}", params=params, timeout=30,
            )
            resp.raise_for_status()
        except Exception:
            break

        root = ET.fromstring(resp.text)

        for node in root.findall(f".//{{{S3_NS}}}Key"):
            key = node.text or ""
            relative = key[len(prefix):]
            parts = relative.split("/")
            if len(parts) >= 3:
                m = DATE_RE.fullmatch(parts[1])
                if m:
                    dates.add(parts[1])
            if len(parts) == 2 and parts[1]:
                ext = os.path.splitext(parts[1])[-1].lower()
                if ext in IMAGE_EXTS:
                    has_root_photos = True

        total_scanned += int(
            (root.findtext(f".//{{{S3_NS}}}KeyCount") or "0")
        )

        is_truncated = (root.findtext(f".//{{{S3_NS}}}IsTruncated") or "").lower()
        if is_truncated == "true":
            ct_node = root.findtext(f".//{{{S3_NS}}}NextContinuationToken")
            if ct_node:
                continuation_token = ct_node
            else:
                break
        else:
            break

    return sorted(dates, reverse=True), has_root_photos


def list_image_keys_for_dates(session, shortname, selected_dates, include_no_date):
    """Получить ключи изображений с фильтрацией по датам."""
    all_keys = list_all_image_keys(session, shortname)
    if not selected_dates and not include_no_date:
        return all_keys

    possible_prefixes = _s3_prefixes_for(shortname)
    result = []
    for key in all_keys:
        relative = None
        for pfx in possible_prefixes:
            if key.startswith(pfx):
                relative = key[len(pfx):]
                break
        if relative is None:
            continue

        if "/" in relative:
            folder = relative.split("/")[0]
            if DATE_RE.fullmatch(folder) and folder in selected_dates:
                result.append(key)
        else:
            if include_no_date:
                result.append(key)
    return result


def download_object(session, key, dest_path):
    url = f"{STORAGE_URL}/{BUCKET}/{key}"
    resp = session.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)


# ══════════════════════════════════════════════════════════════
#  Глобальное состояние
# ══════════════════════════════════════════════════════════════

DATA = []
ALL_TYPES = []
MIN_DATE = "2020-01-01"
MAX_DATE = "2030-12-31"
PHOTOS_ENABLED = bool(OAUTH_TOKEN)

# Состояние фоновой генерации JSON
json_gen_state = {
    "running": False,
    "finished": False,
    "stage": "",
    "pct": 0,
    "message": "",
    "error": None,
    "elapsed": 0,
    "success": False,
}
json_gen_lock = threading.Lock()

# Состояние фонового скачивания
download_state = {
    "running": False,
    "total": 0,
    "done": 0,
    "downloaded": 0,
    "skipped": 0,
    "errors": 0,
    "not_found": 0,
    "message": "",
    "finished": False,
}
download_lock = threading.Lock()


def load_result_json():
    """Загрузить result.json если существует."""
    global DATA, ALL_TYPES, MIN_DATE, MAX_DATE
    if os.path.exists(RESULT_JSON):
        with open(RESULT_JSON, encoding="utf-8") as f:
            DATA = json.load(f)
    else:
        DATA = []

    ALL_TYPES = sorted({
        cam.get("type", "")
        for rec in DATA
        for cam in rec.get("cameras", [])
        if cam.get("type")
    })

    all_dates_start = [d for d in (_parse_date(r.get("date_start")) for r in DATA) if d]
    all_dates_end = [d for d in (_parse_date(r.get("date_end")) for r in DATA) if d]
    MIN_DATE = str(min(all_dates_start)) if all_dates_start else "2020-01-01"
    MAX_DATE = str(max(all_dates_end)) if all_dates_end else "2030-12-31"


def _parse_date(v):
    try:
        return pd.to_datetime(str(v)).date()
    except Exception:
        return None


def make_safe(s):
    return "".join(c if c.isalnum() or c in "-_. " else "_" for c in str(s)).strip()


# Загрузка при старте
load_result_json()
folium.Map(location=MAP_CENTER, zoom_start=MAP_ZOOM).save(MAP_FILE)

# Инициализация таблицы трекинга
try:
    db_utils.init_download_table()
    print("Таблица download_history готова", flush=True)
except Exception as e:
    print(f"Не удалось создать таблицу download_history: {e}", flush=True)

print(f"Загружено {len(DATA)} ордеров из result.json", flush=True)
print(f"Фото из Яндекса: {'включены' if PHOTOS_ENABLED else 'токен не задан'}", flush=True)

# ══════════════════════════════════════════════════════════════
#  Dash-приложение
# ══════════════════════════════════════════════════════════════

app = dash.Dash(__name__, title="Разрытия & Камеры", suppress_callback_exceptions=True)


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
            ".png": "image/png", ".webp": "image/webp",
        }.get(ext, "image/jpeg")
        return Response(r.content, content_type=mime)
    except Exception as e:
        return Response(str(e), status=500)


@app.server.route("/download-zip")
def download_zip():
    zip_path = os.path.join(SCREENSHOTS_DIR, "photos.zip")
    if os.path.exists(zip_path):
        return send_file(zip_path, as_attachment=True, download_name="photos.zip")
    return Response("Архив не найден", status=404)


# ══════════════════════════════════════════════════════════════
#  Layout
# ══════════════════════════════════════════════════════════════

def _modal_style(display="none"):
    return {
        "display": display,
        "position": "fixed",
        "top": "0", "left": "0", "right": "0", "bottom": "0",
        "background": "rgba(0,0,0,0.5)",
        "zIndex": "9999",
        "justifyContent": "center",
        "alignItems": "center",
    }


def _modal_content_style():
    return {
        "background": "#fff",
        "borderRadius": "12px",
        "padding": "28px 32px",
        "maxWidth": "640px",
        "width": "90%",
        "maxHeight": "85vh",
        "overflowY": "auto",
        "boxShadow": "0 8px 32px rgba(0,0,0,0.3)",
        "position": "relative",
    }


app.layout = html.Div([

    # ── Шапка ──
    html.Div([
        html.H2("Разрытия и камеры наблюдения",
                 style={"margin": "0", "color": "#fff", "fontSize": "20px"}),
        html.Div([
            html.Button(
                "Сформировать JSON", id="open-json-modal-btn", n_clicks=0,
                style={
                    "background": "#43a047", "color": "#fff", "border": "none",
                    "borderRadius": "6px", "padding": "8px 18px", "fontSize": "13px",
                    "cursor": "pointer", "fontWeight": "bold", "marginRight": "10px",
                },
            ),
            html.Button(
                "Скачать фотографии", id="open-download-modal-btn", n_clicks=0,
                style={
                    "background": "#ef6c00", "color": "#fff", "border": "none",
                    "borderRadius": "6px", "padding": "8px 18px", "fontSize": "13px",
                    "cursor": "pointer", "fontWeight": "bold",
                },
            ),
        ], style={"display": "flex", "alignItems": "center"}),
    ], style={
        "background": COLORS["header"], "padding": "14px 24px",
        "display": "flex", "alignItems": "center", "justifyContent": "space-between",
    }),

    # ── Модальное окно: формирование JSON ──
    html.Div(id="json-modal", children=[
        html.Div([
            html.Div([
                html.H3("Формирование result.json",
                         style={"margin": "0 0 16px", "color": COLORS["header"]}),
                html.Button("X", id="close-json-modal-btn", n_clicks=0,
                            style={
                                "position": "absolute", "top": "12px", "right": "16px",
                                "background": "none", "border": "none", "fontSize": "20px",
                                "cursor": "pointer", "color": "#888",
                            }),
            ]),

            html.Div([
                html.Label("Источник (выберите один или несколько):",
                           style={"fontWeight": "bold", "marginBottom": "8px", "display": "block"}),
                dcc.Loading(
                    dcc.Checklist(id="source-checklist", options=[], value=[],
                                  labelStyle={"display": "block", "marginBottom": "4px", "fontSize": "13px"}),
                    type="circle",
                ),
            ], style={"marginBottom": "18px"}),

            html.Div([
                html.Label("Интервал дат:", style={"fontWeight": "bold", "marginBottom": "8px", "display": "block"}),
                html.Div([
                    html.Span("Дата начала от:", style={"fontSize": "12px", "marginRight": "6px"}),
                    dcc.Input(id="gen-date-from", type="date", value="",
                              style={"marginRight": "16px", "fontSize": "13px", "padding": "4px 8px"}),
                    html.Span("Дата окончания до:", style={"fontSize": "12px", "marginRight": "6px"}),
                    dcc.Input(id="gen-date-to", type="date", value="",
                              style={"fontSize": "13px", "padding": "4px 8px"}),
                ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "4px"}),
            ], style={"marginBottom": "18px"}),

            html.Div([
                html.Label("Статус:", style={"fontWeight": "bold", "marginBottom": "8px", "display": "block"}),
                dcc.RadioItems(
                    id="gen-status",
                    options=[
                        {"label": "  Любой", "value": "Любой"},
                        {"label": "  Действует", "value": "Действует"},
                        {"label": "  Не действует", "value": "Не действует"},
                    ],
                    value="Любой",
                    labelStyle={"display": "inline-block", "marginRight": "18px", "fontSize": "13px"},
                ),
            ], style={"marginBottom": "18px"}),

            html.Div([
                html.Label("Радиус поиска камер (м):",
                           style={"fontWeight": "bold", "marginBottom": "8px", "display": "block"}),
                dcc.Input(id="gen-radius", type="number", value=150, min=10, max=5000, step=10,
                          style={"width": "120px", "fontSize": "13px", "padding": "4px 8px"}),
            ], style={"marginBottom": "20px"}),

            html.Button(
                "Сформировать", id="generate-json-btn", n_clicks=0,
                style={
                    "background": COLORS["accent"], "color": "#fff", "border": "none",
                    "borderRadius": "6px", "padding": "10px 28px", "fontSize": "14px",
                    "cursor": "pointer", "fontWeight": "bold", "width": "100%",
                },
            ),
            html.Div(id="gen-json-progress",
                     style={"marginTop": "14px", "fontSize": "13px", "color": "#333"}),
            dcc.Interval(id="json-gen-poll", interval=1000, disabled=True),
        ], style=_modal_content_style()),
    ], style=_modal_style()),

    # ── Модальное окно: скачивание фотографий ──
    html.Div(id="download-modal", children=[
        html.Div([
            html.H3("Скачивание фотографий",
                     style={"margin": "0 0 16px", "color": COLORS["header"]}),
            html.Button("X", id="close-download-modal-btn", n_clicks=0,
                        style={
                            "position": "absolute", "top": "12px", "right": "16px",
                            "background": "none", "border": "none", "fontSize": "20px",
                            "cursor": "pointer", "color": "#888",
                        }),

            html.P("По текущему result.json будут скачаны фотографии камер "
                   "из Yandex Object Storage.",
                   style={"fontSize": "13px", "color": "#555", "marginBottom": "12px"}),

            # Даты
            html.Div([
                html.Label("Даты фотографий:",
                           style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
                html.Button("Обнаружить даты в хранилище", id="discover-dates-btn", n_clicks=0,
                            style={
                                "background": "#546e7a", "color": "#fff", "border": "none",
                                "borderRadius": "4px", "padding": "6px 14px", "fontSize": "12px",
                                "cursor": "pointer", "marginBottom": "8px",
                            }),
                dcc.Loading(
                    html.Div(id="dates-container", children=[
                        html.Div("Нажмите кнопку, чтобы загрузить доступные даты.",
                                 style={"fontSize": "12px", "color": "#888"}),
                    ]),
                    type="circle",
                ),
            ], style={"marginBottom": "16px"}),

            # Режим скачивания
            html.Div([
                html.Label("Режим скачивания:",
                           style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
                dcc.RadioItems(
                    id="dl-mode",
                    options=[
                        {"label": "  По участкам (папка на каждый ордер)", "value": "by_orders"},
                        {"label": "  Все уникальные в одну папку", "value": "flat"},
                    ],
                    value="by_orders",
                    labelStyle={"display": "block", "marginBottom": "4px", "fontSize": "13px"},
                ),
            ], style={"marginBottom": "14px"}),

            # Объём скачивания
            html.Div([
                html.Label("Объём скачивания:",
                           style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
                dcc.RadioItems(
                    id="dl-scope",
                    options=[
                        {"label": "  Скачать все", "value": "all"},
                        {"label": "  Скачать новое (пропуск ранее скачанных)", "value": "new_only"},
                    ],
                    value="new_only",
                    labelStyle={"display": "block", "marginBottom": "4px", "fontSize": "13px"},
                ),
            ], style={"marginBottom": "16px"}),

            html.Button(
                "Начать скачивание", id="start-download-btn", n_clicks=0,
                style={
                    "background": "#ef6c00", "color": "#fff", "border": "none",
                    "borderRadius": "6px", "padding": "10px 28px", "fontSize": "14px",
                    "cursor": "pointer", "fontWeight": "bold", "width": "100%",
                    "marginBottom": "16px",
                },
            ),

            html.Div(id="download-progress",
                     style={"fontSize": "13px", "color": "#333"}),

            dcc.Interval(id="download-poll", interval=2000, disabled=True),
            dcc.Store(id="dl-selected-dates-store", data=[]),

            # Очистка истории
            html.Hr(style={"margin": "20px 0 14px", "border": "none",
                            "borderTop": "1px solid #dce0e8"}),
            html.Div([
                html.Label("Очистка истории скачиваний:",
                           style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
                html.Div([
                    dcc.Input(id="clear-history-pw", type="password",
                              placeholder="Введите пароль",
                              style={"fontSize": "13px", "padding": "6px 10px",
                                     "marginRight": "8px", "width": "180px"}),
                    html.Button("Очистить", id="clear-history-btn", n_clicks=0,
                                style={
                                    "background": "#c62828", "color": "#fff", "border": "none",
                                    "borderRadius": "4px", "padding": "6px 16px",
                                    "fontSize": "13px", "cursor": "pointer",
                                }),
                ], style={"display": "flex", "alignItems": "center"}),
                html.Div(id="clear-history-status",
                         style={"marginTop": "6px", "fontSize": "12px"}),
            ]),

        ], style=_modal_content_style()),
    ], style=_modal_style()),

    # ── Панель фильтров карты ──
    html.Div(id="filter-panel", children=[
        html.Div([
            html.Label("Дата начала:", style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
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
            html.Label("Типы камер:", style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
            dcc.Checklist(
                id="camtype-filter",
                options=[{"label": f"  {t}", "value": t} for t in ALL_TYPES],
                value=ALL_TYPES,
                labelStyle={"display": "block", "marginBottom": "4px", "fontSize": "13px"},
            ),
        ], style={"flex": "2", "minWidth": "200px"}),

        html.Div([
            html.Label("Разрытия:", style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
            dcc.RadioItems(
                id="dig-filter",
                options=[
                    {"label": "  Все", "value": "all"},
                    {"label": "  Только с камерами", "value": "with"},
                    {"label": "  Только без камер", "value": "without"},
                ],
                value="all",
                labelStyle={"display": "block", "marginBottom": "4px", "fontSize": "13px"},
            ),
        ], style={"flex": "1.5", "minWidth": "160px"}),

        html.Div([
            html.Label("Слои:", style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
            dcc.Checklist(
                id="layer-toggle",
                options=[
                    {"label": "  Разрытия", "value": "digs"},
                    {"label": "  Камеры", "value": "cameras"},
                    {"label": "  Зоны радиуса", "value": "circles"},
                    {"label": "  Линии связи", "value": "lines"},
                ],
                value=["digs", "cameras", "circles", "lines"],
                labelStyle={"display": "block", "marginBottom": "4px", "fontSize": "13px"},
            ),
        ], style={"flex": "1", "minWidth": "160px"}),

        html.Div([
            html.Label("Легенда:", style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"}),
            html.Div("Разрытие (есть камеры)", style={"fontSize": "12px", "marginBottom": "3px", "color": "orange"}),
            html.Div("Разрытие (нет камер)", style={"fontSize": "12px", "marginBottom": "3px", "color": "gray"}),
            html.Div("Камера в зоне", style={"fontSize": "12px", "marginBottom": "3px", "color": "#1565c0"}),
            html.Div("Камера вне зоны", style={"fontSize": "12px", "marginBottom": "3px", "color": "#546e7a"}),
            html.Div("Линия связи", style={"fontSize": "12px", "color": "#1565c0"}),
        ], style={"flex": "1.5", "minWidth": "160px"}),

        html.Div([
            html.Button(
                "Построить карту", id="build-btn", n_clicks=0,
                style={
                    "background": COLORS["accent"], "color": "#fff",
                    "border": "none", "borderRadius": "6px",
                    "padding": "10px 20px", "fontSize": "14px",
                    "cursor": "pointer", "fontWeight": "bold",
                    "width": "100%", "marginBottom": "12px",
                },
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
        style={"height": "58vh", "padding": "8px 24px"},
    ),

    html.Div([
        html.H4("Пары ордер — камера",
                 style={"margin": "0 0 10px", "color": COLORS["header"]}),
        html.Div(id="info-table"),
    ], style={"padding": "16px 24px 32px"}),

    # hidden store для хранения триггеров
    dcc.Store(id="json-regenerated-trigger", data=0),

], style={"fontFamily": "Segoe UI, Arial, sans-serif", "background": "#fafafa", "minHeight": "100vh"})


# ══════════════════════════════════════════════════════════════
#  Callbacks
# ══════════════════════════════════════════════════════════════

# ── Модалка JSON: открыть / закрыть ──
@app.callback(
    Output("json-modal", "style"),
    Input("open-json-modal-btn", "n_clicks"),
    Input("close-json-modal-btn", "n_clicks"),
    prevent_initial_call=True,
)
def toggle_json_modal(open_clicks, close_clicks):
    trigger = ctx.triggered_id
    if trigger == "open-json-modal-btn":
        return _modal_style("flex")
    return _modal_style("none")


# ── Модалка Download: открыть / закрыть ──
@app.callback(
    Output("download-modal", "style"),
    Input("open-download-modal-btn", "n_clicks"),
    Input("close-download-modal-btn", "n_clicks"),
    prevent_initial_call=True,
)
def toggle_download_modal(open_clicks, close_clicks):
    trigger = ctx.triggered_id
    if trigger == "open-download-modal-btn":
        return _modal_style("flex")
    return _modal_style("none")


# ── Загрузить источники из БД при открытии модалки ──
@app.callback(
    Output("source-checklist", "options"),
    Output("source-checklist", "value"),
    Input("json-modal", "style"),
    prevent_initial_call=True,
)
def load_sources(style):
    if style.get("display") != "flex":
        return no_update, no_update
    try:
        sources = db_utils.get_distinct_sources()
    except Exception as e:
        print(f"Ошибка загрузки источников: {e}", flush=True)
        sources = []
    options = [{"label": f"  {s}", "value": s} for s in sources]
    return options, sources


# ── Сформировать JSON: запуск в фоне ──

def _json_gen_progress_cb(stage, pct, message):
    with json_gen_lock:
        json_gen_state["stage"] = stage
        json_gen_state["pct"] = pct
        json_gen_state["message"] = message


def _json_gen_worker(sources, date_from, date_to, status, radius_m):
    global json_gen_state
    try:
        t0 = time.time()
        result, stats = db_utils.generate_result_json(
            sources=sources,
            date_from=date_from or None,
            date_to=date_to or None,
            status=status,
            radius_m=radius_m,
            progress_cb=_json_gen_progress_cb,
        )

        _json_gen_progress_cb("saving", 95, "Сохранение result.json...")
        with open(RESULT_JSON, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        load_result_json()
        elapsed = time.time() - t0

        msg = (
            f"Готово за {elapsed:.1f} сек. "
            f"Разрытий: {stats['digs']}, камер: {stats['cameras']}, "
            f"пар ордер-камера: {stats['pairs']}, с камерами: {stats['with_cams']}."
        )
        print(f"JSON сформирован: {msg}", flush=True)
        with json_gen_lock:
            json_gen_state["pct"] = 100
            json_gen_state["message"] = msg
            json_gen_state["success"] = True
            json_gen_state["elapsed"] = elapsed
            json_gen_state["running"] = False
            json_gen_state["finished"] = True

    except Exception as e:
        print(f"Ошибка формирования JSON: {e}", flush=True)
        with json_gen_lock:
            json_gen_state["error"] = str(e)
            json_gen_state["message"] = f"Ошибка: {e}"
            json_gen_state["running"] = False
            json_gen_state["finished"] = True


@app.callback(
    Output("json-gen-poll", "disabled"),
    Output("gen-json-progress", "children", allow_duplicate=True),
    Input("generate-json-btn", "n_clicks"),
    State("source-checklist", "value"),
    State("gen-date-from", "value"),
    State("gen-date-to", "value"),
    State("gen-status", "value"),
    State("gen-radius", "value"),
    prevent_initial_call=True,
)
def start_json_generation(n, sources, date_from, date_to, status, radius):
    global json_gen_state
    if not sources:
        return True, html.Span("Выберите хотя бы один источник.", style={"color": "red"})

    radius_m = int(radius) if radius else 150

    with json_gen_lock:
        if json_gen_state["running"]:
            return True, html.Span("Генерация уже запущена...", style={"color": "orange"})
        json_gen_state = {
            "running": True, "finished": False, "stage": "init",
            "pct": 0, "message": "Запуск...", "error": None,
            "elapsed": 0, "success": False,
        }

    thread = threading.Thread(
        target=_json_gen_worker,
        args=(sources, date_from, date_to, status, radius_m),
        daemon=True,
    )
    thread.start()
    return False, html.Span("Генерация запущена...", style={"color": "#1565c0"})


@app.callback(
    Output("gen-json-progress", "children"),
    Output("json-gen-poll", "disabled", allow_duplicate=True),
    Output("json-regenerated-trigger", "data"),
    Input("json-gen-poll", "n_intervals"),
    State("json-regenerated-trigger", "data"),
    prevent_initial_call=True,
)
def poll_json_generation(n_intervals, trigger_val):
    with json_gen_lock:
        st = dict(json_gen_state)

    pct = st["pct"]

    bar_color = "#43a047" if st["finished"] and st["success"] else \
                "#c62828" if st["finished"] and st["error"] else "#1565c0"

    bar = html.Div([
        html.Div(style={
            "width": f"{pct}%", "height": "18px",
            "background": bar_color,
            "borderRadius": "4px", "transition": "width 0.3s",
            "minWidth": "2px" if pct > 0 else "0",
        }),
    ], style={
        "width": "100%", "background": "#e0e0e0",
        "borderRadius": "4px", "marginBottom": "8px",
    })

    text_color = "green" if st["success"] else "red" if st["error"] else "#333"
    info = html.Div([
        html.Div(f"{pct}%", style={"fontWeight": "bold", "marginBottom": "4px"}),
        html.Div(st["message"], style={"color": text_color}),
    ], style={"fontSize": "13px"})

    disable_poll = st["finished"]
    new_trigger = ((trigger_val or 0) + 1) if (st["finished"] and st["success"]) else no_update
    return html.Div([bar, info]), disable_poll, new_trigger


# ── Обновить фильтры карты после генерации JSON ──
@app.callback(
    Output("camtype-filter", "options"),
    Output("camtype-filter", "value"),
    Output("date-from", "value"),
    Output("date-to", "value"),
    Input("json-regenerated-trigger", "data"),
)
def refresh_filters_after_gen(trigger):
    opts = [{"label": f"  {t}", "value": t} for t in ALL_TYPES]
    return opts, ALL_TYPES, MIN_DATE, MAX_DATE


# ══════════════════════════════════════════════════════════════
#  Скачивание фотографий (фоновый поток)
# ══════════════════════════════════════════════════════════════

# -- Обнаружение дат --

@app.callback(
    Output("dates-container", "children"),
    Input("discover-dates-btn", "n_clicks"),
    prevent_initial_call=True,
)
def discover_dates(n):
    session = yandex_session()
    if session is None:
        return html.Span("OAuth токен не задан.", style={"color": "red", "fontSize": "12px"})

    dates, has_root = discover_storage_dates(session)

    options = []
    values = []
    if has_root:
        options.append({"label": "  Без даты (фото в корне папки камеры)", "value": "__no_date__"})
    for d in dates:
        options.append({"label": f"  {d}", "value": d})

    if not options:
        return html.Span("Не удалось обнаружить фото в хранилище.", style={"color": "orange", "fontSize": "12px"})

    return dcc.Checklist(
        id="dl-dates-checklist",
        options=options,
        value=values,
        labelStyle={"display": "block", "marginBottom": "3px", "fontSize": "13px"},
    )


# -- Синхронизация выбранных дат в Store --

@app.callback(
    Output("dl-selected-dates-store", "data"),
    Input("dl-dates-checklist", "value"),
    prevent_initial_call=True,
)
def sync_dates_to_store(values):
    return values or []


# -- Очистка истории --

@app.callback(
    Output("clear-history-status", "children"),
    Input("clear-history-btn", "n_clicks"),
    State("clear-history-pw", "value"),
    prevent_initial_call=True,
)
def clear_history(n, password):
    if password != "удалить":
        return html.Span("Неверный пароль.", style={"color": "red"})
    try:
        db_utils.clear_download_history()
        return html.Span("История очищена.", style={"color": "green"})
    except Exception as e:
        return html.Span(f"Ошибка: {e}", style={"color": "red"})


# -- Скачивание: worker --

def _download_worker(mode, scope, selected_dates, include_no_date):
    """mode: 'by_orders' | 'flat'; scope: 'all' | 'new_only'"""
    global download_state
    session = yandex_session()
    if session is None:
        with download_lock:
            download_state["message"] = "OAuth токен не задан."
            download_state["running"] = False
            download_state["finished"] = True
        return

    try:
        with open(RESULT_JSON, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        with download_lock:
            download_state["message"] = f"Не удалось загрузить result.json: {e}"
            download_state["running"] = False
            download_state["finished"] = True
        return

    cam_to_orders = {}
    for rec in data:
        order = make_safe(str(rec.get("order_number", "UNKNOWN")))
        for cam in rec.get("cameras", []):
            sn = (cam.get("shortname") or "").strip()
            if sn:
                cam_to_orders.setdefault(sn, []).append(order)

    unique_cams = list(cam_to_orders.keys())
    with download_lock:
        download_state["total"] = len(unique_cams)
        download_state["message"] = f"Камер: {len(unique_cams)}"

    if not unique_cams:
        with download_lock:
            download_state["message"] = "Нет камер со shortname в result.json."
            download_state["running"] = False
            download_state["finished"] = True
        return

    already_downloaded = set()
    if scope == "new_only":
        try:
            already_downloaded = db_utils.get_downloaded_keys()
        except Exception:
            pass

    os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
    downloaded = skipped = not_found = errors = 0
    db_records = []
    use_date_filter = bool(selected_dates) or include_no_date

    for idx, shortname in enumerate(unique_cams):
        if use_date_filter:
            image_keys = list_image_keys_for_dates(
                session, shortname, set(selected_dates), include_no_date
            )
        else:
            image_keys = list_all_image_keys(session, shortname)

        if not image_keys:
            not_found += 1
            with download_lock:
                download_state["not_found"] = not_found
                download_state["done"] = idx + 1
            continue

        for key in image_keys:
            relative = key
            for pfx in _s3_prefixes_for(shortname):
                if key.startswith(pfx):
                    relative = key[len(pfx):]
                    break
            ext = os.path.splitext(key)[-1].lower() or ".jpg"

            if "/" in relative:
                date_folder = relative.split("/")[0]
                filename_part = relative.split("/", 1)[1]
                safe_fname = f"{make_safe(shortname)}_{date_folder}_{make_safe(filename_part)}"
            else:
                safe_fname = f"{make_safe(shortname)}_{make_safe(relative)}"

            if mode == "flat":
                dest_dir = SCREENSHOTS_DIR
                dest = os.path.join(dest_dir, safe_fname)

                if scope == "new_only" and (shortname, key) in already_downloaded:
                    skipped += 1
                    continue

                if os.path.exists(dest) and scope == "new_only":
                    skipped += 1
                    continue

                try:
                    download_object(session, key, dest)
                    downloaded += 1
                    db_records.append((shortname, "", key, "downloaded"))
                except Exception:
                    errors += 1
                    db_records.append((shortname, "", key, "error"))
                    if os.path.exists(dest):
                        try:
                            os.remove(dest)
                        except OSError:
                            pass

            else:
                for order in cam_to_orders[shortname]:
                    if scope == "new_only" and (shortname, key) in already_downloaded:
                        skipped += 1
                        continue

                    order_dir = os.path.join(SCREENSHOTS_DIR, order)
                    os.makedirs(order_dir, exist_ok=True)
                    dest = os.path.join(order_dir, safe_fname)

                    if os.path.exists(dest) and scope == "new_only":
                        skipped += 1
                        continue

                    tmp_path = dest + "._tmp"
                    try:
                        download_object(session, key, tmp_path)
                        os.replace(tmp_path, dest)
                        downloaded += 1
                        db_records.append((shortname, order, key, "downloaded"))
                    except Exception:
                        errors += 1
                        db_records.append((shortname, order, key, "error"))
                        if os.path.exists(tmp_path):
                            try:
                                os.remove(tmp_path)
                            except OSError:
                                pass

        with download_lock:
            download_state["done"] = idx + 1
            download_state["downloaded"] = downloaded
            download_state["skipped"] = skipped
            download_state["errors"] = errors
            download_state["not_found"] = not_found

        if len(db_records) >= 100:
            try:
                db_utils.record_downloads_batch(db_records)
            except Exception as e:
                print(f"Ошибка записи в БД: {e}", flush=True)
            db_records = []

    if db_records:
        try:
            db_utils.record_downloads_batch(db_records)
        except Exception as e:
            print(f"Ошибка записи в БД: {e}", flush=True)

    with download_lock:
        download_state["running"] = False
        download_state["finished"] = True
        download_state["message"] = (
            f"Завершено. Скачано: {downloaded}, пропущено: {skipped}, "
            f"не найдено: {not_found}, ошибок: {errors}."
        )
    print(f"Скачивание: {downloaded} скачано, {skipped} пропущено, "
          f"{not_found} не найдено, {errors} ошибок", flush=True)


@app.callback(
    Output("download-poll", "disabled"),
    Output("download-progress", "children", allow_duplicate=True),
    Input("start-download-btn", "n_clicks"),
    State("dl-mode", "value"),
    State("dl-scope", "value"),
    State("dl-selected-dates-store", "data"),
    prevent_initial_call=True,
)
def start_download(n, mode, scope, stored_dates):
    global download_state

    selected_dates = []
    include_no_date = False
    for v in (stored_dates or []):
        if v == "__no_date__":
            include_no_date = True
        else:
            selected_dates.append(v)

    with download_lock:
        if download_state["running"]:
            return True, html.Span("Скачивание уже запущено...", style={"color": "orange"})
        download_state = {
            "running": True, "total": 0, "done": 0,
            "downloaded": 0, "skipped": 0, "errors": 0,
            "not_found": 0, "message": "Запуск...", "finished": False,
        }

    thread = threading.Thread(
        target=_download_worker,
        args=(mode, scope, selected_dates, include_no_date),
        daemon=True,
    )
    thread.start()
    desc_parts = []
    if selected_dates:
        desc_parts.append(f"даты: {', '.join(selected_dates)}")
    if include_no_date:
        desc_parts.append("+ без даты")
    if not desc_parts:
        desc_parts.append("все даты")
    mode_label = "по участкам" if mode == "by_orders" else "в одну папку"
    scope_label = "все" if scope == "all" else "только новое"

    info = f"Запущено ({mode_label}, {scope_label}, {', '.join(desc_parts)})"
    return False, html.Span(info, style={"color": "#1565c0"})


@app.callback(
    Output("download-progress", "children"),
    Output("download-poll", "disabled", allow_duplicate=True),
    Input("download-poll", "n_intervals"),
    prevent_initial_call=True,
)
def poll_download_progress(n):
    with download_lock:
        st = dict(download_state)

    total = st["total"] or 1
    pct = int(st["done"] / total * 100) if total else 0

    bar = html.Div([
        html.Div(style={
            "width": f"{pct}%", "height": "20px",
            "background": "#43a047" if st["finished"] else "#1565c0",
            "borderRadius": "4px", "transition": "width 0.3s",
        }),
    ], style={
        "width": "100%", "background": "#e0e0e0",
        "borderRadius": "4px", "marginBottom": "10px",
    })

    info = html.Div([
        html.Div(f"Прогресс: {st['done']}/{st['total']} камер ({pct}%)"),
        html.Div(f"Скачано: {st['downloaded']} | Пропущено: {st['skipped']} | "
                 f"Не найдено: {st['not_found']} | Ошибок: {st['errors']}"),
        html.Div(st["message"], style={
            "marginTop": "8px", "fontWeight": "bold",
            "color": "green" if st["finished"] and st["errors"] == 0 else
                    "orange" if st["finished"] else "#333",
        }),
    ], style={"fontSize": "13px"})

    disable_poll = st["finished"]
    return html.Div([bar, info]), disable_poll


# ══════════════════════════════════════════════════════════════
#  Карта (существующая логика)
# ══════════════════════════════════════════════════════════════

def cam_popup_html(cam, is_near):
    shortname = cam.get("shortname", "")
    icon = "Камера (в зоне)" if is_near else "Камера (вне зоны)"

    if PHOTOS_ENABLED and shortname:
        url = "/photo/" + shortname
        photo_block = (
            f'<a href="{url}" target="_blank" style="display:block;text-decoration:none;margin-top:10px">'
            '<div style="width:280px;height:180px;background:#e0e0e0;border-radius:8px 8px 0 0;'
            'border:2px solid #1565c0;display:flex;align-items:center;'
            'justify-content:center;overflow:hidden;position:relative">'
            '<span style="color:#aaa;font-size:12px;position:absolute">Загрузка...</span>'
            f'<img data-src="{url}" width="280" height="180"'
            ' style="object-fit:cover;width:280px;height:180px;display:block;position:relative;z-index:1"'
            " onerror=\"this.style.display='none';this.previousSibling.textContent='Фото не найдено'\">"
            '</div>'
            '<div style="background:#1565c0;color:#fff;text-align:center;padding:8px;'
            'border-radius:0 0 8px 8px;font-size:13px;font-weight:bold">'
            'Открыть полный размер</div></a>'
        )
    else:
        photo_block = (
            "<div style='margin-top:8px;padding:10px;background:#f5f5f5;"
            "border-radius:8px;color:#aaa;text-align:center;font-size:12px'>"
            "Фото не подключены</div>"
        )

    info = (
        "<div style='min-width:280px;font-size:13px'>"
        f"<b>{icon}</b><hr style='margin:4px 0'>"
        f"<b>Shortname:</b> <span style='color:#1565c0;font-weight:bold'>{shortname or '—'}</span><br>"
        f"<b>ID:</b> {cam.get('camera_id', '')}<br>"
        f"<b>Тип:</b> {cam.get('type', '')}<br>"
        f"<b>Адрес:</b> {cam.get('address', '')}<br>"
        f"<b>Модель:</b> {cam.get('model', '')}<br>"
        f"<b>Статус:</b> {cam.get('status', '')}"
        f"{photo_block}</div>"
    )
    return info


def build_and_save_map(records, layers):
    m = folium.Map(location=MAP_CENTER, zoom_start=MAP_ZOOM, tiles="OpenStreetMap")
    cam_ids_near = {cam.get("camera_id") for rec in records for cam in rec["_visible_cams"]}

    if "circles" in layers:
        cg = folium.FeatureGroup(name="Зоны радиуса", show=True)
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
        lg = folium.FeatureGroup(name="Линии связи", show=True)
        for rec in records:
            for cam in rec["_visible_cams"]:
                sn = cam.get("shortname") or cam.get("camera_id", "")
                folium.PolyLine(
                    locations=[
                        [rec["centroid_lat"], rec["centroid_lng"]],
                        [cam["lat"], cam["lng"]],
                    ],
                    color="#1565c0", weight=1.2, opacity=0.4,
                    tooltip=f"Ордер {rec.get('order_number', '')} — {sn} | {round(cam.get('distance_m', 0))} м",
                ).add_to(lg)
        lg.add_to(m)

    if "digs" in layers:
        dc = MarkerCluster(
            name="Разрытия",
            options={"maxClusterRadius": 40, "showCoverageOnHover": False},
        )
        for rec in records:
            has = bool(rec["_visible_cams"])
            r = rec.get("search_radius_m", "?")
            folium.Marker(
                location=[rec["centroid_lat"], rec["centroid_lng"]],
                popup=folium.Popup(
                    f"<div style='min-width:240px;font-size:13px'>"
                    f"<b>Разрытие</b><hr style='margin:4px 0'>"
                    f"<b>Ордер:</b> {rec.get('order_number', '')}<br>"
                    f"<b>Работы:</b> {rec.get('work_types', '')}<br>"
                    f"<b>Заказчик:</b> {rec.get('contractor', '')}<br>"
                    f"<b>Период:</b> {rec.get('date_start', '')} — {rec.get('date_end', '')}<br>"
                    f"<b>Камер в радиусе {r}м:</b> "
                    f"<span style='color:{'#c62828' if has else '#555'}'>"
                    f"{len(rec['_visible_cams'])}</span></div>",
                    max_width=300,
                ),
                tooltip=f"Ордер {rec.get('order_number', '')} | {len(rec['_visible_cams'])} камер",
                icon=folium.Icon(
                    color="orange" if has else "gray",
                    icon="exclamation-sign" if has else "minus-sign",
                    prefix="glyphicon",
                ),
            ).add_to(dc)
        dc.add_to(m)

    if "cameras" in layers:
        cam_fg = folium.FeatureGroup(name="Камеры", show=True)
        all_cams = {}
        for rec in records:
            for cam in rec.get("cameras", []):
                all_cams[cam["camera_id"]] = cam

        for cid, cam in all_cams.items():
            is_near = cid in cam_ids_near
            color = "#1565c0" if is_near else "#546e7a"
            radius = 6 if is_near else 4
            sn = cam.get("shortname") or cid
            folium.CircleMarker(
                location=[cam["lat"], cam["lng"]],
                radius=radius,
                color=color, fill=True, fill_color=color,
                fill_opacity=0.9, weight=1.5,
                popup=folium.Popup(cam_popup_html(cam, is_near), max_width=300),
                tooltip=f"cam {sn} | {cam.get('type', '')}",
            ).add_to(cam_fg)
        cam_fg.add_to(m)

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
    Output("map-frame", "src"),
    Output("map-status", "children"),
    Output("info-table", "children"),
    Output("stats-bar", "children"),
    Input("build-btn", "n_clicks"),
    State("camtype-filter", "value"),
    State("dig-filter", "value"),
    State("layer-toggle", "value"),
    State("date-from", "value"),
    State("date-to", "value"),
    prevent_initial_call=True,
)
def update_map(n_clicks, cam_types, dig_filter, layers, date_from, date_to):
    cam_types = set(cam_types or [])
    layers = layers or []
    try:
        df = pd.to_datetime(date_from).date() if date_from else None
        dt = pd.to_datetime(date_to).date() if date_to else None
    except Exception:
        df = dt = None

    records = []
    for rec in DATA:
        if df or dt:
            d = _parse_date(rec.get("date_start"))
            if d:
                if df and d < df:
                    continue
                if dt and d > dt:
                    continue
        visible_cams = [c for c in rec.get("cameras", []) if c.get("type") in cam_types]
        if dig_filter == "with" and not visible_cams:
            continue
        if dig_filter == "without" and visible_cams:
            continue
        records.append({**rec, "_visible_cams": visible_cams})

    print(f"Строю карту: {len(records)} разрытий...", flush=True)
    build_and_save_map(records, layers)
    src = f"/assets/map.html?v={int(time.time())}"

    TABLE_LIMIT = 5000
    rows = []
    for rec in records:
        for cam in rec["_visible_cams"]:
            if len(rows) >= TABLE_LIMIT:
                break
            rows.append({
                "Ордер": rec.get("order_number", ""),
                "Дата начала": rec.get("date_start", ""),
                "Дата конца": rec.get("date_end", ""),
                "Shortname": cam.get("shortname", ""),
                "Камера ID": cam.get("camera_id", ""),
                "Тип камеры": cam.get("type", ""),
                "Адрес камеры": cam.get("address", ""),
                "Расстояние, м": cam.get("distance_m", ""),
            })

    table_el = (
        dash_table.DataTable(
            data=rows,
            columns=[{"name": c, "id": c} for c in rows[0].keys()] if rows else [],
            page_size=20,
            sort_action="native",
            filter_action="native",
            style_table={"overflowX": "auto"},
            style_cell={"fontSize": "13px", "padding": "6px 10px", "textAlign": "left"},
            style_header={"background": COLORS["header"], "color": "#fff", "fontWeight": "bold"},
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "#f5f7ff"},
                {"if": {"column_id": "Shortname"}, "fontWeight": "bold", "color": "#1565c0"},
            ],
            style_filter={"backgroundColor": "#e8eaf6"},
        )
        if rows
        else html.P("Нет совпадений.", style={"color": "#888", "fontStyle": "italic"})
    )

    with_cams = sum(1 for r in records if r["_visible_cams"])
    cam_total = len({c.get("camera_id") for r in records for c in r["_visible_cams"]})
    lines_n = sum(len(r["_visible_cams"]) for r in records)

    stats = html.Div([
        html.Div([html.B(f"{len(DATA):,}"), "  всего ордеров"]),
        html.Div([html.B(f"{len(records):,}"), "  по фильтру"]),
        html.Div([html.B(str(with_cams), style={"color": "#c62828"}), "  с камерами"]),
        html.Div([html.B(str(cam_total), style={"color": COLORS["accent"]}), "  уникальных камер"]),
        html.Div([html.B(f"{lines_n:,}"), "  линий связи"]),
    ])

    status = f"Карта построена: {len(records)} разрытий, {cam_total} камер"
    print(status, flush=True)
    return src, status, table_el, stats


# ══════════════════════════════════════════════════════════════
#  Запуск
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"\nДанные: {len(DATA):,} ордеров")
    print(f"Диапазон дат: {MIN_DATE} — {MAX_DATE}")
    print(f"Фото: {'включены' if PHOTOS_ENABLED else 'выключены'}")
    print("http://127.0.0.1:8050/\n")
    app.run(debug=False, port=8050)
