"""
Утилиты для работы с PostgreSQL: подключение, запросы, трекинг скачиваний.
Использует psycopg 3 (пакет psycopg).
"""

import os
import json
import re
import math
import numpy as np
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()

DB_CONNINFO = (
    f"host={os.getenv('DB_HOST')} "
    f"port={os.getenv('DB_PORT')} "
    f"dbname={os.getenv('DB_NAME')} "
    f"user={os.getenv('DB_USER')} "
    f"password={os.getenv('DB_PASSWORD')}"
)

SCHEMA = "renovation_ii"
CAMERAS_TABLE = f'"{SCHEMA}".echd_camera_solr_dds'
DIGS_TABLE = f'"{SCHEMA}".table_oati_uved_order_raskopki'
DOWNLOAD_TABLE = f'"{SCHEMA}".download_history'

RADIUS_METERS = 150


def get_connection():
    return psycopg.connect(DB_CONNINFO, autocommit=False)


def init_download_table():
    """Создать таблицу трекинга скачиваний, если не существует."""
    with get_connection() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {DOWNLOAD_TABLE} (
                id SERIAL PRIMARY KEY,
                shortname TEXT,
                order_number TEXT,
                file_key TEXT,
                downloaded_at TIMESTAMP DEFAULT NOW(),
                status TEXT
            )
        """)
        conn.commit()


def get_distinct_sources():
    """Уникальные значения колонки 'Источник'."""
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT DISTINCT "Источник"
            FROM {DIGS_TABLE}
            WHERE "Источник" IS NOT NULL AND "Источник" != ''
            ORDER BY "Источник"
        """).fetchall()
        return [row[0] for row in rows]


def query_digs(sources, date_from=None, date_to=None, status=None):
    """
    Запрос разрытий с фильтрами.
    Дедупликация по wkt (DISTINCT ON).
    """
    conditions = ["wkt IS NOT NULL", "wkt != ''"]
    params: list = []

    if sources:
        placeholders = ", ".join(["%s"] * len(sources))
        conditions.append(f'"Источник" IN ({placeholders})')
        params.extend(sources)

    if date_from:
        conditions.append(""""Дата начала работ" >= %s::date""")
        params.append(str(date_from))

    if date_to:
        conditions.append(""""Дата окончания работ" <= %s::date""")
        params.append(str(date_to))

    if status and status not in ("Любой", "any", ""):
        conditions.append('"Статус" = %s')
        params.append(status)

    where = " AND ".join(conditions)

    query = f"""
        SELECT DISTINCT ON (wkt)
            "Номер документа", "Виды работ", "Цели работ",
            "Дата начала работ", "Дата окончания работ",
            "Заказчик/застройщик", wkt, "Источник", "Статус"
        FROM {DIGS_TABLE}
        WHERE {where}
        ORDER BY wkt, "Номер документа"
    """

    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchall()


def query_cameras():
    """Загрузить все камеры из БД."""
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"""
                SELECT id, cameras, shortname, type_h_name,
                       address, model, status, district_name
                FROM {CAMERAS_TABLE}
            """)
            return cur.fetchall()


def record_downloads_batch(records):
    """Пакетная вставка записей скачивания: [(shortname, order, key, status), ...]"""
    if not records:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO {DOWNLOAD_TABLE} "
                f"(shortname, order_number, file_key, status) "
                f"VALUES (%s, %s, %s, %s)",
                records,
            )
        conn.commit()


def get_downloaded_keys():
    """Множество (shortname, file_key) уже скачанных фото."""
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT DISTINCT shortname, file_key
            FROM {DOWNLOAD_TABLE}
            WHERE status = 'downloaded'
        """).fetchall()
        return {(r[0], r[1]) for r in rows}


def get_download_stats():
    """Статистика скачиваний по статусам."""
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT status, COUNT(*)
            FROM {DOWNLOAD_TABLE}
            GROUP BY status
        """).fetchall()
        return dict(rows)


def clear_download_history():
    """Полная очистка таблицы истории скачиваний."""
    with get_connection() as conn:
        conn.execute(f"TRUNCATE TABLE {DOWNLOAD_TABLE}")
        conn.commit()


# ══════════════════════════════════════════════════════════════
#  Логика формирования result.json (из convert_to_csv + match_cameras)
# ══════════════════════════════════════════════════════════════

def _parse_cam_coords(val):
    """Извлечь lat/lng из JSON-поля cameras."""
    try:
        if isinstance(val, str):
            d = json.loads(val)
        elif isinstance(val, dict):
            d = val
        else:
            return None, None
        lat = d.get("lat")
        lng = d.get("lng")
        if lat is not None and lng is not None:
            return float(lat), float(lng)
    except Exception:
        pass
    return None, None


def _extract_centroid(wkt):
    """Центроид из WKT-геометрии."""
    pairs = re.findall(r"([\d.]+)\s+([\d.]+)", str(wkt))
    if not pairs:
        return None, None
    lngs = [float(p[0]) for p in pairs]
    lats = [float(p[1]) for p in pairs]
    return sum(lats) / len(lats), sum(lngs) / len(lngs)


def _haversine_batch(dig_lats, dig_lngs, cam_lats, cam_lngs):
    """Матрица расстояний (N_digs x N_cams) в метрах."""
    R = 6_371_000.0
    phi1 = np.radians(dig_lats)[:, None]
    phi2 = np.radians(cam_lats)[None, :]
    dphi = np.radians(cam_lats - dig_lats[:, None])
    dlam = np.radians(cam_lngs - dig_lngs[:, None])
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2) ** 2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def generate_result_json(sources, date_from=None, date_to=None, status=None,
                         radius_m=RADIUS_METERS, batch_size=500,
                         progress_cb=None):
    """
    Полный pipeline: фильтрация разрытий -> парсинг камер -> haversine -> result list.
    Возвращает (result_list, stats_dict).
    progress_cb(stage, pct, message) вызывается для обновления прогресса.
    """
    def _progress(stage, pct, message):
        if progress_cb:
            progress_cb(stage, pct, message)

    _progress("query_digs", 0, "Запрос разрытий из БД...")
    digs_rows = query_digs(sources, date_from, date_to, status)
    if not digs_rows:
        _progress("done", 100, "Нет данных по выбранным фильтрам.")
        return [], {"digs": 0, "cameras": 0, "pairs": 0, "with_cams": 0}

    _progress("parse_digs", 10, f"Получено {len(digs_rows)} строк. Парсинг центроидов...")
    digs = []
    for row in digs_rows:
        clat, clng = _extract_centroid(row["wkt"])
        if clat is None:
            continue
        digs.append({
            "order_number": row["Номер документа"],
            "work_types": row.get("Виды работ", ""),
            "goal": row.get("Цели работ", ""),
            "date_start": str(row.get("Дата начала работ", ""))[:10],
            "date_end": str(row.get("Дата окончания работ", ""))[:10],
            "contractor": row.get("Заказчик/застройщик", ""),
            "source": row.get("Источник", ""),
            "status": row.get("Статус", ""),
            "centroid_lat": clat,
            "centroid_lng": clng,
        })

    _progress("query_cameras", 20, f"Разрытий с координатами: {len(digs)}. Загрузка камер из БД...")
    cam_rows = query_cameras()

    _progress("parse_cameras", 30, f"Камер в БД: {len(cam_rows)}. Парсинг координат...")
    cameras = []
    for row in cam_rows:
        lat, lng = _parse_cam_coords(row.get("cameras"))
        if lat is None:
            continue
        cameras.append({
            "id": str(row["id"]),
            "shortname": str(row.get("shortname") or ""),
            "type_h_name": row.get("type_h_name", ""),
            "address": row.get("address", ""),
            "model": row.get("model", ""),
            "status": row.get("status", ""),
            "lat": lat,
            "lng": lng,
        })

    _progress("parse_cameras", 35, f"Камер с координатами: {len(cameras)}")

    if not cameras:
        result = []
        for d in digs:
            result.append({
                **{k: v for k, v in d.items() if k not in ("source", "status")},
                "centroid_lat": round(d["centroid_lat"], 6),
                "centroid_lng": round(d["centroid_lng"], 6),
                "search_radius_m": radius_m,
                "cameras_count": 0,
                "cameras": [],
            })
        _progress("done", 100, f"Готово. Камер нет, {len(digs)} разрытий без привязки.")
        return result, {"digs": len(digs), "cameras": 0, "pairs": 0, "with_cams": 0}

    cam_lats = np.array([c["lat"] for c in cameras])
    cam_lngs = np.array([c["lng"] for c in cameras])

    result = []
    total_pairs = 0
    n_digs = len(digs)
    n_batches = math.ceil(n_digs / batch_size)

    _progress("matching", 40,
              f"Haversine matching: {n_digs} разрытий x {len(cameras)} камер, "
              f"радиус {radius_m}м, {n_batches} батчей...")

    for batch_i in range(n_batches):
        start = batch_i * batch_size
        end = min(start + batch_size, n_digs)
        batch = digs[start:end]

        batch_pct = 40 + int((batch_i + 1) / n_batches * 50)
        _progress("matching", batch_pct,
                  f"Matching: батч {batch_i + 1}/{n_batches} "
                  f"({end}/{n_digs} разрытий обработано)")

        b_lats = np.array([d["centroid_lat"] for d in batch])
        b_lngs = np.array([d["centroid_lng"] for d in batch])

        dist_matrix = _haversine_batch(b_lats, b_lngs, cam_lats, cam_lngs)

        for local_i, dig in enumerate(batch):
            distances = dist_matrix[local_i]
            mask = distances <= radius_m
            idx = np.where(mask)[0]

            nearby = []
            for i in idx:
                cam = cameras[i]
                nearby.append({
                    "camera_id": cam["id"],
                    "shortname": cam["shortname"],
                    "type": cam["type_h_name"],
                    "address": cam["address"],
                    "lat": cam["lat"],
                    "lng": cam["lng"],
                    "model": cam["model"],
                    "status": cam["status"],
                    "distance_m": round(float(distances[i]), 1),
                })
            nearby.sort(key=lambda x: x["distance_m"])
            total_pairs += len(nearby)

            result.append({
                "order_number": dig["order_number"],
                "work_types": dig["work_types"],
                "goal": dig["goal"],
                "date_start": dig["date_start"],
                "date_end": dig["date_end"],
                "contractor": dig["contractor"],
                "centroid_lat": round(dig["centroid_lat"], 6),
                "centroid_lng": round(dig["centroid_lng"], 6),
                "search_radius_m": radius_m,
                "cameras_count": len(nearby),
                "cameras": nearby,
            })

    with_cams = sum(1 for r in result if r["cameras_count"] > 0)
    stats = {
        "digs": len(result),
        "cameras": len(cameras),
        "pairs": total_pairs,
        "with_cams": with_cams,
    }
    _progress("done", 100,
              f"Готово! Разрытий: {stats['digs']}, камер: {stats['cameras']}, "
              f"пар: {stats['pairs']}, с камерами: {stats['with_cams']}")
    return result, stats
