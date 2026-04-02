#!/usr/bin/env python3
"""
Конвертер: Excel → CSV (запускается один раз)
После запуска получите cameras.csv и digs.csv
Зависимости: pip install pandas openpyxl numpy
"""

import json
import re
import pandas as pd

CAMERAS_XLSX = "Копия table_echd_camera_solr_dds.xlsx"
DIGS_XLSX    = "Копия table_oati_uved_order_raskopki.xlsx"

def parse_cam_coords(val):
    try:
        d = json.loads(val)
        return d.get("lat"), d.get("lng")
    except Exception:
        return None, None

def extract_centroid(wkt):
    pairs = re.findall(r"([\d.]+)\s+([\d.]+)", str(wkt))
    if not pairs:
        return None, None
    lngs = [float(p[0]) for p in pairs]
    lats = [float(p[1]) for p in pairs]
    return sum(lats) / len(lats), sum(lngs) / len(lngs)

print("Конвертация камер...")
cameras_raw = pd.read_excel(CAMERAS_XLSX)
cameras_raw["lat"], cameras_raw["lng"] = zip(
    *cameras_raw["cameras"].apply(parse_cam_coords)
)

cameras_out = cameras_raw[
    ["id", "lat", "lng", "shortname", "type_h_name", "address", "model", "status", "district_name"]  # ← добавлен shortname
].dropna(subset=["lat", "lng"]).copy()
cameras_out["id"] = cameras_out["id"].astype(str)
cameras_out.to_csv("cameras.csv", index=False)
print(f"  cameras.csv — {len(cameras_out)} строк")

print("Конвертация разрытий...")
digs_raw = pd.read_excel(DIGS_XLSX)
digs_raw["centroid_lat"], digs_raw["centroid_lng"] = zip(
    *digs_raw["wkt"].apply(extract_centroid)
)

digs_out = digs_raw[
    ["Номер документа", "Виды работ", "Цели работ",
     "Дата начала работ", "Дата окончания работ",
     "Заказчик/застройщик", "centroid_lat", "centroid_lng"]
].dropna(subset=["centroid_lat", "centroid_lng"]).copy()
digs_out.to_csv("digs.csv", index=False)
print(f"  digs.csv — {len(digs_out)} строк")

print("\n✅ Готово! Теперь запустите: python match_cameras.py")
