#!/usr/bin/env python3
"""
Скрипт: привязка камер к разрытиям (быстрая версия — CSV + NumPy + прогресс)
Зависимости: pip install pandas numpy tqdm
"""

import json
import math
import time
import numpy as np
import pandas as pd
from tqdm import tqdm

# ═══════════════════════════════════════════════════════════
# CONFIGURATION — меняйте только здесь
# ═══════════════════════════════════════════════════════════

CAMERAS_FILE = "Новая папка (2)/cameras.csv"
DIGS_FILE    = "Новая папка (2)/digs.csv"
OUTPUT_FILE  = "Новая папка (2)/result.json"

RADIUS_METERS = 150

# Сколько разрытий обрабатывать за один батч.
# Увеличьте если много RAM, уменьшите если мало (< 8 GB).
BATCH_SIZE = 500

# ═══════════════════════════════════════════════════════════

def haversine_batch(dig_lats: np.ndarray, dig_lngs: np.ndarray,
                    cam_lats: np.ndarray, cam_lngs: np.ndarray) -> np.ndarray:
    """
    Векторный расчёт расстояний: батч разрытий × все камеры.
    Возвращает матрицу (N_digs × N_cams) расстояний в метрах.
    """
    R = 6_371_000.0
    phi1 = np.radians(dig_lats)[:, None]
    phi2 = np.radians(cam_lats)[None, :]
    dphi = np.radians(cam_lats - dig_lats[:, None])
    dlam = np.radians(cam_lngs - dig_lngs[:, None])
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2) ** 2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

# ─── Загрузка ───────────────────────────────────────────────

print("=" * 55)
print(" Загрузка данных...")
t0 = time.time()

cameras = pd.read_csv(CAMERAS_FILE)
digs    = pd.read_csv(DIGS_FILE)
cameras["id"] = cameras["id"].astype(str)

cam_lats = cameras["lat"].to_numpy()
cam_lngs = cameras["lng"].to_numpy()

print(f"  Разрытий: {len(digs):,}")
print(f"  Камер:    {len(cameras):,}")
print(f"  Радиус:   {RADIUS_METERS} м")
print(f"  Батч:     {BATCH_SIZE} разрытий за итерацию")
print(f"  Загрузка: {time.time() - t0:.1f} с")
print("=" * 55)

# ─── Поиск ──────────────────────────────────────────────────

result      = []
total_pairs = 0
n_digs      = len(digs)
n_batches   = math.ceil(n_digs / BATCH_SIZE)

t_search = time.time()

with tqdm(total=n_digs, unit="разрытий", desc="Обработка",
          bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:

    for batch_i in range(n_batches):
        batch_start = batch_i * BATCH_SIZE
        batch_end   = min(batch_start + BATCH_SIZE, n_digs)
        batch       = digs.iloc[batch_start:batch_end]

        b_lats = batch["centroid_lat"].to_numpy()
        b_lngs = batch["centroid_lng"].to_numpy()

        dist_matrix = haversine_batch(b_lats, b_lngs, cam_lats, cam_lngs)

        for local_i, (_, dig) in enumerate(batch.iterrows()):
            distances = dist_matrix[local_i]
            mask = distances <= RADIUS_METERS
            idx  = np.where(mask)[0]

            nearby_cameras = []
            for i in idx:
                row = cameras.iloc[i]
                nearby_cameras.append({
                    "camera_id":  row["id"],
                    "shortname":  str(row.get("shortname", "") or ""),  # ← добавлено
                    "type":       row.get("type_h_name"),
                    "address":    row.get("address"),
                    "lat":        float(row["lat"]),
                    "lng":        float(row["lng"]),
                    "model":      row.get("model"),
                    "status":     row.get("status"),
                    "distance_m": round(float(distances[i]), 1),
                })
            nearby_cameras.sort(key=lambda x: x["distance_m"])
            total_pairs += len(nearby_cameras)

            result.append({
                "order_number":   dig["Номер документа"],
                "work_types":     dig["Виды работ"],
                "goal":           dig["Цели работ"],
                "date_start":     str(dig["Дата начала работ"])[:10],
                "date_end":       str(dig["Дата окончания работ"])[:10],
                "contractor":     dig["Заказчик/застройщик"],
                "centroid_lat":   round(float(dig["centroid_lat"]), 6),
                "centroid_lng":   round(float(dig["centroid_lng"]), 6),
                "search_radius_m": RADIUS_METERS,
                "cameras_count":  len(nearby_cameras),
                "cameras":        nearby_cameras,
            })

        pbar.update(batch_end - batch_start)

# ─── Сохранение ─────────────────────────────────────────────

print("\nСохранение результата...")
t_save = time.time()
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

with_cams = sum(1 for r in result if r["cameras_count"] > 0)

print("=" * 55)
print(f"  ✅ Готово! → {OUTPUT_FILE}")
print(f"  Поиск:          {time.time() - t_search:.1f} с")
print(f"  Сохранение:     {time.time() - t_save:.1f} с")
print(f"  Разрытий всего: {len(result):,}")
print(f"  С камерами:     {with_cams:,}")
print(f"  Без камер:      {len(result) - with_cams:,}")
print(f"  Пар ордер-кам:  {total_pairs:,}")
print("=" * 55)
