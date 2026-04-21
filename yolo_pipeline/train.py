from ultralytics import YOLO

# ============================================================
#                     КОНФИГУРАЦИЯ
# ============================================================

# Модель:
#   "runs/train/exp/weights/best.pt" — дообучение своей модели
MODEL = "yolo_pipeline/models/yolo26l-cls.pt"

# Путь к dataset.yaml
DATA = "yolo_pipeline/dataset_0"

# Количество эпох
EPOCHS = 50

# Размер изображения (пиксели)
IMGSZ = 640

# Размер батча (-1 = автовыбор)
BATCH = 16

# Начальный learning rate
# При дообучении уменьши до 0.001
LR0 = 0.01

# Заморозить первые N слоёв backbone (0 = не замораживать)
# Рекомендуется 10 при дообучении своей модели
FREEZE = 0

# Устройство: "0" — первая GPU, "0,1" — мульти-GPU, "cpu" — процессор
DEVICE = "cpu"

# Папка для сохранения результатов
PROJECT = "runs/train"
NAME    = "exp"

# Продолжить прерванное обучение (True/False)
RESUME = False

# Сохранять чекпоинт каждые N эпох (0 = не сохранять промежуточные)
SAVE_PERIOD = 10

# ── Аугментации ─────────────────────────────────────────────
AUG_HSV_H  = 0.015   # вариация оттенка
AUG_HSV_S  = 0.7     # вариация насыщенности
AUG_HSV_V  = 0.4     # вариация яркости
AUG_FLIPUD = 0.0     # вертикальный флип (вероятность)
AUG_FLIPLR = 0.5     # горизонтальный флип (вероятность)
AUG_MOSAIC = 1.0     # мозаичная аугментация (0.0 — выкл)

# ============================================================


def main():
    model = YOLO(MODEL)

    results = model.train(
        data=DATA,
        epochs=EPOCHS,
        imgsz=IMGSZ,
        batch=BATCH,
        lr0=LR0,
        freeze=FREEZE if FREEZE > 0 else None,
        device=DEVICE,
        project=PROJECT,
        name=NAME,
        resume=RESUME,
        save=True,
        save_period=SAVE_PERIOD,
        val=True,
        # Аугментации
        hsv_h=AUG_HSV_H,
        hsv_s=AUG_HSV_S,
        hsv_v=AUG_HSV_V,
        flipud=AUG_FLIPUD,
        fliplr=AUG_FLIPLR,
        mosaic=AUG_MOSAIC,
    )

    print(f"\n✅ Обучение завершено.")
    print(f"   Лучшие веса: {PROJECT}/{NAME}/weights/best.pt")


if __name__ == "__main__":
    main()
