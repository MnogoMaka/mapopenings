from ultralytics import YOLO
import cv2
from pathlib import Path

# ============================================================
#                     КОНФИГУРАЦИЯ
# ============================================================

# Путь к обученным весам
MODEL = "yolo_pipeline/best.pt"

# Источник для инференса:
#   "photo.jpg"            — одно изображение
#   "dataset/images/test/" — папка с изображениями
#   "video.mp4"            — видеофайл
#   0                      — вебкамера (целое число, не строка)
SOURCE = "screenshots"

# Размер изображения (должен совпадать с тем, что использовался при обучении)
IMGSZ = 640

# Порог уверенности (0.0–1.0): детекции ниже порога отбрасываются
CONF = 0.25

# Порог IoU для NMS (non-maximum suppression)
IOU = 0.45

# Устройство: "0" — GPU, "cpu" — процессор
DEVICE = "cpu"

# Сохранить изображения с нарисованными bbox (True/False)
SAVE = True

# Сохранить предсказания в .txt файлы (True/False)
SAVE_TXT = False

# Показать результат на экране при инференсе на изображениях (True/False)
SHOW = False

# Папка для сохранения результатов
PROJECT = "runs/predict"
NAME    = "exp"

# Имена классов (должны совпадать с dataset.yaml)
CLASS_NAMES = {
    0: "class_0",
    1: "class_1",
}

# ============================================================


def print_detections(results):
    """Вывести детекции в консоль."""
    for i, result in enumerate(results):
        boxes = result.boxes
        if boxes is None or len(boxes) == 0:
            print(f"  Кадр {i}: объекты не найдены")
            continue
        for box in boxes:
            cls_id = int(box.cls.item())
            conf   = float(box.conf.item())
            xyxy   = box.xyxy[0].tolist()
            name   = CLASS_NAMES.get(cls_id, str(cls_id))
            print(
                f"  Кадр {i} | класс={name} ({cls_id}) | conf={conf:.2f} | "
                f"bbox=[{xyxy[0]:.0f},{xyxy[1]:.0f},{xyxy[2]:.0f},{xyxy[3]:.0f}]"
            )


def run_on_video(model, source):
    """Инференс на видео / вебкамере с live-отображением."""
    cap = cv2.VideoCapture(source if isinstance(source, int) else source)
    if not cap.isOpened():
        raise RuntimeError(f"Не удалось открыть источник: {source}")

    print("▶ Режим видео/вебкамеры. Нажми 'q' для выхода.")
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        results = model(frame, imgsz=IMGSZ, conf=CONF,
                        iou=IOU, device=DEVICE, verbose=False)
        annotated = results[0].plot()
        cv2.imshow("YOLO Inference  |  q — выход", annotated)
        if cv2.waitKey(1) & 0xFF == ord("q"):
            break

    cap.release()
    cv2.destroyAllWindows()


def main():
    model = YOLO(MODEL)

    # Видео / вебкамера
    video_exts = {".mp4", ".avi", ".mov", ".mkv", ".webm"}
    is_video = isinstance(SOURCE, int) or (
        isinstance(SOURCE, str) and Path(SOURCE).suffix.lower() in video_exts
    )

    if is_video:
        run_on_video(model, SOURCE)
        return

    # Изображения / папка
    results = model.predict(
        source=SOURCE,
        imgsz=IMGSZ,
        conf=CONF,
        iou=IOU,
        device=DEVICE,
        save=SAVE,
        save_txt=SAVE_TXT,
        show=SHOW,
        project=PROJECT,
        name=NAME,
        verbose=True,
    )

    print("\n📦 Детекции:")
    print_detections(results)

    if SAVE:
        print(f"\n💾 Результаты сохранены в: {PROJECT}/{NAME}/")


if __name__ == "__main__":
    main()
