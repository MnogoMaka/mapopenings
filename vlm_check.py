import os
import requests
import json
import base64
import shutil
from pathlib import Path
from PIL import Image
import io

# ===================== НАСТРОЙКИ =====================
INPUT_FOLDER  = r"C:\path\to\photos"   # Папка с фотографиями (сканируются все подпапки)
OUTPUT_FOLDER = r"C:\path\to\output"   # Куда копировать найденные фото
PROGRESS_FILE = "progress.json"           # Файл для сохранения прогресса
MODEL_API_ENDPOINT = os.getenv(
    "MODEL_API_ENDPOINT_CHAT",
    "https://lmstudio-tunnel.dev.contextmachine.cloud/v1/chat/completions"
)
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".webp"}
# =====================================================

PROMPT = """
Роль: Ты AI-эксперт по анализу фотографий строительных и земляных работ.

Задача: Определи, ведутся ли на фотографии земляные или строительные работы, либо видны ли их следы.

Признаки наличия земляных или строительных работ (достаточно одного):

- Вскрытый, перекопанный или потревоженный грунт (земля, песок, глина).
- Открытая траншея или котлован любого размера и глубины.
- Строительная техника (экскаватор, бульдозер, каток, трактор и т.п.).
- Строительные материалы на месте (трубы, арматура, бетонные кольца, щебень, песок в кучах).
- Нарушенное или вскрытое дорожное покрытие (асфальт, брусчатка, бетон).
- Строительные ограждения, временные заборы или предупреждающие знаки вокруг места работ.
- Обнажённые подземные коммуникации (трубы, кабели, тройники).
- Строительный мусор или выкопанная земля рядом с ямой или траншеей.
- Рабочие в строительной одежде (каски, жилеты), выполняющие земляные работы.

Правила вывода:

Ответ «0»: На фото НЕТ признаков земляных или строительных работ.
Ответ «1»: На фото ЕСТЬ земляные или строительные работы либо их явные следы.

Формат ответа: Верни ТОЛЬКО одну цифру — 0 или 1 — без каких-либо комментариев, точек или пробелов.
"""


def load_progress(progress_file: str) -> dict:
    """Загружает прогресс из файла (если он существует)."""
    if os.path.exists(progress_file):
        with open(progress_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed": {}}


def save_progress(progress_file: str, progress: dict):
    """Атомарно сохраняет прогресс: сначала во временный файл, потом переименовывает."""
    tmp = progress_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)
    os.replace(tmp, progress_file)


def get_all_images(folder: str) -> list[str]:
    """Рекурсивно собирает пути всех поддерживаемых изображений."""
    images = []
    for root, _, files in os.walk(folder):
        for file in files:
            if Path(file).suffix.lower() in SUPPORTED_EXTENSIONS:
                images.append(os.path.join(root, file))
    return sorted(images)


def image_to_base64(image_path: str, max_side: int = 1280) -> str:
    """Открывает изображение, масштабирует до max_side и кодирует в base64."""
    with Image.open(image_path) as img:
        if img.mode != "RGB":
            img = img.convert("RGB")
        # Уменьшаем, если нужно — экономим трафик и ускоряем ответ модели
        ratio = max_side / max(img.width, img.height)
        if ratio < 1.0:
            new_size = (int(img.width * ratio), int(img.height * ratio))
            img = img.resize(new_size, Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return base64.b64encode(buf.getvalue()).decode("utf-8")


def analyze_image(base64_image: str) -> str:
    """Отправляет изображение в VLM и возвращает '0' или '1'."""
    payload = {
        "model": "qwen/qwen3-vl-30b-a3b-instruct",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": PROMPT},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{base64_image}"
                        },
                    },
                ],
            }
        ],
        "max_tokens": 10,
        "temperature": 0.1,
    }

    response = requests.post(
        MODEL_API_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=90,
    )

    if response.status_code == 200:
        raw = response.json()["choices"][0]["message"]["content"].strip()
        # Берём только первый символ на случай если модель добавила лишнее
        return raw[0] if raw and raw[0] in ("0", "1") else raw
    else:
        raise Exception(f"Ошибка API: {response.status_code}, {response.text}")


def copy_to_output(image_path: str, input_folder: str, output_folder: str):
    """Копирует файл в OUTPUT_FOLDER, сохраняя относительную структуру подпапок."""
    rel_path = os.path.relpath(image_path, input_folder)
    dest_path = os.path.join(output_folder, rel_path)
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    shutil.copy2(image_path, dest_path)
    return dest_path


def process_images():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    progress = load_progress(PROGRESS_FILE)
    processed: dict = progress.setdefault("processed", {})

    images = get_all_images(INPUT_FOLDER)
    total = len(images)
    skipped = sum(1 for p in images if p in processed)

    print(f"Найдено изображений : {total}")
    print(f"Уже обработано      : {skipped}")
    print(f"Осталось обработать : {total - skipped}")
    print("-" * 60)

    for i, image_path in enumerate(images, 1):
        # --- Пропускаем уже обработанные (возобновление после сбоя) ---
        if image_path in processed:
            prev = processed[image_path]
            status_str = "разрытие" if prev.get("result") == "1" else "чисто"
            print(f"[{i:>5}/{total}] SKIP ({status_str}): {image_path}")
            continue

        print(f"[{i:>5}/{total}] Анализ: {image_path}")

        try:
            b64 = image_to_base64(image_path)
            result = analyze_image(b64)
            print(f"           Результат: {result}", end="")

            if result == "1":
                dest = copy_to_output(image_path, INPUT_FOLDER, OUTPUT_FOLDER)
                print(f"  → скопировано в {dest}", end="")

            print()
            processed[image_path] = {"result": result, "status": "ok"}

        except Exception as e:
            print(f"\n           ✗ Ошибка: {e}")
            processed[image_path] = {"result": None, "status": "error", "error": str(e)}

        # Сохраняем прогресс после КАЖДОГО файла — защита от сбоев
        save_progress(PROGRESS_FILE, progress)

    # --- Итог ---
    found  = sum(1 for v in processed.values() if v.get("result") == "1")
    errors = sum(1 for v in processed.values() if v.get("status") == "error")
    print("=" * 60)
    print(f"Готово! Всего файлов: {total}")
    print(f"  Найдено разрытий  : {found}")
    print(f"  Ошибок обработки  : {errors}")
    print(f"  Результаты в      : {OUTPUT_FOLDER}")
    print(f"  Прогресс в        : {PROGRESS_FILE}")


if __name__ == "__main__":
    process_images()
