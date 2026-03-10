from pathlib import Path
from typing import Any, Dict, Optional


try:
    import cv2  # type: ignore
    import imagehash  # type: ignore
    import numpy as np
    import pytesseract  # type: ignore
    from PIL import Image, ImageGrab
except Exception:  # noqa: BLE001
    cv2 = None  # type: ignore
    imagehash = None  # type: ignore
    np = None  # type: ignore
    pytesseract = None  # type: ignore
    Image = None  # type: ignore
    ImageGrab = None  # type: ignore


class VisionTools:
    """Safe vision utilities with graceful dependency fallback."""

    @staticmethod
    def _deps_ok() -> bool:
        return all(dep is not None for dep in (cv2, np, pytesseract, Image, ImageGrab, imagehash))

    @staticmethod
    def capture_screen(region: Optional[tuple] = None):
        if not VisionTools._deps_ok():
            raise RuntimeError("Vision dependencies are not installed.")
        img = ImageGrab.grab(bbox=region)
        return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

    @staticmethod
    def save_screenshot(path: str, region: Optional[tuple] = None) -> str:
        img = VisionTools.capture_screen(region)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        cv2.imwrite(path, img)
        return path

    @staticmethod
    def extract_text_from_image(image_path: str) -> str:
        if not VisionTools._deps_ok():
            raise RuntimeError("Vision dependencies are not installed.")
        img = cv2.imread(image_path)
        if img is None:
            raise FileNotFoundError(image_path)
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        gray = cv2.medianBlur(gray, 3)
        return pytesseract.image_to_string(gray)

    @staticmethod
    def extract_text_targets(
        image_path: str,
        *,
        min_confidence: float = 35.0,
    ) -> list[Dict[str, Any]]:
        if not VisionTools._deps_ok():
            raise RuntimeError("Vision dependencies are not installed.")
        img = cv2.imread(image_path)
        if img is None:
            raise FileNotFoundError(image_path)

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        gray = cv2.medianBlur(gray, 3)
        data = pytesseract.image_to_data(gray, output_type=pytesseract.Output.DICT)
        count = len(data.get("text", []))
        targets: list[Dict[str, Any]] = []
        for index in range(count):
            text = str(data.get("text", [""])[index]).strip()
            if not text:
                continue

            conf_raw = data.get("conf", ["0"])[index]
            try:
                confidence = float(conf_raw)
            except Exception:
                confidence = 0.0
            if confidence < min_confidence:
                continue

            left = int(float(data.get("left", [0])[index]))
            top = int(float(data.get("top", [0])[index]))
            width = int(float(data.get("width", [0])[index]))
            height = int(float(data.get("height", [0])[index]))
            if width <= 0 or height <= 0:
                continue

            targets.append(
                {
                    "text": text,
                    "confidence": round(confidence, 3),
                    "left": left,
                    "top": top,
                    "width": width,
                    "height": height,
                    "center_x": left + (width // 2),
                    "center_y": top + (height // 2),
                    "line_num": int(data.get("line_num", [0])[index]) if data.get("line_num") else 0,
                    "block_num": int(data.get("block_num", [0])[index]) if data.get("block_num") else 0,
                    "page_num": int(data.get("page_num", [0])[index]) if data.get("page_num") else 0,
                }
            )
        return targets

    @staticmethod
    def find_text_targets(
        image_path: str,
        *,
        query: str,
        match_mode: str = "contains",
        min_confidence: float = 35.0,
        limit: int = 20,
    ) -> list[Dict[str, Any]]:
        query_text = str(query or "").strip()
        if not query_text:
            return []
        mode = str(match_mode or "contains").strip().lower() or "contains"
        if mode not in {"contains", "exact", "token_overlap"}:
            mode = "contains"

        query_lower = query_text.lower()
        query_tokens = {item for item in query_lower.split() if item}
        rows = VisionTools.extract_text_targets(image_path, min_confidence=min_confidence)

        scored: list[tuple[float, Dict[str, Any]]] = []
        for row in rows:
            text = str(row.get("text", ""))
            lowered = text.lower()
            confidence = float(row.get("confidence", 0.0))
            score = 0.0

            if mode == "exact":
                if lowered == query_lower:
                    score = 1.0
            elif mode == "token_overlap":
                row_tokens = {item for item in lowered.split() if item}
                if row_tokens and query_tokens:
                    overlap = len(query_tokens.intersection(row_tokens))
                    score = overlap / max(1.0, len(query_tokens))
            else:
                if query_lower in lowered:
                    score = 0.75 + min(0.2, len(query_lower) / max(1.0, len(lowered)))

            if score <= 0:
                continue
            total_score = score + min(0.2, confidence / 100.0)
            scored.append((total_score, dict(row, match_score=round(total_score, 6))))

        scored.sort(key=lambda item: item[0], reverse=True)
        bounded = max(1, min(int(limit), 200))
        return [row for _, row in scored[:bounded]]

    @staticmethod
    def perceptual_hash(image_path: str) -> str:
        if not VisionTools._deps_ok():
            raise RuntimeError("Vision dependencies are not installed.")
        img = Image.open(image_path)
        return str(imagehash.phash(img))

    @staticmethod
    def health() -> Dict[str, Any]:
        ready = VisionTools._deps_ok()
        return {
            "status": "success" if ready else "degraded",
            "capabilities": {
                "screenshot": ready,
                "ocr_text": ready,
                "ocr_targets": ready,
                "perceptual_hash": ready,
            },
        }
