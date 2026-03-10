"""
Advanced Computer Vision Engine for JARVIS.

Provides multi-modal visual understanding:
- Object detection (YOLOv10)
- UI element segmentation (SAM)
- Visual question answering (CLIP/BLIP)
- Grounded object detection (Grounding DINO)
- Screen understanding and analysis
"""

import gc
import hashlib
import io
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from PIL import Image, ImageGrab

from backend.python.utils.logger import Logger


@dataclass
class DetectedObject:
    """Represents a detected object in an image."""
    label: str
    confidence: float
    bbox: Tuple[int, int, int, int]  # (x1, y1, x2, y2)
    center: Tuple[int, int]
    area: int
    class_id: int


@dataclass
class UIElement:
    """Represents a segmented UI element."""
    element_id: str
    element_type: str  # button, input, menu, window, etc.
    bbox: Tuple[int, int, int, int]
    center: Tuple[int, int]
    mask: Optional[np.ndarray]
    confidence: float
    text: Optional[str]  # OCR result if available
    interactable: bool


@dataclass
class VisualContext:
    """Complete visual understanding of current screen state."""
    timestamp: float
    screenshot_hash: str
    active_application: str
    detected_objects: List[DetectedObject]
    ui_elements: List[UIElement]
    screen_summary: str
    dominant_colors: List[Tuple[int, int, int]]
    text_content: List[str]


class VisionEngine:
    """
    Advanced computer vision engine with multiple models for comprehensive visual understanding.
    
    Supports:
    - Real-time object detection
    - UI element segmentation
    - Visual question answering
    - Natural language object grounding
    - Screen state understanding
    """

    def __init__(
        self,
        *,
        models_dir: str = "models/vision",
        device: str = "auto",
        enable_gpu: bool = True,
        cache_embeddings: bool = True,
    ):
        self.log = Logger("VisionEngine").get_logger()
        self.models_dir = Path(models_dir)
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        self.device = self._detect_device(device, enable_gpu)
        self.cache_embeddings = cache_embeddings
        self._embedding_cache: Dict[str, np.ndarray] = {}
        
        # Lazy-loaded models
        self._yolo_model = None
        self._sam_model = None
        self._clip_model = None
        self._clip_processor = None
        self._blip_model = None
        self._blip_processor = None
        self._groundingdino_model = None
        self._model_runtime: Dict[str, Dict[str, Any]] = {}
        
        self.log.info(f"VisionEngine initialized on device: {self.device}")

    def _detect_device(self, device: str, enable_gpu: bool) -> str:
        """Detect best available device for inference."""
        if device != "auto":
            return device
        
        if not enable_gpu:
            return "cpu"
        
        try:
            import torch
            if torch.cuda.is_available():
                return "cuda"
            elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                return "mps"
        except ImportError:
            pass
        
        return "cpu"

    def _load_yolo(self):
        """Lazy load YOLOv10 model."""
        if self._yolo_model is not None:
            self._ensure_model_runtime("yolo", str(self.models_dir / "yolov10n.pt"))["loaded"] = True
            return self._yolo_model
        
        try:
            from ultralytics import YOLO
            
            model_path = self.models_dir / "yolov10n.pt"
            started = time.monotonic()
            if not model_path.exists():
                self.log.info("Downloading YOLOv10 model...")
                self._yolo_model = YOLO("yolov10n.pt")
                self._yolo_model.export(format="torchscript")
            else:
                self._yolo_model = YOLO(str(model_path))
            
            self._mark_model_loaded("yolo", artifact_path=str(model_path), load_latency_s=max(0.0, time.monotonic() - started))
            self.log.info("YOLOv10 model loaded successfully")
            return self._yolo_model
        except Exception as exc:
            self._mark_model_error("yolo", artifact_path=str(self.models_dir / "yolov10n.pt"), error=str(exc))
            self.log.error(f"Failed to load YOLO model: {exc}")
            return None

    def _load_sam(self):
        """Lazy load Segment Anything Model."""
        if self._sam_model is not None:
            self._ensure_model_runtime("sam", str(self.models_dir / "sam_vit_b_01ec64.pth"))["loaded"] = True
            return self._sam_model
        
        try:
            from segment_anything import SamAutomaticMaskGenerator, sam_model_registry
            
            model_path = self.models_dir / "sam_vit_b_01ec64.pth"
            if not model_path.exists():
                self._mark_model_error("sam", artifact_path=str(model_path), error="model_missing")
                self.log.warning("SAM model not found. Please download from Meta AI.")
                return None
            started = time.monotonic()
            
            sam = sam_model_registry["vit_b"](checkpoint=str(model_path))
            if self.device != "cpu":
                sam = sam.to(self.device)
            
            self._sam_model = SamAutomaticMaskGenerator(
                sam,
                points_per_side=32,
                pred_iou_thresh=0.86,
                stability_score_thresh=0.92,
                crop_n_layers=1,
                crop_n_points_downscale_factor=2,
            )
            
            self._mark_model_loaded("sam", artifact_path=str(model_path), load_latency_s=max(0.0, time.monotonic() - started))
            self.log.info("SAM model loaded successfully")
            return self._sam_model
        except Exception as exc:
            self._mark_model_error("sam", artifact_path=str(self.models_dir / "sam_vit_b_01ec64.pth"), error=str(exc))
            self.log.error(f"Failed to load SAM model: {exc}")
            return None

    def _load_clip(self):
        """Lazy load CLIP model for vision-language understanding."""
        if self._clip_model is not None:
            self._ensure_model_runtime("clip", str(self.models_dir / "clip"))["loaded"] = True
            return self._clip_model, self._clip_processor
        
        try:
            from transformers import CLIPModel, CLIPProcessor
            
            model_name = "openai/clip-vit-base-patch32"
            cache_dir = self.models_dir / "clip"
            started = time.monotonic()
            
            self._clip_processor = CLIPProcessor.from_pretrained(
                model_name,
                cache_dir=str(cache_dir),
            )
            self._clip_model = CLIPModel.from_pretrained(
                model_name,
                cache_dir=str(cache_dir),
            )
            
            if self.device != "cpu":
                self._clip_model = self._clip_model.to(self.device)
            
            self._mark_model_loaded("clip", artifact_path=str(cache_dir), load_latency_s=max(0.0, time.monotonic() - started))
            self.log.info("CLIP model loaded successfully")
            return self._clip_model, self._clip_processor
        except Exception as exc:
            self._mark_model_error("clip", artifact_path=str(self.models_dir / "clip"), error=str(exc))
            self.log.error(f"Failed to load CLIP model: {exc}")
            return None, None

    def _load_blip(self):
        """Lazy load BLIP-2 model for visual question answering."""
        if self._blip_model is not None:
            self._ensure_model_runtime("blip", str(self.models_dir / "blip2"))["loaded"] = True
            return self._blip_model, self._blip_processor
        
        try:
            from transformers import Blip2ForConditionalGeneration, Blip2Processor
            
            model_name = "Salesforce/blip2-opt-2.7b"
            cache_dir = self.models_dir / "blip2"
            started = time.monotonic()
            
            self._blip_processor = Blip2Processor.from_pretrained(
                model_name,
                cache_dir=str(cache_dir),
            )
            self._blip_model = Blip2ForConditionalGeneration.from_pretrained(
                model_name,
                cache_dir=str(cache_dir),
                load_in_8bit=True if self.device == "cuda" else False,
            )
            
            if self.device == "cuda" and not hasattr(self._blip_model, "hf_device_map"):
                self._blip_model = self._blip_model.to(self.device)
            
            self._mark_model_loaded("blip", artifact_path=str(cache_dir), load_latency_s=max(0.0, time.monotonic() - started))
            self.log.info("BLIP-2 model loaded successfully")
            return self._blip_model, self._blip_processor
        except Exception as exc:
            self._mark_model_error("blip", artifact_path=str(self.models_dir / "blip2"), error=str(exc))
            self.log.error(f"Failed to load BLIP-2 model: {exc}")
            return None, None

    def capture_screen(self, region: Optional[Tuple[int, int, int, int]] = None) -> Image.Image:
        """Capture screenshot of entire screen or specific region."""
        try:
            if region:
                screenshot = ImageGrab.grab(bbox=region)
            else:
                screenshot = ImageGrab.grab()
            return screenshot
        except Exception as exc:
            self.log.error(f"Failed to capture screen: {exc}")
            raise

    def detect_objects(
        self,
        image: Image.Image,
        *,
        confidence_threshold: float = 0.3,
        class_filter: Optional[List[str]] = None,
    ) -> List[DetectedObject]:
        """
        Detect objects in image using YOLOv10.
        
        Args:
            image: PIL Image to analyze
            confidence_threshold: Minimum confidence score (0-1)
            class_filter: Optional list of class names to filter results
            
        Returns:
            List of detected objects with bounding boxes and metadata
        """
        yolo = self._load_yolo()
        if yolo is None:
            self.log.warning("YOLO model not available, returning empty list")
            return []
        
        try:
            results = yolo(image, verbose=False)
            detections = []
            
            for result in results:
                boxes = result.boxes
                for i in range(len(boxes)):
                    conf = float(boxes.conf[i])
                    if conf < confidence_threshold:
                        continue
                    
                    class_id = int(boxes.cls[i])
                    label = result.names[class_id]
                    
                    if class_filter and label not in class_filter:
                        continue
                    
                    x1, y1, x2, y2 = boxes.xyxy[i].cpu().numpy().astype(int)
                    center_x = int((x1 + x2) / 2)
                    center_y = int((y1 + y2) / 2)
                    area = int((x2 - x1) * (y2 - y1))
                    
                    detections.append(DetectedObject(
                        label=label,
                        confidence=conf,
                        bbox=(int(x1), int(y1), int(x2), int(y2)),
                        center=(center_x, center_y),
                        area=area,
                        class_id=class_id,
                    ))
            
            return sorted(detections, key=lambda d: d.confidence, reverse=True)
        
        except Exception as exc:
            self.log.error(f"Object detection failed: {exc}")
            return []

    def segment_ui_elements(
        self,
        image: Image.Image,
        *,
        min_area: int = 100,
        max_elements: int = 100,
    ) -> List[UIElement]:
        """
        Segment UI elements using SAM.
        
        Args:
            image: PIL Image to segment
            min_area: Minimum area in pixels for a valid element
            max_elements: Maximum number of elements to return
            
        Returns:
            List of segmented UI elements with masks and metadata
        """
        sam = self._load_sam()
        if sam is None:
            self.log.warning("SAM model not available, returning empty list")
            return []
        
        try:
            img_array = np.array(image.convert("RGB"))
            masks = sam.generate(img_array)
            
            elements = []
            for idx, mask_data in enumerate(masks[:max_elements]):
                segmentation = mask_data["segmentation"]
                bbox = mask_data["bbox"]  # [x, y, w, h]
                area = mask_data["area"]
                
                if area < min_area:
                    continue
                
                x, y, w, h = bbox
                x1, y1, x2, y2 = int(x), int(y), int(x + w), int(y + h)
                center_x = int(x + w / 2)
                center_y = int(y + h / 2)
                
                element_id = hashlib.md5(
                    f"{x1}_{y1}_{x2}_{y2}_{area}".encode()
                ).hexdigest()[:12]
                
                # Classify element type based on aspect ratio and position
                aspect_ratio = w / h if h > 0 else 1.0
                element_type = self._classify_ui_element(aspect_ratio, area, y1, img_array.shape[0])
                
                elements.append(UIElement(
                    element_id=element_id,
                    element_type=element_type,
                    bbox=(x1, y1, x2, y2),
                    center=(center_x, center_y),
                    mask=segmentation,
                    confidence=float(mask_data.get("predicted_iou", 0.0)),
                    text=None,
                    interactable=element_type in {"button", "input", "menu", "tab"},
                ))
            
            return sorted(elements, key=lambda e: e.confidence, reverse=True)
        
        except Exception as exc:
            self.log.error(f"UI segmentation failed: {exc}")
            return []

    def _classify_ui_element(
        self,
        aspect_ratio: float,
        area: int,
        y_pos: int,
        screen_height: int,
    ) -> str:
        """Heuristic classification of UI element type."""
        # Top 10% of screen
        if y_pos < screen_height * 0.1:
            if aspect_ratio > 3.0:
                return "menu_bar"
            return "toolbar"
        
        # Small square elements
        if 0.8 < aspect_ratio < 1.2 and area < 2500:
            return "icon"
        
        # Wide rectangular elements
        if aspect_ratio > 2.5:
            if area < 5000:
                return "button"
            return "input"
        
        # Tall elements
        if aspect_ratio < 0.5:
            return "sidebar"
        
        # Large elements
        if area > 50000:
            return "window"
        
        return "element"

    def understand_screen(
        self,
        image: Image.Image,
        query: Optional[str] = None,
    ) -> str:
        """
        Generate natural language description of screen content.
        
        Args:
            image: PIL Image to analyze
            query: Optional specific question about the image
            
        Returns:
            Natural language description or answer
        """
        blip_model, blip_processor = self._load_blip()
        if blip_model is None or blip_processor is None:
            return "Visual understanding model not available"
        
        try:
            if query:
                # Visual question answering
                inputs = blip_processor(image, query, return_tensors="pt")
                if self.device != "cpu":
                    inputs = {k: v.to(self.device) for k, v in inputs.items()}
                
                generated_ids = blip_model.generate(**inputs, max_length=50)
                answer = blip_processor.decode(generated_ids[0], skip_special_tokens=True)
                return answer.strip()
            else:
                # Image captioning
                inputs = blip_processor(image, return_tensors="pt")
                if self.device != "cpu":
                    inputs = {k: v.to(self.device) for k, v in inputs.items()}
                
                generated_ids = blip_model.generate(**inputs, max_length=50)
                caption = blip_processor.decode(generated_ids[0], skip_special_tokens=True)
                return caption.strip()
        
        except Exception as exc:
            self.log.error(f"Screen understanding failed: {exc}")
            return "Failed to analyze screen content"

    def find_visual_target(
        self,
        image: Image.Image,
        description: str,
        *,
        confidence_threshold: float = 0.25,
    ) -> Optional[Tuple[int, int, int, int]]:
        """
        Find object or UI element by natural language description using CLIP similarity.
        
        Args:
            image: PIL Image to search
            description: Natural language description of target
            confidence_threshold: Minimum similarity score
            
        Returns:
            Bounding box (x1, y1, x2, y2) of best match, or None
        """
        clip_model, clip_processor = self._load_clip()
        if clip_model is None or clip_processor is None:
            self.log.warning("CLIP model not available")
            return None
        
        try:
            # Get UI elements or object detections
            ui_elements = self.segment_ui_elements(image, max_elements=50)
            
            if not ui_elements:
                return None
            
            # Crop image patches for each element
            img_array = np.array(image)
            patches = []
            bboxes = []
            
            for element in ui_elements:
                x1, y1, x2, y2 = element.bbox
                patch = Image.fromarray(img_array[y1:y2, x1:x2])
                patches.append(patch)
                bboxes.append(element.bbox)
            
            # Compute CLIP similarity
            inputs = clip_processor(
                text=[description],
                images=patches,
                return_tensors="pt",
                padding=True,
            )
            
            if self.device != "cpu":
                inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            outputs = clip_model(**inputs)
            logits_per_text = outputs.logits_per_text
            probs = logits_per_text.softmax(dim=1).cpu().numpy()[0]
            
            best_idx = int(np.argmax(probs))
            best_score = float(probs[best_idx])
            
            if best_score >= confidence_threshold:
                return bboxes[best_idx]
            
            return None
        
        except Exception as exc:
            self.log.error(f"Visual target search failed: {exc}")
            return None

    def analyze_screen_context(
        self,
        image: Optional[Image.Image] = None,
        *,
        detect_objects: bool = True,
        segment_ui: bool = True,
        generate_summary: bool = True,
    ) -> VisualContext:
        """
        Comprehensive screen analysis combining all vision capabilities.
        
        Args:
            image: PIL Image to analyze (captures screen if None)
            detect_objects: Whether to run object detection
            segment_ui: Whether to segment UI elements
            generate_summary: Whether to generate text summary
            
        Returns:
            Complete visual context with all analysis results
        """
        if image is None:
            image = self.capture_screen()
        
        timestamp = time.time()
        img_bytes = io.BytesIO()
        image.save(img_bytes, format="PNG")
        screenshot_hash = hashlib.sha256(img_bytes.getvalue()).hexdigest()[:16]
        
        detected_objects = []
        if detect_objects:
            detected_objects = self.detect_objects(image, confidence_threshold=0.4)
        
        ui_elements = []
        if segment_ui:
            ui_elements = self.segment_ui_elements(image, max_elements=50)
        
        screen_summary = ""
        if generate_summary:
            screen_summary = self.understand_screen(image)
        
        # Extract dominant colors
        img_small = image.resize((100, 100))
        pixels = np.array(img_small).reshape(-1, 3)
        from sklearn.cluster import KMeans
        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        kmeans.fit(pixels)
        dominant_colors = [tuple(map(int, color)) for color in kmeans.cluster_centers_]
        
        return VisualContext(
            timestamp=timestamp,
            screenshot_hash=screenshot_hash,
            active_application="",  # Will be filled by context engine
            detected_objects=detected_objects,
            ui_elements=ui_elements,
            screen_summary=screen_summary,
            dominant_colors=dominant_colors,
            text_content=[],  # OCR will be handled separately
        )

    def compare_screens(
        self,
        image1: Image.Image,
        image2: Image.Image,
    ) -> Dict[str, Any]:
        """
        Compare two screenshots to detect changes.
        
        Returns:
            Dictionary with change metrics and regions
        """
        try:
            # Convert to numpy arrays
            arr1 = np.array(image1.convert("RGB"))
            arr2 = np.array(image2.convert("RGB"))
            
            if arr1.shape != arr2.shape:
                return {"changed": True, "reason": "Different image dimensions"}
            
            # Compute pixel-wise difference
            diff = np.abs(arr1.astype(float) - arr2.astype(float))
            mean_diff = float(np.mean(diff))
            max_diff = float(np.max(diff))
            changed_pixels = int(np.sum(np.any(diff > 10, axis=2)))
            total_pixels = arr1.shape[0] * arr1.shape[1]
            change_percentage = (changed_pixels / total_pixels) * 100
            
            return {
                "changed": change_percentage > 1.0,
                "change_percentage": round(change_percentage, 2),
                "mean_difference": round(mean_diff, 2),
                "max_difference": round(max_diff, 2),
                "changed_pixels": changed_pixels,
            }
        
        except Exception as exc:
            self.log.error(f"Screen comparison failed: {exc}")
            return {"changed": False, "error": str(exc)}

    def health_check(self) -> Dict[str, Any]:
        """Check health status of all vision models."""
        return {
            "status": "operational",
            "device": self.device,
            "models": {
                "yolo": self._yolo_model is not None,
                "sam": self._sam_model is not None,
                "clip": self._clip_model is not None,
                "blip": self._blip_model is not None,
            },
            "cache_size": len(self._embedding_cache),
            "runtime": self.runtime_status(),
        }

    def runtime_status(self) -> Dict[str, Any]:
        items = [
            self._runtime_view("yolo", artifact_path=str(self.models_dir / "yolov10n.pt"), loaded=self._yolo_model is not None),
            self._runtime_view("sam", artifact_path=str(self.models_dir / "sam_vit_b_01ec64.pth"), loaded=self._sam_model is not None),
            self._runtime_view("clip", artifact_path=str(self.models_dir / "clip"), loaded=self._clip_model is not None),
            self._runtime_view("blip", artifact_path=str(self.models_dir / "blip2"), loaded=self._blip_model is not None),
        ]
        loaded_count = sum(1 for row in items if bool(row.get("loaded", False)))
        error_count = sum(1 for row in items if str(row.get("last_error", "")).strip())
        return {
            "status": "success",
            "device": self.device,
            "models_dir": str(self.models_dir),
            "cache_embeddings": bool(self.cache_embeddings),
            "embedding_cache_size": len(self._embedding_cache),
            "loaded_count": loaded_count,
            "error_count": error_count,
            "items": items,
        }

    def warm_models(self, *, models: Optional[List[str]] = None, force_reload: bool = False) -> Dict[str, Any]:
        selected = [str(item or "").strip().lower() for item in (models or ["yolo", "clip"]) if str(item or "").strip()]
        if not selected:
            selected = ["yolo", "clip"]
        if force_reload:
            self.reset_models(models=selected, clear_cache=False)
        loaders = {
            "yolo": self._load_yolo,
            "sam": self._load_sam,
            "clip": lambda: self._load_clip()[0],
            "blip": lambda: self._load_blip()[0],
        }
        results: List[Dict[str, Any]] = []
        for name in selected:
            loader = loaders.get(name)
            if loader is None:
                results.append({"status": "error", "model": name, "message": "unsupported_vision_runtime"})
                continue
            started = time.monotonic()
            loaded = loader()
            row = self._runtime_view(name, loaded=loaded is not None)
            row["status"] = "success" if loaded is not None else "error"
            row["warm_latency_s"] = round(max(0.0, time.monotonic() - started), 4)
            results.append(row)
        return {
            "status": "success" if any(str(item.get("status", "")).strip().lower() == "success" for item in results) else "error",
            "count": len(results),
            "items": results,
            "runtime": self.runtime_status(),
        }

    def reset_models(self, *, models: Optional[List[str]] = None, clear_cache: bool = False) -> Dict[str, Any]:
        selected = {str(item or "").strip().lower() for item in (models or ["yolo", "sam", "clip", "blip"]) if str(item or "").strip()}
        if not selected:
            selected = {"yolo", "sam", "clip", "blip"}
        removed: List[str] = []
        if "yolo" in selected:
            self._yolo_model = None
            removed.append("yolo")
        if "sam" in selected:
            self._sam_model = None
            removed.append("sam")
        if "clip" in selected:
            self._clip_model = None
            self._clip_processor = None
            removed.append("clip")
        if "blip" in selected:
            self._blip_model = None
            self._blip_processor = None
            removed.append("blip")
        for name in removed:
            row = self._ensure_model_runtime(name, self._artifact_path_for(name))
            row["loaded"] = False
        if clear_cache:
            self._embedding_cache.clear()
        gc.collect()
        return {
            "status": "success",
            "removed": removed,
            "clear_cache": bool(clear_cache),
            "runtime": self.runtime_status(),
        }

    def _artifact_path_for(self, model_name: str) -> str:
        mapping = {
            "yolo": str(self.models_dir / "yolov10n.pt"),
            "sam": str(self.models_dir / "sam_vit_b_01ec64.pth"),
            "clip": str(self.models_dir / "clip"),
            "blip": str(self.models_dir / "blip2"),
        }
        return mapping.get(str(model_name or "").strip().lower(), "")

    def _ensure_model_runtime(self, model_name: str, artifact_path: str) -> Dict[str, Any]:
        clean_name = str(model_name or "").strip().lower()
        row = self._model_runtime.get(clean_name)
        if isinstance(row, dict):
            if artifact_path:
                row["artifact_path"] = artifact_path
            return row
        row = {
            "model": clean_name,
            "artifact_path": artifact_path,
            "loaded": False,
            "attempts": 0,
            "successes": 0,
            "failures": 0,
            "last_error": "",
            "last_loaded_at": 0.0,
            "load_latency_s": 0.0,
        }
        self._model_runtime[clean_name] = row
        return row

    def _mark_model_loaded(self, model_name: str, *, artifact_path: str, load_latency_s: float) -> None:
        row = self._ensure_model_runtime(model_name, artifact_path)
        row["attempts"] = int(row.get("attempts", 0) or 0) + 1
        row["successes"] = int(row.get("successes", 0) or 0) + 1
        row["loaded"] = True
        row["last_error"] = ""
        row["last_loaded_at"] = time.time()
        row["load_latency_s"] = round(max(0.0, float(load_latency_s)), 4)

    def _mark_model_error(self, model_name: str, *, artifact_path: str, error: str) -> None:
        row = self._ensure_model_runtime(model_name, artifact_path)
        row["attempts"] = int(row.get("attempts", 0) or 0) + 1
        row["failures"] = int(row.get("failures", 0) or 0) + 1
        row["loaded"] = False
        row["last_error"] = str(error or "").strip()

    def _runtime_view(self, model_name: str, *, artifact_path: str = "", loaded: bool = False) -> Dict[str, Any]:
        row = self._ensure_model_runtime(model_name, artifact_path or self._artifact_path_for(model_name))
        row["loaded"] = bool(loaded)
        path = str(row.get("artifact_path", "")).strip()
        return {
            "model": str(row.get("model", model_name)).strip().lower(),
            "artifact_path": path,
            "artifact_exists": bool(Path(path).exists()) if path else False,
            "loaded": bool(row.get("loaded", False)),
            "attempts": int(row.get("attempts", 0) or 0),
            "successes": int(row.get("successes", 0) or 0),
            "failures": int(row.get("failures", 0) or 0),
            "last_error": str(row.get("last_error", "")).strip(),
            "last_loaded_at": float(row.get("last_loaded_at", 0.0) or 0.0),
            "load_latency_s": float(row.get("load_latency_s", 0.0) or 0.0),
        }
