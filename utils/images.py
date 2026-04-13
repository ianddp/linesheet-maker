"""Image download, caching, and processing utilities."""
from __future__ import annotations

import hashlib
import io
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse, urljoin

import requests
from PIL import Image

# Persistent image cache directory
CACHE_DIR = Path(tempfile.gettempdir()) / "linesheet_images"
CACHE_DIR.mkdir(exist_ok=True)

# Request headers to avoid bot blocking
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
}


def url_to_cache_path(url: str) -> Path:
    """Generate a deterministic cache path for an image URL."""
    url_hash = hashlib.md5(url.encode()).hexdigest()
    ext = Path(urlparse(url).path).suffix or ".jpg"
    if ext.lower() not in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg"):
        ext = ".jpg"
    return CACHE_DIR / f"{url_hash}{ext}"


def download_image(url: str, timeout: int = 30) -> str:
    """
    Download an image from URL and cache it locally.
    Returns the local file path, or empty string on failure.
    """
    if not url or not url.startswith(("http://", "https://")):
        return ""

    cache_path = url_to_cache_path(url)
    if cache_path.exists() and cache_path.stat().st_size > 0:
        return str(cache_path)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, stream=True)
        resp.raise_for_status()

        content_type = resp.headers.get("content-type", "")
        if "image" not in content_type and "octet-stream" not in content_type:
            # Try to open it as an image anyway
            pass

        data = resp.content
        if len(data) < 100:
            return ""

        # Validate it's actually an image
        try:
            img = Image.open(io.BytesIO(data))
            img.verify()
        except Exception:
            return ""

        cache_path.write_bytes(data)
        return str(cache_path)

    except Exception:
        return ""


def resize_for_thumbnail(image_path: str, max_size: tuple = (150, 150)) -> str:
    """Resize image to thumbnail dimensions. Returns path to resized image."""
    if not image_path or not os.path.exists(image_path):
        return ""

    try:
        thumb_path = Path(image_path).with_suffix(".thumb.jpg")
        if thumb_path.exists():
            return str(thumb_path)

        img = Image.open(image_path)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        img.thumbnail(max_size, Image.LANCZOS)
        img.save(str(thumb_path), "JPEG", quality=85)
        return str(thumb_path)
    except Exception:
        return ""


def resize_for_excel(image_path: str, max_size: tuple = (120, 120)) -> str:
    """Resize image for embedding in Excel. Returns path to resized image."""
    if not image_path or not os.path.exists(image_path):
        return ""

    try:
        excel_path = Path(image_path).with_suffix(".excel.png")
        if excel_path.exists():
            return str(excel_path)

        img = Image.open(image_path)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        img.thumbnail(max_size, Image.LANCZOS)
        img.save(str(excel_path), "PNG")
        return str(excel_path)
    except Exception:
        return ""


def save_pil_image(pil_image: Image.Image, prefix: str = "pdf_img") -> str:
    """Save a PIL Image to the cache directory. Returns the local file path."""
    try:
        img_hash = hashlib.md5(pil_image.tobytes()[:1024]).hexdigest()
        path = CACHE_DIR / f"{prefix}_{img_hash}.png"
        if path.exists():
            return str(path)
        if pil_image.mode in ("RGBA", "P"):
            pil_image = pil_image.convert("RGB")
        pil_image.save(str(path), "PNG")
        return str(path)
    except Exception:
        return ""


def pick_best_image_url(urls: list[str]) -> str:
    """
    Given a list of image URLs, pick the one most likely to be high-res.
    Prefers larger srcset variants, avoids thumbnails.
    """
    if not urls:
        return ""

    scored = []
    for url in urls:
        score = 0
        lower = url.lower()

        # Penalize thumbnails
        if any(t in lower for t in ["thumb", "tiny", "small", "icon", "50x", "75x", "100x"]):
            score -= 50
        # Penalize placeholder images
        if any(t in lower for t in ["placeholder", "no-image", "noimage", "blank"]):
            score -= 100
        # Prefer large indicators
        if any(t in lower for t in ["large", "1024", "1200", "2048", "original", "master"]):
            score += 30
        # Prefer common CDN image paths
        if any(t in lower for t in ["/products/", "/images/", "/media/"]):
            score += 10
        # Prefer jpg/png over svg/gif
        if lower.endswith((".jpg", ".jpeg", ".png", ".webp")):
            score += 10
        # Penalize tracking pixels and spacers
        if "spacer" in lower or "pixel" in lower or "1x1" in lower:
            score -= 100

        scored.append((score, url))

    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1] if scored else ""
