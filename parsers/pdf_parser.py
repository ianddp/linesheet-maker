"""
PDF parser for extracting product data from catalog PDFs.

HARD RULE: Output is at the sellable-variant level, NOT the product-family level.
- If a source page shows 8 colorways, output 8 rows.
- Each row gets its own cropped single-product image.
- The Color column is populated whenever the source provides color names.
- No grouped/collage images for single-item products.
"""
from __future__ import annotations

import io
import os
import re
import tempfile
from pathlib import Path

from PIL import Image

from utils.images import save_pil_image, CACHE_DIR
from utils.schema import Product

try:
    import fitz  # PyMuPDF
    HAS_FITZ = True
except ImportError:
    HAS_FITZ = False

try:
    import pytesseract
    HAS_TESSERACT = True
except ImportError:
    HAS_TESSERACT = False


# ── OCR correction map ──
# Common Tesseract misreads in wholesale catalog color names
OCR_CORRECTIONS = {
    # Cotopaxi brand name variations
    "cotopaxiblack": "Cotopaxi Black",
    "cotopaxi black": "Cotopaxi Black",
    "cotopakiblack": "Cotopaxi Black",
    "cotopam black": "Cotopaxi Black",
    "cotopanm black": "Cotopaxi Black",
    "cotopaxi blk": "Cotopaxi Black",
    "cotopax black": "Cotopaxi Black",
    "cotoran black": "Cotopaxi Black",
    "cotoran blk": "Cotopaxi Black",
    "cotopan black": "Cotopaxi Black",
    "ootoran black": "Cotopaxi Black",
    "cororan black": "Cotopaxi Black",
    # Fatigue
    "fangue": "Fatigue",
    "fangue c/o": "Fatigue",
    "fatigue": "Fatigue",
    "fanigue": "Fatigue",
    # Terra
    "terra noo": "Terra",
    "terra moco": "Terra",
    "terra": "Terra",
    # Mineral
    "menerai": "Mineral",
    "menerial": "Mineral",
    "meneral": "Mineral",
    "mineral": "Mineral",
    # Everglade
    "everglade go": "Everglade",
    "everglade": "Everglade",
    # Cinder
    "cinder": "Cinder",
    # Other common misreads
    "coto!": "Cotopaxi",
    "coto! glace": "Cotopaxi Black",
    "cotopax!": "Cotopaxi",
    "cotopaxl": "Cotopaxi",
}

# Known valid colors for strict matching
KNOWN_COLORS = {
    # Basic colors
    "black", "white", "red", "blue", "green", "yellow", "orange", "purple",
    "pink", "brown", "grey", "gray", "navy", "cream", "ivory", "beige",
    "tan", "khaki", "olive", "teal", "coral", "maroon", "burgundy",
    "charcoal", "silver", "gold", "rose", "lavender", "mint", "sage",
    "indigo", "crimson", "scarlet", "cyan", "magenta", "turquoise",
    "plum", "mauve", "peach", "salmon", "rust", "copper", "bronze",
    "sand", "stone", "slate", "ash", "smoke", "fog", "mist",
    "midnight", "cobalt", "royal blue", "sky blue", "baby blue", "powder blue",
    "forest", "hunter", "lime", "emerald", "jade", "moss",
    "wine", "berry", "cherry", "raspberry", "blush", "fuchsia",
    "taupe", "mocha", "espresso", "chocolate", "camel", "cognac",
    "mustard", "amber", "honey", "lemon", "canary",
    "ocean", "aqua", "sea", "marine", "arctic", "ice",
    # Outdoor / brand-specific
    "fatigue", "camo", "camouflage", "heather", "denim",
    "graphite", "iron", "steel", "pewter", "titanium",
    "oat", "wheat", "flax", "linen", "natural",
    "bone", "parchment", "eggshell", "pearl", "oyster",
    "spruce", "pine", "fern", "clover", "basil",
    "clay", "terracotta", "sienna", "adobe", "brick",
    "storm", "thunder", "cloud", "dusk", "dawn",
    # Cotopaxi specific
    "del dia", "cada dia", "cotopaxi black", "terra", "mineral",
    "everglade", "cinder", "palmetto", "putty", "cornflower",
    "crust", "kimchy", "gumball", "log cabin", "flame",
    "sandstone", "butter", "deep sea", "kaleidoscope",
    "crimson fare", "blue spruce",
}

# Words that should NEVER be treated as colors
NON_COLOR_WORDS = {
    "fall", "ats", "msrp", "cotopaxi", "wholesale", "retail", "spring",
    "summer", "winter", "description", "product", "price", "sku", "upc",
    "style", "size", "notes", "moq", "qty", "quantity", "units",
    "available", "stock", "pack", "packs", "bags", "bag", "travel",
    "daypack", "backpack", "duffel", "tote", "sling", "hip", "fanny",
    "august", "september", "october", "november", "december",
    "january", "february", "march", "april", "may", "june", "july",
    "shipping", "ship", "date", "delivery", "page", "total",
    "item", "items", "collection", "category", "new", "sale",
    "men", "women", "unisex", "kids", "youth", "adult",
    "small", "medium", "large", "xlarge", "one size",
    "accessories", "gear", "hauler", "organizer", "organizers",
    "pouch", "kit", "mini", "mesh", "convertible", "shoulder",
    "capsule", "prebook", "template",
    # OCR garbage
    "3k", "1k", "2k", "5k", "10k",
}


def check_dependencies() -> list[str]:
    """Check which PDF dependencies are available."""
    missing = []
    if not HAS_FITZ:
        missing.append("PyMuPDF (pip install PyMuPDF)")
    if not HAS_TESSERACT:
        missing.append("pytesseract (pip install pytesseract + brew install tesseract)")
    return missing


def parse_pdf(file_bytes: bytes, filename: str = "upload.pdf", progress_callback=None) -> list[Product]:
    """
    Parse a PDF and extract products at the VARIANT level.
    Every offered color → its own row with its own cropped image.
    """
    if not HAS_FITZ:
        raise ImportError("PyMuPDF is required for PDF parsing. Install with: pip install PyMuPDF")

    doc = fitz.open(stream=file_bytes, filetype="pdf")
    all_products = []
    total_pages = len(doc)

    for page_num in range(total_pages):
        if progress_callback:
            progress_callback(page_num + 1, total_pages)

        page = doc[page_num]
        images = _extract_page_images(page, page_num)

        # Try text extraction first
        text = page.get_text("text").strip()
        has_product_text = (
            text and len(text) > 20 and _has_product_indicators(text)
        )

        if has_product_text:
            products = _parse_page_products(text, page_num, filename, images)
            if products:
                all_products.extend(products)
                continue

        # Fall back to OCR
        if HAS_TESSERACT:
            ocr_text = _ocr_page(page)
            if ocr_text and len(ocr_text.strip()) > 10:
                products = _parse_page_products(ocr_text, page_num, filename, images)
                if products:
                    all_products.extend(products)
                    continue

        # Last resort placeholder
        big_images = [img for img in images if _is_product_image(img)]
        if big_images and not all_products:
            for img_path in big_images[:1]:
                all_products.append(Product(
                    product_image=img_path,
                    _source="pdf",
                    _source_detail=f"{filename}, page {page_num + 1}",
                    notes=f"Page {page_num + 1} - needs manual data entry",
                ))

    doc.close()
    return all_products


# ─────────────────────────────────────────────────────────
# Page-level product parsing
# ─────────────────────────────────────────────────────────

def _parse_page_products(text: str, page_num: int, filename: str, images: list[str]) -> list[Product]:
    """
    Parse a page's text into variant-level Product rows.

    ACCURACY-FIRST RULES:
    - Prefer NO image over a WRONG image
    - Prefer BLANK color over a WRONG color
    - Pull listed colors aggressively from BEFORE and AFTER each SKU
    - For single-product pages, gather ALL colors from the entire page
    - Never reuse the same image file for different SKUs
    - Use composite cropping to get per-SKU images when available
    - Use smaller catalog-specific images when they are the best per-SKU match
    - Track assigned images so no image is used twice across SKUs
    """
    products = []

    # ── Strategy 1: SKU + Name + Price lines ──
    sku_pattern = re.compile(
        r'([A-Z$]\d{4,}\w*)\s+'
        r'(.+?)'
        r'(?:\s*\|\s*)?'
        r'(?:MSRP\s*)?'
        r'\$\s*(\d+(?:\.\d{2})?)'
    )
    sku_matches = list(sku_pattern.finditer(text))

    if sku_matches:
        page_qtys = _extract_all_qtys_from_page(text)
        n_skus = len(sku_matches)

        # Collect per-SKU colors from text BEFORE and AFTER each SKU
        per_sku_colors = []
        all_page_colors = []
        for i, match in enumerate(sku_matches):
            # Text before this SKU (between previous SKU end and this SKU start)
            text_before = text[:match.start()]
            if i > 0:
                text_before = text[sku_matches[i - 1].end():match.start()]

            # Text after this SKU (between this SKU end and next SKU start)
            text_after = ""
            if i + 1 < len(sku_matches):
                text_after = text[match.end():sku_matches[i + 1].start()]
            else:
                text_after = text[match.end():match.end() + 400]

            colors_before = _extract_colors_from_context(
                text_before, text, match.group(1), match.group(2)
            )
            colors_after = _extract_colors_from_context(
                text_after, text, match.group(1), match.group(2)
            )

            # Merge: prefer before (catalog standard), add any new from after
            combined = list(colors_before)
            seen = {c.lower() for c in combined}
            for c in colors_after:
                if c.lower() not in seen:
                    combined.append(c)
                    seen.add(c.lower())

            per_sku_colors.append(combined)
            all_page_colors.extend(combined)

        # Dedupe the full page color list
        all_page_colors = _dedupe_colors(all_page_colors)

        # Check if SKUs are for DIFFERENT products
        unique_names = set()
        for m in sku_matches:
            name = m.group(2).strip().rstrip('|').rstrip('-').strip()
            base_name = re.sub(r'\s*[-–]\s*(Del\s+Dia|Del).*$', '', name, flags=re.IGNORECASE).strip()
            unique_names.add(base_name.lower())

        is_multi_product_page = n_skus >= 2 and len(unique_names) > 1

        # ── IMAGE STRATEGY ──
        # Classify all page images
        all_product_images = _get_all_product_images(images)
        hero_images = [img for img in all_product_images if not _is_composite_image(img)]
        composite_images = [img for img in all_product_images if _is_composite_image(img)]

        # Track which images have been assigned to prevent reuse
        used_images = set()

        for i, match in enumerate(sku_matches):
            sku = match.group(1)
            name = match.group(2).strip().rstrip('|').rstrip('-').strip()
            price = match.group(3)

            if len(name) < 3:
                continue

            qty = page_qtys[i] if i < len(page_qtys) else ""

            # ── IMAGE ASSIGNMENT ──
            img = ""
            if is_multi_product_page:
                # Try composite crop first — get the i-th item from the panel
                if composite_images:
                    best_composite = max(composite_images, key=lambda p: Image.open(p).width)
                    crops = _crop_composite_image(best_composite, n_skus, page_num, sku)
                    if i < len(crops) and crops[i]:
                        img = crops[i]

                # Fall back to positional hero match (i-th hero for i-th SKU)
                if not img and len(hero_images) == n_skus:
                    candidate = hero_images[i]
                    if candidate not in used_images:
                        img = candidate

                # Fall back to smaller individual images
                if not img:
                    for candidate in hero_images:
                        if candidate not in used_images:
                            img = candidate
                            break
            else:
                # Single-product page
                if len(hero_images) == 1:
                    img = hero_images[0]
                elif len(hero_images) >= 1 and n_skus == 1:
                    # Use the first unused hero
                    for candidate in hero_images:
                        if candidate not in used_images:
                            img = candidate
                            break

            if img:
                used_images.add(img)

            # ── COLOR ASSIGNMENT ──
            sku_colors = per_sku_colors[i]
            name_color = _extract_color_from_name(name)

            if is_multi_product_page:
                # Multi-product page: each SKU gets its own color(s)
                if sku_colors:
                    # If we found colors adjacent to this SKU, use the first one
                    color = sku_colors[0]
                elif name_color:
                    color = name_color
                else:
                    color = ""

                products.append(Product(
                    product_image=img,
                    sku_upc=sku,
                    product_name=name,
                    color=color,
                    msrp=f"${price}",
                    qty=qty,
                    _source="pdf",
                    _source_detail=f"{filename}, page {page_num + 1}",
                ))
            else:
                # Single-product / detail page: expand color variants
                # Use per-SKU colors first, fall back to ALL page colors
                if sku_colors:
                    expand_colors = sku_colors
                elif all_page_colors:
                    expand_colors = all_page_colors
                elif name_color:
                    expand_colors = [name_color]
                else:
                    expand_colors = []

                if len(expand_colors) > 1:
                    # Multiple colors — one row per color
                    # Try to get per-variant images from composite
                    variant_images = [""] * len(expand_colors)
                    if composite_images:
                        best_comp = max(composite_images, key=lambda p: Image.open(p).width)
                        crops = _crop_composite_image(best_comp, len(expand_colors), page_num, sku)
                        for vi in range(min(len(crops), len(expand_colors))):
                            if crops[vi]:
                                variant_images[vi] = crops[vi]

                    for ci, color in enumerate(expand_colors):
                        v_img = variant_images[ci] if ci < len(variant_images) and variant_images[ci] else img
                        products.append(Product(
                            product_image=v_img,
                            sku_upc=sku,
                            product_name=name,
                            color=color,
                            msrp=f"${price}",
                            qty=qty,
                            _source="pdf",
                            _source_detail=f"{filename}, page {page_num + 1}",
                        ))
                else:
                    color = expand_colors[0] if expand_colors else name_color
                    products.append(Product(
                        product_image=img,
                        sku_upc=sku,
                        product_name=name,
                        color=color if color else "",
                        msrp=f"${price}",
                        qty=qty,
                        _source="pdf",
                        _source_detail=f"{filename}, page {page_num + 1}",
                    ))

        if products:
            return products

    # ── Strategy 2: Name + Price (no SKU) ──
    name_price = re.findall(
        r'([A-Z][A-Za-z0-9é\s]+(?:[-–]\s*[A-Za-zé\s]+)?)\s*'
        r'\$\s*(\d+(?:\.\d{2})?)',
        text
    )

    if name_price:
        all_product_images = _get_all_product_images(images)
        hero_imgs = [img for img in all_product_images if not _is_composite_image(img)]
        used_images = set()

        for i, (name, price) in enumerate(name_price):
            name = re.sub(r'\s+', ' ', name).strip()
            if len(name) < 4:
                continue
            if name.lower().startswith(('fall ats', 'august ship', 'packs &')):
                continue

            # Assign unique image per product — no reuse
            img = ""
            if len(hero_imgs) == len(name_price) and i < len(hero_imgs):
                img = hero_imgs[i]
            elif len(hero_imgs) >= 1:
                for candidate in hero_imgs:
                    if candidate not in used_images:
                        img = candidate
                        break
            if img:
                used_images.add(img)

            color = _extract_color_from_name(name)

            products.append(Product(
                product_image=img,
                product_name=name,
                color=color,
                msrp=f"${price}",
                _source="pdf",
                _source_detail=f"{filename}, page {page_num + 1}",
            ))

        if products:
            return products

    # ── Strategy 3: Table-style ──
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    table_products = _try_parse_table(lines, page_num, filename, images)
    if table_products:
        return table_products

    # ── Strategy 4: Block-style ──
    block_products = _try_parse_blocks(text, page_num, filename, images)
    if block_products:
        return block_products

    return products


def _get_all_product_images(images: list[str]) -> list[str]:
    """
    Get all usable product images from a page — heroes, small catalog images,
    and composites. Filters out only tiny icons/logos.
    Uses a lower size threshold than before to capture smaller catalog-specific images.
    """
    result = []
    for img_path in images:
        try:
            img = Image.open(img_path)
            w, h = img.size

            # Filter out very tiny images (icons, logos, bullets)
            if w < 60 or h < 60:
                continue

            result.append(img_path)
        except Exception:
            continue

    return result


def _get_hero_images(images: list[str]) -> list[str]:
    """
    Get only confident hero images — substantial single-product shots.
    Filters OUT composites, tiny crops, panels, and logos.
    Includes smaller catalog images (80px+) that are likely product-specific.
    """
    heroes = []
    for img_path in images:
        try:
            img = Image.open(img_path)
            w, h = img.size
            ratio = w / max(h, 1)

            # Must be at least a small catalog image
            if w < 80 or h < 80:
                continue

            # Must NOT be a wide composite panel
            if ratio > 2.0 and w > 400:
                continue

            # Must NOT be a very wide strip (color swatches, etc.)
            if ratio > 3.0:
                continue

            heroes.append(img_path)
        except Exception:
            continue

    return heroes


# ─────────────────────────────────────────────────────────
# Color extraction — fuzzy OCR-aware, variant-level
# ─────────────────────────────────────────────────────────

# Flat list of known color names for fuzzy matching
_FUZZY_COLORS = sorted(KNOWN_COLORS, key=len, reverse=True)


def _edit_distance(a: str, b: str) -> int:
    """Simple Levenshtein edit distance."""
    if len(a) < len(b):
        return _edit_distance(b, a)
    if len(b) == 0:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            curr.append(min(
                prev[j + 1] + 1,
                curr[j] + 1,
                prev[j] + (0 if ca == cb else 1),
            ))
        prev = curr
    return prev[len(b)]


def _fuzzy_match_color(text: str) -> str:
    """
    Fuzzy-match garbled OCR text to a known color name.
    Returns the matched color (title case) or "" if no match.
    Allows up to ~30% character errors for short names.
    """
    lower = text.strip().lower()
    if not lower or len(lower) < 3:
        return ""

    # Reject obvious non-colors
    if lower in NON_COLOR_WORDS:
        return ""
    words = lower.split()
    if all(w in NON_COLOR_WORDS for w in words):
        return ""

    # Reject pure numbers / quantity patterns
    if re.match(r'^\d+[Kk]?\s*\d*[Kk]?$', lower):
        return ""

    # Direct match
    if lower in KNOWN_COLORS:
        return lower.title()

    # Direct OCR correction
    corrected = _correct_ocr_color(lower)
    if corrected.lower() in KNOWN_COLORS:
        return corrected.title()

    # Fuzzy match — find closest known color within edit distance threshold
    best_match = ""
    best_dist = 999

    for known in _FUZZY_COLORS:
        # Only compare colors of similar length (±40%)
        if abs(len(known) - len(lower)) > max(len(known), len(lower)) * 0.4:
            continue

        dist = _edit_distance(lower, known)
        # Threshold: allow ~30% errors
        threshold = max(2, len(known) // 3)

        if dist < best_dist and dist <= threshold:
            best_dist = dist
            best_match = known

    if best_match:
        return best_match.title()

    return ""


def _extract_colors_from_context(text_before: str, full_text: str, sku: str, name: str) -> list[str]:
    """
    Extract color variants for a product.
    In wholesale catalogs, color names appear BEFORE the SKU line.
    Uses fuzzy matching to handle OCR errors.
    """
    colors = []

    # Clean OCR text
    cleaned = _clean_ocr_text(text_before)

    # Process each line before the SKU
    for line in cleaned.split('\n'):
        line = line.strip()
        if not line:
            continue

        # Skip prices, SKUs, quantities
        if re.match(r'^\$\d+', line):
            continue
        if re.match(r'^[A-Z$]\d{4,}', line):
            continue
        if re.match(r'^\d+[Kk]\s', line):
            continue
        if len(line) > 80:
            continue

        # Extract color tokens from this line
        line_colors = _parse_color_line_fuzzy(line)
        colors.extend(line_colors)

    # Check for explicit "Color:" label
    sku_pos = full_text.find(sku)
    if sku_pos >= 0:
        window = full_text[sku_pos:sku_pos + 300]
        color_label = re.search(r'[Cc]olou?rs?\s*[:=]\s*(.+?)(?:\n|$)', window)
        if color_label:
            raw = color_label.group(1)
            for part in re.split(r'[,/|]+', raw):
                matched = _fuzzy_match_color(part.strip())
                if matched:
                    colors.append(matched)

    # Check for "Del Dia" in product name
    if 'del dia' in name.lower() and not any('del dia' in c.lower() for c in colors):
        colors.append("Del Dia")

    return _dedupe_colors(colors)


def _parse_color_line_fuzzy(line: str) -> list[str]:
    """
    Parse a line for color tokens using fuzzy matching.
    Handles OCR-garbled lines like:
      "TERRA noo FAnGUE CD CoToPAKIBLAck C/O"
    Should extract: [Terra, Fatigue, Cotopaxi Black]
    """
    colors = []

    # Clean noise
    line = re.sub(r'[®©™@(){}]', '', line)
    line = re.sub(r'\s+', ' ', line).strip()
    if not line:
        return []

    # Remove C/O markers and split on them (they delimit colors)
    # "FATIGUE C/O COTOPAXI BLACK C/O" → ["FATIGUE", "COTOPAXI BLACK"]
    # Also handle garbled C/O: "CD", "CiO", "CYO", "c/o", "cro", "cio"
    co_pattern = r'\s*(?:C/?O|c/?o|C/0|c/0|CD|CiO|CYO|cro|cio|YO)\b'

    # Split line into candidate tokens using C/O as delimiters
    # First mark each C/O position, then split
    marked = re.sub(co_pattern, ' |SPLIT| ', line)

    # Also split on 2+ spaces
    marked = re.sub(r'\s{2,}', ' |SPLIT| ', marked)

    tokens = [t.strip() for t in marked.split('|SPLIT|') if t.strip()]

    if not tokens:
        tokens = [line]

    for token in tokens:
        token = token.strip()
        if not token or len(token) < 3:
            continue

        # Skip quantity-like tokens
        if re.match(r'^\d+[Kk]?\s*$', token):
            continue

        # Try fuzzy matching the token directly
        matched = _fuzzy_match_color(token)
        if matched:
            colors.append(matched)
            continue

        # If token is multi-word, try splitting further and matching each word
        # This handles "TERRA noo" → "Terra" (noo is garbage)
        words = token.split()
        if len(words) >= 2:
            # Try pairs first (for "Cotopaxi Black", "Deep Sea", "Log Cabin")
            i = 0
            while i < len(words):
                if i + 1 < len(words):
                    pair = f"{words[i]} {words[i+1]}"
                    pair_match = _fuzzy_match_color(pair)
                    if pair_match:
                        colors.append(pair_match)
                        i += 2
                        continue

                # Try single word
                single_match = _fuzzy_match_color(words[i])
                if single_match:
                    colors.append(single_match)
                i += 1

    return colors


def _correct_ocr_color(text: str) -> str:
    """Apply OCR corrections to a color name."""
    lower = text.strip().lower()

    # Direct lookup
    if lower in OCR_CORRECTIONS:
        return OCR_CORRECTIONS[lower]

    # No-space lookup
    no_space = lower.replace(' ', '')
    for key, val in OCR_CORRECTIONS.items():
        if no_space == key.replace(' ', ''):
            return val

    # Pattern matching for Cotopaxi Black variants
    if 'cotop' in lower and ('black' in lower or 'blk' in lower or 'blac' in lower):
        return "Cotopaxi Black"
    if 'cotoran' in lower or 'cotopan' in lower or 'cotopam' in lower:
        if 'black' in lower or 'blk' in lower or 'bak' in lower or 'mak' in lower:
            return "Cotopaxi Black"
        return "Cotopaxi Black"  # Most garbled cotopaxi refs are Cotopaxi Black

    return text


def _extract_color_from_name(name: str) -> str:
    """Extract a color from a product name (e.g., 'Product - Del Dia' → 'Del Dia')."""
    # "Product Name - Color"
    dash_match = re.search(r'\s[-–]\s+(.+)$', name)
    if dash_match:
        candidate = dash_match.group(1).strip()
        if 'del dia' in candidate.lower():
            return "Del Dia"
        matched = _fuzzy_match_color(candidate)
        if matched:
            return matched

    # "(Color)" in name
    paren_match = re.search(r'\(([^)]+)\)', name)
    if paren_match:
        matched = _fuzzy_match_color(paren_match.group(1).strip())
        if matched:
            return matched

    # "Del Dia" anywhere in name
    if 'del dia' in name.lower():
        return "Del Dia"

    return ""


def _dedupe_colors(colors: list[str]) -> list[str]:
    """Remove duplicate colors while preserving order."""
    seen = set()
    result = []
    for c in colors:
        key = c.strip().lower()
        if key and key not in seen:
            seen.add(key)
            result.append(c)
    return result


# ─────────────────────────────────────────────────────────
# OCR text cleanup
# ─────────────────────────────────────────────────────────

def _clean_ocr_text(text: str) -> str:
    """Clean OCR text to improve color extraction."""
    # Remove common OCR noise characters
    text = re.sub(r'[®©™]', '', text)
    # Normalize spaces
    text = re.sub(r'[ \t]+', ' ', text)
    # Remove isolated single characters that are OCR noise
    text = re.sub(r'(?<=\s)[^\w\s$](?=\s)', '', text)
    return text


# ─────────────────────────────────────────────────────────
# Composite image detection and cropping
# ─────────────────────────────────────────────────────────

def _is_composite_image(img_path: str) -> bool:
    """Detect if an image is a multi-product panel."""
    try:
        img = Image.open(img_path)
        w, h = img.size

        # Very wide images are panels (3:1+ aspect ratio)
        if w > h * 2.5 and w > 400:
            return True

        # Large images that may be grids
        if w > 600 and h > 400:
            gaps = _detect_vertical_gaps(img)
            if len(gaps) >= 1:
                return True

        return False
    except Exception:
        return False


def _detect_vertical_gaps(img: Image.Image, min_gap_width: int = 5) -> list[int]:
    """Detect vertical whitespace gaps between products."""
    try:
        gray = img.convert('L')
        w, h = gray.size

        y_start = h // 4
        y_end = 3 * h // 4

        gap_columns = []
        for x in range(w):
            total = 0
            samples = 0
            for y in range(y_start, y_end, max(1, (y_end - y_start) // 20)):
                total += gray.getpixel((x, y))
                samples += 1
            avg = total / max(samples, 1)
            if avg > 230:
                gap_columns.append(x)

        gaps = []
        if gap_columns:
            start = gap_columns[0]
            prev = gap_columns[0]
            for x in gap_columns[1:]:
                if x - prev > 3:
                    gap_w = prev - start
                    if gap_w >= min_gap_width:
                        center = (start + prev) // 2
                        if center > w * 0.1 and center < w * 0.9:
                            gaps.append(center)
                    start = x
                prev = x
            gap_w = prev - start
            if gap_w >= min_gap_width:
                center = (start + prev) // 2
                if center > w * 0.1 and center < w * 0.9:
                    gaps.append(center)

        return gaps
    except Exception:
        return []


def _detect_horizontal_gaps(img: Image.Image, min_gap_height: int = 5) -> list[int]:
    """Detect horizontal whitespace gaps."""
    try:
        gray = img.convert('L')
        w, h = gray.size

        x_start = w // 4
        x_end = 3 * w // 4

        gap_rows = []
        for y in range(h):
            total = 0
            samples = 0
            for x in range(x_start, x_end, max(1, (x_end - x_start) // 20)):
                total += gray.getpixel((x, y))
                samples += 1
            avg = total / max(samples, 1)
            if avg > 230:
                gap_rows.append(y)

        gaps = []
        if gap_rows:
            start = gap_rows[0]
            prev = gap_rows[0]
            for y in gap_rows[1:]:
                if y - prev > 3:
                    gap_h = prev - start
                    if gap_h >= min_gap_height:
                        center = (start + prev) // 2
                        if center > h * 0.1 and center < h * 0.9:
                            gaps.append(center)
                    start = y
                prev = y
            gap_h = prev - start
            if gap_h >= min_gap_height:
                center = (start + prev) // 2
                if center > h * 0.1 and center < h * 0.9:
                    gaps.append(center)

        return gaps
    except Exception:
        return []


def _estimate_items_in_composite(img_path: str) -> int:
    """Estimate how many items are in a composite image."""
    try:
        img = Image.open(img_path)
        w, h = img.size

        if w > h * 2.5:
            # Assume roughly square items
            estimated = round(w / max(h, 1))
            return max(2, min(estimated, 12))

        gaps = _detect_vertical_gaps(img)
        if gaps:
            return len(gaps) + 1

        if w > 600 and h > 400:
            h_gaps = _detect_horizontal_gaps(img)
            v_gaps = _detect_vertical_gaps(img)
            rows = len(h_gaps) + 1
            cols = len(v_gaps) + 1
            total = rows * cols
            if total > 1:
                return total

        return 1
    except Exception:
        return 1


def _crop_composite_image(img_path: str, n_items: int, page_num: int, sku: str) -> list[str]:
    """Crop a composite image into n individual product images."""
    try:
        img = Image.open(img_path)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        w, h = img.size

        crop_regions = []

        # Try gap-based cropping
        v_gaps = _detect_vertical_gaps(img)
        h_gaps = _detect_horizontal_gaps(img)

        if v_gaps and not h_gaps:
            boundaries = [0] + v_gaps + [w]
            for i in range(len(boundaries) - 1):
                x1, x2 = boundaries[i], boundaries[i + 1]
                if x2 - x1 > 30:
                    crop_regions.append((x1, 0, x2, h))
        elif h_gaps and not v_gaps:
            boundaries = [0] + h_gaps + [h]
            for i in range(len(boundaries) - 1):
                y1, y2 = boundaries[i], boundaries[i + 1]
                if y2 - y1 > 30:
                    crop_regions.append((0, y1, w, y2))
        elif v_gaps and h_gaps:
            x_bounds = [0] + v_gaps + [w]
            y_bounds = [0] + h_gaps + [h]
            for yi in range(len(y_bounds) - 1):
                for xi in range(len(x_bounds) - 1):
                    x1, x2 = x_bounds[xi], x_bounds[xi + 1]
                    y1, y2 = y_bounds[yi], y_bounds[yi + 1]
                    if x2 - x1 > 30 and y2 - y1 > 30:
                        crop_regions.append((x1, y1, x2, y2))

        # Fall back to equal division
        if len(crop_regions) < n_items:
            crop_regions = []
            if w > h * 1.5:
                strip_w = w // n_items
                for i in range(n_items):
                    x1 = i * strip_w
                    x2 = (i + 1) * strip_w if i < n_items - 1 else w
                    crop_regions.append((x1, 0, x2, h))
            elif h > w * 1.5:
                strip_h = h // n_items
                for i in range(n_items):
                    y1 = i * strip_h
                    y2 = (i + 1) * strip_h if i < n_items - 1 else h
                    crop_regions.append((0, y1, w, y2))
            else:
                cols = round(n_items ** 0.5)
                rows = (n_items + cols - 1) // cols
                cell_w = w // cols
                cell_h = h // rows
                for r in range(rows):
                    for c in range(cols):
                        if len(crop_regions) >= n_items:
                            break
                        x1 = c * cell_w
                        y1 = r * cell_h
                        x2 = (c + 1) * cell_w if c < cols - 1 else w
                        y2 = (r + 1) * cell_h if r < rows - 1 else h
                        crop_regions.append((x1, y1, x2, y2))

        # Save crops — add padding and ensure minimum quality
        cropped_paths = []
        safe_sku = re.sub(r'[^a-zA-Z0-9]', '', sku)
        for i, (x1, y1, x2, y2) in enumerate(crop_regions[:n_items]):
            crop = img.crop((x1, y1, x2, y2))
            if _is_blank_crop(crop):
                continue
            # Ensure crop is large enough to be a useful product image
            if crop.width < 40 or crop.height < 40:
                continue
            crop_path = CACHE_DIR / f"crop_p{page_num}_{safe_sku}_v{i}.png"
            crop.save(str(crop_path), "PNG")
            cropped_paths.append(str(crop_path))

        return cropped_paths if cropped_paths else [img_path]

    except Exception:
        return [img_path]


def _is_blank_crop(img: Image.Image, threshold: float = 0.95) -> bool:
    """Check if a crop is mostly blank or too small to be useful."""
    try:
        # Reject crops that are too small to be a product image
        if img.width < 60 or img.height < 60:
            return True
        gray = img.convert('L')
        pixels = list(gray.getdata())
        white = sum(1 for p in pixels if p > 240)
        return white / max(len(pixels), 1) > threshold
    except Exception:
        return False


def _split_images_for_variants(
    source_images: list[str],
    n_variants: int,
    page_num: int,
    sku: str,
) -> list[str]:
    """
    Produce one image per variant from source images.

    KEY RULE: When we have a composite/panel image (multiple products in a row),
    ALWAYS prefer cropping it into individual variant images rather than
    reusing a hero image for every variant.
    """
    if not source_images:
        return [""] * n_variants

    # Classify images into hero (big single product), composites (panels),
    # and small individuals
    heroes = []
    composites = []
    small_individuals = []

    for img_path in source_images:
        try:
            img = Image.open(img_path)
            w, h = img.size
        except Exception:
            continue

        if _is_composite_image(img_path):
            composites.append((img_path, w, h))
        elif w >= 300 and h >= 300 and w / max(h, 1) < 1.5:
            heroes.append((img_path, w, h))
        elif w >= 60 and h >= 60:
            small_individuals.append((img_path, w, h))

    # BEST CASE: We have a composite panel — crop it to get one image per variant
    # This is the primary path for wholesale catalogs
    if composites:
        # Pick the widest composite (most likely the color panel)
        best_composite = max(composites, key=lambda x: x[1])
        crops = _crop_composite_image(best_composite[0], n_variants, page_num, sku)
        # Filter out any failed crops (blank/tiny)
        good_crops = [c for c in crops if c]
        if len(good_crops) >= n_variants:
            return good_crops[:n_variants]
        # If some crops failed, pad with hero image
        result = good_crops
        hero = heroes[0][0] if heroes else ""
        while len(result) < n_variants:
            result.append(hero)
        return result[:n_variants]

    # If we have enough small individual images, use them 1:1
    if len(small_individuals) >= n_variants:
        return [s[0] for s in small_individuals[:n_variants]]

    # Fall back: reuse hero for all variants (not ideal but better than nothing)
    hero = heroes[0][0] if heroes else (small_individuals[0][0] if small_individuals else "")
    return [hero] * n_variants


def _get_indexed_hero_image(source_images: list[str], product_idx: int, total_products: int, page_num: int, sku: str) -> str:
    """
    For multi-product pages, pick the correct hero image by index.
    If there's a composite panel, crop the i-th item from it instead.
    """
    if not source_images:
        return ""

    # Separate heroes and composites
    heroes = []
    composites = []
    for img_path in source_images:
        try:
            img = Image.open(img_path)
            w, h = img.size
        except Exception:
            continue
        if _is_composite_image(img_path):
            composites.append((img_path, w, h))
        elif w >= 200 and h >= 200 and w / max(h, 1) < 1.5:
            heroes.append((img_path, w, h))

    # If we have a composite, crop the i-th item
    if composites:
        best = max(composites, key=lambda x: x[1])
        crops = _crop_composite_image(best[0], total_products, page_num, sku + f"_multi")
        if product_idx < len(crops) and crops[product_idx]:
            return crops[product_idx]

    # Pick the i-th hero by index
    if product_idx < len(heroes):
        return heroes[product_idx][0]

    # Fall back to first hero
    if heroes:
        return heroes[0][0]

    return source_images[0] if source_images else ""


def _get_single_product_image(source_images: list[str], page_num: int, sku: str) -> str:
    """
    Get a single product image. Prefers hero/individual images over composites.
    If only a composite is available, crops the first item from it.
    """
    if not source_images:
        return ""

    # Prefer non-composite images (hero shots)
    for img_path in source_images:
        if not _is_composite_image(img_path):
            try:
                img = Image.open(img_path)
                if img.width >= 80 and img.height >= 80:
                    return img_path
            except Exception:
                continue

    # Only composites available — crop first item
    for img_path in source_images:
        if _is_composite_image(img_path):
            crops = _crop_composite_image(img_path, 1, page_num, sku)
            if crops:
                return crops[0]

    return source_images[0]


# ─────────────────────────────────────────────────────────
# Image extraction helpers
# ─────────────────────────────────────────────────────────

def _is_product_image(img_path: str) -> bool:
    """Check if an image is likely a product photo."""
    try:
        img = Image.open(img_path)
        return img.width >= 150 and img.height >= 150
    except Exception:
        return False


def _extract_page_images(page, page_num: int) -> list[str]:
    """Extract images from a PDF page."""
    images = []

    try:
        image_list = page.get_images(full=True)
    except Exception:
        return images

    for img_idx, img_info in enumerate(image_list):
        try:
            xref = img_info[0]
            base_image = page.parent.extract_image(xref)
            if not base_image:
                continue

            image_bytes = base_image["image"]
            image_ext = base_image.get("ext", "png")

            pil_img = Image.open(io.BytesIO(image_bytes))
            if pil_img.width < 50 or pil_img.height < 50:
                continue

            img_path = CACHE_DIR / f"pdf_p{page_num}_i{img_idx}.{image_ext}"
            img_path.write_bytes(image_bytes)
            images.append(str(img_path))

        except Exception:
            continue

    return images


def _get_product_images(images: list[str], product_idx: int, total_products: int) -> list[str]:
    """
    Get all relevant images for a product from the page.
    ALWAYS returns ALL substantial images so the variant splitter can
    find composites to crop. Individual image selection happens downstream.
    """
    if not images:
        return []

    product_images = []
    for img_path in images:
        try:
            img = Image.open(img_path)
            if img.width >= 80 and img.height >= 80:
                product_images.append(img_path)
        except Exception:
            continue

    if not product_images:
        return images[:1] if images else []

    # Always return ALL images — let _split_images_for_variants and
    # _get_single_product_image handle selection/cropping
    return product_images


def _pick_image_for_product(images: list[str], product_idx: int, total_products: int) -> str:
    """Legacy wrapper — pick single best image."""
    result = _get_product_images(images, product_idx, total_products)
    return result[0] if result else ""


# ─────────────────────────────────────────────────────────
# OCR
# ─────────────────────────────────────────────────────────

def _ocr_page(page) -> str:
    """OCR a PDF page."""
    if not HAS_TESSERACT:
        return ""
    try:
        mat = fitz.Matrix(250 / 72, 250 / 72)
        pix = page.get_pixmap(matrix=mat)
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        text = pytesseract.image_to_string(img)
        return text
    except Exception:
        return ""


def _has_product_indicators(text: str) -> bool:
    """Check if text contains product-like content."""
    lower = text.lower()
    if re.search(r'\$\d+', text):
        return True
    if re.search(r'[A-Z]\d{4,}', text):
        return True
    if 'msrp' in lower:
        return True
    if re.search(r'\b\d+\.?\d*\s*[Kk]\b', text):
        return True
    if re.search(r'\b\d+\s*ea\b', text, re.IGNORECASE):
        return True
    return False


# ─────────────────────────────────────────────────────────
# Quantity detection
# ─────────────────────────────────────────────────────────

_QTY_PATTERNS = [
    re.compile(r'\b(\d+(?:\.\d+)?)\s*[Kk]\b'),
    re.compile(r'\b(\d[\d,]*)\s*(?:ea|pcs?|units?|each|pieces?)\b', re.IGNORECASE),
    re.compile(r'\b(?:ATS|QTY|MOQ|AVAIL)[:\s]+(\d[\d,]*(?:\.\d+)?(?:\s*[Kk])?)\b', re.IGNORECASE),
]


def _extract_qty_from_text(text: str) -> str:
    if not text:
        return ""
    for pattern in _QTY_PATTERNS:
        match = pattern.search(text)
        if match:
            return _normalize_qty_value(match.group(0).strip())
    return ""


def _normalize_qty_value(raw: str) -> str:
    raw = raw.strip()
    raw = re.sub(r'^(?:ATS|QTY|MOQ|AVAIL)[:\s]+', '', raw, flags=re.IGNORECASE).strip()
    k_match = re.match(r'^(\d+(?:\.\d+)?)\s*[Kk]$', raw)
    if k_match:
        num = float(k_match.group(1)) * 1000
        return f"{int(num):,}"
    num_match = re.match(r'^(\d[\d,]*)', raw)
    if num_match:
        num_str = num_match.group(1).replace(',', '')
        try:
            return f"{int(num_str):,}"
        except ValueError:
            return num_str
    return raw


def _extract_all_qtys_from_page(text: str) -> list[str]:
    qtys = []
    for match in re.finditer(r'\b(\d+(?:\.\d+)?)\s*[Kk]\b|\b(\d[\d,]*)\s*ea\b', text, re.IGNORECASE):
        raw = match.group(0).strip()
        qtys.append(_normalize_qty_value(raw))
    return qtys


# ─────────────────────────────────────────────────────────
# Strategy 3 & 4: Table and block parsing
# ─────────────────────────────────────────────────────────

def _try_parse_table(lines: list[str], page_num: int, filename: str, images: list[str]) -> list[Product]:
    """Parse text as a table."""
    products = []
    header_idx = -1
    header_fields = {}

    for i, line in enumerate(lines[:10]):
        lower = line.lower()
        score = 0
        if any(w in lower for w in ["sku", "style", "item", "upc"]):
            score += 1
        if any(w in lower for w in ["name", "product", "description", "title"]):
            score += 1
        if any(w in lower for w in ["price", "msrp", "retail", "wholesale", "cost"]):
            score += 1
        if any(w in lower for w in ["color", "colour"]):
            score += 1
        if any(w in lower for w in ["qty", "quantity", "ats", "units", "stock"]):
            score += 1

        if score >= 2:
            header_idx = i
            parts = re.split(r'\s{2,}|\t', line)
            for j, part in enumerate(parts):
                pl = part.strip().lower()
                if any(w in pl for w in ["sku", "style", "item", "upc"]):
                    header_fields[j] = "sku_upc"
                elif any(w in pl for w in ["name", "product", "description", "title"]):
                    header_fields[j] = "product_name"
                elif any(w in pl for w in ["wholesale", "cost", "your cost"]):
                    header_fields[j] = "your_cost"
                elif any(w in pl for w in ["price", "msrp", "retail"]):
                    header_fields[j] = "msrp"
                elif any(w in pl for w in ["color", "colour"]):
                    header_fields[j] = "color"
                elif any(w in pl for w in ["qty", "quantity", "ats", "units", "stock", "moq"]):
                    header_fields[j] = "qty"
                elif any(w in pl for w in ["note"]):
                    header_fields[j] = "notes"
            break

    if header_idx < 0 or len(header_fields) < 2:
        return []

    for i, line in enumerate(lines[header_idx + 1:]):
        parts = re.split(r'\s{2,}|\t', line)
        if len(parts) < 2:
            continue
        row = {}
        for j, part in enumerate(parts):
            if j in header_fields:
                field_name = header_fields[j]
                val = part.strip()
                if field_name == "qty":
                    normalized = _normalize_qty_value(val)
                    row[field_name] = normalized if normalized else val
                else:
                    row[field_name] = val
        if row.get("product_name") or row.get("sku_upc"):
            img = _pick_image_for_product(images, len(products), 0)
            products.append(Product(
                product_image=img,
                sku_upc=row.get("sku_upc", ""),
                product_name=row.get("product_name", ""),
                color=row.get("color", ""),
                msrp=row.get("msrp", ""),
                your_cost=row.get("your_cost", ""),
                qty=row.get("qty", ""),
                notes=row.get("notes", ""),
                _source="pdf",
                _source_detail=f"{filename}, page {page_num + 1}",
            ))

    return products


def _try_parse_blocks(text: str, page_num: int, filename: str, images: list[str]) -> list[Product]:
    """Parse text as blocks."""
    blocks = re.split(r'\n\s*\n|\n-{3,}\n|\n={3,}\n', text)

    if len(blocks) < 2:
        return []

    products = []
    for block in blocks:
        block = block.strip()
        if not block or len(block) < 10:
            continue

        data = {}
        lines = block.split("\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            kv_match = re.match(r'^([\w\s/]+?):\s*(.+)$', line)
            if kv_match:
                key = kv_match.group(1).strip().lower()
                val = kv_match.group(2).strip()
                if any(w in key for w in ["sku", "style", "item", "upc", "code"]):
                    data["sku_upc"] = val
                elif any(w in key for w in ["name", "product", "title"]):
                    data["product_name"] = val
                elif "color" in key or "colour" in key:
                    data["color"] = val
                elif any(w in key for w in ["wholesale", "cost", "your cost"]):
                    data["your_cost"] = val
                elif any(w in key for w in ["price", "msrp", "retail"]):
                    data["msrp"] = val
                elif any(w in key for w in ["qty", "quantity", "ats", "units", "moq"]):
                    data["qty"] = _normalize_qty_value(val)
                continue

            qty_val = _extract_qty_from_text(line)
            if qty_val and "qty" not in data:
                data["qty"] = qty_val

            prices = re.findall(r'\$\s*(\d+(?:\.\d{2})?)', line)
            if prices and "msrp" not in data:
                data["msrp"] = f"${prices[0]}"
                if len(prices) > 1 and "your_cost" not in data:
                    data["your_cost"] = f"${prices[1]}"

            if "product_name" not in data and len(line) > 5 and not line.startswith("$"):
                data["product_name"] = line

        if data.get("product_name") or data.get("sku_upc"):
            img = _pick_image_for_product(images, len(products), 0)
            products.append(Product(
                product_image=img,
                sku_upc=data.get("sku_upc", ""),
                product_name=data.get("product_name", ""),
                color=data.get("color", ""),
                msrp=data.get("msrp", ""),
                your_cost=data.get("your_cost", ""),
                qty=data.get("qty", ""),
                notes=data.get("notes", ""),
                _source="pdf",
                _source_detail=f"{filename}, page {page_num + 1}",
            ))

    return products
