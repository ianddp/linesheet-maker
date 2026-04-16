"""
parsers/scraper.py — Intelligence layer for product extraction.

Provides confidence scoring, title quality detection, page classification,
and DOM card extraction to separate real products from marketing noise.

Public API (imported by website.py):
    classify_page_type(url, soup)            → str
    score_product(p, page_url, page_type)    → (float, str)  score + bucket
    extract_product_cards(soup, base_url)    → list[Product]
    filter_products(products, ...)           → list[Product]

Buckets:
    confirmed  ≥ 0.65   always exported
    likely     ≥ 0.40   exported by default
    uncertain  ≥ 0.15   review bucket (optional)
    suppressed < 0.15   excluded
"""
from __future__ import annotations

import json
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from utils.schema import Product

import logging
logger = logging.getLogger(__name__)

# ─────────────────────────── Thresholds ───────────────────────────

CONFIRMED_THRESHOLD  = 0.65
LIKELY_THRESHOLD     = 0.40
UNCERTAIN_THRESHOLD  = 0.15

# ─────────────────────────── URL Patterns ───────────────────────────

_PDP_URL_RE = re.compile(
    r"(/products?/|/p/|/item/|/dp/|/sku/|/detail[s]?/"
    r"|\-[A-Z0-9]{5,15}\.html$|[_\-][0-9]{4,}(?:\.html)?$"
    r"|/[A-Z0-9]{6,20}$)",
    re.IGNORECASE,
)

_NON_PRODUCT_URL_RE = re.compile(
    r"(/category/|/collection[s]?/|/catalog/|/blog/|/article/"
    r"|/editorial/|/guide/|/inspiration/|/about/|/contact/"
    r"|/search\?|/tag/|#|/page/\d+|/content/)",
    re.IGNORECASE,
)

# ─────────────────────────── Title word-lists ───────────────────────────

# Broad merchandising / grouping phrases — strong negative signal
_MERCH_PHRASES = {
    "shop", "explore", "discover", "browse", "view all", "see all",
    "collection", "collections", "essentials", "tools", "accessories",
    "cookware", "bakeware", "cutlery", "gadgets", "everyday", "everyday cooking",
    "performance", "durability", "timeless", "precision",
    "elevate", "style meets", "natural beauty", "smooth cooking",
    "flexible performance", "make every meal", "dependable tools",
    "bake with", "prep like", "meal prep", "meal count",
    "grill game", "outdoor tools", "food preparation",
    "nonstick tools", "silicone tools", "stainless steel tools",
    "wood tools", "textiles", "linens",
}

# Verbs / adjectives that open a marketing sentence, not a product name
_SENTENCE_OPENERS = {
    "smooth", "flexible", "timeless", "natural", "style", "make", "prep",
    "elevate", "bake", "dependable", "create", "discover", "unlock",
    "experience", "transform", "achieve", "master", "enjoy", "get",
    "find", "upgrade", "shop", "introducing",
}

# Connective phrases inside titles that signal marketing copy
_MARKETING_CONNECTOR_RE = re.compile(
    r"\b(meets|using|for everyday|like a pro|game with|"
    r"count with|precision using|beauty meets|function with"
    r"|made for|built for|designed for)\b",
    re.IGNORECASE,
)

# Real item nouns — presence boosts confidence
_PRODUCT_NOUNS = {
    "pan", "skillet", "saucepan", "sauté pan", "saute pan", "fry pan",
    "stockpot", "stock pot", "dutch oven", "wok", "pot", "lid",
    "colander", "strainer", "grater", "peeler", "spatula", "tong", "tongs",
    "ladle", "spoon", "whisk", "bowl", "rack", "roaster", "griddle",
    "sheet pan", "baking sheet", "mold", "steamer",
    "baking pan", "loaf pan", "cake pan", "roasting pan", "broiler pan",
    "knife", "knives", "shears", "scissors", "cutting board",
    "casserole", "braiser", "sauteuse", "insert", "basket", "trivet",
    "kettle", "teakettle", "brush", "mitt", "apron",
    "jacket", "shirt", "pants", "shoes", "sneakers", "boot", "boots",
    "bag", "backpack", "wallet", "belt", "hat", "cap",
    "bottle", "tumbler", "mug", "cup", "glass",
    "speaker", "headphones", "earbuds", "charger", "cable",
    "lamp", "light", "bulb", "set", "piece set",
    "cooker", "pressure cooker", "slow cooker",
}

# ─────────────────────────── DOM Selectors ───────────────────────────

_CARD_SELECTORS = [
    "[data-product-id]", "[data-sku]", "[data-item-id]",
    "[data-product]", "[data-product-handle]",
    ".product-card", ".product-tile", ".product-item",
    ".product-grid-item", ".product-list-item",
    ".ProductCard", ".ProductItem", ".ProductTile",
    "[class*='product-card']", "[class*='product-tile']",
    "[class*='ProductCard']", "[class*='ProductTile']",
    ".search-result-item", ".plp-item",
    "[class*='plp-']",
    ".grid__item .grid-product",
    "li.product",
    ".product-item-info",
    "[data-product-listing]",
]

_PRICE_SELECTORS = [
    "[class*='price']", "[data-price]", "[itemprop='price']",
    "[class*='Price']", ".Price", ".money",
    "[data-regular-price]", "[data-sale-price]",
    "span.price", "p.price", "div.price",
]

_SKU_SELECTORS = [
    "[itemprop='sku']", "[data-sku]", "[data-product-id]",
    "[data-item-id]", "[class*='sku']", "[class*='model']",
    "[data-model]", "[data-upc]",
]

_JSONLD_PRODUCT_TYPES = {"product", "productgroup"}


# ═══════════════════════════════════════════════════════════════════
#  Stage 1 — Page Classifier
# ═══════════════════════════════════════════════════════════════════

def classify_page_type(url: str, soup: BeautifulSoup) -> str:
    """
    Classify the page as: pdp / category / search / editorial / homepage / unknown.
    Called before extraction so card vs. PDP strategies can be chosen correctly.
    """
    url_lower = url.lower()

    has_product_schema = _has_product_schema(soup)
    has_product_grid   = _count_card_matches(soup) >= 4
    has_single_price   = _has_single_price_block(soup)
    has_atc            = _has_add_to_cart(soup)

    if re.search(r"(/search\?|/search/|\?q=|&q=|/find\?)", url_lower):
        return "search"
    if re.search(r"/(blog|article|editorial|guide|inspiration|about|contact|faq|press)/", url_lower):
        return "editorial"
    if re.match(r"^https?://[^/]+/?$", url_lower):
        return "homepage"

    if has_product_schema and has_atc:
        return "pdp"
    if has_atc and has_single_price and not has_product_grid:
        return "pdp"
    if _PDP_URL_RE.search(url) and has_single_price:
        return "pdp"

    if has_product_grid:
        return "category"
    if re.search(r"(/category/|/collection[s]?/|/catalog/|/c/|/shop/)", url_lower):
        return "category"

    return "unknown"


def _has_product_schema(soup: BeautifulSoup) -> bool:
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if str(item.get("@type", "")).lower() in _JSONLD_PRODUCT_TYPES:
                    return True
                for g in item.get("@graph", []):
                    if str(g.get("@type", "")).lower() in _JSONLD_PRODUCT_TYPES:
                        return True
        except Exception:
            pass
    return bool(soup.find(attrs={"itemtype": re.compile(r"schema.org/product", re.I)}))


def _count_card_matches(soup: BeautifulSoup) -> int:
    best = 0
    for sel in _CARD_SELECTORS:
        try:
            best = max(best, len(soup.select(sel)))
        except Exception:
            pass
    return best


def _has_single_price_block(soup: BeautifulSoup) -> bool:
    for sel in _PRICE_SELECTORS[:4]:
        try:
            if len(soup.select(sel)) == 1:
                return True
        except Exception:
            pass
    return False


def _has_add_to_cart(soup: BeautifulSoup) -> bool:
    atc_re = re.compile(
        r"(add.to.cart|add.to.bag|buy.now|quick.view|quick.add|addtocart)", re.I
    )
    return bool(
        soup.find("button", string=atc_re)
        or soup.find(attrs={"class": lambda c: c and atc_re.search(" ".join(c))})
        or soup.find(attrs={"id": atc_re})
    )


# ═══════════════════════════════════════════════════════════════════
#  Stage 2 — DOM Card Extractor
# ═══════════════════════════════════════════════════════════════════

def extract_product_cards(soup: BeautifulSoup, base_url: str) -> list[Product]:
    """
    Find repeating product-card containers in the DOM and extract one
    Product per card.  Does NOT scrape free-floating headlines, hero
    sections, or editorial modules.
    """
    cards = _find_best_card_set(soup)
    if not cards:
        return []

    results = []
    for card in cards:
        p = _card_to_product(card, base_url)
        if p:
            results.append(p)
    return results


def _find_best_card_set(soup: BeautifulSoup) -> list[Tag]:
    """Return the largest coherent set of product-card elements."""
    best_sel, best_count = None, 0
    for sel in _CARD_SELECTORS:
        try:
            found = soup.select(sel)
            if len(found) >= 3 and len(found) > best_count:
                best_count = len(found)
                best_sel = sel
        except Exception:
            pass

    if best_sel:
        return soup.select(best_sel)

    # Auto-detect repeating sibling grid
    return _detect_repeating_grid(soup)


def _detect_repeating_grid(soup: BeautifulSoup) -> list[Tag]:
    """
    Find a parent whose children form a consistent repeating grid:
    same tag, each child has an image + text, ideally a price too.
    """
    candidates = []
    for parent in soup.find_all(["ul", "div", "section", "ol"], recursive=True):
        children = [c for c in parent.children if isinstance(c, Tag)]
        if len(children) < 4:
            continue
        tags = [c.name for c in children]
        top_tag = max(set(tags), key=tags.count)
        if tags.count(top_tag) / len(tags) < 0.75:
            continue
        valid = [c for c in children if c.find("img") and c.get_text(strip=True)]
        if len(valid) < 4:
            continue
        price_hits = sum(1 for c in valid if re.search(r'\$[\d,]+', c.get_text()))
        candidates.append((len(valid) + price_hits * 2, valid))

    if candidates:
        candidates.sort(key=lambda x: -x[0])
        return candidates[0][1]
    return []


def _card_to_product(card: Tag, base_url: str) -> Optional[Product]:
    """Extract a Product from a single card element."""
    # Image
    image = ""
    img = card.find("img")
    if img:
        src = img.get("data-src") or img.get("data-lazy-src") or img.get("src") or ""
        if src and not src.startswith("data:"):
            image = urljoin(base_url, src)
        if not image:
            srcset = img.get("srcset", "")
            if srcset:
                last = srcset.split(",")[-1].strip().split()[0]
                if last:
                    image = urljoin(base_url, last)

    # Product URL
    product_url = ""
    link = card.find("a", href=True)
    if link:
        product_url = urljoin(base_url, link["href"])

    # SKU from data attributes
    sku = ""
    for attr in ("data-product-id", "data-sku", "data-item-id", "data-pid", "data-id"):
        val = card.get(attr, "")
        if val:
            sku = validate_sku(str(val).strip())
            break
    if not sku:
        for sel in _SKU_SELECTORS:
            try:
                el = card.select_one(sel)
                if el:
                    raw_sku = (el.get("content") or el.get_text(strip=True)).strip()
                    sku = validate_sku(raw_sku)
                    if sku:
                        break
            except Exception:
                pass

    # Price — use normalize_price to handle $1,579.99 correctly
    msrp = ""
    for sel in _PRICE_SELECTORS:
        try:
            el = card.select_one(sel)
            if el:
                # Try data attributes first (cleanest source)
                data_price = el.get("data-price") or el.get("content") or ""
                if data_price:
                    msrp = normalize_price(data_price)
                if not msrp:
                    msrp = extract_best_price(el.get_text(strip=True))
                if msrp:
                    break
        except Exception:
            pass

    # Name — try explicit name selectors first, then headings, then fallback
    name = ""
    for name_sel in [
        "[class*='product-name']", "[class*='product-title']",
        "[class*='ProductName']", "[class*='ProductTitle']",
        "[class*='item-title']", "[class*='item-name']",
        "[itemprop='name']", "h2", "h3", "h4",
    ]:
        try:
            el = card.select_one(name_sel)
            if el:
                text = el.get_text(strip=True)
                if 3 < len(text) < 150:
                    name = text
                    break
        except Exception:
            pass
    if not name:
        for t in card.stripped_strings:
            t = t.strip()
            if len(t) > 4 and not re.match(r'^[\$\d\.\,\s]+$', t) and len(t) < 150:
                name = t
                break

    # Color
    color = ""
    color_el = card.select_one("[class*='color'], [data-color]")
    if color_el:
        color = color_el.get_text(strip=True)[:50]

    if not (name or sku or image):
        return None

    return Product(
        product_image=image,
        sku_upc=sku,
        product_name=name,
        color=color,
        msrp=msrp,
        _source="website_card",
        _source_detail=product_url or base_url,
    )


# ═══════════════════════════════════════════════════════════════════
#  Stage 3 — Confidence Scorer
# ═══════════════════════════════════════════════════════════════════

def score_product(
    p: Product,
    page_url: str = "",
    page_type: str = "unknown",
) -> tuple[float, str]:
    """
    Score a Product on 0.0–1.0 and return (score, bucket).
    bucket is one of: confirmed / likely / uncertain / suppressed
    """
    score = 0.50  # neutral start

    # ── Positive signals ──────────────────────────────────────────
    if p.sku_upc and len(p.sku_upc.strip()) >= 3:
        score += 0.20

    msrp_val = _parse_price(p.msrp)
    if msrp_val and msrp_val > 0:
        score += 0.15

    if p.product_image and p.product_image.startswith("http"):
        score += 0.05

    if p.color:
        score += 0.05

    if page_type == "pdp":
        score += 0.10

    # Product URL signals
    if page_url:
        if _PDP_URL_RE.search(page_url):
            score += 0.10
        elif not _NON_PRODUCT_URL_RE.search(page_url):
            score += 0.03

    source_url = p._source_detail or page_url
    if source_url and _PDP_URL_RE.search(source_url):
        score += 0.08

    # ── Title scoring ─────────────────────────────────────────────
    title_delta = _score_title(p.product_name)
    score += title_delta

    # ── Negative signals ──────────────────────────────────────────
    if not p.sku_upc:
        score -= 0.08

    if (not msrp_val or msrp_val == 0) and not p.sku_upc:
        score -= 0.08

    if not p.product_image:
        score -= 0.05

    # Clamp and bucket
    score = max(0.0, min(1.0, score))
    bucket = _bucket(score)
    return score, bucket


def _parse_price(msrp: str) -> float:
    if not msrp:
        return 0.0
    try:
        return float(re.sub(r'[^\d.]', '', msrp))
    except (ValueError, TypeError):
        return 0.0


def _score_title(title: str) -> float:
    """Return a delta (positive or negative) based on title quality."""
    if not title or not title.strip():
        return -0.25

    title_lower = title.lower().strip()
    delta = 0.0
    words = title_lower.split()

    # Too short (single word like "Cookware", "Tools")
    if len(words) < 2:
        delta -= 0.20

    # Too long / sentence-like
    elif len(words) > 12:
        delta -= 0.18

    # Sentence-ending punctuation
    if re.search(r'[.!?]$', title.strip()):
        delta -= 0.10

    # Marketing connector phrases ("meets", "using", "like a pro")
    if _MARKETING_CONNECTOR_RE.search(title):
        delta -= 0.22

    # Sentence opener (first word is an action verb or generic adjective)
    if words and words[0] in _SENTENCE_OPENERS:
        delta -= 0.18

    # Merchandising phrase match — penalise once per match, up to 3
    merch_hits = sum(1 for phrase in _MERCH_PHRASES if phrase in title_lower)
    delta -= 0.10 * min(merch_hits, 3)

    # "with" used in a connecting/merchandising way
    if re.search(r'\bwith\b.{5,}', title_lower) and len(words) > 5:
        delta -= 0.10

    # Real product noun present — positive signal
    if any(noun in title_lower for noun in _PRODUCT_NOUNS):
        delta += 0.10

    # Proper Title Case and reasonable length (2–8 words)
    if re.match(r'^([A-Z][a-z]*[\s\-]?)+$', title.strip()) and 2 <= len(words) <= 8:
        delta += 0.05

    # Contains a model / variant code like "D5", "HA1", "E785S264"
    if re.search(r'\b[A-Z]{1,4}\d{2,}|\b\d{4,}\b', title):
        delta += 0.10

    return delta


def _bucket(score: float) -> str:
    if score >= CONFIRMED_THRESHOLD:
        return "confirmed"
    if score >= LIKELY_THRESHOLD:
        return "likely"
    if score >= UNCERTAIN_THRESHOLD:
        return "uncertain"
    return "suppressed"


# ═══════════════════════════════════════════════════════════════════
#  Stage 4 — Filter / bucket
# ═══════════════════════════════════════════════════════════════════

def filter_products(
    products: list[Product],
    page_url: str = "",
    page_type: str = "unknown",
    include_uncertain: bool = False,
) -> list[Product]:
    """
    Score every product and return only exportable ones.
    Sets p._confidence on each product as a side effect.

    Args:
        products:         raw list from any extractor
        page_url:         the URL that was scraped
        page_type:        result of classify_page_type()
        include_uncertain: if True, also return uncertain-bucket products
    """
    exportable = []
    for p in products:
        score, bucket = score_product(p, page_url, page_type)
        p._confidence = score

        if bucket == "confirmed":
            exportable.append(p)
        elif bucket == "likely":
            exportable.append(p)
        elif bucket == "uncertain" and include_uncertain:
            exportable.append(p)
        # suppressed → dropped silently

    return exportable


# ═══════════════════════════════════════════════════════════════════
#  Price normalization & SKU validation  (used by scraper + website)
# ═══════════════════════════════════════════════════════════════════

# Robust regex:  $1,579.99  |  $899.99  |  1579.99  |  $0.00
# Handles thousands separators properly — never confuses comma with decimal.
_PRICE_RE = re.compile(
    r'[\$£€]\s?'                          # currency symbol
    r'(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?'  # e.g. 1,579.99  or  899.99
    r'|\d+(?:\.\d{1,2})?)',                # e.g. 1579.99  or  0.00
)

# Fallback: any number with optional decimal (no currency symbol required)
_PRICE_RE_BARE = re.compile(
    r'(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?'  # 1,579.99
    r'|\d{4,}(?:\.\d{1,2})?)',             # 1579.99  (must be ≥4 digits if no comma)
)


def normalize_price(raw: str) -> str:
    """
    Parse a price string and return a clean '$X,XXX.XX' value.

    Handles:
      '$1,579.99'  → '$1,579.99'
      '1579.99'    → '$1,579.99'
      '$2,899.99'  → '$2,899.99'
      'Regular Price$1,579.99Items onall-clad.com...'  → '$1,579.99'

    Never truncates — if source has $1,579.99 the output will never be $1.57.
    Returns '' if no valid price found.
    """
    if not raw or not raw.strip():
        return ""

    raw = str(raw).strip()

    # If it's already a clean numeric value (e.g. from JSON-LD: "1579.99")
    try:
        val = float(raw.replace(",", ""))
        if val >= 0:
            return _format_price(val)
    except (ValueError, TypeError):
        pass

    # Try with currency symbol first (most reliable)
    m = _PRICE_RE.search(raw)
    if m:
        num_str = m.group(1) if m.group(1) else m.group(0).lstrip("$£€ ")
        return _safe_parse_price(num_str, raw)

    # Fallback: bare number (≥4 digits or has comma-thousands)
    m = _PRICE_RE_BARE.search(raw)
    if m:
        return _safe_parse_price(m.group(1), raw)

    return ""


def _safe_parse_price(num_str: str, original_raw: str = "") -> str:
    """
    Convert a numeric string to a formatted price, with magnitude sanity checks.
    If the parsed value is suspiciously smaller than what appears in the raw text,
    log a warning and prefer the larger value.
    """
    try:
        val = float(num_str.replace(",", ""))
    except (ValueError, TypeError):
        return ""

    if val < 0:
        return ""

    # Magnitude sanity check: if raw text contains a 4+ digit number,
    # the parsed result should not be < $10
    if original_raw and val < 10:
        big_numbers = re.findall(r'\d{4,}', original_raw.replace(",", ""))
        for bn in big_numbers:
            try:
                big_val = float(bn) if "." not in bn else float(bn)
                if big_val > 100:
                    # The raw text had a big number but we parsed a tiny one — suspicious
                    logger.warning(
                        f"Price magnitude mismatch: parsed ${val:.2f} but raw text "
                        f"contains {bn}. Using larger value."
                    )
                    val = big_val
                    break
            except ValueError:
                pass

    return _format_price(val)


def _format_price(val: float) -> str:
    """Format a float as $X,XXX.XX"""
    if val == 0:
        return "$0.00"
    if val >= 1000:
        whole = int(val)
        cents = round((val - whole) * 100)
        return f"${whole:,}.{cents:02d}"
    return f"${val:,.2f}"


def validate_sku(raw: str) -> str:
    """
    Validate and clean a SKU/UPC/ASIN/GTIN value.

    Only returns the value if it matches known identifier patterns.
    Rejects long text blocks, pricing text, and merchandising copy.
    Returns '' if the value is not a valid product identifier.
    """
    if not raw or not raw.strip():
        return ""

    raw = str(raw).strip()

    # Hard reject: too long (no real identifier is > 50 chars)
    if len(raw) > 50:
        return ""

    # Hard reject: contains pricing text or marketing copy
    reject_patterns = re.compile(
        r'(regular\s*price|sale\s*price|strikethrough|display|search results'
        r'|product display|items on|may display|price\$|\.com)',
        re.IGNORECASE,
    )
    if reject_patterns.search(raw):
        return ""

    # Hard reject: contains more than 2 spaces (sentence-like)
    if raw.count(" ") > 3:
        return ""

    # Hard reject: contains dollar sign (it's a price, not a SKU)
    if "$" in raw or "€" in raw or "£" in raw:
        return ""

    # Valid patterns:
    #  UPC/EAN/GTIN: 8-14 digits
    #  ASIN: B0 + 8 alphanumeric
    #  SKU: alphanumeric with optional dashes/dots, 3-30 chars
    if re.match(r'^\d{8,14}$', raw):
        return raw  # UPC / EAN / GTIN
    if re.match(r'^B0[A-Z0-9]{8}$', raw):
        return raw  # ASIN
    if re.match(r'^[A-Z0-9][\w\-\.]{2,29}$', raw, re.IGNORECASE):
        return raw  # general SKU
    if re.match(r'^\d{4,7}$', raw):
        return raw  # short numeric ID

    return ""


def extract_best_price(text: str) -> str:
    """
    Extract the most likely MSRP from a block of text that may contain
    multiple prices, marketing copy, and other noise.

    Prefers prices labeled 'Regular Price', 'Price', 'MSRP'.
    If multiple prices found, returns the one most clearly labeled.
    """
    if not text:
        return ""

    # Look for labeled prices first
    labeled_patterns = [
        re.compile(r'(?:Regular\s*Price|MSRP|Price)\s*:?\s*\$?\s?(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?)', re.I),
        re.compile(r'(?:Regular\s*Price|MSRP|Price)\s*:?\s*\$?\s?(\d+(?:\.\d{1,2})?)', re.I),
    ]
    for pat in labeled_patterns:
        m = pat.search(text)
        if m:
            return _safe_parse_price(m.group(1), text)

    # Fall back to first currency-prefixed price
    m = _PRICE_RE.search(text)
    if m:
        num_str = m.group(1) if m.group(1) else m.group(0).lstrip("$£€ ")
        return _safe_parse_price(num_str, text)

    return ""
