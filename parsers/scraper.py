"""
scraper.py — Intelligent ecommerce product scraper with confidence scoring.

Staged pipeline:
  Stage 1: Classify page type (homepage / category / collection / search / PDP / editorial)
  Stage 2: Find product containers or structured product data
  Stage 3: Extract candidate rows
  Stage 4: Confidence scoring and non-product filtering
  Stage 5: Normalize and bucket results

Output buckets:
  confirmed  — score >= 0.65  → always exported
  likely     — score >= 0.40  → exported by default
  uncertain  — score >= 0.15  → review bucket (optional export)
  suppressed — score <  0.15  → excluded from export

Supports:
  • requests + BeautifulSoup  (fast path for static / server-rendered HTML)
  • Playwright sync API        (fallback for JS-heavy / SPA sites)
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants & word-lists
# ---------------------------------------------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 20
MAX_JS_WAIT_MS = 8000          # ms to wait for JS to stabilise
MAX_RETRIES = 2

# ---------- URL patterns that suggest a PDP ----------
PDP_URL_PATTERNS = re.compile(
    r"(/product[s]?/|/p/|/item/|/dp/|/sku/|/detail[s]?/|"
    r"\-[A-Z0-9]{5,15}\.html$|[_\-][0-9]{4,}(?:\.html)?$|"
    r"/[A-Z0-9]{6,20}$)",
    re.IGNORECASE,
)

# ---------- URL patterns that suggest category/editorial ----------
NON_PRODUCT_URL_PATTERNS = re.compile(
    r"(/category/|/collection[s]?/|/catalog/|/blog/|/article/|"
    r"/editorial/|/guide/|/inspiration/|/about/|/contact/|"
    r"/search\?|/tag/|#|/page/\d+|/content/)",
    re.IGNORECASE,
)

# ---------- Title keyword groups ----------
# Broad grouping / merchandising words — strong negative signal
MERCHANDISING_PHRASES = {
    "shop", "explore", "discover", "browse", "view all", "see all",
    "collection", "collections", "essentials", "tools", "accessories",
    "cookware", "bakeware", "cutlery", "gadgets", "everyday", "everyday cooking",
    "performance", "durability", "timeless", "precision",
    "elevate", "style meets", "natural beauty", "smooth cooking",
    "flexible performance", "make every meal", "dependable tools",
    "bake with", "prep like", "meal prep", "meal count",
    "grill game", "outdoor tools", "food preparation",
}

# Verbs / opener words that suggest a marketing sentence, not a product name
SENTENCE_OPENERS = {
    "smooth", "flexible", "timeless", "natural", "style", "make", "prep",
    "elevate", "bake", "dependable", "create", "discover", "elevate",
    "unlock", "experience", "transform", "achieve", "master", "enjoy",
    "get", "find", "upgrade", "shop",
}

# Connective marketing patterns inside titles
MARKETING_CONNECTORS = re.compile(
    r"\b(meets|with|using|for everyday|like a pro|game with|"
    r"count with|precision using|beauty meets|function with)\b",
    re.IGNORECASE,
)

# Actual item-level nouns that indicate a real product title
PRODUCT_NOUNS = {
    "pan", "skillet", "saucepan", "sauté pan", "saute pan", "fry pan",
    "stockpot", "stock pot", "dutch oven", "wok", "pot", "lid",
    "colander", "strainer", "grater", "peeler", "spatula", "tong", "tongs",
    "ladle", "spoon", "whisk", "bowl", "rack", "roaster", "griddle",
    "sheet pan", "baking sheet", "mold", "mould", "steamer", "steamer insert",
    "baking pan", "loaf pan", "cake pan", "roasting pan", "broiler pan",
    "knife", "knives", "shears", "scissors", "cutting board",
    "casserole", "braiser", "rondeau", "sauteuse",
    "set", "piece set",  # e.g. "10-Piece Set" — borderline but specific
    "insert", "basket", "trivet", "rack",
    "cooker", "pressure cooker", "slow cooker",
    "kettle", "teakettle",
    "brush", "scrubber", "mitt", "pad",
    "apron", "towel", "oven mitt",
    # Apparel/general
    "jacket", "shirt", "pants", "shoes", "sneakers", "boot",
    "bag", "backpack", "wallet", "belt", "hat", "cap",
    "bottle", "tumbler", "mug", "cup", "glass",
    # Electronics
    "speaker", "headphone", "earbuds", "charger", "cable",
    "lamp", "light", "bulb",
}

# ---------- Schema.org / structured data ----------
JSONLD_PRODUCT_TYPES = {
    "product", "productgroup",
}

# ---------- DOM selectors that strongly suggest a product card ----------
PRODUCT_CARD_SELECTORS = [
    "[data-product-id]", "[data-sku]", "[data-item-id]",
    "[data-product]", "[data-product-handle]",
    ".product-card", ".product-tile", ".product-item",
    ".product-grid-item", ".product-list-item",
    ".ProductCard", ".ProductItem", ".ProductTile",
    "[class*='product-card']", "[class*='product-tile']",
    "[class*='ProductCard']", "[class*='ProductTile']",
    ".search-result-item", ".plp-item",
    "[class*='plp-']", "[class*='grid-item']",
    # Shopify
    ".grid__item .grid-product", "[data-product-id]",
    # WooCommerce
    ".woocommerce-LoopProduct-link", "li.product",
    # Magento
    ".product-item-info", ".product.details.product-item-details",
    # BigCommerce
    "[data-product-listing]",
]

# ---------- Selectors for price elements ----------
PRICE_SELECTORS = [
    "[class*='price']", "[data-price]", "[itemprop='price']",
    "[class*='Price']", ".Price", ".money",
    "[data-regular-price]", "[data-sale-price]",
    "span.price", "p.price", "div.price",
]

# ---------- Selectors for SKU / item ID ----------
SKU_SELECTORS = [
    "[itemprop='sku']", "[data-sku]", "[data-product-id]",
    "[data-item-id]", "[class*='sku']", "[class*='model']",
    "[data-model]", "[data-upc]",
]

# ---------- Add-to-cart indicators ----------
ATC_PATTERNS = re.compile(
    r"(add.to.cart|add.to.bag|buy.now|quick.view|quick.add|"
    r"addtocart|add_to_cart|atc|quickview|product-json)",
    re.IGNORECASE,
)

# ---------- Classify page type ----------
PAGE_TYPES = {
    "pdp": "Product Detail Page",
    "category": "Category / Collection Listing",
    "search": "Search Results",
    "editorial": "Editorial / Non-Product Page",
    "homepage": "Homepage",
    "unknown": "Unknown",
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ScrapedProduct:
    """A single extracted product candidate with confidence metadata."""
    name: str = ""
    sku: str = ""
    upc: str = ""
    mpn: str = ""
    msrp: str = ""
    color: str = ""
    image_url: str = ""
    product_url: str = ""
    source_url: str = ""
    brand: str = ""
    description: str = ""
    variants: list[dict] = field(default_factory=list)
    quantity: str = ""

    # Confidence scoring
    confidence_score: float = 0.0
    confidence_signals: list[str] = field(default_factory=list)
    confidence_penalties: list[str] = field(default_factory=list)
    bucket: str = "uncertain"          # confirmed / likely / uncertain / suppressed

    # Source metadata
    extraction_method: str = ""        # jsonld / microdata / card / fallback
    page_type: str = "unknown"
    from_structured_data: bool = False

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "sku": self.sku or self.upc or self.mpn,
            "msrp": self.msrp,
            "color": self.color,
            "image": self.image_url,
            "product_url": self.product_url,
            "brand": self.brand,
            "quantity": self.quantity,
            "confidence_score": round(self.confidence_score, 2),
            "bucket": self.bucket,
            "extraction_method": self.extraction_method,
        }


@dataclass
class ScrapeResult:
    """Full result from scraping a URL."""
    url: str
    page_type: str = "unknown"
    confirmed: list[ScrapedProduct] = field(default_factory=list)
    likely: list[ScrapedProduct] = field(default_factory=list)
    uncertain: list[ScrapedProduct] = field(default_factory=list)
    suppressed: list[ScrapedProduct] = field(default_factory=list)
    error: str = ""
    extraction_method: str = ""
    raw_candidate_count: int = 0

    @property
    def exportable(self) -> list[ScrapedProduct]:
        """Products for the main export (confirmed + likely)."""
        return self.confirmed + self.likely

    @property
    def all_buckets(self) -> dict:
        return {
            "confirmed": [p.to_dict() for p in self.confirmed],
            "likely": [p.to_dict() for p in self.likely],
            "uncertain": [p.to_dict() for p in self.uncertain],
            "suppressed": [p.to_dict() for p in self.suppressed],
        }


# ---------------------------------------------------------------------------
# Stage 1: Page classifier
# ---------------------------------------------------------------------------

class PageClassifier:
    """Determines the type of page being scraped."""

    def classify(self, url: str, soup: BeautifulSoup) -> str:
        url_lower = url.lower()

        # Structural HTML signals
        has_atc = bool(soup.find(
            attrs={"class": lambda c: c and any(
                x in " ".join(c).lower()
                for x in ("add-to-cart", "addtocart", "add_to_cart", "buy-now", "buynow")
            )}
        ) or soup.find("button", string=ATC_PATTERNS))

        has_product_schema = self._has_product_schema(soup)
        has_breadcrumb = bool(soup.find(attrs={"class": lambda c: c and "breadcrumb" in " ".join(c).lower()}))
        has_product_grid = self._count_product_cards(soup) >= 4
        has_single_price_block = self._has_pdp_price_block(soup)

        # URL-based signals
        if re.search(r"(/search\?|/search/|\?q=|&q=|/find\?)", url_lower):
            return "search"
        if re.search(r"(/(blog|article|editorial|guide|inspiration|about|contact|faq|press)/)", url_lower):
            return "editorial"
        if url_lower in ("https://", "http://") or re.search(r"^https?://[^/]+/?$", url_lower):
            return "homepage"

        # PDP signals
        if has_product_schema and has_atc:
            return "pdp"
        if has_atc and has_single_price_block and not has_product_grid:
            return "pdp"
        if PDP_URL_PATTERNS.search(url) and has_single_price_block:
            return "pdp"

        # Category / listing
        if has_product_grid:
            return "category"
        if re.search(r"(/category/|/collection[s]?/|/catalog/|/c/|/shop/)", url_lower):
            return "category"

        # Editorial fallback
        if not has_breadcrumb and not has_product_schema and not has_product_grid:
            return "editorial"

        return "unknown"

    def _has_product_schema(self, soup: BeautifulSoup) -> bool:
        for tag in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                data = json.loads(tag.string or "")
                if isinstance(data, list):
                    data = data[0] if data else {}
                t = str(data.get("@type", "")).lower()
                if t in JSONLD_PRODUCT_TYPES:
                    return True
                # Check @graph
                for item in data.get("@graph", []):
                    if str(item.get("@type", "")).lower() in JSONLD_PRODUCT_TYPES:
                        return True
            except Exception:
                pass
        return bool(soup.find(attrs={"itemtype": re.compile(r"schema.org/product", re.I)}))

    def _count_product_cards(self, soup: BeautifulSoup) -> int:
        count = 0
        for sel in PRODUCT_CARD_SELECTORS:
            try:
                cards = soup.select(sel)
                count = max(count, len(cards))
            except Exception:
                pass
        return count

    def _has_pdp_price_block(self, soup: BeautifulSoup) -> bool:
        for sel in PRICE_SELECTORS[:4]:
            try:
                elements = soup.select(sel)
                if len(elements) == 1:
                    return True
            except Exception:
                pass
        return False


# ---------------------------------------------------------------------------
# Stage 2a: Structured data extractor (JSON-LD, microdata, OG)
# ---------------------------------------------------------------------------

class StructuredDataExtractor:
    """
    Extracts product data from JSON-LD, microdata, and OpenGraph tags.
    This is the highest-confidence source and is always tried first.
    """

    def extract(self, soup: BeautifulSoup, base_url: str) -> list[ScrapedProduct]:
        products: list[ScrapedProduct] = []
        products.extend(self._extract_jsonld(soup, base_url))
        if not products:
            products.extend(self._extract_microdata(soup, base_url))
        if not products:
            og = self._extract_opengraph(soup, base_url)
            if og:
                products.append(og)
        return products

    # ── JSON-LD ──────────────────────────────────────────────────────────
    def _extract_jsonld(self, soup: BeautifulSoup, base_url: str) -> list[ScrapedProduct]:
        results = []
        for tag in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                raw = json.loads(tag.string or "")
            except Exception:
                continue

            # Normalise to list
            items = raw if isinstance(raw, list) else [raw]

            for item in items:
                # Unwrap @graph
                if "@graph" in item:
                    items.extend(item["@graph"])
                    continue

                t = str(item.get("@type", "")).lower()
                if t not in JSONLD_PRODUCT_TYPES:
                    continue

                p = self._parse_jsonld_product(item, base_url)
                if p:
                    p.extraction_method = "jsonld"
                    p.from_structured_data = True
                    results.append(p)

        return results

    def _parse_jsonld_product(self, data: dict, base_url: str) -> Optional[ScrapedProduct]:
        p = ScrapedProduct()
        p.name = data.get("name", "").strip()
        p.description = data.get("description", "")
        p.brand = (data.get("brand") or {}).get("name", "") if isinstance(data.get("brand"), dict) else str(data.get("brand", ""))
        p.sku = data.get("sku", "").strip()
        p.mpn = data.get("mpn", "").strip()
        p.upc = (data.get("gtin13") or data.get("gtin12") or data.get("gtin") or "").strip()

        # URL
        url = data.get("url", "")
        p.product_url = urljoin(base_url, url) if url else ""

        # Image
        img = data.get("image")
        if isinstance(img, list):
            img = img[0] if img else ""
        if isinstance(img, dict):
            img = img.get("url", "")
        if img:
            p.image_url = urljoin(base_url, str(img))

        # Price — from offers
        offers = data.get("offers", [])
        if isinstance(offers, dict):
            offers = [offers]
        for offer in offers:
            price = offer.get("price") or offer.get("lowPrice")
            if price:
                try:
                    p.msrp = f"${float(price):.2f}"
                except (ValueError, TypeError):
                    p.msrp = str(price)
                break
            else:
                p.msrp = ""

        # Color / variants — productionVariant or additionalProperty
        for prop in data.get("additionalProperty", []):
            name_prop = str(prop.get("name", "")).lower()
            if "color" in name_prop or "colour" in name_prop:
                p.color = str(prop.get("value", ""))

        if not p.name:
            return None
        return p

    # ── Microdata ─────────────────────────────────────────────────────────
    def _extract_microdata(self, soup: BeautifulSoup, base_url: str) -> list[ScrapedProduct]:
        results = []
        product_items = soup.find_all(
            attrs={"itemtype": re.compile(r"schema\.org/[Pp]roduct", re.I)}
        )
        for item in product_items:
            p = ScrapedProduct()
            p.extraction_method = "microdata"
            p.from_structured_data = True

            name_el = item.find(attrs={"itemprop": "name"})
            if name_el:
                p.name = name_el.get_text(strip=True)

            sku_el = item.find(attrs={"itemprop": "sku"})
            if sku_el:
                p.sku = sku_el.get_text(strip=True)

            price_el = item.find(attrs={"itemprop": "price"})
            if price_el:
                price_val = price_el.get("content") or price_el.get_text(strip=True)
                try:
                    p.msrp = f"${float(str(price_val).replace(',', '')):.2f}"
                except (ValueError, TypeError):
                    p.msrp = ""

            img_el = item.find(attrs={"itemprop": "image"})
            if img_el:
                src = img_el.get("src") or img_el.get("content") or ""
                p.image_url = urljoin(base_url, src)

            url_el = item.find(attrs={"itemprop": "url"})
            if url_el:
                href = url_el.get("href") or url_el.get("content") or ""
                p.product_url = urljoin(base_url, href)

            if p.name:
                results.append(p)
        return results

    # ── OpenGraph ─────────────────────────────────────────────────────────
    def _extract_opengraph(self, soup: BeautifulSoup, base_url: str) -> Optional[ScrapedProduct]:
        og = {}
        for tag in soup.find_all("meta", property=True):
            prop = tag.get("property", "")
            if prop.startswith("og:") or prop.startswith("product:"):
                og[prop] = tag.get("content", "")

        if og.get("og:type", "").lower() not in ("product", "og:product"):
            return None

        p = ScrapedProduct()
        p.extraction_method = "opengraph"
        p.from_structured_data = True
        p.name = og.get("og:title", "")
        p.description = og.get("og:description", "")
        p.image_url = og.get("og:image", "")
        p.product_url = og.get("og:url", base_url)

        price = og.get("product:price:amount") or og.get("og:price:amount")
        if price:
            try:
                p.msrp = f"${float(price):.2f}"
            except (ValueError, TypeError):
                p.msrp = ""

        return p if p.name else None


# ---------------------------------------------------------------------------
# Stage 2b: DOM product-card extractor
# ---------------------------------------------------------------------------

class ProductCardExtractor:
    """
    Finds repeated product-card containers in the DOM and extracts
    candidate products only from within those containers.
    Does NOT scrape free-floating headlines, hero sections, or editorial modules.
    """

    def extract(self, soup: BeautifulSoup, base_url: str, page_type: str) -> list[ScrapedProduct]:
        # Find the best repeating card container set
        cards = self._find_product_cards(soup)
        if not cards:
            # Fall back to a looser extraction only for PDP pages
            if page_type == "pdp":
                return self._extract_pdp_fallback(soup, base_url)
            return []

        results = []
        for card in cards:
            p = self._extract_from_card(card, base_url)
            if p:
                results.append(p)
        return results

    def _find_product_cards(self, soup: BeautifulSoup) -> list[Tag]:
        """Return the best list of product-card elements found."""
        best_selector = None
        best_count = 0

        for sel in PRODUCT_CARD_SELECTORS:
            try:
                found = soup.select(sel)
                # Only consider selectors that match at least 3 items (repeating grid)
                if len(found) >= 3 and len(found) > best_count:
                    best_count = len(found)
                    best_selector = sel
            except Exception:
                pass

        if best_selector:
            return soup.select(best_selector)

        # Fallback: find repeating sibling structures automatically
        return self._detect_repeating_siblings(soup)

    def _detect_repeating_siblings(self, soup: BeautifulSoup) -> list[Tag]:
        """
        Heuristic: find a parent element that has >= 4 direct children
        with nearly identical structure (same tag + same class pattern).
        """
        candidates = []
        for parent in soup.find_all(["ul", "div", "section", "ol"], recursive=True):
            children = [c for c in parent.children if isinstance(c, Tag)]
            if len(children) < 4:
                continue

            # Check that majority of children share the same tag
            tags = [c.name for c in children]
            most_common_tag = max(set(tags), key=tags.count)
            if tags.count(most_common_tag) / len(tags) < 0.75:
                continue

            # Check that children have at least one image + text each
            valid_children = [
                c for c in children
                if c.find("img") and c.get_text(strip=True)
            ]
            if len(valid_children) >= 4:
                # Score: larger grids that contain price text rank higher
                price_hit = sum(
                    1 for c in valid_children
                    if re.search(r'\$[\d,]+', c.get_text())
                )
                score = len(valid_children) + price_hit * 2
                candidates.append((score, valid_children))

        if candidates:
            candidates.sort(key=lambda x: -x[0])
            return candidates[0][1]
        return []

    def _extract_from_card(self, card: Tag, base_url: str) -> Optional[ScrapedProduct]:
        p = ScrapedProduct()
        p.extraction_method = "card"

        # ── Image ──────────────────────────────────────────────────
        img = card.find("img")
        if img:
            src = (
                img.get("data-src") or
                img.get("data-lazy-src") or
                img.get("src") or ""
            )
            if src and not src.startswith("data:"):
                p.image_url = urljoin(base_url, src)
            # Also try srcset (first URL in srcset = smallest; last = largest)
            srcset = img.get("srcset", "")
            if srcset and not p.image_url:
                first_src = srcset.split(",")[-1].strip().split(" ")[0]
                if first_src:
                    p.image_url = urljoin(base_url, first_src)

        # ── Product URL ─────────────────────────────────────────────
        link = card.find("a", href=True)
        if link:
            href = link["href"]
            p.product_url = urljoin(base_url, href)

        # ── SKU / item ID (from data attributes) ─────────────────────
        for attr in ("data-product-id", "data-sku", "data-item-id",
                     "data-pid", "data-product", "data-id"):
            val = card.get(attr) or ""
            if val:
                p.sku = str(val).strip()
                break
        if not p.sku:
            for sel in SKU_SELECTORS:
                try:
                    el = card.select_one(sel)
                    if el:
                        p.sku = (el.get("content") or el.get_text(strip=True)).strip()
                        break
                except Exception:
                    pass

        # ── Price ──────────────────────────────────────────────────
        for sel in PRICE_SELECTORS:
            try:
                el = card.select_one(sel)
                if el:
                    raw = el.get_text(strip=True)
                    m = re.search(r'\$[\d,]+\.?\d{0,2}', raw)
                    if m:
                        p.msrp = m.group(0)
                        break
            except Exception:
                pass

        # ── Product name ────────────────────────────────────────────
        # Try explicit product-name elements first
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
                        p.name = text
                        break
            except Exception:
                pass

        # Fallback: largest text block inside the card
        if not p.name:
            texts = [t.strip() for t in card.stripped_strings if len(t.strip()) > 4]
            for t in texts:
                if not re.match(r'^[\$\d\.\,\s]+$', t) and len(t) < 150:
                    p.name = t
                    break

        # ── Color ──────────────────────────────────────────────────
        color_el = card.select_one("[class*='color'], [data-color]")
        if color_el:
            p.color = color_el.get_text(strip=True)[:50]

        return p if (p.name or p.sku or p.image_url) else None

    def _extract_pdp_fallback(self, soup: BeautifulSoup, base_url: str) -> list[ScrapedProduct]:
        """Minimal extraction for a single product detail page."""
        p = ScrapedProduct()
        p.extraction_method = "pdp_fallback"

        # Name
        for sel in ["h1", "[class*='product-title']", "[itemprop='name']"]:
            el = soup.select_one(sel)
            if el:
                p.name = el.get_text(strip=True)
                break

        # Price
        for sel in PRICE_SELECTORS:
            el = soup.select_one(sel)
            if el:
                raw = el.get_text(strip=True)
                m = re.search(r'\$[\d,]+\.?\d{0,2}', raw)
                if m:
                    p.msrp = m.group(0)
                    break

        # Image
        og_img = soup.find("meta", property="og:image")
        if og_img:
            p.image_url = og_img.get("content", "")

        p.product_url = base_url
        return [p] if p.name else []


# ---------------------------------------------------------------------------
# Stage 4: Confidence scorer
# ---------------------------------------------------------------------------

class ConfidenceScorer:
    """
    Assigns a 0.0–1.0 confidence score to each product candidate.
    Higher = more likely to be a real purchasable product.
    """

    def score(self, p: ScrapedProduct, page_type: str = "unknown") -> ScrapedProduct:
        score = 0.5  # neutral start
        signals = []
        penalties = []

        # ── Positive signals ──────────────────────────────────────────
        if p.from_structured_data:
            score += 0.25
            signals.append("structured_data")

        if p.sku or p.upc or p.mpn:
            score += 0.20
            signals.append("has_sku_upc_mpn")

        if p.msrp and p.msrp not in ("$0.00", "$0", ""):
            try:
                price_val = float(re.sub(r'[^\d.]', '', p.msrp))
                if price_val > 0:
                    score += 0.15
                    signals.append("has_real_price")
            except ValueError:
                pass

        if p.product_url:
            if PDP_URL_PATTERNS.search(p.product_url):
                score += 0.15
                signals.append("pdp_url")
            elif not NON_PRODUCT_URL_PATTERNS.search(p.product_url):
                score += 0.05
                signals.append("plausible_url")

        if p.image_url:
            score += 0.05
            signals.append("has_image")

        if p.color:
            score += 0.05
            signals.append("has_color_variant")

        if page_type == "pdp":
            score += 0.10
            signals.append("on_pdp_page")

        # ── Title quality scoring ─────────────────────────────────────
        title_score, title_signals, title_penalties = self._score_title(p.name)
        score += title_score
        signals.extend(title_signals)
        penalties.extend(title_penalties)

        # ── Negative signals ──────────────────────────────────────────
        # All bad signals together: no sku, no real price, no pdp url
        if not p.sku and not p.upc and not p.mpn:
            score -= 0.10
            penalties.append("no_identifier")

        msrp_val = 0.0
        if p.msrp:
            try:
                msrp_val = float(re.sub(r'[^\d.]', '', p.msrp))
            except ValueError:
                pass

        if msrp_val == 0.0 and not (p.sku or p.upc or p.mpn):
            score -= 0.10
            penalties.append("zero_price_no_sku")

        if not p.image_url:
            score -= 0.05
            penalties.append("no_image")

        # Clamp
        score = max(0.0, min(1.0, score))
        p.confidence_score = score
        p.confidence_signals = signals
        p.confidence_penalties = penalties
        p.bucket = self._bucket(score)
        return p

    def _score_title(self, title: str) -> tuple[float, list[str], list[str]]:
        """Returns (delta_score, positive_signals, negative_penalties) for the title."""
        if not title:
            return -0.20, [], ["no_title"]

        title_lower = title.lower().strip()
        signals = []
        penalties = []
        delta = 0.0

        # 1. Too short or too long
        word_count = len(title.split())
        if word_count < 2:
            delta -= 0.10
            penalties.append("title_too_short")
        elif word_count > 12:
            delta -= 0.15
            penalties.append("title_too_long_sentence_like")

        # 2. Sentence-like: ends with punctuation, has verb structure
        if re.search(r'[.!?]$', title.strip()):
            delta -= 0.10
            penalties.append("title_ends_sentence_punct")

        # 3. Marketing connectors ("meets", "with", "using", "like a pro")
        if MARKETING_CONNECTORS.search(title):
            delta -= 0.20
            penalties.append("marketing_connector_phrase")

        # 4. Sentence opener (starts with an action or adjective verb)
        first_word = title_lower.split()[0] if title_lower.split() else ""
        if first_word in SENTENCE_OPENERS:
            delta -= 0.15
            penalties.append(f"sentence_opener:{first_word}")

        # 5. Merchandising phrases
        matched_merch = [p for p in MERCHANDISING_PHRASES if p in title_lower]
        if matched_merch:
            delta -= 0.10 * min(len(matched_merch), 3)
            penalties.append(f"merchandising_phrase:{matched_merch[0]}")

        # 6. Positive: contains a real product noun
        product_noun_found = any(noun in title_lower for noun in PRODUCT_NOUNS)
        if product_noun_found:
            delta += 0.10
            signals.append("has_product_noun")

        # 7. Positive: looks like a proper product name (Title Case or brand+model)
        is_title_case = re.match(r'^([A-Z][a-z]*[\s\-]?)+$', title.strip())
        if is_title_case and 2 <= word_count <= 8:
            delta += 0.05
            signals.append("title_case_product_name")

        # 8. Contains a model/variant code (e.g. "D5 5-ply", "E785S264")
        if re.search(r'\b[A-Z]{1,4}\d{2,}|\b\d{4,}\b', title):
            delta += 0.10
            signals.append("has_model_code")

        # 9. Heavily promotional language
        promo_words = {"premium", "exclusive", "limited", "special", "introducing"}
        if any(w in title_lower for w in promo_words):
            delta -= 0.05
            penalties.append("promotional_language")

        return delta, signals, penalties

    @staticmethod
    def _bucket(score: float) -> str:
        if score >= 0.65:
            return "confirmed"
        if score >= 0.40:
            return "likely"
        if score >= 0.15:
            return "uncertain"
        return "suppressed"


# ---------------------------------------------------------------------------
# Stage 5: Normaliser
# ---------------------------------------------------------------------------

class ProductNormaliser:
    """Cleans and standardises product fields before export."""

    def normalise(self, p: ScrapedProduct) -> ScrapedProduct:
        p.name = self._clean_text(p.name)
        p.sku = self._clean_text(p.sku)
        p.color = self._clean_text(p.color)
        p.brand = self._clean_text(p.brand)
        p.msrp = self._normalise_price(p.msrp)
        p.image_url = self._clean_url(p.image_url)
        p.product_url = self._clean_url(p.product_url)
        return p

    @staticmethod
    def _clean_text(text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:200]

    @staticmethod
    def _normalise_price(price: str) -> str:
        if not price:
            return ""
        price = price.strip()
        # Already formatted
        if re.match(r'^\$[\d,]+\.\d{2}$', price):
            return price
        # Extract numeric value
        m = re.search(r'[\d,]+\.?\d{0,2}', price.replace(',', ''))
        if m:
            try:
                val = float(m.group(0))
                if val > 0:
                    return f"${val:.2f}"
            except ValueError:
                pass
        return ""

    @staticmethod
    def _clean_url(url: str) -> str:
        if not url:
            return ""
        url = url.strip()
        if url.startswith("//"):
            url = "https:" + url
        return url


# ---------------------------------------------------------------------------
# Stage 3: Deduplication
# ---------------------------------------------------------------------------

def _deduplicate(products: list[ScrapedProduct]) -> list[ScrapedProduct]:
    """Remove near-duplicate rows by URL, SKU, or name."""
    seen_urls: set[str] = set()
    seen_skus: set[str] = set()
    seen_names: set[str] = set()
    unique = []
    for p in products:
        # Prefer structured-data products over DOM-scraped duplicates
        key_url = p.product_url.lower().rstrip("/") if p.product_url else ""
        key_sku = p.sku.strip().lower() if p.sku else ""
        key_name = re.sub(r'\s+', ' ', p.name.lower().strip()) if p.name else ""

        if key_url and key_url in seen_urls:
            continue
        if key_sku and key_sku in seen_skus:
            continue
        if key_name and key_name in seen_names:
            continue

        if key_url:
            seen_urls.add(key_url)
        if key_sku:
            seen_skus.add(key_sku)
        if key_name:
            seen_names.add(key_name)

        unique.append(p)
    return unique


# ---------------------------------------------------------------------------
# HTML fetcher (requests + optional Playwright fallback)
# ---------------------------------------------------------------------------

def _fetch_static(url: str, retries: int = MAX_RETRIES) -> Optional[str]:
    """Fetch page HTML via requests. Returns HTML string or None."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    }
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code in (403, 429) and attempt < retries - 1:
                time.sleep(2 ** attempt)
        except requests.RequestException as e:
            logger.warning(f"[fetch_static] attempt {attempt+1} failed: {e}")
            time.sleep(1)
    return None


def _fetch_rendered(url: str) -> Optional[str]:
    """
    Fetch page HTML using Playwright (handles JS-rendered sites).
    Returns HTML string or None if Playwright is unavailable.
    Waits for network idle + repeating product cards to appear.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.info("Playwright not installed; skipping JS render.")
        return None

    html = None
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
            ctx = browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1400, "height": 900},
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
            page = ctx.new_page()

            # Block images / fonts / ads to speed up rendering
            def block_unnecessary(route):
                if route.request.resource_type in ("image", "font", "media"):
                    route.abort()
                else:
                    route.continue_()
            page.route("**/*", block_unnecessary)

            page.goto(url, wait_until="domcontentloaded", timeout=MAX_JS_WAIT_MS)

            # Try to wait for a product card selector to appear
            for sel in [
                ".product-card", ".product-tile", ".product-item",
                "[data-product-id]", "[data-sku]", "li.product",
            ]:
                try:
                    page.wait_for_selector(sel, timeout=4000)
                    break
                except PWTimeout:
                    continue

            # Scroll to trigger lazy-loading
            page.evaluate("window.scrollTo(0, document.body.scrollHeight/2)")
            page.wait_for_timeout(1500)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1500)

            html = page.content()
            browser.close()
    except Exception as e:
        logger.warning(f"[fetch_rendered] Playwright error: {e}")

    return html


# ---------------------------------------------------------------------------
# Main orchestrator: ProductScraper
# ---------------------------------------------------------------------------

class ProductScraper:
    """
    Orchestrates the full 5-stage product extraction pipeline.

    Usage:
        scraper = ProductScraper()
        result = scraper.scrape("https://www.all-clad.com/cookware.html")
        for p in result.exportable:
            print(p.name, p.msrp, p.bucket)
    """

    def __init__(self, use_playwright: bool = True):
        self.use_playwright = use_playwright
        self.classifier = PageClassifier()
        self.structured_extractor = StructuredDataExtractor()
        self.card_extractor = ProductCardExtractor()
        self.scorer = ConfidenceScorer()
        self.normaliser = ProductNormaliser()

    def scrape(self, url: str) -> ScrapeResult:
        result = ScrapeResult(url=url)

        # ── Fetch HTML ────────────────────────────────────────────────
        html = _fetch_static(url)
        used_playwright = False

        if html:
            soup = BeautifulSoup(html, "html.parser")
            # Quick check: does the page have meaningful product cards?
            card_count = PageClassifier()._count_product_cards(soup)
            if card_count < 3 and self.use_playwright:
                logger.info(f"Sparse static HTML ({card_count} cards). Trying Playwright…")
                rendered_html = _fetch_rendered(url)
                if rendered_html:
                    soup = BeautifulSoup(rendered_html, "html.parser")
                    html = rendered_html
                    used_playwright = True
        elif self.use_playwright:
            logger.info("Static fetch failed. Trying Playwright…")
            rendered_html = _fetch_rendered(url)
            if rendered_html:
                soup = BeautifulSoup(rendered_html, "html.parser")
                html = rendered_html
                used_playwright = True
            else:
                result.error = "Could not fetch page (static + Playwright both failed)"
                return result
        else:
            result.error = "Could not fetch page HTML"
            return result

        result.extraction_method = "playwright" if used_playwright else "static"

        # ── Stage 1: Classify page ────────────────────────────────────
        page_type = self.classifier.classify(url, soup)
        result.page_type = page_type
        logger.info(f"Page classified as: {page_type}")

        # ── Stage 2: Extract candidates ───────────────────────────────
        candidates: list[ScrapedProduct] = []

        # 2a: Structured data (highest confidence)
        structured = self.structured_extractor.extract(soup, url)
        for p in structured:
            p.page_type = page_type
            p.source_url = url
        candidates.extend(structured)
        logger.info(f"Structured data: {len(structured)} products")

        # 2b: DOM card extraction (fills in what structured data misses)
        if not structured or page_type == "category":
            dom_products = self.card_extractor.extract(soup, url, page_type)
            for p in dom_products:
                p.page_type = page_type
                p.source_url = url
            candidates.extend(dom_products)
            logger.info(f"DOM cards: {len(dom_products)} candidates")

        result.raw_candidate_count = len(candidates)

        # ── Stage 3: Normalise ────────────────────────────────────────
        candidates = [self.normaliser.normalise(p) for p in candidates]

        # ── Stage 4: Deduplicate ──────────────────────────────────────
        candidates = _deduplicate(candidates)

        # ── Stage 5: Score + bucket ───────────────────────────────────
        for p in candidates:
            p = self.scorer.score(p, page_type)
            if p.bucket == "confirmed":
                result.confirmed.append(p)
            elif p.bucket == "likely":
                result.likely.append(p)
            elif p.bucket == "uncertain":
                result.uncertain.append(p)
            else:
                result.suppressed.append(p)

        logger.info(
            f"Buckets — confirmed:{len(result.confirmed)}, "
            f"likely:{len(result.likely)}, "
            f"uncertain:{len(result.uncertain)}, "
            f"suppressed:{len(result.suppressed)}"
        )
        return result


# ---------------------------------------------------------------------------
# Public helper: scrape_url()
# ---------------------------------------------------------------------------

def scrape_url(
    url: str,
    include_uncertain: bool = False,
    use_playwright: bool = True,
) -> dict:
    """
    Convenience wrapper.  Returns a dict with:
        products   — list of exportable product dicts (confirmed + likely, optionally uncertain)
        uncertain  — list of uncertain product dicts (for review UI)
        suppressed — count of suppressed rows
        page_type  — classified page type string
        raw_count  — number of raw candidates before filtering
        error      — error string if fetch failed
    """
    scraper = ProductScraper(use_playwright=use_playwright)
    result = scraper.scrape(url)

    export_products = result.exportable
    if include_uncertain:
        export_products = export_products + result.uncertain

    return {
        "products": [p.to_dict() for p in export_products],
        "uncertain": [p.to_dict() for p in result.uncertain],
        "suppressed_count": len(result.suppressed),
        "confirmed_count": len(result.confirmed),
        "likely_count": len(result.likely),
        "page_type": PAGE_TYPES.get(result.page_type, result.page_type),
        "raw_count": result.raw_candidate_count,
        "extraction_method": result.extraction_method,
        "error": result.error,
    }


# ---------------------------------------------------------------------------
# CLI test entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    test_url = sys.argv[1] if len(sys.argv) > 1 else "https://www.all-clad.com/cookware.html"
    print(f"\nScraping: {test_url}\n{'='*60}")

    out = scrape_url(test_url, include_uncertain=False)

    if out["error"]:
        print(f"ERROR: {out['error']}")
        sys.exit(1)

    print(f"Page type   : {out['page_type']}")
    print(f"Raw candidates  : {out['raw_count']}")
    print(f"Confirmed   : {out['confirmed_count']}")
    print(f"Likely      : {out['likely_count']}")
    print(f"Suppressed  : {out['suppressed_count']}")
    print(f"Uncertain   : {len(out['uncertain'])}")
    print(f"\nExported products ({len(out['products'])}):")
    print("-" * 60)
    for p in out["products"]:
        print(f"  [{p['bucket']:.9s}] {p['name'][:50]:<50}  MSRP:{p['msrp'] or '—':>8}  SKU:{p['sku'] or '—'}")

    if out["uncertain"]:
        print(f"\nUncertain (review bucket) — {len(out['uncertain'])}:")
        for p in out["uncertain"]:
            print(f"  [uncertain] {p['name'][:50]}")
