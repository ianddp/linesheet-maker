"""Website scraper for extracting product data from e-commerce sites."""
from __future__ import annotations

import json
import re
import time
from urllib.parse import urlparse, urljoin, urldefrag

import requests
from bs4 import BeautifulSoup

from utils.images import HEADERS, pick_best_image_url
from utils.schema import Product


def scrape_url(url: str, timeout: int = 30) -> dict:
    """Fetch a URL and return parsed soup + raw html."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        return {"soup": soup, "html": resp.text, "url": resp.url, "ok": True}
    except Exception as e:
        return {"soup": None, "html": "", "url": url, "ok": False, "error": str(e)}


def extract_jsonld_products(soup: BeautifulSoup, page_url: str) -> list[Product]:
    """Extract products from JSON-LD structured data (schema.org/Product)."""
    products = []
    scripts = soup.find_all("script", type="application/ld+json")

    for script in scripts:
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue

        # Handle single object or array
        items = data if isinstance(data, list) else [data]

        for item in items:
            # Handle @graph wrapper
            if item.get("@type") == "ItemList" and "itemListElement" in item:
                for elem in item["itemListElement"]:
                    p = elem.get("item", elem)
                    if p.get("@type") == "Product" or "name" in p:
                        products.append(_jsonld_to_product(p, page_url))
            elif "@graph" in item:
                for node in item["@graph"]:
                    if node.get("@type") == "Product":
                        products.append(_jsonld_to_product(node, page_url))
            elif item.get("@type") == "Product":
                products.append(_jsonld_to_product(item, page_url))

    return products


def _jsonld_to_product(data: dict, page_url: str) -> Product:
    """Convert a JSON-LD Product object to our Product dataclass."""
    name = data.get("name", "")
    sku = data.get("sku", data.get("productID", data.get("gtin13", "")))
    image = ""
    img_data = data.get("image", "")
    if isinstance(img_data, list):
        image = pick_best_image_url(img_data)
    elif isinstance(img_data, dict):
        image = img_data.get("url", img_data.get("contentUrl", ""))
    elif isinstance(img_data, str):
        image = img_data

    # Make image URL absolute
    if image and not image.startswith("http"):
        image = urljoin(page_url, image)

    # Price from offers
    msrp = ""
    offers = data.get("offers", {})
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        msrp = str(offers.get("price", ""))
        if not msrp:
            msrp = str(offers.get("highPrice", offers.get("lowPrice", "")))

    # Try to extract color from name or variant
    color = data.get("color", "")
    if not color:
        color = _extract_color_from_name(name)

    return Product(
        product_image=image,
        sku_upc=str(sku),
        product_name=name,
        color=color,
        msrp=msrp,
        _source="website",
        _source_detail=page_url,
    )


def extract_og_product(soup: BeautifulSoup, page_url: str) -> Product | None:
    """Extract product info from Open Graph meta tags."""
    og_type = soup.find("meta", property="og:type")
    og_title = soup.find("meta", property="og:title")

    if not og_title:
        return None

    name = og_title.get("content", "") if og_title else ""
    image = ""
    og_image = soup.find("meta", property="og:image")
    if og_image:
        image = og_image.get("content", "")
        if image and not image.startswith("http"):
            image = urljoin(page_url, image)

    price = ""
    og_price = soup.find("meta", property="product:price:amount") or soup.find(
        "meta", property="og:price:amount"
    )
    if og_price:
        price = og_price.get("content", "")

    sku_meta = soup.find("meta", property="product:retailer_item_id")
    sku = sku_meta.get("content", "") if sku_meta else ""

    if not name:
        return None

    return Product(
        product_image=image,
        sku_upc=sku,
        product_name=name,
        color=_extract_color_from_name(name),
        msrp=price,
        _source="website",
        _source_detail=page_url,
    )


def extract_products_from_page(soup: BeautifulSoup, page_url: str) -> list[Product]:
    """
    Extract products from a single page using multiple strategies.
    Priority: JSON-LD > OG tags > HTML patterns.
    """
    products = []

    # Strategy 1: JSON-LD structured data
    jsonld_products = extract_jsonld_products(soup, page_url)
    if jsonld_products:
        products.extend(jsonld_products)

    # Strategy 2: Open Graph tags (if no JSON-LD found)
    if not products:
        og_product = extract_og_product(soup, page_url)
        if og_product:
            products.append(og_product)

    # Strategy 3: HTML pattern matching for product detail pages
    if not products:
        html_product = _extract_from_html_patterns(soup, page_url)
        if html_product:
            products.append(html_product)

    return products


def _extract_from_html_patterns(soup: BeautifulSoup, page_url: str) -> Product | None:
    """Fallback: extract product data from common HTML patterns."""
    # Try to find product name from h1
    name = ""
    h1 = soup.find("h1")
    if h1:
        name = h1.get_text(strip=True)

    if not name:
        title = soup.find("title")
        if title:
            name = title.get_text(strip=True).split("|")[0].split("–")[0].strip()

    if not name:
        return None

    # Find price
    price = ""
    price_selectors = [
        '[class*="price"]',
        '[class*="Price"]',
        '[data-price]',
        '.price',
        '#price',
        '[itemprop="price"]',
    ]
    for selector in price_selectors:
        el = soup.select_one(selector)
        if el:
            price_text = el.get_text(strip=True)
            price_match = re.search(r'[\$£€]?\s*(\d+[.,]?\d*)', price_text)
            if price_match:
                price = price_match.group(0).strip()
                break
            # Check data attribute
            if el.get("data-price"):
                price = el["data-price"]
                break
            if el.get("content"):
                price = el["content"]
                break

    # Find main product image
    image = _find_main_product_image(soup, page_url)

    # Find SKU
    sku = ""
    sku_selectors = [
        '[class*="sku"]', '[class*="Sku"]', '[itemprop="sku"]',
        '[class*="product-id"]', '[class*="style-number"]',
    ]
    for selector in sku_selectors:
        el = soup.select_one(selector)
        if el:
            sku = el.get_text(strip=True)
            if sku:
                break

    return Product(
        product_image=image,
        sku_upc=sku,
        product_name=name,
        color=_extract_color_from_name(name),
        msrp=price,
        _source="website",
        _source_detail=page_url,
    )


def _find_main_product_image(soup: BeautifulSoup, page_url: str) -> str:
    """Find the main product image on a page."""
    candidates = []

    # Look for product image containers
    img_selectors = [
        '[class*="product-image"] img',
        '[class*="ProductImage"] img',
        '[class*="product-photo"] img',
        '[class*="gallery"] img',
        '[class*="main-image"] img',
        '[id*="product-image"] img',
        '[itemprop="image"]',
        '.product img',
        '#product img',
    ]

    for selector in img_selectors:
        imgs = soup.select(selector)
        for img in imgs:
            urls = _get_image_urls_from_tag(img, page_url)
            candidates.extend(urls)

    # Fallback: largest image on page
    if not candidates:
        for img in soup.find_all("img"):
            urls = _get_image_urls_from_tag(img, page_url)
            candidates.extend(urls)

    return pick_best_image_url(candidates)


def _get_image_urls_from_tag(img_tag, page_url: str) -> list[str]:
    """Extract all possible image URLs from an img tag, preferring high-res."""
    urls = []

    # srcset parsing (prefer largest)
    srcset = img_tag.get("srcset", "")
    if srcset:
        parts = [s.strip() for s in srcset.split(",") if s.strip()]
        srcset_urls = []
        for part in parts:
            tokens = part.split()
            if tokens:
                url = tokens[0]
                width = 0
                if len(tokens) > 1 and tokens[1].endswith("w"):
                    try:
                        width = int(tokens[1][:-1])
                    except ValueError:
                        pass
                srcset_urls.append((width, url))
        srcset_urls.sort(key=lambda x: x[0], reverse=True)
        for _, url in srcset_urls:
            abs_url = urljoin(page_url, url)
            urls.append(abs_url)

    # data-src (lazy loading)
    for attr in ["data-src", "data-original", "data-zoom-image", "data-large"]:
        val = img_tag.get(attr, "")
        if val:
            urls.append(urljoin(page_url, val))

    # Regular src
    src = img_tag.get("src", "")
    if src and not src.startswith("data:"):
        urls.append(urljoin(page_url, src))

    return urls


# URL path segments that indicate non-product pages — skip these entirely
NON_PRODUCT_URL_SEGMENTS = {
    "faq", "faqs", "about", "about-us", "contact", "contact-us",
    "privacy", "privacy-policy", "terms", "terms-of-service",
    "terms-and-conditions", "cookie-policy", "gdpr", "accessibility",
    "careers", "jobs", "press", "blog", "blogs", "news", "newsletter",
    "shipping", "shipping-policy", "returns", "return-policy",
    "refund-policy", "warranty", "sitemap", "store-locator",
    "gift-cards", "gift-card", "login", "sign-in", "register",
    "account", "cart", "checkout", "wishlist", "rewards", "loyalty",
    "affiliate", "affiliates", "ambassador", "ambassadors",
    "sustainability", "our-story", "our-mission", "recipes",
    "culinary-council", "pages",
}


def _is_non_product_url(url: str) -> bool:
    """Check if a URL is clearly a non-product page that should be skipped."""
    parsed = urlparse(url)
    path_parts = [p.lower() for p in parsed.path.strip("/").split("/") if p]

    # If the last path segment (the page slug) is a known non-product page, skip it
    for part in path_parts:
        if part in NON_PRODUCT_URL_SEGMENTS:
            return True

    return False


def find_product_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    """Find links to individual product pages from a collection/listing page."""
    links = set()
    parsed_base = urlparse(base_url)

    # Common product link patterns
    product_patterns = [
        r'/products?/',
        r'/shop/',
        r'/item/',
        r'/p/',
        r'/dp/',
        r'/collections?/.+/.+',
    ]
    combined_pattern = re.compile('|'.join(product_patterns), re.IGNORECASE)

    # Look for product card links
    card_selectors = [
        '[class*="product"] a',
        '[class*="Product"] a',
        '[class*="card"] a',
        '[class*="item"] a',
        '.grid a',
        '.collection a',
    ]

    found_elements = set()
    for selector in card_selectors:
        for a in soup.select(selector):
            found_elements.add(a)

    # Also check all links that match product URL patterns
    for a in soup.find_all("a", href=True):
        href = a["href"]
        abs_url = urljoin(base_url, href)
        parsed = urlparse(abs_url)

        # Must be same domain
        if parsed.netloc != parsed_base.netloc:
            continue

        # Remove fragment
        abs_url = urldefrag(abs_url)[0]

        # Skip known non-product URLs
        if _is_non_product_url(abs_url):
            continue

        if combined_pattern.search(parsed.path):
            links.add(abs_url)
        elif a in found_elements:
            # From a product card selector
            if parsed.path != "/" and len(parsed.path) > 5:
                links.add(abs_url)

    return sorted(links)


def find_collection_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    """Find links to collection/category pages."""
    links = set()
    parsed_base = urlparse(base_url)

    collection_patterns = [
        r'/collections?/',
        r'/categories?/',
        r'/catalog/',
        r'/shop/',
        r'/department/',
    ]
    combined_pattern = re.compile('|'.join(collection_patterns), re.IGNORECASE)

    for a in soup.find_all("a", href=True):
        href = a["href"]
        abs_url = urljoin(base_url, href)
        parsed = urlparse(abs_url)

        if parsed.netloc != parsed_base.netloc:
            continue

        abs_url = urldefrag(abs_url)[0]

        # Skip non-product collection pages
        if _is_non_product_url(abs_url):
            continue

        if combined_pattern.search(parsed.path):
            links.add(abs_url)

    return sorted(links)


def scrape_single_product(url: str) -> list[Product]:
    """Scrape a single product URL."""
    result = scrape_url(url)
    if not result["ok"]:
        return []
    return extract_products_from_page(result["soup"], result["url"])


def scrape_collection_page(url: str, progress_callback=None) -> list[Product]:
    """Scrape a collection page: find product links, then scrape each."""
    result = scrape_url(url)
    if not result["ok"]:
        return []

    # First try to extract products directly from collection page
    products = extract_products_from_page(result["soup"], result["url"])

    # Then find and follow product links for richer data
    product_links = find_product_links(result["soup"], result["url"])

    if product_links:
        total = len(product_links)
        for i, link in enumerate(product_links):
            if progress_callback:
                progress_callback(i + 1, total, link)
            try:
                page_products = scrape_single_product(link)
                if page_products:
                    products.extend(page_products)
                time.sleep(0.5)  # Be polite
            except Exception:
                continue

    return products


def scrape_website(url: str, progress_callback=None) -> list[Product]:
    """Scrape an entire website: find collections, then scrape each."""
    result = scrape_url(url)
    if not result["ok"]:
        return []

    all_products = []

    # Find collection pages
    collection_links = find_collection_links(result["soup"], result["url"])

    # Also check if the landing page itself has products
    landing_products = extract_products_from_page(result["soup"], result["url"])
    all_products.extend(landing_products)

    # Scrape each collection
    if collection_links:
        for coll_url in collection_links[:20]:  # Limit to 20 collections
            if progress_callback:
                progress_callback(f"Scanning collection: {coll_url}")
            try:
                coll_products = scrape_collection_page(coll_url)
                all_products.extend(coll_products)
                time.sleep(1.0)
            except Exception:
                continue

    # If no collections found, try finding product links on main page
    if not collection_links:
        product_links = find_product_links(result["soup"], result["url"])
        total = len(product_links)
        for i, link in enumerate(product_links[:50]):  # Limit to 50 products
            if progress_callback:
                progress_callback(f"Scraping product {i+1}/{min(total,50)}: {link}")
            try:
                page_products = scrape_single_product(link)
                all_products.extend(page_products)
                time.sleep(0.5)
            except Exception:
                continue

    return all_products


# Common color names for extraction
COMMON_COLORS = [
    "black", "white", "red", "blue", "green", "yellow", "orange", "purple",
    "pink", "brown", "grey", "gray", "navy", "teal", "maroon", "olive",
    "cream", "beige", "tan", "ivory", "coral", "salmon", "burgundy",
    "charcoal", "silver", "gold", "khaki", "lavender", "mint", "sage",
    "rust", "copper", "bronze", "indigo", "violet", "magenta", "crimson",
    "slate", "ash", "graphite", "onyx", "pearl", "bone", "oat", "sand",
    "stone", "chalk", "ink", "midnight", "ocean", "forest", "moss",
    "camel", "espresso", "chocolate", "cognac", "taupe",
    # Multi-word
    "light blue", "dark blue", "light grey", "dark grey", "light gray",
    "dark gray", "light green", "dark green", "light pink", "hot pink",
    "royal blue", "baby blue", "sky blue", "ice blue", "dusty rose",
    "blush pink", "rose gold", "off white", "heather grey",
]


def _extract_color_from_name(name: str) -> str:
    """Try to extract a color from a product name. Only if high confidence."""
    if not name:
        return ""
    name_lower = name.lower()

    # Check for " - color" or " / color" patterns
    for sep in [" - ", " / ", " | ", ", "]:
        if sep in name:
            parts = name.split(sep)
            for part in parts[1:]:  # Skip first part (usually product name)
                part_lower = part.strip().lower()
                for color in COMMON_COLORS:
                    if part_lower == color or part_lower.startswith(color):
                        return part.strip().title()

    # Check for color in parentheses
    paren_match = re.search(r'\(([^)]+)\)', name)
    if paren_match:
        inner = paren_match.group(1).strip().lower()
        for color in COMMON_COLORS:
            if inner == color or inner.startswith(color):
                return paren_match.group(1).strip().title()

    return ""
