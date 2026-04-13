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


# ─────────────────────── Shopify JSON API ───────────────────────

def _is_shopify(soup: BeautifulSoup, html: str) -> bool:
    """Detect if a site is built on Shopify."""
    indicators = [
        'cdn.shopify.com' in html,
        'Shopify.theme' in html,
        'myshopify.com' in html,
        bool(soup.find("meta", attrs={"name": "shopify-checkout-api-token"})),
        bool(soup.find("link", href=re.compile(r'cdn\.shopify\.com'))),
        '"shopify"' in html.lower()[:5000],
    ]
    return sum(indicators) >= 1


def scrape_shopify_api(base_url: str, progress_callback=None) -> list[Product]:
    """Scrape products via Shopify's /products.json API."""
    parsed = urlparse(base_url)
    shop_root = f"{parsed.scheme}://{parsed.netloc}"
    products = []
    page = 1

    while True:
        api_url = f"{shop_root}/products.json?limit=250&page={page}"
        if progress_callback:
            progress_callback(f"Fetching Shopify API page {page}...")
        try:
            resp = requests.get(api_url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            break

        shop_products = data.get("products", [])
        if not shop_products:
            break

        for sp in shop_products:
            name = sp.get("title", "")
            vendor = sp.get("vendor", "")
            product_type = sp.get("product_type", "")

            # Get main image
            images = sp.get("images", [])
            main_image = images[0].get("src", "") if images else ""

            # Create one Product per variant (color/size)
            variants = sp.get("variants", [{}])
            seen_colors = set()

            for variant in variants:
                color = ""
                option1 = variant.get("option1", "")
                option2 = variant.get("option2", "")
                # Typically option1 is color, option2 is size
                for opt in [option1, option2]:
                    if opt and opt.lower() not in ("default title", "default", "one size", "os"):
                        if _looks_like_color(opt):
                            color = opt
                            break

                # Skip duplicate colors (different sizes)
                color_key = color.lower() if color else f"_var_{variant.get('id','')}"
                if color_key in seen_colors:
                    continue
                seen_colors.add(color_key)

                sku = variant.get("sku", "")
                price = variant.get("price", "")

                # Find variant-specific image
                var_image = main_image
                var_img_id = variant.get("image_id")
                if var_img_id and images:
                    for img in images:
                        if img.get("id") == var_img_id:
                            var_image = img.get("src", main_image)
                            break

                products.append(Product(
                    product_image=var_image,
                    sku_upc=sku,
                    product_name=name,
                    color=color,
                    msrp=f"${float(price):.2f}" if price else "",
                    _source="website",
                    _source_detail=f"{shop_root}/products/{sp.get('handle', '')}",
                    _category=product_type,
                ))

        if len(shop_products) < 250:
            break
        page += 1
        time.sleep(0.5)

    return products


def _looks_like_color(text: str) -> bool:
    """Check if a string looks like a color name rather than a size."""
    lower = text.strip().lower()
    # Definitely a size
    size_patterns = [
        r'^\d+(\.\d+)?\s*(oz|ml|l|g|kg|lb|in|cm|mm|qt|gal)\.?$',
        r'^(xs|s|m|l|xl|xxl|xxxl|2xl|3xl|one size)$',
        r'^\d+x\d+',
        r'^\d+\s*(piece|pc|pack|count|ct|set)s?$',
        r'^\d+$',
    ]
    for pat in size_patterns:
        if re.match(pat, lower):
            return False

    # Known colors = definitely color
    for c in COMMON_COLORS:
        if lower == c or lower.startswith(c):
            return True

    # If it has no digits and is short-ish, probably a color
    if not any(ch.isdigit() for ch in text) and len(text) < 30:
        return True

    return False


# ─────────────────── Sitemap product URL finder ───────────────────

def _fetch_sitemap_product_urls(base_url: str, max_urls: int = 100) -> list[str]:
    """Parse sitemap.xml to find product URLs."""
    parsed = urlparse(base_url)
    site_root = f"{parsed.scheme}://{parsed.netloc}"
    product_urls = []

    sitemap_locations = [
        f"{site_root}/sitemap.xml",
        f"{site_root}/sitemap_products_1.xml",
        f"{site_root}/sitemap_index.xml",
    ]

    product_patterns = re.compile(
        r'/(products?|shop|items?|p|catalog)/[^?#]+', re.IGNORECASE
    )

    visited = set()

    for sitemap_url in sitemap_locations:
        if sitemap_url in visited:
            continue
        visited.add(sitemap_url)

        try:
            resp = requests.get(sitemap_url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            sm_soup = BeautifulSoup(resp.text, "lxml-xml")
        except Exception:
            try:
                resp = requests.get(sitemap_url, headers=HEADERS, timeout=15)
                sm_soup = BeautifulSoup(resp.text, "lxml")
            except Exception:
                continue

        # Check for sitemap index (links to other sitemaps)
        sub_sitemaps = sm_soup.find_all("sitemap")
        for sub in sub_sitemaps:
            loc = sub.find("loc")
            if loc and loc.text:
                sub_url = loc.text.strip()
                if "product" in sub_url.lower() and sub_url not in visited:
                    sitemap_locations.append(sub_url)

        # Extract product URLs from <url><loc>
        for url_tag in sm_soup.find_all("url"):
            loc = url_tag.find("loc")
            if loc and loc.text:
                url_text = loc.text.strip()
                if product_patterns.search(url_text):
                    if not _is_non_product_url(url_text):
                        product_urls.append(url_text)
                        if len(product_urls) >= max_urls:
                            return product_urls

    return product_urls


# ─────────────────── Next.js __NEXT_DATA__ extraction ───────────────────

def _extract_nextdata_products(soup: BeautifulSoup, page_url: str) -> list[Product]:
    """Extract products from Next.js __NEXT_DATA__ script tag."""
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return []

    try:
        data = json.loads(script.string)
    except json.JSONDecodeError:
        return []

    products = []
    # Recursively search for product-like objects
    _find_products_in_data(data, products, page_url, depth=0)
    return products


def _find_products_in_data(obj, products: list, page_url: str, depth: int):
    """Recursively find product data in nested JSON structures."""
    if depth > 10:
        return

    if isinstance(obj, dict):
        # Check if this dict looks like a product
        has_name = any(k in obj for k in ("name", "title", "productName", "product_name"))
        has_price = any(k in obj for k in ("price", "msrp", "amount", "salePrice", "retailPrice"))
        has_image = any(k in obj for k in ("image", "images", "imageUrl", "image_url", "thumbnail"))

        if has_name and (has_price or has_image):
            name = obj.get("name") or obj.get("title") or obj.get("productName") or obj.get("product_name", "")
            if isinstance(name, str) and len(name) > 2:
                # Extract image
                image = ""
                for key in ("image", "imageUrl", "image_url", "thumbnail", "primaryImage"):
                    val = obj.get(key)
                    if isinstance(val, str) and val:
                        image = val if val.startswith("http") else urljoin(page_url, val)
                        break
                    elif isinstance(val, list) and val:
                        first = val[0]
                        if isinstance(first, str):
                            image = first if first.startswith("http") else urljoin(page_url, first)
                        elif isinstance(first, dict):
                            image = first.get("src") or first.get("url") or ""
                            if image and not image.startswith("http"):
                                image = urljoin(page_url, image)
                        break
                    elif isinstance(val, dict):
                        image = val.get("src") or val.get("url") or ""
                        if image and not image.startswith("http"):
                            image = urljoin(page_url, image)
                        break

                # Extract images list
                if not image:
                    imgs = obj.get("images", [])
                    if isinstance(imgs, list) and imgs:
                        first = imgs[0]
                        if isinstance(first, str):
                            image = first
                        elif isinstance(first, dict):
                            image = first.get("src") or first.get("url") or ""

                # Extract price
                price = ""
                for key in ("price", "msrp", "retailPrice", "salePrice", "amount"):
                    val = obj.get(key)
                    if val is not None:
                        if isinstance(val, dict):
                            price = str(val.get("amount") or val.get("value") or "")
                        elif isinstance(val, (int, float)):
                            price = f"${val:.2f}"
                        elif isinstance(val, str) and val:
                            price = val
                        if price:
                            break

                sku = str(obj.get("sku") or obj.get("productId") or obj.get("id") or "")
                color = obj.get("color") or obj.get("colorName") or ""
                if isinstance(color, dict):
                    color = color.get("name") or color.get("label") or ""

                products.append(Product(
                    product_image=image,
                    sku_upc=sku,
                    product_name=str(name),
                    color=str(color),
                    msrp=str(price),
                    _source="website",
                    _source_detail=page_url,
                ))
                return  # Don't recurse into this product's children

        # Recurse into all values
        for v in obj.values():
            _find_products_in_data(v, products, page_url, depth + 1)

    elif isinstance(obj, list):
        for item in obj:
            _find_products_in_data(item, products, page_url, depth + 1)


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


def extract_products_from_page(soup: BeautifulSoup, page_url: str, html: str = "") -> list[Product]:
    """
    Extract products from a single page using multiple strategies.
    Priority: JSON-LD > Next.js data > OG tags > HTML patterns.
    """
    products = []

    # Strategy 1: JSON-LD structured data
    jsonld_products = extract_jsonld_products(soup, page_url)
    if jsonld_products:
        products.extend(jsonld_products)

    # Strategy 2: Next.js __NEXT_DATA__
    if not products:
        next_products = _extract_nextdata_products(soup, page_url)
        if next_products:
            products.extend(next_products)

    # Strategy 3: Open Graph tags (if no JSON-LD found)
    if not products:
        og_product = extract_og_product(soup, page_url)
        if og_product:
            products.append(og_product)

    # Strategy 4: HTML pattern matching for product detail pages
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
    soup = result["soup"]
    html = result["html"]

    # ── Strategy A: Shopify JSON API (most reliable for Shopify stores) ──
    if _is_shopify(soup, html):
        if progress_callback:
            progress_callback("Shopify store detected! Using product API...")
        shopify_products = scrape_shopify_api(result["url"], progress_callback)
        if shopify_products:
            return shopify_products

    # ── Strategy B: Extract from landing page ──
    landing_products = extract_products_from_page(soup, result["url"], html)
    all_products.extend(landing_products)

    # ── Strategy C: Find and scrape collection pages ──
    collection_links = find_collection_links(soup, result["url"])
    if collection_links:
        for coll_url in collection_links[:20]:
            if progress_callback:
                progress_callback(f"Scanning collection: {coll_url}")
            try:
                coll_products = scrape_collection_page(coll_url)
                all_products.extend(coll_products)
                time.sleep(1.0)
            except Exception:
                continue

    # ── Strategy D: Find product links on main page ──
    if not collection_links:
        product_links = find_product_links(soup, result["url"])
        total = len(product_links)
        for i, link in enumerate(product_links[:50]):
            if progress_callback:
                progress_callback(f"Scraping product {i+1}/{min(total,50)}: {link}")
            try:
                page_products = scrape_single_product(link)
                all_products.extend(page_products)
                time.sleep(0.5)
            except Exception:
                continue

    # ── Strategy E: Sitemap fallback (if we still have nothing) ──
    if not all_products:
        if progress_callback:
            progress_callback("Trying sitemap.xml for product URLs...")
        sitemap_urls = _fetch_sitemap_product_urls(result["url"])
        if sitemap_urls:
            total = len(sitemap_urls)
            for i, purl in enumerate(sitemap_urls[:50]):
                if progress_callback:
                    progress_callback(f"Scraping sitemap product {i+1}/{min(total,50)}")
                try:
                    page_products = scrape_single_product(purl)
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
