"""Data normalization, filtering, and deduplication for product lists."""
from __future__ import annotations

import re
from utils.schema import Product


# ── Non-product page indicators ──
# If a "product name" is really just one of these, it's not a product.
NON_PRODUCT_EXACT = {
    "affiliate", "affiliates", "contact", "contact us", "faq", "faqs",
    "faq's", "about", "about us", "privacy", "privacy policy",
    "terms", "terms of service", "terms & conditions", "terms and conditions",
    "gdpr", "gdpr compliance", "cookie policy", "cookies",
    "careers", "jobs", "press", "blog", "news", "newsletter",
    "shipping", "shipping policy", "returns", "return policy",
    "refund policy", "warranty", "sitemap", "accessibility",
    "store locator", "find a store", "gift cards", "gift card",
    "login", "sign in", "register", "my account", "account",
    "cart", "checkout", "wishlist", "rewards", "loyalty",
    "culinary council", "ambassador", "ambassadors",
    "sustainability", "our story", "our mission",
}

# If a product name contains these phrases, it's likely a non-product page
NON_PRODUCT_PHRASES = [
    "all rights reserved", "copyright", "terms of", "privacy",
    "cookie", "gdpr", "compliance", "unsubscribe",
]

# If a product name matches these patterns, it's a collection/category page, not a product
COLLECTION_PAGE_PATTERNS = [
    r'^all\s+\w+',                       # "All Cookware Sets", "All Kitchen Tools"
    r'^shop\s+(all|our)',                 # "Shop All HexWear", "Shop Our Collection"
    r'best\s+\w+\s*\|',                  # "Best Hybrid Cookware |"
    r'\|\s*\w+\s+cookware$',             # "... | HexClad Cookware"
    r'\|\s*\w+\s+\w+\s*$',              # trailing "| Brand Name" pattern
]


def normalize_products(products: list[Product]) -> list[Product]:
    """Normalize all product data for consistency."""
    for p in products:
        p.product_name = _clean_text(p.product_name)
        p.color = _clean_text(p.color)
        p.sku_upc = _clean_sku(p.sku_upc)
        p.msrp = _normalize_price(p.msrp)
        p.your_cost = _normalize_price(p.your_cost)
        p.qty = _clean_qty(p.qty)
        p.notes = _clean_text(p.notes)

        # Default qty to "Catalog" if no units were found
        # This means the source didn't provide availability numbers
        if not p.qty:
            p.qty = "Catalog"

    return products


def filter_non_products(products: list[Product]) -> list[Product]:
    """
    Remove entries that are clearly not products — info pages, category headers,
    collection landing pages, etc. Uses thoughtful heuristics:
    - Checks against known non-product page titles
    - Checks for collection/category page naming patterns
    - Does NOT require MSRP (PDFs and some sites won't have pricing)
    - DOES require at least a product name or SKU to keep
    """
    filtered = []

    for p in products:
        name = p.product_name.strip()
        name_lower = name.lower()

        # Must have at least a name OR a SKU to be worth keeping
        if not name and not p.sku_upc:
            continue

        # ── Check 0: OCR garbage / marketing text filter ──
        # Names that are clearly OCR noise or marketing copy, not product names
        if not p.sku_upc and not p.msrp:
            # Very short fragments or obvious non-product text
            if len(name) < 5:
                continue
            # Ends with common OCR cut-off patterns
            if name.endswith(')') or name.endswith('-'):
                continue
        # Marketing/promo text that got picked up as a "product"
        if any(phrase in name_lower for phrase in [
            "gear up", "back to campus", "has this pack", "no one else",
            "made with durable", "deadstock", "one-of-a-kind", "edition of one",
            "meet you on", "literally", "styles inside",
        ]):
            continue

        # ── Check 1: Exact match against known non-product pages ──
        if name_lower in NON_PRODUCT_EXACT:
            continue

        # ── Check 2: Contains non-product phrases ──
        if any(phrase in name_lower for phrase in NON_PRODUCT_PHRASES):
            continue

        # ── Check 3: Looks like a collection/category page title ──
        # These have pipe-separated multi-part titles like "All Cookware Sets | Best Hybrid Cookware | HexClad Cookware"
        # Real products can have pipes too (e.g. variants), but collection pages
        # typically have 2+ pipes with brand name repeated
        pipe_parts = [p.strip() for p in name.split("|")]
        if len(pipe_parts) >= 2:
            # If no SKU and no price, and name has 2+ pipe segments, likely a page title
            if not p.sku_upc and not p.msrp:
                # Check if it's a known collection-style title
                if _looks_like_page_title(name_lower, pipe_parts):
                    continue

        # ── Check 4: Very short generic single-word names without any identifiers ──
        # e.g. "Aprons", "HexWear" — but only if there's nothing else to go on
        if (
            not p.sku_upc
            and not p.msrp
            and not p.your_cost
            and not p.product_image
            and len(name.split()) <= 2
            and not re.search(r'\d', name)  # no numbers = likely a category, not "Pan 8-inch"
        ):
            continue

        # ── Check 5: URL-in-name check — some scrapers grab page titles with site name ──
        if name_lower.endswith((" cookware", " - home", " - official site")):
            # Only skip if no SKU and no price
            if not p.sku_upc and not p.msrp:
                # Check if name is really a site/brand tagline
                if _looks_like_page_title(name_lower, pipe_parts):
                    continue

        # Passed all filters — keep it
        filtered.append(p)

    return filtered


def _looks_like_page_title(name_lower: str, pipe_parts: list[str]) -> bool:
    """Check if a name looks like a website page title rather than a product name."""
    # Collection/category keywords
    category_words = [
        "all ", "shop ", "best ", "top ", "new ", "featured",
        "collection", "category", "department", "browse",
        "what is", "about", "how to", "why ", "our ",
        "science", "technology",
    ]

    for part in pipe_parts:
        part_lower = part.strip().lower()
        if any(part_lower.startswith(w) for w in category_words):
            return True

    # If multiple pipe parts and one is just a brand name (short, no numbers)
    if len(pipe_parts) >= 3:
        return True

    # "Gordon Ramsay Cookbooks | Recipe Books | HexClad" pattern
    if any("recipe" in p.lower() or "cookbook" in p.lower() or "book" in p.lower() for p in pipe_parts):
        return True

    return False


def deduplicate_products(products: list[Product]) -> list[Product]:
    """
    Remove duplicate products. Keeps the entry with the most data.
    Deduplicates by:
    1. SKU/UPC (exact match)
    2. Exact name match (preserving color variants)
    3. Substring/phantom duplicates (e.g. "Cocktail Shaker" absorbed by
       "Cocktail Shaker, 25 oz (Chrome)" if the short one has less data)
    """
    if not products:
        return products

    # ── Phase 1: Group by SKU + Color ──
    # Same SKU with different colors are DIFFERENT variants, not duplicates.
    # Only collapse entries with same SKU AND same color (or both no color).
    sku_groups: dict[str, list[Product]] = {}
    no_sku = []

    for p in products:
        if p.sku_upc:
            sku_key = p.sku_upc.strip().lower()
            color_key = p.color.strip().lower() if p.color else ""
            key = f"{sku_key}|{color_key}"
            sku_groups.setdefault(key, []).append(p)
        else:
            no_sku.append(p)

    deduped = []

    for key, group in sku_groups.items():
        best = _pick_best(group)
        deduped.append(best)

    # ── Phase 2: Deduplicate no-SKU products by exact name ──
    name_groups: dict[str, list[Product]] = {}
    for p in no_sku:
        if p.product_name:
            key = p.product_name.strip().lower()
            if p.color:
                key += f"|{p.color.strip().lower()}"
            name_groups.setdefault(key, []).append(p)
        else:
            deduped.append(p)

    for key, group in name_groups.items():
        best = _pick_best(group)
        deduped.append(best)

    # ── Phase 3: Remove phantom/substring duplicates ──
    # e.g. "Cocktail Shaker" is a phantom of "Cocktail Shaker, 25 oz (Chrome)"
    deduped = _remove_phantom_duplicates(deduped)

    return deduped


def _remove_phantom_duplicates(products: list[Product]) -> list[Product]:
    """
    Remove entries where the product name is a substring of another product's name
    AND the shorter entry has strictly less data (fewer populated fields).
    This catches phantom listings like "Cocktail Shaker" when
    "Cocktail Shaker, 25 oz (Chrome)" already exists with richer data.
    """
    if len(products) <= 1:
        return products

    to_remove = set()

    for i, p1 in enumerate(products):
        if not p1.product_name:
            continue
        name1 = p1.product_name.strip().lower()

        for j, p2 in enumerate(products):
            if i == j or not p2.product_name:
                continue
            name2 = p2.product_name.strip().lower()

            # Check if p1's name is a substring of p2's name (or very close)
            # p1 = "Cocktail Shaker", p2 = "Cocktail Shaker, 25 oz (Chrome)"
            if len(name1) < len(name2) and name2.startswith(name1):
                # p1 is the shorter (phantom) entry
                score1 = _data_score(p1)
                score2 = _data_score(p2)

                # Only remove the phantom if the longer entry has equal or more data
                if score2 >= score1:
                    to_remove.add(i)
                    break  # p1 is already marked, move on

    return [p for i, p in enumerate(products) if i not in to_remove]


def _data_score(p: Product) -> int:
    """Score how much data a product entry has."""
    s = 0
    if p.product_image:
        s += 3
    if p.sku_upc:
        s += 2
    if p.product_name:
        s += 2
    if p.color:
        s += 1
    if p.msrp:
        s += 1
    if p.your_cost:
        s += 1
    if p.qty:
        s += 1
    if p.notes:
        s += 1
    return s


def _pick_best(group: list[Product]) -> Product:
    """Pick the product with the most populated fields."""
    if len(group) == 1:
        return group[0]

    group.sort(key=_data_score, reverse=True)
    best = group[0]

    # Merge in any missing fields from other entries
    for other in group[1:]:
        if not best.product_image and other.product_image:
            best.product_image = other.product_image
        if not best.sku_upc and other.sku_upc:
            best.sku_upc = other.sku_upc
        if not best.product_name and other.product_name:
            best.product_name = other.product_name
        if not best.color and other.color:
            best.color = other.color
        if not best.msrp and other.msrp:
            best.msrp = other.msrp
        if not best.your_cost and other.your_cost:
            best.your_cost = other.your_cost
        if not best.qty and other.qty:
            best.qty = other.qty

    return best


def _clean_text(text: str) -> str:
    """Clean whitespace and normalize text."""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _clean_sku(val: str) -> str:
    """Clean SKU/UPC value."""
    if not val:
        return ""
    val = val.strip()
    # Remove trailing .0 from numeric values
    if re.match(r'^\d+\.0$', val):
        val = val[:-2]
    # Fix common OCR misread: $ at start of SKU should be S
    # e.g. "$26494U1607" -> "S26494U1607"
    if re.match(r'^\$\d{4,}', val):
        val = 'S' + val[1:]
    return val


def _normalize_price(val: str) -> str:
    """Normalize a price to $XX.XX format."""
    if not val:
        return ""
    val = val.strip()

    match = re.search(r'(\d+(?:[.,]\d{1,2})?)', val)
    if not match:
        return val

    num_str = match.group(1).replace(',', '.')
    try:
        num = float(num_str)
        return f"${num:.2f}"
    except ValueError:
        return val


def _clean_qty(val: str) -> str:
    """Clean quantity value."""
    if not val:
        return ""
    val = val.strip()
    if re.match(r'^\d+\.0$', val):
        val = val[:-2]
    return val
