"""Product schema and data structures for the line sheet maker."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class Product:
    """Represents a single product row in the line sheet."""
    product_image: str = ""       # URL or local path to product image
    sku_upc: str = ""
    product_name: str = ""
    color: str = ""
    msrp: str = ""
    qty: str = ""
    your_cost: str = ""
    notes: str = ""
    # Internal tracking fields (not exported)
    _source: str = ""             # "website", "pdf", "excel", "csv", "manual"
    _source_detail: str = ""      # URL, filename, page number, row number
    _confidence: float = 1.0      # 0.0 to 1.0 confidence in data accuracy
    _image_local_path: str = ""   # Local cached image path
    _category: str = ""           # Detected category/collection

    def to_dict(self):
        return asdict(self)

    def to_export_dict(self):
        """Return only the columns that go into the line sheet."""
        return {
            "Product Image": self.product_image,
            "UPC/SKU/ASIN": self.sku_upc,
            "Product Name": self.product_name,
            "Color": self.color,
            "MSRP": self.msrp,
            "Qty": self.qty,
            "Your Cost": self.your_cost,
            "Notes": self.notes,
        }


# The exact export column order
EXPORT_COLUMNS = [
    "Product Image",
    "UPC/SKU/ASIN",
    "Product Name",
    "Color",
    "MSRP",
    "Qty",
    "Your Cost",
    "Notes",
]

# Internal field names matching the export columns
INTERNAL_FIELDS = [
    "product_image",
    "sku_upc",
    "product_name",
    "color",
    "msrp",
    "qty",
    "your_cost",
    "notes",
]

# Common column name mappings for auto-detection
COLUMN_ALIASES = {
    "product_image": [
        "image", "photo", "picture", "img", "product image", "image url",
        "image_url", "photo_url", "thumbnail", "main image",
    ],
    "sku_upc": [
        "sku", "upc", "asin", "style", "style number", "style #", "style no",
        "item number", "item #", "item no", "barcode", "ean", "gtin",
        "product code", "code", "article", "article number", "model",
        "upc/sku/asin", "sku/upc", "upc/sku",
    ],
    "product_name": [
        "name", "product name", "product", "title", "product title",
        "description", "item name", "item", "style name",
    ],
    "color": [
        "color", "colour", "colorway", "color name", "color/pattern",
        "shade", "variant", "color variant",
    ],
    "msrp": [
        "msrp", "retail", "retail price", "price", "rrp", "srp",
        "suggested retail", "list price", "selling price",
    ],
    "your_cost": [
        "your cost", "wholesale", "wholesale price", "cost", "unit cost",
        "net price", "dealer price", "trade price", "landed cost", "fob",
    ],
    "qty": [
        "qty", "quantity", "ats", "available", "stock", "inventory",
        "on hand", "available qty", "available quantity", "moq",
        "units", "unit", "pcs", "pieces", "ea", "each",
        "fall ats", "spring ats", "summer ats", "winter ats",
        "allocation", "alloc",
    ],
    "notes": [
        "notes", "note", "comments", "comment", "description", "details",
        "remarks", "memo", "info",
    ],
}


def auto_map_columns(source_columns: list[str]) -> dict[str, str]:
    """
    Given a list of source column names, return a mapping of
    source_column -> internal_field based on fuzzy matching.
    """
    mapping = {}
    used_fields = set()

    for src_col in source_columns:
        src_lower = src_col.strip().lower()
        best_match = None
        best_score = 0

        for field_name, aliases in COLUMN_ALIASES.items():
            if field_name in used_fields:
                continue
            for alias in aliases:
                # Exact match
                if src_lower == alias:
                    score = 100
                # Source contains alias
                elif alias in src_lower:
                    score = 80
                # Alias contains source
                elif src_lower in alias:
                    score = 60
                else:
                    continue

                if score > best_score:
                    best_score = score
                    best_match = field_name

        if best_match and best_score >= 60:
            mapping[src_col] = best_match
            used_fields.add(best_match)

    return mapping
