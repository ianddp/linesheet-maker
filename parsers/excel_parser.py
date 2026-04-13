"""Excel and CSV parser for importing product data from spreadsheets."""
from __future__ import annotations

import io
import re

import pandas as pd

from utils.schema import Product, auto_map_columns


def parse_excel(file_bytes: bytes, filename: str) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Parse an Excel file and return the raw DataFrame and suggested column mapping.
    Returns (dataframe, suggested_mapping).
    """
    if filename.lower().endswith(".csv"):
        # Try different encodings
        for encoding in ["utf-8", "latin-1", "cp1252"]:
            try:
                df = pd.read_csv(io.BytesIO(file_bytes), encoding=encoding)
                break
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        else:
            raise ValueError("Could not parse CSV file with any common encoding.")
    else:
        df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")

    # Clean up column names
    df.columns = [str(c).strip() for c in df.columns]

    # Drop completely empty rows and columns
    df = df.dropna(how="all").dropna(axis=1, how="all")

    # If first row looks like a header that pandas missed, fix it
    if _looks_like_header(df.iloc[0] if len(df) > 0 else pd.Series()):
        new_headers = [str(v).strip() for v in df.iloc[0]]
        df = df[1:].reset_index(drop=True)
        df.columns = new_headers

    # Auto-map columns
    mapping = auto_map_columns(list(df.columns))

    return df, mapping


def _looks_like_header(row: pd.Series) -> bool:
    """Check if a row looks like it should be the header."""
    if len(row) == 0:
        return False

    header_words = [
        "sku", "upc", "name", "product", "color", "price", "msrp",
        "wholesale", "qty", "quantity", "image", "description", "style",
    ]

    text_values = [str(v).strip().lower() for v in row if pd.notna(v)]
    matches = sum(1 for v in text_values if any(w in v for w in header_words))
    return matches >= 2


def apply_mapping(df: pd.DataFrame, mapping: dict[str, str]) -> list[Product]:
    """
    Apply column mapping to a DataFrame and return a list of Products.
    mapping: {source_column_name: internal_field_name}
    """
    products = []

    for idx, row in df.iterrows():
        data = {}
        for src_col, field_name in mapping.items():
            if src_col in df.columns:
                val = row[src_col]
                if pd.notna(val):
                    val = str(val).strip()
                    # Clean up price values
                    if field_name in ("msrp", "your_cost"):
                        val = _clean_price(val)
                    # Preserve leading zeroes for SKU/UPC
                    if field_name == "sku_upc":
                        val = _clean_sku(val)
                    data[field_name] = val
                else:
                    data[field_name] = ""

        # Skip rows with no meaningful data
        if not any(data.get(f) for f in ["product_name", "sku_upc", "msrp"]):
            continue

        source_detail = f"row {idx + 2}"  # +2 for 1-based + header

        products.append(Product(
            product_image=data.get("product_image", ""),
            sku_upc=data.get("sku_upc", ""),
            product_name=data.get("product_name", ""),
            color=data.get("color", ""),
            msrp=data.get("msrp", ""),
            your_cost=data.get("your_cost", ""),
            qty=data.get("qty", ""),
            notes=data.get("notes", ""),
            _source="excel",
            _source_detail=source_detail,
        ))

    return products


def _clean_price(val: str) -> str:
    """Clean up a price value."""
    if not val:
        return ""
    # Remove currency symbols and spaces
    cleaned = re.sub(r'[^\d.,]', '', val)
    if not cleaned:
        return val
    # If it looks like a number, format it
    try:
        num = float(cleaned.replace(',', ''))
        return f"${num:.2f}"
    except ValueError:
        return val


def _clean_sku(val: str) -> str:
    """Clean a SKU/UPC value, preserving leading zeroes."""
    if not val:
        return ""
    # If it looks like a float from pandas (e.g., "12345.0"), fix it
    try:
        num = float(val)
        if num == int(num):
            return str(int(num))
    except ValueError:
        pass
    return val
