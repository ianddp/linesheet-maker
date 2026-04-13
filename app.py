"""
Line Sheet Maker — Streamlit App
Ingest products from websites, PDFs, or Excel/CSV files and export polished line sheets.
"""
from __future__ import annotations

import os
import re
import sys
import io
import json
import time
from datetime import datetime
from pathlib import Path
from dataclasses import asdict

import streamlit as st
import pandas as pd
from PIL import Image

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from utils.schema import Product, EXPORT_COLUMNS, INTERNAL_FIELDS, auto_map_columns
from utils.images import download_image, resize_for_thumbnail, CACHE_DIR
from parsers.website import (
    scrape_single_product, scrape_collection_page, scrape_website, scrape_url,
    find_product_links, find_collection_links,
)
from parsers.pdf_parser import parse_pdf, check_dependencies as check_pdf_deps
from parsers.excel_parser import parse_excel, apply_mapping
from parsers.normalizer import normalize_products, deduplicate_products, filter_non_products
from export.excel_export import export_linesheet

# ─────────────────────────── Page Config ───────────────────────────

st.set_page_config(
    page_title="Dime Deployment",
    page_icon="favicon.png",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────── Custom CSS ───────────────────────────

st.markdown("""
<style>
    /* Global — match Dime Portal dark navy */
    .stApp { background-color: #0F1219; }

    /* Quote banner */
    .quote-banner {
        text-align: center;
        padding: 1.5rem 2rem;
        margin-bottom: 0.5rem;
    }
    .quote-banner em {
        color: #D4A843;
        font-size: 1.35rem;
        line-height: 1.7;
        font-weight: 500;
        letter-spacing: 0.01em;
    }

    /* Header area — Dime blue gradient */
    .main-header {
        background: linear-gradient(135deg, #1A1F2E 0%, #1E3A5F 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        border: 1px solid #2A3448;
    }
    .main-header h1 {
        color: #E2E8F0;
        font-size: 2rem;
        margin: 0;
    }
    .main-header p {
        color: #94A3B8;
        margin: 0.3rem 0 0 0;
    }

    /* Upload zone — blue accent */
    .upload-zone {
        border: 2px dashed #3B82F6;
        border-radius: 12px;
        padding: 2rem;
        text-align: center;
        background: #1A1F2E;
        transition: border-color 0.2s;
    }
    .upload-zone:hover { border-color: #60A5FA; }

    /* Stats bar */
    .stats-bar {
        display: flex;
        gap: 1.5rem;
        padding: 0.75rem 1rem;
        background: #1A1F2E;
        border-radius: 8px;
        margin-bottom: 1rem;
        border: 1px solid #2A3448;
    }
    .stat-item {
        color: #94A3B8;
        font-size: 0.9rem;
    }
    .stat-value {
        color: #3B82F6;
        font-weight: bold;
        font-size: 1.1rem;
    }

    /* Product grid thumbnail */
    .product-thumb {
        width: 80px;
        height: 80px;
        object-fit: contain;
        border-radius: 6px;
        background: #1A1F2E;
    }

    /* Action buttons */
    .stButton > button {
        border-radius: 8px;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 0.5rem 1.5rem;
    }

    /* Hide Streamlit branding */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }

    /* Data editor improvements */
    [data-testid="stDataEditor"] {
        border-radius: 8px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────── Session State ───────────────────────────

if "products" not in st.session_state:
    st.session_state.products = []
if "brand_name" not in st.session_state:
    st.session_state.brand_name = ""
if "sheet_title" not in st.session_state:
    st.session_state.sheet_title = "Line Sheet"
if "sheet_date" not in st.session_state:
    st.session_state.sheet_date = datetime.now()
if "pending_mapping" not in st.session_state:
    st.session_state.pending_mapping = None
if "pending_df" not in st.session_state:
    st.session_state.pending_df = None
if "pending_filename" not in st.session_state:
    st.session_state.pending_filename = None


def add_products(new_products: list[Product]):
    """Add products to session state, filtering, normalizing, and deduplicating."""
    combined = st.session_state.products + new_products
    combined = normalize_products(combined)
    combined = filter_non_products(combined)
    combined = deduplicate_products(combined)
    st.session_state.products = combined


def products_to_df() -> pd.DataFrame:
    """Convert current products to a display DataFrame."""
    if not st.session_state.products:
        return pd.DataFrame(columns=EXPORT_COLUMNS)

    rows = []
    for p in st.session_state.products:
        rows.append({
            "Product Image": p.product_image,
            "SKU / UPC": p.sku_upc,
            "Product Name": p.product_name,
            "Color": p.color,
            "MSRP": p.msrp,
            "Qty": p.qty,
            "Your Cost": p.your_cost,
            "Notes": p.notes,
        })
    return pd.DataFrame(rows, columns=EXPORT_COLUMNS)


def df_to_products(df: pd.DataFrame) -> list[Product]:
    """Convert an edited DataFrame back to Product objects."""
    products = []
    for _, row in df.iterrows():
        products.append(Product(
            product_image=str(row.get("Product Image", "") or ""),
            sku_upc=str(row.get("SKU / UPC", "") or ""),
            product_name=str(row.get("Product Name", "") or ""),
            color=str(row.get("Color", "") or ""),
            msrp=str(row.get("MSRP", "") or ""),
            qty=str(row.get("Qty", "") or ""),
            your_cost=str(row.get("Your Cost", "") or ""),
            notes=str(row.get("Notes", "") or ""),
            _source="manual",
        ))
    return products


# ─────────────────────────── Header ───────────────────────────

st.markdown("""
<div class="quote-banner">
    <em>"In this industry, work is like standing on stage at the biggest concert you've ever seen and pulling your pants down. Then doing it again. And again. Until you win."<br>— Rene Servin, Dime Co-Founder</em>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1>Dime Deployment</h1>
    <p>Import products from websites, PDFs, or spreadsheets — export polished line sheets</p>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────── Top Fields ───────────────────────────

top_col1, top_col2, top_col3 = st.columns([2, 2, 1])

with top_col1:
    st.session_state.brand_name = st.text_input(
        "Brand / Sheet Name",
        value=st.session_state.brand_name,
        placeholder="e.g., Nike Fall 2026"
    )

with top_col2:
    st.session_state.sheet_title = st.text_input(
        "Title",
        value=st.session_state.sheet_title,
        placeholder="Line Sheet"
    )

with top_col3:
    st.session_state.sheet_date = st.date_input(
        "Date",
        value=st.session_state.sheet_date,
    )

st.markdown("---")

# ─────────────────────────── Import Section ───────────────────────────

tab_web, tab_pdf, tab_excel = st.tabs(["🌐  Website", "📄  PDF Catalog", "📊  Excel / CSV"])

# ── Website Tab ──
with tab_web:
    web_col1, web_col2 = st.columns([3, 1])

    with web_col1:
        url_input = st.text_area(
            "Enter URLs (one per line)",
            placeholder="https://brand.com/collections/fall-2026\nhttps://brand.com/products/shoe-name",
            height=100,
        )

    with web_col2:
        scan_mode = st.radio(
            "Scan mode",
            ["Single page(s)", "Collection page", "Whole site"],
            help="Single: scrape each URL as-is. Collection: find and follow product links. Whole site: discover collections and products."
        )
        scan_button = st.button("🔍 Scan Website", type="primary", use_container_width=True)

    if scan_button and url_input.strip():
        # Auto-clear previous products on new scan
        st.session_state.products = []
        urls = [u.strip() for u in url_input.strip().split("\n") if u.strip()]
        all_new = []
        progress_bar = st.progress(0, text="Starting scan...")

        for i, url in enumerate(urls):
            progress_bar.progress(
                (i) / len(urls),
                text=f"Scanning {url}..."
            )

            try:
                if scan_mode == "Single page(s)":
                    products = scrape_single_product(url)
                elif scan_mode == "Collection page":
                    def prog_cb(current, total, link):
                        progress_bar.progress(
                            (i + current / total) / len(urls),
                            text=f"Product {current}/{total}: {link[:60]}..."
                        )
                    products = scrape_collection_page(url, progress_callback=prog_cb)
                else:  # Whole site
                    def site_prog_cb(msg):
                        progress_bar.progress(
                            (i + 0.5) / len(urls),
                            text=str(msg)[:80]
                        )
                    products = scrape_website(url, progress_callback=site_prog_cb)

                all_new.extend(products)
            except Exception as e:
                st.error(f"Error scanning {url}: {e}")

        progress_bar.progress(1.0, text="Done!")

        if all_new:
            add_products(all_new)
            st.success(f"Found {len(all_new)} product(s) from {len(urls)} URL(s).")
        else:
            st.warning("No products found. The site may use JavaScript rendering or have an unusual structure. Try a direct product URL.")

# ── PDF Tab ──
with tab_pdf:
    st.markdown('<div class="upload-zone">', unsafe_allow_html=True)
    pdf_files = st.file_uploader(
        "Drop PDF catalogs here",
        type=["pdf"],
        accept_multiple_files=True,
        key="pdf_upload",
    )
    st.markdown('</div>', unsafe_allow_html=True)

    if pdf_files:
        missing = check_pdf_deps()
        if missing:
            st.warning(f"Missing dependencies: {', '.join(missing)}. OCR may not work.")

        if st.button("📄 Parse PDFs", type="primary"):
            # Auto-clear previous products on new import
            st.session_state.products = []
            for pdf_file in pdf_files:
                with st.spinner(f"Parsing {pdf_file.name}..."):
                    progress_bar = st.progress(0, text=f"Reading {pdf_file.name}...")

                    def pdf_prog(current, total):
                        progress_bar.progress(
                            current / total,
                            text=f"Page {current}/{total} of {pdf_file.name}"
                        )

                    try:
                        products = parse_pdf(
                            pdf_file.read(),
                            filename=pdf_file.name,
                            progress_callback=pdf_prog,
                        )
                        progress_bar.progress(1.0, text="Done!")

                        if products:
                            add_products(products)
                            st.success(f"Extracted {len(products)} product(s) from {pdf_file.name}.")
                        else:
                            st.warning(f"No products found in {pdf_file.name}. You may need to add entries manually.")
                    except Exception as e:
                        st.error(f"Error parsing {pdf_file.name}: {e}")

# ── Excel/CSV Tab ──
with tab_excel:
    st.markdown('<div class="upload-zone">', unsafe_allow_html=True)
    excel_files = st.file_uploader(
        "Drop Excel or CSV files here",
        type=["xlsx", "xls", "csv"],
        accept_multiple_files=True,
        key="excel_upload",
    )
    st.markdown('</div>', unsafe_allow_html=True)

    if excel_files:
        for excel_file in excel_files:
            try:
                df, suggested_mapping = parse_excel(
                    excel_file.read(),
                    excel_file.name,
                )

                st.markdown(f"**{excel_file.name}** — {len(df)} rows, {len(df.columns)} columns")

                # Show preview
                with st.expander("Preview raw data", expanded=False):
                    st.dataframe(df.head(10), use_container_width=True)

                # Column mapping UI
                st.markdown("**Column Mapping** — map your columns to line sheet fields")

                mapping = {}
                target_options = ["(skip)"] + INTERNAL_FIELDS

                # Display mapping in a grid
                map_cols = st.columns(min(4, len(df.columns)))
                for i, src_col in enumerate(df.columns):
                    with map_cols[i % len(map_cols)]:
                        default_idx = 0
                        if src_col in suggested_mapping:
                            target = suggested_mapping[src_col]
                            if target in target_options:
                                default_idx = target_options.index(target)

                        selected = st.selectbox(
                            src_col,
                            options=target_options,
                            index=default_idx,
                            key=f"map_{excel_file.name}_{src_col}",
                        )
                        if selected != "(skip)":
                            mapping[src_col] = selected

                if st.button(f"✅ Import {excel_file.name}", type="primary", key=f"import_{excel_file.name}"):
                    if not mapping:
                        st.error("Please map at least one column.")
                    else:
                        # Auto-clear previous products on new import
                        st.session_state.products = []
                        products = apply_mapping(df, mapping)
                        if products:
                            # Download images for any products that have URLs or Amazon image IDs
                            imgs_found = 0
                            with st.spinner("Fetching images..."):
                                for p in products:
                                    if not p.product_image:
                                        continue
                                    img_val = p.product_image.strip()
                                    if img_val.startswith(("http://", "https://")):
                                        local = download_image(img_val)
                                        if local:
                                            p._image_local_path = local
                                            imgs_found += 1
                                    elif re.match(r'^[A-Za-z0-9+\-_]{5,}\.(jpg|jpeg|png|webp)$', img_val):
                                        # Amazon-style image filename — try Amazon CDN
                                        amazon_url = f"https://m.media-amazon.com/images/I/{img_val}"
                                        local = download_image(amazon_url)
                                        if local:
                                            p._image_local_path = local
                                            p.product_image = amazon_url
                                            imgs_found += 1
                            add_products(products)
                            msg = f"Imported {len(products)} product(s) from {excel_file.name}."
                            if imgs_found:
                                msg += f" Downloaded {imgs_found} image(s)."
                            st.success(msg)
                        else:
                            st.warning("No valid product rows found with the selected mapping.")

            except Exception as e:
                st.error(f"Error reading {excel_file.name}: {e}")

# ─────────────────────────── Product Grid ───────────────────────────

st.markdown("---")

# Stats bar
total = len(st.session_state.products)
populated = sum(1 for p in st.session_state.products if p.product_name or p.sku_upc)
with_images = sum(1 for p in st.session_state.products if p.product_image)

st.markdown(f"""
<div class="stats-bar">
    <div class="stat-item">Total Rows: <span class="stat-value">{total}</span></div>
    <div class="stat-item">Populated: <span class="stat-value">{populated}</span></div>
    <div class="stat-item">With Images: <span class="stat-value">{with_images}</span></div>
</div>
""", unsafe_allow_html=True)

# Action buttons row
act_col1, act_col2, act_col3, act_col4, act_col5 = st.columns(5)

with act_col1:
    if st.button("➕ Add Row", use_container_width=True):
        st.session_state.products.append(Product(_source="manual"))
        st.rerun()

with act_col2:
    if st.button("📋 Duplicate Last", use_container_width=True):
        if st.session_state.products:
            from copy import deepcopy
            last = deepcopy(st.session_state.products[-1])
            st.session_state.products.append(last)
            st.rerun()

with act_col3:
    if st.button("🗑️ Delete Last", use_container_width=True):
        if st.session_state.products:
            st.session_state.products.pop()
            st.rerun()

with act_col4:
    if st.button("🔄 Clean & Dedupe", use_container_width=True):
        st.session_state.products = deduplicate_products(
            filter_non_products(
                normalize_products(st.session_state.products)
            )
        )
        st.rerun()

with act_col5:
    if st.button("🧹 Clear All", use_container_width=True, type="secondary"):
        st.session_state.products = []
        st.rerun()

# ── Image preview row ──
if st.session_state.products and any(p.product_image for p in st.session_state.products):
    with st.expander("🖼️ Image Preview", expanded=False):
        img_cols = st.columns(min(6, max(1, len(st.session_state.products))))
        for i, p in enumerate(st.session_state.products[:12]):
            if p.product_image:
                with img_cols[i % len(img_cols)]:
                    img_path = p._image_local_path or p.product_image
                    if img_path.startswith(("http://", "https://")):
                        st.image(img_path, caption=p.product_name[:20] or f"Row {i+1}", width=120)
                    elif os.path.exists(img_path):
                        st.image(img_path, caption=p.product_name[:20] or f"Row {i+1}", width=120)
                    else:
                        st.caption(f"Row {i+1}: image not found")

# ── Editable data table ──
st.markdown("### Product Data")

if st.session_state.products:
    df = products_to_df()

    edited_df = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "Product Image": st.column_config.TextColumn(
                "Product Image",
                help="Image URL or file path",
                width="medium",
            ),
            "SKU / UPC": st.column_config.TextColumn(
                "SKU / UPC",
                width="small",
            ),
            "Product Name": st.column_config.TextColumn(
                "Product Name",
                width="large",
            ),
            "Color": st.column_config.TextColumn(
                "Color",
                width="small",
            ),
            "MSRP": st.column_config.TextColumn(
                "MSRP",
                width="small",
            ),
            "Qty": st.column_config.TextColumn(
                "Qty",
                width="small",
            ),
            "Your Cost": st.column_config.TextColumn(
                "Your Cost",
                width="small",
            ),
            "Notes": st.column_config.TextColumn(
                "Notes",
                width="medium",
            ),
        },
        key="product_editor",
    )

    # Sync edits back to session state
    if edited_df is not None:
        st.session_state.products = df_to_products(edited_df)

    # Source traceability
    with st.expander("📍 Source Traceability", expanded=False):
        trace_data = []
        for i, p in enumerate(st.session_state.products):
            trace_data.append({
                "Row": i + 1,
                "Product": p.product_name[:40],
                "Source": p._source,
                "Detail": p._source_detail,
                "Confidence": f"{p._confidence:.0%}",
            })
        if trace_data:
            st.dataframe(pd.DataFrame(trace_data), use_container_width=True)

else:
    st.info("No products yet. Use the tabs above to import from a website, PDF, or spreadsheet — or click 'Add Row' to start manually.")

# ─────────────────────────── Export Section ───────────────────────────

st.markdown("---")
st.markdown("### Export")

if st.session_state.products:
    exp_col1, exp_col2 = st.columns([1, 3])

    with exp_col1:
        group_export = st.checkbox(
            "Group by category",
            value=False,
            help="Create separate tabs for each product category/collection"
        )
        embed_images = st.checkbox("Embed images in Excel", value=True)

    with exp_col2:
        if st.button("📥 Export Line Sheet", type="primary", use_container_width=True):
            with st.spinner("Generating Excel line sheet..."):
                # Download images if embedding
                if embed_images:
                    progress = st.progress(0, text="Downloading images...")
                    total_products = len(st.session_state.products)
                    for i, p in enumerate(st.session_state.products):
                        if p.product_image and p.product_image.startswith(("http://", "https://")):
                            local_path = download_image(p.product_image)
                            if local_path:
                                p._image_local_path = local_path
                        progress.progress((i + 1) / total_products, text=f"Downloading images... {i+1}/{total_products}")
                    progress.progress(1.0, text="Images ready!")

                try:
                    excel_bytes = export_linesheet(
                        products=st.session_state.products,
                        brand_name=st.session_state.brand_name,
                        title=st.session_state.sheet_title,
                        date_str=st.session_state.sheet_date.strftime("%B %d, %Y"),
                        group_by_category=group_export,
                    )

                    # Generate filename
                    safe_brand = st.session_state.brand_name.replace(" ", "_").replace("/", "-") or "linesheet"
                    filename = f"{safe_brand}_{datetime.now().strftime('%Y%m%d')}.xlsx"

                    st.download_button(
                        label="⬇️ Download Excel File",
                        data=excel_bytes,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary",
                        use_container_width=True,
                    )

                    # Also save to output directory
                    output_dir = Path(__file__).parent / "output"
                    output_dir.mkdir(exist_ok=True)
                    output_path = output_dir / filename
                    output_path.write_bytes(excel_bytes)
                    st.success(f"Line sheet exported! Also saved to: {output_path}")

                except Exception as e:
                    st.error(f"Export error: {e}")
                    import traceback
                    st.code(traceback.format_exc())

else:
    st.info("Import products first, then export your line sheet here.")

# ─────────────────────────── Load Sample Data ───────────────────────────

st.markdown("---")
with st.expander("🧪 Load Sample Data (for testing)"):
    st.markdown("Load the included sample CSV to test the app without a real data source.")
    if st.button("Load Sample Products"):
        sample_path = Path(__file__).parent / "sample_data" / "sample_products.csv"
        if sample_path.exists():
            try:
                df = pd.read_csv(sample_path)
                mapping = auto_map_columns(list(df.columns))
                products = apply_mapping(df, mapping)
                add_products(products)
                st.session_state.brand_name = "Nike Fall 2026"
                st.success(f"Loaded {len(products)} sample products!")
                st.rerun()
            except Exception as e:
                st.error(f"Error loading sample: {e}")
        else:
            st.error("Sample file not found.")
