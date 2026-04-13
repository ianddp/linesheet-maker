{ pkgs }:

{
  deps = [
    pkgs.python311
    pkgs.python311Packages.pip
    pkgs.tesseract         # OCR engine for image-based PDF pages
    pkgs.libwebp           # WebP image support
    pkgs.libjpeg           # JPEG support for Pillow
    pkgs.libpng            # PNG support for Pillow
    pkgs.zlib              # Compression
    pkgs.freetype          # Font rendering
    pkgs.lcms2             # Color management for Pillow
    pkgs.openjpeg          # JPEG 2000 support
    pkgs.libtiff           # TIFF support
  ];
}
