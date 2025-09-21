#!/usr/bin/env python3
"""
PDF Converter Module
Handles conversion of various file formats to PDF using reportlab and LibreOffice
"""

import subprocess
import sys
import os
from pathlib import Path


def convert_txt_to_pdf_reportlab(input_path, output_path):
    """Convert TXT to PDF using Python reportlab library"""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont

        # Read text content
        with open(input_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Create PDF with better spacing
        doc = SimpleDocTemplate(output_path, pagesize=A4, topMargin=inch, bottomMargin=inch,
                              leftMargin=0.75*inch, rightMargin=0.75*inch)
        styles = getSampleStyleSheet()

        # Modify the Normal style for better Chinese text spacing
        styles['Normal'].fontSize = 12
        styles['Normal'].leading = 18  # Line spacing (1.5x font size)
        styles['Normal'].spaceAfter = 6  # Space after each paragraph
        styles['Normal'].wordWrap = 'CJK'  # Better word wrapping for Chinese

        # Try to register a Chinese font if available
        try:
            # Working Chinese font paths in order of preference
            chinese_fonts = [
                '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',
                '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',
                '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
                '/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc'
            ]

            for font_path in chinese_fonts:
                try:
                    if Path(font_path).exists():
                        # For TTC fonts, try without subfont index first
                        if font_path.endswith('.ttc'):
                            try:
                                pdfmetrics.registerFont(TTFont('Chinese', font_path))
                                styles['Normal'].fontName = 'Chinese'
                                break
                            except Exception:
                                # Try with subfont indices
                                for subfont_index in [0, 1, 2, 3]:
                                    try:
                                        pdfmetrics.registerFont(TTFont('Chinese', font_path, subfontIndex=subfont_index))
                                        styles['Normal'].fontName = 'Chinese'
                                        break
                                    except Exception:
                                        continue
                                else:
                                    continue  # Try next font if all subfonts failed
                                break  # Success, stop trying other fonts
                        else:
                            pdfmetrics.registerFont(TTFont('Chinese', font_path))
                            styles['Normal'].fontName = 'Chinese'
                            break
                except Exception:
                    continue
        except Exception:
            pass  # Use default font if no Chinese font available

        story = []

        # Split content into paragraphs
        paragraphs = content.split('\n')
        for para in paragraphs:
            if para.strip():
                try:
                    # Clean up text and create paragraph with better spacing
                    clean_para = para.strip()
                    # Add spaces between Chinese characters and punctuation for better readability
                    if any(ord(c) > 127 for c in clean_para):  # Contains non-ASCII (likely Chinese)
                        # Add subtle spacing adjustments for Chinese text
                        clean_para = clean_para.replace('。', '。 ')  # Space after period
                        clean_para = clean_para.replace('，', '， ')  # Space after comma
                        clean_para = clean_para.replace('；', '； ')  # Space after semicolon
                        clean_para = clean_para.replace('：', '： ')  # Space after colon

                    p = Paragraph(clean_para, styles['Normal'])
                    story.append(p)
                    story.append(Spacer(1, 8))  # Reduced spacer between paragraphs
                except Exception:
                    # If paragraph creation fails, create a simple text line
                    safe_text = ''.join(c for c in para.strip() if ord(c) < 65536)
                    if safe_text:
                        # Apply same spacing fixes to fallback text
                        if any(ord(c) > 127 for c in safe_text):
                            safe_text = safe_text.replace('。', '。 ')
                            safe_text = safe_text.replace('，', '， ')
                            safe_text = safe_text.replace('；', '； ')
                            safe_text = safe_text.replace('：', '： ')

                        p = Paragraph(safe_text, styles['Normal'])
                        story.append(p)
                        story.append(Spacer(1, 8))

        doc.build(story)
        return True

    except ImportError:
        return False
    except Exception:
        return False


def convert_file_to_pdf(input_path, output_path):
    """Main conversion function - converts TXT and PDF files only"""
    try:
        if Path(input_path).suffix.lower() == ".pdf":
            import shutil
            shutil.copy(input_path, output_path)
            return True

        # For TXT files, use Python-based conversion
        if Path(input_path).suffix.lower() == ".txt":
            return convert_txt_to_pdf_reportlab(input_path, output_path)

        # For other file types, return False (not supported)
        return False

    except Exception:
        return False
    finally:
        # Clean up temp file
        if Path(input_path).exists() and "temp_" in Path(input_path).name:
            try:
                Path(input_path).unlink()
            except:
                pass


if __name__ == "__main__":
    """Command line interface for testing"""
    if len(sys.argv) != 3:
        print("Usage: python pdf_converter.py <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    if not os.path.exists(input_file):
        print(f"Error: Input file does not exist: {input_file}")
        sys.exit(1)

    success = convert_file_to_pdf(input_file, output_file)

    if success:
        print(f"[DONE] Converted {Path(input_file).name}")
    else:
        print(f"[FAILED] Failed to convert {Path(input_file).name}")