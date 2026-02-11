from pathlib import Path
from zipfile import ZipFile

from services.doc_extract import detect_images_in_docx, extract_text


def _write_minimal_docx(path: Path) -> None:
    content_types = """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Default Extension="png" ContentType="image/png"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>
"""
    rels = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"></Relationships>
"""
    document_xml = """<?xml version="1.0" encoding="UTF-8"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p><w:r><w:t>FIRE BINDER</w:t></w:r></w:p>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>Item</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>Qty</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>Onions</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>50#</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
  </w:body>
</w:document>
"""
    with ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", rels)
        archive.writestr("word/document.xml", document_xml)
        archive.writestr("word/media/image1.png", b"\x89PNG\r\n\x1a\n")


def test_extract_text_docx_includes_tables_and_metrics(tmp_path: Path):
    docx_path = tmp_path / "sample.docx"
    _write_minimal_docx(docx_path)

    text, metrics = extract_text(str(docx_path))

    assert "FIRE BINDER" in text
    assert "| Onions | 50# |" in text
    assert metrics["docx_paragraph_count"] >= 1
    assert metrics["docx_table_count"] == 1
    assert metrics["docx_table_cell_count"] == 4
    assert metrics["extracted_from_tables_chars"] > 0
    assert metrics["embedded_image_count"] == 1


def test_detect_images_in_docx(tmp_path: Path):
    docx_path = tmp_path / "sample2.docx"
    _write_minimal_docx(docx_path)
    assert detect_images_in_docx(str(docx_path)) == 1
