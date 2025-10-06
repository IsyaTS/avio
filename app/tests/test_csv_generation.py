import importlib
import io
import os
import csv
import sys
import pathlib

# Ensure project root is importable as a package root (so `app` resolves)
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))


def test_write_catalog_csv_semicolon_bom_and_sanitization(tmp_path, monkeypatch):
    # Point tenants dir to a temp location before importing core/io
    tenants_dir = tmp_path / "tenants"
    monkeypatch.setenv("TENANTS_DIR", str(tenants_dir))

    # Import after env is set
    from app import core as core_module
    importlib.reload(core_module)

    # Alias 'core' for modules that import it as a top-level name
    import sys as _sys
    _sys.modules.setdefault("core", core_module)

    # Load pipeline and io under a lightweight synthetic 'catalog' package
    import types as _types
    catalog_dir = pathlib.Path(__file__).resolve().parents[1] / "catalog"
    pkg = _types.ModuleType("catalog")
    pkg.__path__ = [str(catalog_dir)]  # type: ignore[attr-defined]
    _sys.modules.setdefault("catalog", pkg)

    pipeline_spec = importlib.util.spec_from_file_location("catalog.pipeline", str(catalog_dir / "pipeline.py"))
    pipeline_mod = importlib.util.module_from_spec(pipeline_spec)  # type: ignore[arg-type]
    assert pipeline_spec and pipeline_spec.loader
    _sys.modules["catalog.pipeline"] = pipeline_mod
    pipeline_spec.loader.exec_module(pipeline_mod)  # type: ignore[assignment]

    io_spec = importlib.util.spec_from_file_location("catalog.io", str(catalog_dir / "io.py"))
    io_mod = importlib.util.module_from_spec(io_spec)  # type: ignore[arg-type]
    assert io_spec and io_spec.loader
    _sys.modules["catalog.io"] = io_mod
    io_spec.loader.exec_module(io_mod)  # type: ignore[assignment]
    write_catalog_csv = io_mod.write_catalog_csv

    tenant = 1
    core_module.ensure_tenant_files(tenant)

    rows = [
        {
            "title": "Стальная полка; модель A\nXL\t",
            "price": "25 000 ₽",
            "color": "  графит  ",
            "desc": "многострочная\nстрока\r\nзначения\tс табами",
        },
        {
            "title": "Простая полка",
            "price": "9900",
            "color": "черный",
            "desc": "однострочное значение",
        },
    ]

    rel_path, header = write_catalog_csv(tenant, rows, base_name="catalog")

    # Path correctness
    csv_path = tenants_dir / str(tenant) / rel_path
    assert csv_path.exists()

    # BOM check and delimiter check
    raw = csv_path.read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")  # UTF-8 BOM

    text = raw.decode("utf-8-sig")
    # Parse using semicolon delimiter
    reader = csv.reader(io.StringIO(text), delimiter=";")
    parsed = list(reader)
    assert parsed, "CSV should not be empty"

    # Header assertion: contains id, title, price and our attributes
    header_row = parsed[0]
    assert header_row[:3] == ["id", "title", "price"]
    assert "color" in header_row and "desc" in header_row

    # Row sanitization: newlines/tabs collapsed to spaces; semicolon in title is preserved via quoting
    first = parsed[1]
    # Build mapping from header to cell for easy checks
    row_map = {h: first[i] if i < len(first) else "" for i, h in enumerate(header_row)}
    assert ";" in row_map["title"]  # original semicolon should survive and be parsed
    assert "\n" not in row_map["title"] and "\t" not in row_map["title"]
    assert "  " not in row_map["title"], "collapsed spaces expected"
    assert row_map["price"] in {"25000", "25000.0", "25000.00", "25000.000"}  # normalized by pipeline
    assert "\n" not in row_map["desc"] and "\t" not in row_map["desc"]
    assert "  " not in row_map["desc"], "collapsed spaces expected"
