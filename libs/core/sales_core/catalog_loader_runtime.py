from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple


@dataclass(frozen=True)
class CatalogLoaderDeps:
    catalog_csv_path: pathlib.Path
    logger: Any
    load_workbook: Any
    catalog_cache: MutableMapping[
        Tuple[Optional[int], Tuple[Tuple[str, float, int], ...]], List[Dict[str, Any]]
    ]
    tenant_dir: Callable[[int], pathlib.Path]
    load_tenant: Callable[[int], dict]
    persona_meta_config: Callable[[int | None], Dict[str, Any]]
    persona_catalog_csv: Callable[[int], Optional[pathlib.Path]]
    merge_csv_mapping_meta: Callable[[Mapping[str, Any] | None, Mapping[str, Any] | None], dict[str, Any]]
    normalize_catalog_items: Callable[[List[Dict[str, Any]], Dict[str, Any] | Any], List[Dict[str, Any]]]
    read_csv_rows_best: Callable[..., List[Dict[str, Any]]]
    apply_catalog_attribute_rules: Callable[[List[Dict[str, Any]], Mapping[str, Any] | None], None]
    enrich_catalog_color_aliases: Callable[[List[Dict[str, Any]], Mapping[str, Any] | None], None]
    normalize_text: Callable[[Any], str]
    collect_item_text: Callable[[Dict[str, Any]], str]
    persist_pdf_index_metadata: Callable[[int, str, str, Dict[str, Any]], None]
    format_items_for_prompt: Callable[[List[Dict[str, Any]], str], str]


class CatalogLoaderRuntime:
    def __init__(self, deps: CatalogLoaderDeps) -> None:
        self.deps = deps

    def read_catalog(self, tenant: int | None = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        candidates: List[tuple[pathlib.Path, Dict[str, Any]]] = []
        persona_meta: Dict[str, Any] = {}

        if tenant is not None:
            try:
                cfg = self.deps.load_tenant(tenant)
                persona_meta = self.deps.persona_meta_config(int(tenant))
                catalogs = cfg.get("catalogs") or []
                if isinstance(catalogs, list):
                    for entry in catalogs:
                        if not isinstance(entry, dict):
                            continue

                        def _resolve_path(raw: str | pathlib.Path | None) -> pathlib.Path | None:
                            if not raw:
                                return None
                            path = pathlib.Path(str(raw))
                            if not path.is_absolute():
                                path = self.deps.tenant_dir(tenant) / path
                            return path

                        raw_csv = entry.get("csv_path")
                        csv_path = _resolve_path(raw_csv)
                        if csv_path:
                            csv_meta = dict(entry)
                            csv_meta["type"] = "csv"
                            csv_meta["path"] = raw_csv
                            merged_csv_meta = self.deps.merge_csv_mapping_meta(csv_meta, persona_meta)
                            candidates.append((csv_path, merged_csv_meta))

                        raw_path = entry.get("path")
                        path = _resolve_path(raw_path)
                        if not path:
                            continue
                        merged_meta = self.deps.merge_csv_mapping_meta(entry, persona_meta)
                        candidates.append((path, merged_meta))
            except Exception:
                pass
            persona_csv_path = self.deps.persona_catalog_csv(int(tenant))
            if persona_csv_path:
                csv_delimiter = str(persona_meta.get("catalog_csv_delimiter") or "").strip()
                csv_encoding = (
                    str(persona_meta.get("catalog_csv_encoding") or "utf-8").strip() or "utf-8"
                )
                meta = self.deps.merge_csv_mapping_meta(
                    {
                        "type": "csv",
                        "delimiter": csv_delimiter or None,
                        "encoding": csv_encoding,
                    },
                    persona_meta,
                )
                candidate_tuple = (persona_csv_path, meta)
                if candidate_tuple not in candidates:
                    candidates.insert(0, candidate_tuple)

        if not candidates:
            default_meta = self.deps.merge_csv_mapping_meta(
                {"delimiter": ",", "encoding": "utf-8"},
                persona_meta,
            )
            candidates.append((self.deps.catalog_csv_path, default_meta))

        key_fps: List[Tuple[str, float, int]] = []
        try:
            for pth, meta in candidates:
                meta = meta if isinstance(meta, dict) else {}
                meta_type = (meta.get("type") or pth.suffix.lstrip(".")).lower()
                stat_target = pth
                if pth.suffix.lower() == ".pdf" or meta_type == "pdf":
                    idx_val = meta.get("index_path")
                    if idx_val and tenant is not None:
                        cand = pathlib.Path(str(idx_val))
                        if not cand.is_absolute():
                            cand = self.deps.tenant_dir(int(tenant)) / cand
                        if cand.exists():
                            stat_target = cand
                if stat_target.exists():
                    st = stat_target.stat()
                    key_fps.append(
                        (str(stat_target.resolve()), st.st_mtime, int(getattr(st, "st_size", 0) or 0))
                    )
        except Exception:
            key_fps = []
        cache_key: Tuple[Optional[int], Tuple[Tuple[str, float, int], ...]] = (
            (int(tenant) if tenant is not None else None),
            tuple(sorted(key_fps)),
        )
        cached = self.deps.catalog_cache.get(cache_key)
        if cached:
            return cached

        for path, meta in candidates:
            try:
                if not path.exists():
                    continue

                meta = meta if isinstance(meta, dict) else {}
                encoding = meta.get("encoding", "utf-8")
                meta_type = (meta.get("type") or path.suffix.lstrip(".")).lower()

                if path.suffix.lower() in {".xlsx", ".xls"} or meta_type == "excel":
                    if self.deps.load_workbook is None:
                        continue
                    wb = self.deps.load_workbook(filename=str(path), read_only=True, data_only=True)
                    ws = wb.active
                    headers = []
                    for cell in next(ws.iter_rows(min_row=1, max_row=1), []):
                        headers.append(str(cell.value or "").strip())
                    if not headers:
                        wb.close()
                        continue
                    collected: List[Dict[str, Any]] = []
                    for idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=1):
                        record = {}
                        for h, val in zip(headers, row):
                            record[str(h or "").strip()] = str(val).strip() if val is not None else ""
                        if any(record.values()):
                            collected.append(record)
                        if idx >= 500:
                            break
                    wb.close()
                    if collected:
                        items = self.deps.normalize_catalog_items(collected, meta)
                        if items:
                            break
                    continue

                if path.suffix.lower() == ".pdf" or meta_type == "pdf":
                    if not isinstance(meta, dict):
                        continue
                    raw_source_key = str(meta.get("path") or path)
                    index_path_value = meta.get("index_path")
                    index_path_obj: pathlib.Path | None = None

                    if index_path_value:
                        candidate = pathlib.Path(str(index_path_value))
                        if not candidate.is_absolute() and tenant is not None:
                            candidate = self.deps.tenant_dir(int(tenant)) / candidate
                        if candidate.exists():
                            index_path_obj = candidate

                    if index_path_obj is None and tenant is not None:
                        try:
                            from catalog_index import build_pdf_index

                            try:
                                rel_source = str(path.relative_to(self.deps.tenant_dir(int(tenant))))
                            except Exception:
                                rel_source = str(meta.get("path") or path.name)

                            index_dir = self.deps.tenant_dir(int(tenant)) / "indexes"
                            built_index = build_pdf_index(
                                path,
                                output_dir=index_dir,
                                source_relpath=rel_source,
                                original_name=path.name,
                            )

                            index_path_obj = built_index.index_path
                            try:
                                rel_index_path = str(
                                    index_path_obj.relative_to(self.deps.tenant_dir(int(tenant)))
                                )
                            except Exception:
                                rel_index_path = str(index_path_obj)

                            meta["index_path"] = rel_index_path
                            meta["indexed_at"] = built_index.generated_at
                            meta["chunk_count"] = built_index.chunk_count
                            meta["sha1"] = built_index.sha1

                            self.deps.persist_pdf_index_metadata(
                                int(tenant),
                                raw_source_key,
                                rel_index_path,
                                {
                                    "generated_at": built_index.generated_at,
                                    "chunk_count": built_index.chunk_count,
                                    "sha1": built_index.sha1,
                                    "page_count": built_index.page_count,
                                    "source_path": built_index.source_path,
                                },
                            )
                        except Exception:
                            continue

                    if not index_path_obj:
                        continue

                    try:
                        from catalog_index import index_to_catalog_items, load_index

                        index = load_index(index_path_obj)
                        indexed_items = index_to_catalog_items(index)
                        if indexed_items:
                            items = indexed_items
                            break
                    except Exception:
                        continue
                    continue

                delimiter = meta.get("delimiter")
                enc_candidates: List[str] = []
                if isinstance(encoding, str) and encoding:
                    enc_candidates.append(encoding)
                for fallback in ("utf-8", "utf-8-sig", "cp1251", "windows-1251", "koi8-r"):
                    if fallback not in enc_candidates:
                        enc_candidates.append(fallback)

                used_items: List[Dict[str, Any]] = []
                for enc in enc_candidates or ["utf-8"]:
                    try:
                        local_items = self.deps.read_csv_rows_best(
                            path,
                            encoding=enc,
                            delimiter=delimiter,
                            row_limit=500,
                        )
                        if local_items:
                            used_items = local_items
                            break
                    except UnicodeDecodeError:
                        continue
                if used_items:
                    items = self.deps.normalize_catalog_items(used_items, meta)
                    break
            except Exception:
                continue

        if not items:
            return []

        try:
            self.deps.apply_catalog_attribute_rules(items, persona_meta)
        except Exception:
            self.deps.logger.debug("catalog_attribute_rules_failed", exc_info=True)
        try:
            self.deps.enrich_catalog_color_aliases(items, persona_meta)
        except Exception:
            self.deps.logger.debug("catalog_color_alias_failed", exc_info=True)

        try:
            for item in items:
                if isinstance(item, dict):
                    item["_search_text"] = self.deps.normalize_text(self.deps.collect_item_text(item))
        except Exception:
            pass

        try:
            self.deps.catalog_cache[cache_key] = items
        except Exception:
            pass
        return items

    def read_all_catalog(
        self,
        cfg: Optional[Dict[str, Any]] = None,
        tenant: int | None = None,
    ) -> List[Dict[str, Any]]:
        tenant_id: Optional[int] = None
        if tenant is not None:
            try:
                tenant_id = int(tenant)
            except Exception:
                tenant_id = None
        elif isinstance(cfg, dict):
            passport = cfg.get("passport") if isinstance(cfg.get("passport"), dict) else {}
            raw_id = passport.get("tenant_id")
            try:
                tenant_id = int(raw_id) if raw_id is not None else None
            except Exception:
                tenant_id = None
        return self.read_catalog(tenant_id)

    def paginate_catalog_text(
        self,
        items: List[Dict[str, Any]],
        cfg: Optional[Dict[str, Any]] = None,
        page_size: int = 10,
    ) -> List[str]:
        if not items:
            return []

        try:
            page_size = int(page_size)
        except Exception:
            page_size = 10
        if page_size <= 0:
            page_size = 10

        currency = "₽"
        if isinstance(cfg, dict):
            passport = cfg.get("passport") if isinstance(cfg.get("passport"), dict) else {}
            cur = passport.get("currency")
            if cur:
                currency = str(cur)

        formatted_lines = self.deps.format_items_for_prompt(items, currency).splitlines()
        pages: List[str] = []
        for idx in range(0, len(formatted_lines), page_size):
            chunk = formatted_lines[idx : idx + page_size]
            if not chunk:
                continue
            page_no = idx // page_size + 1
            header = f"Каталог, страница {page_no}:"
            pages.append("\n".join([header, *chunk]))
        return pages
