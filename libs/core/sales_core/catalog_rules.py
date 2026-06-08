from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Sequence


@dataclass(frozen=True)
class CatalogRulesDeps:
    match_key: Callable[[Any], str]


class CatalogRulesRuntime:
    def __init__(self, deps: CatalogRulesDeps) -> None:
        self.deps = deps

    def apply_catalog_attribute_rules(
        self,
        items: List[Dict[str, Any]],
        persona_meta: Mapping[str, Any] | None,
    ) -> None:
        if not items or not isinstance(persona_meta, Mapping):
            return
        rules_section = persona_meta.get("catalog_tags") or persona_meta.get("catalog_attributes")
        if isinstance(rules_section, Mapping):
            candidates = rules_section.get("tag_rules") or rules_section.get("rules") or []
        else:
            candidates = rules_section or []
        if not isinstance(candidates, Sequence):
            return
        for item in items:
            for rule in candidates:
                if not isinstance(rule, Mapping):
                    continue
                if not self.catalog_rule_matches(rule, item):
                    continue
                name = str(rule.get("name") or "").strip() or None
                tags_to_add = []
                rule_tags = rule.get("tags")
                if isinstance(rule_tags, str):
                    tags_to_add = [rule_tags]
                elif isinstance(rule_tags, Sequence):
                    tags_to_add = [str(tag) for tag in rule_tags if tag]
                elif name:
                    tags_to_add = [name]
                elif rule.get("name"):
                    tags_to_add = [str(rule["name"])]
                if not tags_to_add and name:
                    tags_to_add = [name]
                if tags_to_add:
                    bucket = item.setdefault("tags", [])
                    if isinstance(bucket, list):
                        for tag in tags_to_add:
                            if tag not in bucket:
                                bucket.append(tag)
                set_fields = rule.get("set") or {}
                if isinstance(set_fields, Mapping):
                    for field, value in set_fields.items():
                        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                            bucket = item.setdefault(str(field), [])
                            if isinstance(bucket, list):
                                for val in value:
                                    if val not in bucket:
                                        bucket.append(val)
                        else:
                            current = item.get(field)
                            if not current:
                                item[field] = value

    def catalog_rule_matches(self, rule: Mapping[str, Any], item: Mapping[str, Any]) -> bool:
        if not item:
            return False
        any_rules = rule.get("any")
        all_rules = rule.get("all")
        matched = True
        if isinstance(any_rules, Sequence) and any_rules:
            matched = any(
                self.catalog_condition_matches(item, cond)
                for cond in any_rules
                if isinstance(cond, Mapping)
            )
        if matched and isinstance(all_rules, Sequence) and all_rules:
            matched = all(
                self.catalog_condition_matches(item, cond)
                for cond in all_rules
                if isinstance(cond, Mapping)
            )
        return bool(matched)

    def catalog_condition_matches(
        self,
        item: Mapping[str, Any],
        condition: Mapping[str, Any],
    ) -> bool:
        field = str(condition.get("field") or "").strip()
        values: List[str] = []
        if field:
            raw_value = item.get(field)
            if isinstance(raw_value, (list, tuple, set)):
                values = [str(val or "") for val in raw_value if val]
            else:
                values = [str(raw_value or "")]
        else:
            values = [" ".join(str(val or "") for val in item.values())]
        lowered = [value.casefold() for value in values if value]
        keyed = [self.deps.match_key(value) for value in values if value]
        contains = condition.get("contains")
        if contains:
            needles = contains
            if isinstance(needles, str):
                needles = [needles]
            if isinstance(needles, Sequence):
                normalized_needles = [
                    str(needle or "").strip().casefold() for needle in needles if needle
                ]
                keyed_needles = [self.deps.match_key(needle) for needle in needles if needle]
                for hay in lowered:
                    if any(needle in hay for needle in normalized_needles):
                        return True
                for hay in keyed:
                    if any(needle and needle in hay for needle in keyed_needles):
                        return True
        regex = condition.get("regex")
        if regex:
            patterns = (
                regex
                if isinstance(regex, Sequence) and not isinstance(regex, (str, bytes))
                else [regex]
            )
            for pattern in patterns:
                try:
                    compiled = re.compile(str(pattern), re.IGNORECASE)
                except re.error:
                    continue
                for value in values:
                    if compiled.search(value):
                        return True
        equals = condition.get("equals")
        if equals is not None:
            eq_values = (
                equals
                if isinstance(equals, Sequence) and not isinstance(equals, (str, bytes))
                else [equals]
            )
            normalized = [str(val or "").strip().casefold() for val in eq_values]
            keyed_equals = [self.deps.match_key(val) for val in eq_values]
            for hay in lowered:
                if hay in normalized:
                    return True
            for hay in keyed:
                if hay and hay in keyed_equals:
                    return True
        return False

    def filter_catalog_items_by_rules(
        self,
        items: List[Dict[str, Any]],
        needs: Mapping[str, Any],
        persona_meta: Mapping[str, Any] | None,
    ) -> List[Dict[str, Any]]:
        if not items or not isinstance(persona_meta, Mapping):
            return items
        rules = persona_meta.get("sales_rules")
        if not isinstance(rules, Sequence):
            return items
        filtered = list(items)
        for raw_rule in rules:
            if not isinstance(raw_rule, Mapping):
                continue
            rule_needs = raw_rule.get("needs")
            if rule_needs and not self.needs_block_matches(rule_needs, needs):
                continue
            require_tags = self.ensure_list(str, raw_rule.get("require_tags"))
            forbid_tags = self.ensure_list(str, raw_rule.get("forbid_tags"))
            require_fields = raw_rule.get("require_fields")
            if not require_tags and not forbid_tags and not require_fields:
                continue
            current: List[Dict[str, Any]] = []
            for item in filtered:
                tags = {str(tag) for tag in (item.get("tags") or []) if tag}
                if require_tags and not tags.issuperset(require_tags):
                    continue
                if forbid_tags and tags.intersection(forbid_tags):
                    continue
                if require_fields and not self.item_fields_match(item, require_fields):
                    continue
                current.append(item)
            if current:
                filtered = current
            else:
                filtered = []
                break
        return filtered

    @staticmethod
    def ensure_list(caster, value: Any) -> set[str]:
        if value is None:
            return set()
        if isinstance(value, (list, tuple, set)):
            values = value
        else:
            values = [value]
        normalized = set()
        for val in values:
            try:
                converted = caster(val)
            except Exception:
                continue
            clean = str(converted).strip()
            if clean:
                normalized.add(clean)
        return normalized

    @staticmethod
    def needs_block_matches(rule_needs: Mapping[str, Any], actual: Mapping[str, Any]) -> bool:
        if not isinstance(rule_needs, Mapping):
            return True
        for key, expected in rule_needs.items():
            if expected is None:
                continue
            actual_value = actual.get(key)
            if isinstance(expected, (list, tuple, set)):
                normalized = {str(val).casefold() for val in expected}
                if str(actual_value).casefold() not in normalized:
                    return False
            else:
                if str(actual_value).casefold() != str(expected).casefold():
                    return False
        return True

    @staticmethod
    def item_fields_match(item: Mapping[str, Any], requirements: Mapping[str, Any]) -> bool:
        if not isinstance(requirements, Mapping):
            return True
        for field_name, expected in requirements.items():
            value = item.get(field_name)
            if isinstance(expected, (list, tuple, set)):
                norm_expected = {str(val).casefold() for val in expected}
                if str(value).casefold() not in norm_expected:
                    return False
            else:
                if str(value).casefold() != str(expected).casefold():
                    return False
        return True
