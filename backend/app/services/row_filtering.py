"""Row-level search, filter, and sort predicates.

Shared by FileParser (flat-row views) and UnitViewService (unit-grouped views)
so both apply identical semantics to the same row shape.
"""

import re
from functools import cmp_to_key
from typing import Any, Dict, List, Optional, Sequence


def apply_search(
    rows: List[Dict],
    search: str,
    extra_top_level_fields: Optional[Sequence[str]] = None,
) -> List[Dict]:
    """Filter rows by a global search term (case-insensitive substring).

    Always checks ``row_name`` and every value in ``data``. Additional top-level
    field names (e.g. ``_unit_name``, ``_source_document``) are checked when
    supplied — lets unit-grouped callers search metadata that flat-row callers
    don't expose.
    """
    search_lower = search.lower()
    extras = tuple(extra_top_level_fields or ())
    return [row for row in rows if _row_matches_search(row, search_lower, extras)]


def _row_matches_search(row: Dict, search_lower: str, extras: tuple) -> bool:
    if row.get('row_name') and search_lower in str(row['row_name']).lower():
        return True
    for field in extras:
        value = row.get(field)
        if value and search_lower in str(value).lower():
            return True
    data = row.get('data', row)
    for value in data.values():
        if _value_contains_search(value, search_lower):
            return True
    return False


def _value_contains_search(value: Any, search_lower: str) -> bool:
    if value is None:
        return False
    if isinstance(value, dict) and 'answer' in value:
        value = value['answer']
    return search_lower in str(value).lower()


def apply_filters(rows: List[Dict], filters: List[Dict]) -> List[Dict]:
    """Apply filter rules to rows (AND logic across rules)."""
    for filter_rule in filters:
        rows = [row for row in rows if _row_matches_filter(row, filter_rule)]
    return rows


def _row_matches_filter(row: Dict, filter_rule: Dict) -> bool:
    column = filter_rule.get('column', '')
    operator = filter_rule.get('operator', '')
    filter_value = filter_rule.get('value')
    case_sensitive = filter_rule.get('caseSensitive', False)

    if column == '_row_name':
        cell_value = row.get('row_name')
    elif column == '_papers':
        cell_value = row.get('papers', [])
    else:
        data = row.get('data', row)
        cell_value = data.get(column)

    return _evaluate_filter(cell_value, operator, filter_value, case_sensitive)


def _evaluate_filter(cell_value: Any, operator: str, filter_value: Any, case_sensitive: bool) -> bool:
    if isinstance(cell_value, dict) and 'answer' in cell_value:
        cell_value = cell_value['answer']

    is_empty = cell_value is None or cell_value == '' or cell_value == [] or cell_value == {}
    if isinstance(cell_value, str) and cell_value.lower() in ['none', 'n/a', 'null']:
        is_empty = True

    if operator == 'isNull':
        return is_empty
    if operator == 'isNotNull':
        return not is_empty

    if is_empty:
        return False

    if operator == 'isTrue':
        return cell_value in [True, 'true', 'True', '1', 1]
    if operator == 'isFalse':
        return cell_value in [False, 'false', 'False', '0', 0]

    str_value = str(cell_value)
    filter_str = str(filter_value or '')
    if not case_sensitive:
        str_value = str_value.lower()
        filter_str = filter_str.lower()

    if operator == 'contains':
        return filter_str in str_value
    if operator == 'equals':
        return str_value == filter_str
    if operator == 'startsWith':
        return str_value.startswith(filter_str)
    if operator == 'endsWith':
        return str_value.endswith(filter_str)
    if operator == 'regex':
        try:
            flags = 0 if case_sensitive else re.IGNORECASE
            return bool(re.search(str(filter_value), str(cell_value), flags))
        except Exception:
            return False

    if operator in ['eq', 'gt', 'lt', 'gte', 'lte', 'between']:
        try:
            num_value = float(str(cell_value))
            if operator == 'eq':
                return num_value == float(filter_value)
            if operator == 'gt':
                return num_value > float(filter_value)
            if operator == 'lt':
                return num_value < float(filter_value)
            if operator == 'gte':
                return num_value >= float(filter_value)
            if operator == 'lte':
                return num_value <= float(filter_value)
            if operator == 'between' and isinstance(filter_value, list) and len(filter_value) >= 2:
                return float(filter_value[0]) <= num_value <= float(filter_value[1])
        except Exception:
            return False

    if operator == 'in':
        allowed = filter_value if isinstance(filter_value, list) else [filter_value]
        str_val_lower = str(cell_value).lower()
        return any(str(v).lower() == str_val_lower for v in allowed)
    if operator == 'notIn':
        allowed = filter_value if isinstance(filter_value, list) else [filter_value]
        str_val_lower = str(cell_value).lower()
        return not any(str(v).lower() == str_val_lower for v in allowed)

    return True


def apply_sort(rows: List[Dict], sort_columns: List[Dict]) -> List[Dict]:
    """Apply multi-column sorting (priority ascending = higher importance)."""
    if not sort_columns:
        return rows

    sorted_cols = sorted(sort_columns, key=lambda x: x.get('priority', 1))

    def compare_rows(a: Dict, b: Dict) -> int:
        for sort_col in sorted_cols:
            column = sort_col.get('column', '')
            direction = sort_col.get('direction', 'asc')

            a_val = _get_sortable_value(a, column)
            b_val = _get_sortable_value(b, column)

            a_null = a_val is None or a_val == ''
            b_null = b_val is None or b_val == ''

            if a_null and b_null:
                continue
            if a_null:
                return 1
            if b_null:
                return -1

            if isinstance(a_val, (int, float)) and isinstance(b_val, (int, float)):
                cmp = (a_val > b_val) - (a_val < b_val)
            else:
                cmp = (str(a_val).lower() > str(b_val).lower()) - \
                      (str(a_val).lower() < str(b_val).lower())

            if cmp != 0:
                return cmp if direction == 'asc' else -cmp

        return 0

    return sorted(rows, key=cmp_to_key(compare_rows))


def _get_sortable_value(row: Dict, column: str) -> Any:
    if column == '_row_name':
        return row.get('row_name')
    if column == '_papers':
        papers = row.get('papers', [])
        return papers[0] if papers else None

    data = row.get('data', row)
    value = data.get(column)

    if isinstance(value, dict) and 'answer' in value:
        value = value['answer']

    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            pass

    return value
