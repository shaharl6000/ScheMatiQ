"""Row-level search, filter, and sort predicates.

Shared by FileParser (flat-row views) and UnitViewService (unit-grouped views)
so both apply identical semantics to the same row shape.
"""

import math
import re
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

_EMPTY_STRINGS = frozenset(("none", "n/a", "null"))
_TRUE_VALUES = (True, "true", "True", "1", 1)
_FALSE_VALUES = (False, "false", "False", "0", 0)


class _DescStr:
    """Order-reversing wrapper for pre-lowercased string sort keys."""

    __slots__ = ("value",)

    def __init__(self, value: str):
        self.value = value

    def __lt__(self, other: "_DescStr") -> bool:
        return self.value > other.value

    def __gt__(self, other: "_DescStr") -> bool:
        return self.value < other.value

    def __le__(self, other: "_DescStr") -> bool:
        return self.value >= other.value

    def __ge__(self, other: "_DescStr") -> bool:
        return self.value <= other.value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _DescStr) and self.value == other.value


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


def _extract_filter_cell_value(row: Dict, column: str, col_mode: int) -> Any:
    if col_mode == 0:
        cell_value = row.get('row_name')
    elif col_mode == 1:
        cell_value = row.get('papers', [])
    else:
        data = row.get('data', row)
        cell_value = data.get(column)
    if isinstance(cell_value, dict) and 'answer' in cell_value:
        cell_value = cell_value['answer']
    return cell_value


def _is_empty_filter_value(cell_value: Any) -> bool:
    if cell_value is None or cell_value == '' or cell_value == [] or cell_value == {}:
        return True
    return isinstance(cell_value, str) and cell_value.lower() in _EMPTY_STRINGS


def _compile_filter_rule(filter_rule: Dict) -> Callable[[Dict], bool]:
    """Pre-compile a FilterRule dict into a fast per-row predicate."""
    column = filter_rule.get('column', '')
    operator = filter_rule.get('operator', '')
    filter_value = filter_rule.get('value')
    case_sensitive = bool(filter_rule.get('caseSensitive', False))

    if column == '_row_name':
        col_mode = 0
    elif column == '_papers':
        col_mode = 1
    else:
        col_mode = 2

    if operator == 'isNull':
        return lambda row: _is_empty_filter_value(
            _extract_filter_cell_value(row, column, col_mode)
        )
    if operator == 'isNotNull':
        return lambda row: not _is_empty_filter_value(
            _extract_filter_cell_value(row, column, col_mode)
        )
    if operator == 'isTrue':
        def _pred_true(row: Dict) -> bool:
            val = _extract_filter_cell_value(row, column, col_mode)
            return False if _is_empty_filter_value(val) else val in _TRUE_VALUES
        return _pred_true
    if operator == 'isFalse':
        def _pred_false(row: Dict) -> bool:
            val = _extract_filter_cell_value(row, column, col_mode)
            return False if _is_empty_filter_value(val) else val in _FALSE_VALUES
        return _pred_false

    if operator in ('contains', 'equals', 'startsWith', 'endsWith'):
        target_str = str(filter_value or '')
        if not case_sensitive:
            target_str = target_str.lower()

        if operator == 'contains':
            def _pred_contains(row: Dict) -> bool:
                val = _extract_filter_cell_value(row, column, col_mode)
                if _is_empty_filter_value(val):
                    return False
                s = str(val)
                return target_str in (s if case_sensitive else s.lower())
            return _pred_contains

        if operator == 'equals':
            def _pred_equals(row: Dict) -> bool:
                val = _extract_filter_cell_value(row, column, col_mode)
                if _is_empty_filter_value(val):
                    return False
                s = str(val)
                return (s if case_sensitive else s.lower()) == target_str
            return _pred_equals

        if operator == 'startsWith':
            def _pred_starts(row: Dict) -> bool:
                val = _extract_filter_cell_value(row, column, col_mode)
                if _is_empty_filter_value(val):
                    return False
                s = str(val)
                return (s if case_sensitive else s.lower()).startswith(target_str)
            return _pred_starts

        def _pred_ends(row: Dict) -> bool:
            val = _extract_filter_cell_value(row, column, col_mode)
            if _is_empty_filter_value(val):
                return False
            s = str(val)
            return (s if case_sensitive else s.lower()).endswith(target_str)
        return _pred_ends

    if operator == 'regex':
        flags = 0 if case_sensitive else re.IGNORECASE
        try:
            compiled_re: Optional[re.Pattern] = re.compile(str(filter_value), flags)
        except Exception:
            compiled_re = None

        if compiled_re is None:
            return lambda _row: False

        def _pred_regex(row: Dict) -> bool:
            val = _extract_filter_cell_value(row, column, col_mode)
            if _is_empty_filter_value(val):
                return False
            return bool(compiled_re.search(str(val)))
        return _pred_regex

    if operator in ('eq', 'gt', 'lt', 'gte', 'lte'):
        try:
            target_num: Optional[float] = float(filter_value)
        except Exception:
            target_num = None

        if target_num is None:
            return lambda _row: False

        def _pred_num(row: Dict) -> bool:
            val = _extract_filter_cell_value(row, column, col_mode)
            if _is_empty_filter_value(val):
                return False
            try:
                num_value = float(str(val))
            except Exception:
                return False
            if operator == 'eq':
                return num_value == target_num
            if operator == 'gt':
                return num_value > target_num
            if operator == 'lt':
                return num_value < target_num
            if operator == 'gte':
                return num_value >= target_num
            return num_value <= target_num
        return _pred_num

    if operator == 'between':
        has_bounds = isinstance(filter_value, list) and len(filter_value) >= 2
        low_num: Optional[float] = None
        high_num: Optional[float] = None
        if has_bounds:
            try:
                low_num = float(filter_value[0])
                high_num = float(filter_value[1])
            except Exception:
                return lambda _row: False

        def _pred_between(row: Dict) -> bool:
            val = _extract_filter_cell_value(row, column, col_mode)
            if _is_empty_filter_value(val):
                return False
            try:
                num_value = float(str(val))
            except Exception:
                return False
            if has_bounds and low_num is not None and high_num is not None:
                return low_num <= num_value <= high_num
            return True
        return _pred_between

    if operator in ('in', 'notIn', 'not_in'):
        allowed_raw = filter_value if isinstance(filter_value, list) else [filter_value]
        allowed_set = frozenset(str(v).lower() for v in allowed_raw)
        negate = operator in ('notIn', 'not_in')

        def _pred_membership(row: Dict) -> bool:
            val = _extract_filter_cell_value(row, column, col_mode)
            if _is_empty_filter_value(val):
                return False
            matched = str(val).lower() in allowed_set
            return not matched if negate else matched
        return _pred_membership

    def _pred_default(row: Dict) -> bool:
        val = _extract_filter_cell_value(row, column, col_mode)
        return not _is_empty_filter_value(val)

    return _pred_default


def apply_filters(rows: List[Dict], filters: List[Dict]) -> List[Dict]:
    """Apply filter rules to rows (AND logic across rules)."""
    if not filters or not rows:
        return rows
    predicates = [_compile_filter_rule(rule) for rule in filters]
    if len(predicates) == 1:
        pred = predicates[0]
        return [row for row in rows if pred(row)]
    return [row for row in rows if all(pred(row) for pred in predicates)]


def _row_matches_filter(row: Dict, filter_rule: Dict) -> bool:
    return _compile_filter_rule(filter_rule)(row)


def _evaluate_filter(cell_value: Any, operator: str, filter_value: Any, case_sensitive: bool) -> bool:
    return _compile_filter_rule({
        'column': '_val',
        'operator': operator,
        'value': filter_value,
        'caseSensitive': case_sensitive,
    })({'data': {'_val': cell_value}})


def _column_sort_key(row: Dict, column: str, is_desc: bool) -> Tuple[int, Any]:
    """Build a single-column sort key where null/empty ALWAYS sorts last (tier 2),
    and non-null values sort numeric-before-string in asc (reversed in desc)."""
    val = _get_sortable_value(row, column)
    if val is None or val == '' or (isinstance(val, float) and math.isnan(val)):
        return (2, 0)
    if isinstance(val, (int, float)):
        num = float(val)
        return (1, -num) if is_desc else (0, num)
    s = val.lower() if isinstance(val, str) else str(val).lower()
    return (0, _DescStr(s)) if is_desc else (1, s)


def apply_sort(rows: List[Dict], sort_columns: List[Dict]) -> List[Dict]:
    """Apply multi-column sorting (priority ascending = higher importance)."""
    if not sort_columns:
        return rows
    if len(rows) <= 1:
        return list(rows)

    sorted_cols = sorted(sort_columns, key=lambda x: x.get('priority', 1))
    specs = [
        (sc.get('column', ''), sc.get('direction', 'asc') != 'asc')
        for sc in sorted_cols
    ]

    if len(specs) == 1:
        col, is_desc = specs[0]
        return sorted(rows, key=lambda row: _column_sort_key(row, col, is_desc))

    return sorted(
        rows,
        key=lambda row: tuple(
            _column_sort_key(row, col, is_desc) for col, is_desc in specs
        ),
    )


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
