"""Unit view service for observation unit grouping and merging."""

import json
import logging
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from app.models.unit import (
    UnitSummary,
    UnitListResponse,
    MergeUnitsRequest,
    MergeUnitsResponse,
    UnitSimilarity,
    UnitSuggestionsResponse,
    AutoMergeResult,
)
from app.models.session import DataRow
from app.core.config import DEFAULT_DATA_DIR

logger = logging.getLogger(__name__)


class UnitViewService:
    """Service for managing observation unit views and merges."""

    def __init__(self, data_dir: str = DEFAULT_DATA_DIR):
        self.data_dir = Path(data_dir)
        self.work_dir = Path("./qbsd_work")

    def _get_all_data_files(self, session_id: str) -> List[Path]:
        """Get all data file paths for a session, checking multiple locations.

        Returns list of existing files from:
        1. qbsd_work/{session_id}/extracted_data.jsonl (QBSD sessions)
        2. qbsd_work/{session_id}/data.jsonl (fallback if extracted_data doesn't exist)
        3. data/{session_id}/data.jsonl (always check - may have additional docs)
        """
        data_files = []

        # Check QBSD work directory (original QBSD extraction)
        qbsd_extracted = self.work_dir / session_id / "extracted_data.jsonl"
        if qbsd_extracted.exists():
            data_files.append(qbsd_extracted)

        # Check qbsd_work for data.jsonl (only if extracted_data.jsonl doesn't exist)
        if not data_files:
            qbsd_data = self.work_dir / session_id / "data.jsonl"
            if qbsd_data.exists():
                data_files.append(qbsd_data)

        # Always check data directory - may contain additional documents
        data_file = self.data_dir / session_id / "data.jsonl"
        if data_file.exists():
            # Use resolve() to handle symlinks and relative paths correctly
            resolved_files = [f.resolve() for f in data_files]
            if data_file.resolve() not in resolved_files:
                data_files.append(data_file)

        return data_files

    def _get_data_file(self, session_id: str) -> Optional[Path]:
        """Get the primary data file path for a session (for backwards compatibility)."""
        files = self._get_all_data_files(session_id)
        return files[0] if files else None

    def _load_all_rows(self, session_id: str) -> List[Dict]:
        """Load all data rows from all session data files.

        Deduplicates rows by row_name to prevent duplicates when data exists
        in multiple locations (e.g., both qbsd_work/ and data/ directories).
        """
        data_files = self._get_all_data_files(session_id)
        if not data_files:
            return []

        rows = []
        seen_row_names = set()
        for data_file in data_files:
            try:
                with open(data_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            row = json.loads(line)
                            # Deduplicate by row_name
                            row_name = row.get('_row_name') or row.get('row_name')
                            if row_name and row_name in seen_row_names:
                                continue
                            if row_name:
                                seen_row_names.add(row_name)
                            rows.append(row)
            except Exception as e:
                logger.warning(f"Error reading {data_file}: {e}")
        return rows

    def _save_all_rows(self, session_id: str, rows: List[Dict]) -> None:
        """Save all data rows to a session's JSONL file."""
        data_file = self._get_data_file(session_id)
        if not data_file:
            # Default to qbsd_work directory for new files
            data_file = self.work_dir / session_id / "extracted_data.jsonl"
        data_file.parent.mkdir(parents=True, exist_ok=True)

        with open(data_file, 'w', encoding='utf-8') as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')

    def _get_unit_name(self, row: Dict) -> Optional[str]:
        """Extract unit name from a row, checking multiple possible fields."""
        # Check for unit_name with and without underscore prefix at root level
        for field in ['_unit_name', 'unit_name']:
            if field in row and row[field]:
                return str(row[field]).strip()

        # Also check within the 'data' field for unit-related columns
        # This handles cases where the observation unit is stored as a regular column
        data = row.get('data', {})
        if isinstance(data, dict):
            for key, value in data.items():
                key_lower = key.lower()
                # Match columns like "Observation Unit", "Unit", "Study Unit", etc.
                if ('unit' in key_lower or 'observation' in key_lower) and value:
                    # Get the actual value (handle CellValue structure)
                    if isinstance(value, dict):
                        # CellValue might have 'value' or 'answer' field
                        actual_value = value.get('value') or value.get('answer')
                        if actual_value:
                            return str(actual_value).strip()
                    elif isinstance(value, str) and value.strip():
                        return value.strip()

        return None

    def _get_source_document(self, row: Dict) -> Optional[str]:
        """Extract source document from a row."""
        for field in ['_source_document', 'source_document', '_parent_document', 'papers']:
            if field in row:
                value = row[field]
                if isinstance(value, list) and value:
                    return str(value[0]).strip()
                elif value:
                    return str(value).strip()
        return None

    def get_units_summary(self, session_id: str) -> UnitListResponse:
        """
        Get a summary of all observation units in a session.

        Args:
            session_id: The session ID to analyze

        Returns:
            UnitListResponse with unit summaries and totals
        """
        rows = self._load_all_rows(session_id)

        # Group rows by unit name
        unit_data: Dict[str, Dict] = defaultdict(lambda: {
            'row_count': 0,
            'source_documents': set(),
            'is_merged': False,
            'original_units': None
        })

        # Track rows without unit names
        no_unit_rows = 0

        for row in rows:
            unit_name = self._get_unit_name(row)
            if unit_name:
                unit_data[unit_name]['row_count'] += 1
                source_doc = self._get_source_document(row)
                if source_doc:
                    unit_data[unit_name]['source_documents'].add(source_doc)

                # Check if this row was part of a merge (has _original_units field)
                if '_original_units' in row:
                    unit_data[unit_name]['is_merged'] = True
                    original = row['_original_units']
                    if isinstance(original, list):
                        unit_data[unit_name]['original_units'] = original
            else:
                no_unit_rows += 1

        # Convert to list of UnitSummary objects
        units = []
        for name, data in sorted(unit_data.items(), key=lambda x: x[0].lower()):
            units.append(UnitSummary(
                name=name,
                row_count=data['row_count'],
                source_documents=sorted(list(data['source_documents'])),
                is_merged=data['is_merged'],
                original_units=data['original_units']
            ))

        total_rows = sum(u.row_count for u in units) + no_unit_rows

        logger.debug(f"Session {session_id}: Found {len(units)} units, {total_rows} total rows")

        return UnitListResponse(
            units=units,
            total_units=len(units),
            total_rows=total_rows
        )

    def get_unit_grouped_data(
        self,
        session_id: str,
        unit_filter: Optional[str] = None,
        page: int = 0,
        page_size: int = 50
    ) -> Tuple[List[Dict], int, int]:
        """
        Get data grouped by observation unit with optional filtering.

        Args:
            session_id: The session ID
            unit_filter: Optional unit name to filter by
            page: Page number (0-indexed)
            page_size: Number of rows per page

        Returns:
            Tuple of (rows, total_count, filtered_count)
        """
        rows = self._load_all_rows(session_id)
        total_count = len(rows)

        # Filter by unit if specified
        if unit_filter:
            rows = [r for r in rows if self._get_unit_name(r) == unit_filter]

        filtered_count = len(rows)

        # Sort by unit name for consistent grouping
        rows = sorted(rows, key=lambda r: ((self._get_unit_name(r) or '').lower(), r.get('row_name', '')))

        # Apply pagination
        start_idx = page * page_size
        end_idx = start_idx + page_size
        paginated_rows = rows[start_idx:end_idx]

        return paginated_rows, total_count, filtered_count

    def merge_units(self, session_id: str, request: MergeUnitsRequest) -> MergeUnitsResponse:
        """
        Merge multiple observation units into a single unit.

        Args:
            session_id: The session ID
            request: Merge request with source units and target name

        Returns:
            MergeUnitsResponse with result details
        """
        rows = self._load_all_rows(session_id)
        if not rows:
            return MergeUnitsResponse(
                success=False,
                message="No data found for session",
                rows_affected=0
            )

        # Find rows that belong to source units
        rows_affected = 0
        source_units_lower = {u.lower() for u in request.source_units}

        for row in rows:
            unit_name = self._get_unit_name(row)
            if unit_name and unit_name.lower() in source_units_lower:
                # Store original unit name(s) for undo capability
                original_units = row.get('_original_units', [])
                if not original_units:
                    original_units = [unit_name]
                elif unit_name not in original_units:
                    original_units.append(unit_name)

                # Update row with new unit name
                if '_unit_name' in row:
                    row['_unit_name'] = request.target_unit
                else:
                    row['unit_name'] = request.target_unit
                row['_original_units'] = original_units

                rows_affected += 1

        if rows_affected == 0:
            return MergeUnitsResponse(
                success=False,
                message=f"No rows found for source units: {request.source_units}",
                rows_affected=0
            )

        # Save updated rows
        self._save_all_rows(session_id, rows)

        # Get updated summary for merged unit
        updated_summary = self.get_units_summary(session_id)
        merged_unit = next(
            (u for u in updated_summary.units if u.name == request.target_unit),
            None
        )

        logger.info(
            f"Session {session_id}: Merged {len(request.source_units)} units into "
            f"'{request.target_unit}', {rows_affected} rows affected"
        )

        return MergeUnitsResponse(
            success=True,
            message=f"Successfully merged {len(request.source_units)} units into '{request.target_unit}'",
            merged_unit=merged_unit,
            rows_affected=rows_affected
        )

    def _select_best_name(self, names: List[str]) -> str:
        """Pick the best name from a group: prefer non-all-caps, then longest."""
        non_caps = [n for n in names if n != n.upper() or n == n.lower()]
        candidates = non_caps if non_caps else names
        return max(candidates, key=len)

    def _calculate_suggestions(
        self,
        session_id: str,
        threshold: float
    ) -> List[UnitSimilarity]:
        """Calculate pairwise similarity suggestions for all units."""
        summary = self.get_units_summary(session_id)
        unit_names = [u.name for u in summary.units]

        suggestions = []
        processed_pairs = set()

        for i, name1 in enumerate(unit_names):
            for name2 in unit_names[i + 1:]:
                pair_key = tuple(sorted([name1.lower(), name2.lower()]))
                if pair_key in processed_pairs:
                    continue
                processed_pairs.add(pair_key)

                similarity = self._calculate_similarity(name1, name2)

                if similarity >= threshold:
                    suggested_name, reason = self._generate_merge_suggestion(
                        name1, name2, similarity
                    )
                    suggestions.append(UnitSimilarity(
                        units=[name1, name2],
                        similarity=round(similarity, 3),
                        suggested_name=suggested_name,
                        reason=reason
                    ))

        suggestions.sort(key=lambda s: s.similarity, reverse=True)
        return suggestions

    def _auto_merge_exact_matches(
        self,
        session_id: str,
        suggestions: List[UnitSimilarity]
    ) -> List[AutoMergeResult]:
        """Auto-merge all 100% similarity matches using union-find grouping."""
        perfect_matches = [s for s in suggestions if s.similarity >= 1.0]
        if not perfect_matches:
            return []

        # Union-find to group connected units
        parent: Dict[str, str] = {}

        def find(x: str) -> str:
            if x not in parent:
                parent[x] = x
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(a: str, b: str) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        for match in perfect_matches:
            for i in range(1, len(match.units)):
                union(match.units[0], match.units[i])

        # Build groups
        groups: Dict[str, List[str]] = {}
        for name in parent:
            root = find(name)
            if root not in groups:
                groups[root] = []
            groups[root].append(name)

        # Merge each group
        results = []
        for group in groups.values():
            if len(group) < 2:
                continue
            target_name = self._select_best_name(group)
            merge_result = self.merge_units(
                session_id,
                MergeUnitsRequest(
                    source_units=group,
                    target_unit=target_name,
                    strategy='rename',
                ),
            )
            results.append(AutoMergeResult(
                merged_units=group,
                target_unit=target_name,
                rows_affected=merge_result.rows_affected,
            ))
            logger.info(
                f"Session {session_id}: Auto-merged {group} into '{target_name}'"
            )

        return results

    def suggest_similar_units(
        self,
        session_id: str,
        threshold: float = 0.8,
        auto_merge: bool = False
    ) -> UnitSuggestionsResponse:
        """
        Find similar observation units that might be candidates for merging.

        Args:
            session_id: The session ID
            threshold: Minimum similarity score (0-1) to suggest merge
            auto_merge: If True, auto-merge 100% similarity matches first

        Returns:
            UnitSuggestionsResponse with merge suggestions (and auto_merged results)
        """
        suggestions = self._calculate_suggestions(session_id, threshold)
        auto_merged: List[AutoMergeResult] = []

        if auto_merge:
            auto_merged = self._auto_merge_exact_matches(session_id, suggestions)
            if auto_merged:
                # Re-calculate after merges changed the data
                suggestions = self._calculate_suggestions(session_id, threshold)

        logger.debug(
            f"Session {session_id}: Found {len(suggestions)} similar unit pairs "
            f"(threshold={threshold}, auto_merged={len(auto_merged)} groups)"
        )

        return UnitSuggestionsResponse(
            suggestions=suggestions,
            threshold=threshold,
            auto_merged=auto_merged,
        )

    def _calculate_similarity(self, name1: str, name2: str) -> float:
        """
        Calculate similarity between two unit names.

        Uses multiple strategies:
        1. Direct SequenceMatcher ratio
        2. Normalized (lowercase, stripped) comparison
        3. Word overlap comparison
        """
        # Normalize names
        n1 = name1.lower().strip()
        n2 = name2.lower().strip()

        # Direct sequence matching
        seq_ratio = SequenceMatcher(None, n1, n2).ratio()

        # Word-based overlap
        words1 = set(n1.split())
        words2 = set(n2.split())
        if words1 and words2:
            word_overlap = len(words1 & words2) / max(len(words1), len(words2))
        else:
            word_overlap = 0.0

        # Return weighted average (favor sequence matching)
        return 0.7 * seq_ratio + 0.3 * word_overlap

    def _generate_merge_suggestion(
        self,
        name1: str,
        name2: str,
        similarity: float
    ) -> Tuple[str, str]:
        """
        Generate a suggested merged name and reason.

        Args:
            name1: First unit name
            name2: Second unit name
            similarity: Similarity score

        Returns:
            Tuple of (suggested_name, reason)
        """
        n1 = name1.strip()
        n2 = name2.strip()

        # If one is a substring of the other, use the longer one
        if n1.lower() in n2.lower():
            suggested = n2
            reason = f"'{n1}' appears to be a variation of '{n2}'"
        elif n2.lower() in n1.lower():
            suggested = n1
            reason = f"'{n2}' appears to be a variation of '{n1}'"
        # If they differ only in case or whitespace
        elif n1.lower().replace(' ', '') == n2.lower().replace(' ', ''):
            suggested = n1  # Use first one
            reason = "Names differ only in case or spacing"
        # Otherwise use the shorter one (more concise)
        elif len(n1) <= len(n2):
            suggested = n1
            reason = f"High similarity ({similarity:.0%}) - names appear to refer to the same entity"
        else:
            suggested = n2
            reason = f"High similarity ({similarity:.0%}) - names appear to refer to the same entity"

        return suggested, reason


# Create a singleton instance for use across the application
unit_view_service = UnitViewService()
