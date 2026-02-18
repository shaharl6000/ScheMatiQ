"""Table building orchestration for value extraction."""

import json
import time
import shutil
from pathlib import Path
from typing import Dict, Any, List, Set, Callable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from schematiq.core.schema import Schema, Column
from schematiq.core.llm_backends import LLMInterface
from schematiq.core import utils

from .paper_processor import PaperProcessor, OnValueExtractedCallback, OnWarningCallback
from .row_manager import RowDataManager
from .llm_cache import LLMCache
from ..config.constants import DEFAULT_MAX_NEW_TOKENS, DEFAULT_MAX_WORKERS


# Type alias for the should_stop callback
ShouldStopCallback = Callable[[], bool]


class TableBuilder:
    """Orchestrates the table building process."""

    def __init__(self, llm: LLMInterface, retriever=None, cache: LLMCache = None,
                 on_value_extracted: Optional[OnValueExtractedCallback] = None,
                 should_stop: Optional[ShouldStopCallback] = None,
                 on_warning: Optional[OnWarningCallback] = None):
        self.llm = llm
        self.retriever = retriever
        self.cache = cache or LLMCache()
        self.on_value_extracted = on_value_extracted
        self.should_stop = should_stop
        self.on_warning = on_warning
        # Pass should_stop and on_warning to PaperProcessor for fine-grained stop checking and warning reporting
        self.paper_processor = PaperProcessor(llm, self.cache, retriever, on_value_extracted, should_stop, on_warning)
        self.row_manager = RowDataManager()
        self._stopped = False  # Track if we stopped early
        self._skipped_documents: List[str] = []  # Track documents with no observation units found

    def get_suggested_values(self, threshold: int = 2) -> Dict[str, Dict[str, Any]]:
        """Get suggested values that meet the threshold from PaperProcessor."""
        return self.paper_processor.get_suggested_values(threshold)

    def get_all_suggested_values(self) -> Dict[str, Dict[str, Any]]:
        """Get all suggested values regardless of threshold."""
        return self.paper_processor.get_all_suggested_values()

    def get_skipped_documents(self) -> List[str]:
        """Get list of documents that were skipped due to no observation units found."""
        return self._skipped_documents.copy()

    def clear_skipped_documents(self) -> None:
        """Clear the list of skipped documents."""
        self._skipped_documents = []

    def _report_skipped_documents_summary(self) -> None:
        """Report summary of documents skipped due to no observation units found.

        Prints to console and sends via on_warning callback for UI display.
        """
        if not self._skipped_documents:
            return

        count = len(self._skipped_documents)

        # Console summary
        print(f"\n⚠️  {count} document(s) skipped - no observation units found:")
        for doc in self._skipped_documents:
            print(f"    • {doc}")

        # Send via callback for UI display
        if self.on_warning:
            # Send a summary warning
            summary_message = f"No observation units found in {count} document(s): {', '.join(self._skipped_documents)}"
            try:
                self.on_warning("_extraction_summary", "no_observation_units", summary_message)
            except Exception as e:
                print(f"⚠️  Warning callback error: {e}")
    
    def _is_system_file(self, filename: str) -> bool:
        """Check if a filename is a system file that should be skipped."""
        system_files = {'.DS_Store', '._.DS_Store', 'Thumbs.db', '.gitkeep', '.gitignore'}
        if filename in system_files:
            return True
        # Skip hidden files (starting with .)
        if filename.startswith('.'):
            return True
        return False

    def _load_schema(self, schema_path: Path) -> Schema:
        """Load schema from JSON file, including observation_unit if present."""
        data = json.loads(schema_path.read_text(encoding="utf-8"))
        return Schema.from_dict(data)
    
    def _load_existing_data(self, output_path: Path, papers_by_row: Dict[str, List[Path]]) -> tuple[Dict[str, Dict[str, Any]], Set[str], Set[str]]:
        """Load existing data for resume mode."""
        existing_rows: Dict[str, Dict[str, Any]] = {}
        processed_papers: Set[str] = set()
        completed_rows: Set[str] = set()
        
        if not output_path.exists():
            return existing_rows, processed_papers, completed_rows
        
        print("🔄 Loading existing data for resume...")
        try:
            with output_path.open(encoding="utf-8") as f_prev:
                for line_num, line in enumerate(f_prev, 1):
                    if line.strip():
                        try:
                            row_data = json.loads(line)
                            row_name = row_data.get("_row_name")
                            if row_name:
                                existing_rows[row_name] = row_data
                                # Track all papers that contributed to this row
                                papers = row_data.get("_papers", [])
                                processed_papers.update(papers)
                                
                                # Check if this row is complete (has data for all expected papers)
                                expected_papers_for_row = {p.stem for p in papers_by_row.get(row_name, [])}
                                if self.row_manager.validate_row_completion(row_data, expected_papers_for_row):
                                    completed_rows.add(row_name)
                        except json.JSONDecodeError as e:
                            print(f"⚠️  Skipping invalid JSON line {line_num}: {e}")
        except Exception as e:
            print(f"⚠️  Error reading existing file: {e}")
            # If we can't read the existing file safely, don't use resume mode
            existing_rows.clear()
            processed_papers.clear()
            completed_rows.clear()
        
        print(f"🔄 Loaded {len(existing_rows)} existing rows, {len(processed_papers)} papers already processed")
        print(f"🔄 {len(completed_rows)} rows are complete, {len(existing_rows) - len(completed_rows)} rows may need updates")
        
        return existing_rows, processed_papers, completed_rows
    
    def _create_metadata(self, schema: Schema, docs_directory: Path) -> Dict[str, Any]:
        """Create metadata for row output."""
        return {
            "query": schema.query,
            "retriever": {
                "type": self.retriever.__class__.__name__ if self.retriever else None,
                "model": getattr(self.retriever, 'model_name', None) if self.retriever else None
            },
            "backend": {
                "type": self.llm.__class__.__name__,
                "model": getattr(self.llm, 'model', None),
                "temperature": getattr(self.llm, 'temperature', None),
                "max_output_tokens": getattr(self.llm, 'max_output_tokens', None)
            }
        }
    
    def _write_row_if_complete(self, row_name: str, new_paper_results: Dict[str, tuple[str, Dict[str, Any]]], 
                              papers_by_row: Dict[str, List[Path]], existing_rows: Dict[str, Dict[str, Any]], 
                              written_rows: Set[str], completed_rows: Set[str], output_path: Path, 
                              schema: Schema, docs_directory: Path):
        """Write a row to output file if all its papers are processed."""
        if row_name in written_rows or row_name in completed_rows:
            return
            
        # Check if all papers for this row have been processed
        expected_papers = {p.stem for p in papers_by_row.get(row_name, [])}
        processed_papers_for_row = {title for title, (r_name, _) in new_paper_results.items() if r_name == row_name}
        existing_papers = set(existing_rows.get(row_name, {}).get("_papers", []))
        all_papers_for_row = processed_papers_for_row | existing_papers
        
        if not expected_papers.issubset(all_papers_for_row):
            return  # Row not complete yet
            
        # Row is complete - assemble and write it
        if row_name in existing_rows:
            current_row = existing_rows[row_name].copy()
        else:
            current_row = {
                "_row_name": row_name,
                "_papers": [],
                "_metadata": self._create_metadata(schema, docs_directory),
                "document_directory": str(docs_directory)
            }
        
        # Merge new paper results for this row
        for paper_title, (paper_row_name, paper_data) in new_paper_results.items():
            if paper_row_name == row_name:
                existing_papers = current_row.get("_papers", [])
                if paper_title in existing_papers:
                    continue  # Skip duplicates

                if not existing_papers:
                    current_row.update(paper_data)
                    current_row["_papers"] = [paper_title]
                    current_row["_row_name"] = row_name
                else:
                    current_row = self.row_manager.merge_row_data(current_row, paper_data, paper_title)

        # Write row if it has actual data
        if any(key for key in current_row.keys() if not key.startswith('_')):
            try:
                with output_path.open('a', encoding="utf-8") as f_out:
                    f_out.write(json.dumps(current_row, ensure_ascii=False) + "\n")
                written_rows.add(row_name)
                print(f"✅ Completed and wrote row: {row_name}")
            except Exception as e:
                print(f"❌ Failed to write row {row_name}: {e}")
    
    def build_table_jsonl_multi_dirs(self,
                                    schema_path: Path,
                                    docs_directories: list[Path],
                                    output_path: Path,
                                    *,
                                    max_new_tokens: int = DEFAULT_MAX_NEW_TOKENS,
                                    resume: bool = False,
                                    mode: str = "all",
                                    retrieval_k: int = 8,
                                    max_workers: int = DEFAULT_MAX_WORKERS,
                                    known_units: Optional[Dict[str, List[str]]] = None) -> None:
        """
        Extract values from papers across multiple directories and write to JSONL.
        """
        # Aggregate all documents from all directories with their source info
        all_docs_with_source = []
        for docs_directory in docs_directories:
            docs = sorted(docs_directory.glob("*"))
            for doc in docs:
                if doc.is_file() and not self._is_system_file(doc.name):
                    all_docs_with_source.append((doc, docs_directory))
        
        if not all_docs_with_source:
            raise RuntimeError(f"No documents found across directories: {[str(d) for d in docs_directories]}")
        
        print(f"📁 Processing {len(all_docs_with_source)} documents from {len(docs_directories)} directories")
        
        # Use the existing build logic but with enhanced directory tracking
        self._build_table_multi_dirs_impl(
            schema_path, all_docs_with_source, output_path,
            max_new_tokens=max_new_tokens, resume=resume, mode=mode,
            retrieval_k=retrieval_k, max_workers=max_workers,
            known_units=known_units
        )
    
    def _build_table_multi_dirs_impl(self,
                                    schema_path: Path,
                                    docs_with_source: list[tuple[Path, Path]],  # (doc_path, source_dir)
                                    output_path: Path,
                                    *,
                                    max_new_tokens: int,
                                    resume: bool,
                                    mode: str,
                                    retrieval_k: int,
                                    max_workers: int,
                                    known_units: Optional[Dict[str, List[str]]] = None) -> None:
        """Implementation for multi-directory process
        ing."""
        schema = self._load_schema(schema_path)
        print(f"🗂️  Loaded schema with {len(schema.columns)} columns")
        
        # Group by row names
        papers_by_row = {}
        doc_to_source = {}  # Track source directory for each document
        
        for doc_path, source_dir in docs_with_source:
            doc_to_source[doc_path] = source_dir
            row_name = self.row_manager.extract_row_name_from_filename(doc_path.name)
            if row_name not in papers_by_row:
                papers_by_row[row_name] = []
            papers_by_row[row_name].append(doc_path)
        
        print(f"📊 Found {len(papers_by_row)} unique rows from documents")
        
        # Handle resume logic
        existing_rows, processed_papers, completed_rows = {}, set(), set()
        if resume and output_path.exists():
            existing_rows, processed_papers, completed_rows = self._load_existing_data(
                output_path, papers_by_row
            )
        
        # Process rows
        written_rows = set()
        total_rows = len(papers_by_row)

        for row_idx, (row_name, papers) in enumerate(papers_by_row.items(), 1):
            # Check for stop request before processing each row
            if self.should_stop and self.should_stop():
                print(f"\n🛑 Stop requested - exiting after {row_idx-1}/{total_rows} rows")
                self._stopped = True
                break

            print(f"\n🔄 Processing row {row_idx}/{total_rows}: {row_name}")

            if row_name in completed_rows:
                print(f"⏭️  Row {row_name} already completed, skipping...")
                written_rows.add(row_name)
                continue

            # Process with directory-specific GT_NES logic
            self._process_row_multi_dirs(
                row_name, papers, doc_to_source, schema, mode, retrieval_k,
                max_new_tokens, max_workers, existing_rows, processed_papers,
                written_rows, completed_rows, output_path,
                known_units=known_units
            )

        print(f"\n✅ Processing complete! Wrote {len(written_rows)} rows to {output_path}")

        # Print summary of skipped documents (no observation units found)
        self._report_skipped_documents_summary()
    
    def _process_row_multi_dirs(self,
                               row_name: str,
                               papers: list[Path],
                               doc_to_source: dict[Path, Path],
                               schema: Schema,
                               mode: str,
                               retrieval_k: int,
                               max_new_tokens: int,
                               max_workers: int,
                               existing_rows: dict,
                               processed_papers: set,
                               written_rows: set,
                               completed_rows: set,
                               output_path: Path,
                               known_units: Optional[Dict[str, List[str]]] = None) -> None:
        """Process a single row across multiple directories.

        Uses observation unit extraction - each document may produce multiple rows
        (one per observation unit instance).

        Raises:
            ValueError: If schema does not have observation_unit set.
        """
        observation_unit = schema.observation_unit

        if not observation_unit:
            raise ValueError("Schema must have observation_unit set for value extraction")

        # Observation unit extraction: each paper may produce multiple rows
        self._process_papers_with_observation_units(
            row_name, papers, doc_to_source, schema, retrieval_k,
            max_new_tokens, existing_rows, processed_papers,
            written_rows, output_path,
            known_units=known_units
        )

    def _process_papers_with_observation_units(
        self,
        row_name: str,
        papers: list[Path],
        doc_to_source: dict[Path, Path],
        schema: Schema,
        retrieval_k: int,
        max_new_tokens: int,
        existing_rows: dict,
        processed_papers: set,
        written_rows: set,
        output_path: Path,
        known_units: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        """
        Process papers using observation unit extraction, potentially producing
        multiple rows per document.

        Args:
            row_name: Base row name (original document grouping)
            papers: List of paper paths to process
            doc_to_source: Mapping from paper path to source directory
            schema: Schema with observation_unit defined
            retrieval_k: Number of passages for retrieval
            max_new_tokens: Max tokens for LLM
            existing_rows: Previously extracted rows
            processed_papers: Set of already processed papers
            written_rows: Set of already written row names
            output_path: Output file path
        """
        observation_unit = schema.observation_unit
        total_units_written = 0

        for paper in papers:
            if self.should_stop and self.should_stop():
                print(f"🛑 Stop requested during observation unit processing")
                self._stopped = True
                return

            if paper in processed_papers:
                continue

            try:
                paper_text = paper.read_text(encoding="utf-8", errors="ignore")
                paper_title = paper.stem
                source_dir = doc_to_source.get(paper, paper.parent)

                print(f"🔍 Processing {paper_title} for observation units ({observation_unit.name})...")

                # Extract values with observation units (may return multiple rows)
                paper_known_units = known_units.get(paper_title) if known_units else None
                unit_rows = self.paper_processor.extract_values_for_paper_with_units(
                    paper_title=paper_title,
                    paper_text=paper_text,
                    schema=schema,
                    max_new_tokens=max_new_tokens,
                    mode="all",
                    retrieval_k=retrieval_k,
                    known_units=paper_known_units,
                )

                if self.should_stop and self.should_stop():
                    self._stopped = True
                    return

                # Track documents with no observation units found
                if not unit_rows:
                    self._skipped_documents.append(paper_title)

                # Write each unit row
                for unit_row in unit_rows:
                    # Add metadata
                    unit_name = unit_row.get("_unit_name", paper_title)
                    unit_row["_row_name"] = unit_name  # Use unit name as row name
                    unit_row["_papers"] = [paper.name]
                    unit_row["document_directory"] = str(source_dir)
                    unit_row["_metadata"] = {
                        "query": schema.query,
                        "source_directories": [str(source_dir)],
                        "observation_unit": observation_unit.name,
                        "base_row_name": row_name,
                    }

                    # Write row if it has actual data columns
                    if any(key for key in unit_row.keys() if not key.startswith('_') and key != "document_directory"):
                        try:
                            with output_path.open('a', encoding="utf-8") as f_out:
                                f_out.write(json.dumps(unit_row, ensure_ascii=False) + "\n")
                            total_units_written += 1
                            print(f"  ✅ Wrote unit row: {unit_name}")

                            # Notify callback
                            if self.on_value_extracted:
                                for col_name, col_value in unit_row.items():
                                    if not col_name.startswith('_') and col_name != "document_directory":
                                        self.on_value_extracted(unit_name, col_name, col_value)

                        except Exception as e:
                            print(f"  ❌ Failed to write unit row {unit_name}: {e}")

                processed_papers.add(paper)

            except Exception as e:
                print(f"⚠️  Error processing {paper.name} for observation units: {e}")

        # Mark original row_name as written if any units were written
        if total_units_written > 0:
            written_rows.add(row_name)
            print(f"✅ Completed {row_name}: {total_units_written} observation unit rows written")
    
    def _process_row_one_by_one_multi_dirs(self, current_row: dict, papers: list[Path],
                                          doc_to_source: dict, schema: Schema,
                                          retrieval_k: int, max_new_tokens: int,
                                          existing_rows: dict, processed_papers: set) -> None:
        """Process row one column at a time (multi-directory version).

        For each column, try each paper until we find a value.
        Papers are NOT marked as 'processed' here - that tracking is for resume logic,
        not for skipping papers within a single row's extraction.
        """
        row_name = current_row.get("_row_name")

        for column in schema.columns:
            # Check for stop request
            if self.should_stop and self.should_stop():
                print(f"🛑 Stop requested during column processing")
                self._stopped = True
                return

            # Handle both dict and Column object formats
            if isinstance(column, dict):
                column_name = column.get('column') or column.get('name')
            else:
                column_name = column.name

            if column_name in current_row:
                continue

            for paper in papers:
                # Check for stop request
                if self.should_stop and self.should_stop():
                    print(f"🛑 Stop requested during paper processing")
                    self._stopped = True
                    return
                # Note: Don't skip based on processed_papers here - we need to try
                # each paper for EACH column, not skip after first column extraction

                try:
                    paper_text = paper.read_text(encoding="utf-8", errors="ignore")
                    # Create a single-column schema for this extraction
                    single_column_schema = Schema(
                        query=schema.query,
                        columns=[column],
                        max_keys=1
                    )
                    result = self.paper_processor.extract_values_for_paper(
                        paper.stem, paper_text, single_column_schema,
                        max_new_tokens, mode="one_by_one", retrieval_k=retrieval_k,
                        row_name=row_name
                    )
                    # Check stop after extraction returns
                    if self.should_stop and self.should_stop():
                        self._stopped = True
                        return
                    if result and column_name in result:
                        current_row.update(result)
                        break  # Found value for this column, move to next column
                except Exception as e:
                    print(f"⚠️  Error processing {paper.name} for column {column_name}: {e}")

        # Mark all papers as processed AFTER extracting all columns for this row
        for paper in papers:
            processed_papers.add(paper)
    
    def _process_row_all_multi_dirs(self, current_row: dict, papers: list[Path],
                                   doc_to_source: dict, schema: Schema,
                                   retrieval_k: int, max_new_tokens: int,
                                   max_workers: int, existing_rows: dict,
                                   processed_papers: set) -> None:
        """Process row with all columns at once (multi-directory version)."""
        unprocessed_papers = [p for p in papers if p not in processed_papers]
        row_name = current_row.get("_row_name")

        if max_workers > 1 and len(unprocessed_papers) > 1:
            # Parallel processing
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_paper = {
                    executor.submit(
                        self._extract_values_from_paper_file,
                        paper, schema, retrieval_k, max_new_tokens, row_name
                    ): paper for paper in unprocessed_papers
                }

                for future in concurrent.futures.as_completed(future_to_paper):
                    # Check for stop request
                    if self.should_stop and self.should_stop():
                        print(f"🛑 Stop requested - cancelling pending futures")
                        self._stopped = True
                        # Cancel any pending futures
                        for pending_future in future_to_paper:
                            pending_future.cancel()
                        return

                    paper = future_to_paper[future]
                    try:
                        result = future.result()
                        if result:
                            # Merge result into current row
                            for key, value in result.items():
                                if key not in current_row:
                                    current_row[key] = value
                            processed_papers.add(paper)
                    except Exception as e:
                        print(f"⚠️  Error processing {paper.name}: {e}")
        else:
            # Sequential processing
            for paper in unprocessed_papers:
                # Check for stop request
                if self.should_stop and self.should_stop():
                    print(f"🛑 Stop requested during sequential processing")
                    self._stopped = True
                    return

                try:
                    result = self._extract_values_from_paper_file(
                        paper, schema, retrieval_k, max_new_tokens, row_name
                    )
                    # Check stop after extraction returns
                    if self.should_stop and self.should_stop():
                        self._stopped = True
                        return
                    if result:
                        # Merge result into current row
                        for key, value in result.items():
                            if key not in current_row:
                                current_row[key] = value
                        processed_papers.add(paper)
                except Exception as e:
                    print(f"⚠️  Error processing {paper.name}: {e}")
    
    def _extract_values_from_paper_file(self, paper_path: Path, schema: Schema,
                                       retrieval_k: int, max_new_tokens: int,
                                       row_name: str = None) -> dict:
        """Helper method to extract values from a paper file."""
        try:
            paper_text = paper_path.read_text(encoding="utf-8", errors="ignore")
            paper_title = paper_path.stem
            # Derive row_name if not provided
            if row_name is None:
                row_name = self.row_manager.extract_row_name_from_filename(paper_path.name)
            return self.paper_processor.extract_values_for_paper(
                paper_title, paper_text, schema, max_new_tokens,
                mode="all", retrieval_k=retrieval_k,
                row_name=row_name
            )
        except Exception as e:
            print(f"⚠️  Error reading or processing {paper_path.name}: {e}")
            return {}
    
    def build_table_jsonl(self,
                         schema_path: Path,
                         docs_directory: Path,
                         output_path: Path,
                         *,
                         max_new_tokens: int = DEFAULT_MAX_NEW_TOKENS,
                         resume: bool = False,
                         mode: str = "all",
                         retrieval_k: int = 8,
                         max_workers: int = DEFAULT_MAX_WORKERS) -> None:
        """
        Extract values from papers and write to JSONL, grouping by row names and merging intelligently.
        Each row represents a unique row name with data from potentially multiple papers.
        """
        # Load schema
        schema = self._load_schema(schema_path)

        docs = sorted(docs_directory.glob("*"))
        # Filter out system/hidden files
        docs = [d for d in docs if d.is_file() and not self._is_system_file(d.name)]
        if not docs:
            raise RuntimeError(f"No docs found under {docs_directory.resolve()}")

        # Group papers by row name
        print("📋 Grouping papers by row name...")
        papers_by_row = self.row_manager.group_papers_by_row(docs)
        
        print(f"📊 Found {len(papers_by_row)} unique row names from {len(docs)} papers")
        for row_name, papers in papers_by_row.items():
            print(f"  • {row_name}: {len(papers)} papers")

        # Resume logic - load existing rows and track completion
        existing_rows, processed_papers, completed_rows = (
            self._load_existing_data(output_path, papers_by_row) if resume else ({}, set(), set())
        )

        # Determine what work needs to be done
        papers_to_process = []
        for row_name, papers_for_row in papers_by_row.items():
            # Skip rows that are already complete
            if row_name in completed_rows:
                continue
                
            for doc_path in papers_for_row:
                paper_title = doc_path.stem
                if paper_title not in processed_papers:
                    papers_to_process.append(doc_path)
        
        if not papers_to_process:
            print("✅ All papers already processed. Nothing to do.")
            return
        
        # Track which rows have been written to avoid duplicates
        written_rows = set()
        
        # Initialize output file
        if not resume or not output_path.exists():
            # Start fresh
            with output_path.open('w', encoding="utf-8") as f:
                pass  # Create empty file
        else:
            # Create backup and start fresh for incremental writing
            backup_path = output_path.with_suffix(f".backup.{int(time.time())}")
            try:
                shutil.copy2(output_path, backup_path)
                print(f"🔒 Created backup: {backup_path}")
            except Exception as e:
                print(f"⚠️  Could not create backup: {e}")
            
            # Write existing complete rows first - preserve them exactly as they were
            with output_path.open('w', encoding="utf-8") as f_out:
                for row_name, row_data in existing_rows.items():
                    if row_name in completed_rows:
                        # Write existing completed rows exactly as they were (no modifications)
                        f_out.write(json.dumps(row_data, ensure_ascii=False) + "\n")
                        written_rows.add(row_name)

        # Process papers with incremental row writing
        paper_results = {}
        papers_processed = 0

        if max_workers == 0:
            # Sequential processing
            print(f"🔄 Processing {len(papers_to_process)} papers sequentially with incremental writing...")
            for doc_path in tqdm(papers_to_process, desc="processing papers"):
                # Check for stop request
                if self.should_stop and self.should_stop():
                    print(f"\n🛑 Stop requested - exiting after {papers_processed} papers")
                    self._stopped = True
                    break

                try:
                    row_name, paper_title, extracted_data = self.paper_processor.process_single_paper(
                        doc_path, schema, max_new_tokens, mode, retrieval_k, processed_papers
                    )
                    if extracted_data is not None:
                        paper_results[paper_title] = (row_name, extracted_data)
                        papers_processed += 1

                        # Try to write row if complete
                        self._write_row_if_complete(row_name, paper_results, papers_by_row, existing_rows,
                                                  written_rows, completed_rows, output_path, schema, docs_directory)

                except Exception as e:
                    print(f"⚠️  Error processing {doc_path.stem}: {e}")
        else:
            # Parallel processing with incremental writing
            print(f"🚀 Processing {len(papers_to_process)} papers using {max_workers} parallel workers with incremental writing...")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_paper = {
                    executor.submit(
                        self.paper_processor.process_single_paper,
                        doc_path, schema, max_new_tokens, mode, retrieval_k, processed_papers
                    ): doc_path
                    for doc_path in papers_to_process
                }

                for future in tqdm(as_completed(future_to_paper), total=len(papers_to_process), desc="processing papers"):
                    # Check for stop request
                    if self.should_stop and self.should_stop():
                        print(f"\n🛑 Stop requested - cancelling pending futures after {papers_processed} papers")
                        self._stopped = True
                        # Cancel any pending futures
                        for pending_future in future_to_paper:
                            pending_future.cancel()
                        break

                    doc_path = future_to_paper[future]
                    try:
                        row_name, paper_title, extracted_data = future.result()
                        if extracted_data is not None:
                            paper_results[paper_title] = (row_name, extracted_data)
                            papers_processed += 1

                            # Try to write row if complete
                            self._write_row_if_complete(row_name, paper_results, papers_by_row, existing_rows,
                                                      written_rows, completed_rows, output_path, schema, docs_directory)

                    except Exception as e:
                        print(f"⚠️  Error in parallel processing for {doc_path.stem}: {e}")

        # Final check: write any remaining incomplete rows
        print("📝 Writing any remaining incomplete rows...")
        remaining_rows = 0
        for row_name in papers_by_row.keys():
            if row_name not in written_rows and row_name not in completed_rows:
                self._write_row_if_complete(row_name, paper_results, papers_by_row, existing_rows,
                                          written_rows, completed_rows, output_path, schema, docs_directory)
                if row_name in written_rows:
                    remaining_rows += 1

        total_rows = len(written_rows) + len(completed_rows)
        print(f"✅ Incremental processing complete: {papers_processed} papers processed into {total_rows} total rows ➜ {output_path.resolve()}")
        if remaining_rows > 0:
            print(f"📄 Wrote {remaining_rows} additional incomplete rows at the end")