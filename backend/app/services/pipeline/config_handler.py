"""Configuration validation, conversion, and document path resolution."""

import logging
from typing import Dict, Any, Optional, List
from pathlib import Path

from app.core.config import MAX_DOCUMENTS, DEVELOPER_MODE
from app.models.schematiq import ScheMatiQConfig
from app.storage import get_storage

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


async def download_supabase_dataset(dataset_name: str, session_dir: Path) -> Optional[str]:
    """Download a Supabase dataset to local directory.

    Returns:
        Local path to downloaded dataset, or None if not a Supabase dataset
    """
    storage = get_storage()

    try:
        datasets = await storage.list_datasets()
        dataset_names = [d.name for d in datasets]

        if dataset_name in dataset_names:
            local_dataset_dir = session_dir / "datasets" / dataset_name
            local_dataset_dir.mkdir(parents=True, exist_ok=True)

            logger.info("Downloading Supabase dataset '%s' to %s", dataset_name, local_dataset_dir)
            downloaded_files = await storage.download_dataset_to_local(dataset_name, str(local_dataset_dir))
            logger.info("Downloaded %d files from '%s'", len(downloaded_files), dataset_name)

            return str(local_dataset_dir)
    except Exception as e:
        logger.warning("Error checking/downloading Supabase dataset '%s': %s", dataset_name, e)

    return None


async def resolve_docs_paths(config: ScheMatiQConfig, session_id: str, work_dir: Path) -> List[str]:
    """Resolve document paths - download from Supabase if needed, or use uploaded files."""
    session_dir = work_dir / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    # Check for uploaded documents in data/{session_id}/pending_documents/ directory
    data_dir = Path("./data") / session_id / "pending_documents"
    if data_dir.exists():
        uploaded_files = [f for f in sorted(data_dir.iterdir())
                        if f.is_file() and not f.name.startswith('.')]
        if uploaded_files:
            logger.info("Using %d uploaded documents from %s", len(uploaded_files), data_dir)
            return [str(data_dir.absolute())]

    # No uploaded files - resolve from config.docs_path
    docs_paths = config.docs_path if isinstance(config.docs_path, list) else [config.docs_path]
    docs_paths = [p for p in docs_paths if p]

    if not docs_paths:
        logger.warning("No document paths configured and no uploaded documents found")
        return []

    resolved_docs_paths = []

    for path in docs_paths:
        supabase_path = await download_supabase_dataset(path, session_dir)
        if supabase_path:
            resolved_docs_paths.append(supabase_path)
            continue

        doc_path = Path(path)
        if not doc_path.is_absolute():
            candidates = [
                PROJECT_ROOT / path,
                PROJECT_ROOT / "research" / "data" / Path(path).name,
                PROJECT_ROOT / "test" / "files",
                Path.cwd() / path,
                Path.cwd().parent / path,
            ]
            for candidate in candidates:
                if candidate.exists():
                    resolved_docs_paths.append(str(candidate.absolute()))
                    logger.info("Resolved document path: %s -> %s", path, candidate.absolute())
                    break
            else:
                logger.warning("Document path not found: %s", path)
                resolved_docs_paths.append(path)
        else:
            resolved_docs_paths.append(str(doc_path))

    return resolved_docs_paths


def convert_config_to_schematiq_format(
    config: ScheMatiQConfig,
    session_id: str,
    work_dir: Path,
    resolved_docs_paths: List[str]
) -> Dict[str, Any]:
    """Convert frontend config to ScheMatiQ format with pre-resolved paths."""
    session_dir = work_dir / session_id

    schematiq_config = {
        "query": config.query,
        "docs_path": resolved_docs_paths[0] if len(resolved_docs_paths) == 1 else resolved_docs_paths,
        "max_keys_schema": config.max_keys_schema,
        "documents_batch_size": config.documents_batch_size,
        "output_path": str(session_dir / "discovered_schema.json"),
        "document_randomization_seed": config.document_randomization_seed,
        "skip_value_extraction": config.skip_value_extraction,
        "convergence_threshold": config.convergence_threshold,
        "schema_creation_backend": {
            "provider": config.schema_creation_backend.provider,
            "model": config.schema_creation_backend.model,
            "max_output_tokens": config.schema_creation_backend.max_output_tokens,
            "temperature": config.schema_creation_backend.temperature,
            "context_window_size": config.schema_creation_backend.context_window_size,
            "api_key": config.schema_creation_backend.api_key
        },
        "value_extraction_backend": {
            "provider": config.value_extraction_backend.provider,
            "model": config.value_extraction_backend.model,
            "max_output_tokens": config.value_extraction_backend.max_output_tokens,
            "temperature": config.value_extraction_backend.temperature,
            "context_window_size": config.value_extraction_backend.context_window_size,
            "api_key": config.value_extraction_backend.api_key
        }
    }

    if config.retriever:
        schematiq_config["retriever"] = {
            "type": "embedding",
            "model_name": config.retriever.model_name,
            "k": config.retriever.k,
            "passage_chars": config.retriever.passage_chars,
            "overlap": config.retriever.overlap,
            "enable_dynamic_k": config.retriever.enable_dynamic_k,
            "dynamic_k_threshold": config.retriever.dynamic_k_threshold,
            "dynamic_k_minimum": config.retriever.dynamic_k_minimum
        }

    if config.initial_schema:
        schematiq_config["initial_schema"] = [
            {
                "name": col.name,
                "definition": col.definition,
                "rationale": col.rationale,
                "allowed_values": col.allowed_values,
                "locked": col.locked,
            }
            for col in config.initial_schema
        ]
    elif config.initial_schema_path:
        initial_schema_path = Path(config.initial_schema_path)
        if not initial_schema_path.is_absolute():
            initial_schema_path = (PROJECT_ROOT / initial_schema_path).resolve()
        if initial_schema_path.exists():
            schematiq_config["initial_schema_path"] = str(initial_schema_path)

    if config.initial_observation_unit:
        schematiq_config["initial_observation_unit"] = {
            "name": config.initial_observation_unit.name,
            "definition": config.initial_observation_unit.definition
        }

    schematiq_config["review_observation_unit"] = config.review_observation_unit

    return schematiq_config


async def validate_config(config: ScheMatiQConfig) -> Dict[str, Any]:
    """Validate ScheMatiQ configuration.

    Supports three modes:
    - Standard: Both query and documents provided
    - Document-only: Documents provided, no query
    - Query-only: Query provided, no documents

    At least one of query or documents must be provided.
    """
    errors = []
    warnings = []

    has_query = bool(config.query and config.query.strip())
    docs_paths = config.docs_path if isinstance(config.docs_path, list) else [config.docs_path]
    has_documents = bool(docs_paths and any(p for p in docs_paths if p)) or config.upload_pending

    if not has_query and not has_documents:
        errors.append("At least one of query or documents must be provided")

    actual_paths = [p for p in docs_paths if p]
    if actual_paths:
        for path in actual_paths:
            doc_path = Path(path)
            logger.debug("Checking document path: %s -> %s", path, doc_path.absolute())

            paths_to_try = [
                doc_path,
                Path("..") / path,
                Path("../..") / path,
                Path("../../..") / path,
                Path("../../..") / "test" / "files",
                PROJECT_ROOT / "test" / "files",
                PROJECT_ROOT / "research" / "data" / "file",
                Path("../test/files"),
            ]

            path_exists = False
            actual_path = None
            for try_path in paths_to_try:
                if try_path.exists():
                    path_exists = True
                    actual_path = try_path.absolute()
                    logger.debug("Found path at: %s", actual_path)
                    try:
                        if actual_path.is_dir():
                            file_count = len(list(try_path.glob("*.txt"))) + len(list(try_path.glob("*.md")))
                            if file_count == 0:
                                warnings.append(f"Document path appears to be empty: {path} (no .txt or .md files)")
                            else:
                                logger.debug("Found %d document files", file_count)
                    except Exception as e:
                        warnings.append(f"Could not check document count in {path}: {e}")
                    break

            if not path_exists:
                warnings.append(f"Document path does not exist: {path}. Try using 'test/files' which contains sample documents.")

    if config.initial_schema_path:
        schema_path = Path(config.initial_schema_path)
        if not schema_path.is_absolute():
            schema_path = (PROJECT_ROOT / schema_path).resolve()
        if not schema_path.exists():
            errors.append(f"Initial schema file does not exist: {config.initial_schema_path} (resolved: {schema_path})")

    if not config.schema_creation_backend.provider:
        errors.append("Schema creation LLM provider must be specified")

    if not config.schema_creation_backend.model:
        if config.schema_creation_backend.provider.lower() not in ["gemini"]:
            errors.append("Schema creation LLM model must be specified (required for non-Gemini providers)")

    if not config.value_extraction_backend.provider:
        errors.append("Value extraction LLM provider must be specified")

    if not config.value_extraction_backend.model:
        if config.value_extraction_backend.provider.lower() not in ["gemini"]:
            errors.append("Value extraction LLM model must be specified (required for non-Gemini providers)")

    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }
