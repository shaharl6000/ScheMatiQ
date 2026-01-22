"""Local filesystem storage backend implementation."""

import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
import aiofiles.os

from app.storage.interface import StorageInterface, DatasetInfo, FileInfo, TemplateInfo, InitialSchemaInfo


class LocalStorageBackend(StorageInterface):
    """Storage backend using local filesystem.

    Maps logical buckets to local directories:
    - sessions -> ./sessions/
    - documents -> ./data/{session_id}/documents/
    - data -> ./data/{session_id}/
    - exports -> ./data/{session_id}/exports/
    - qbsd_work -> ./qbsd_work/{session_id}/
    """

    def __init__(
        self,
        sessions_dir: str = "./sessions",
        data_dir: str = "./data",
        qbsd_work_dir: str = "./qbsd_work",
        datasets_dir: str = "../research/data",
        templates_dir: str = "./templates",
        initial_schemas_dir: str = "./initial_schemas"
    ):
        """Initialize local storage backend.

        Args:
            sessions_dir: Directory for session files
            data_dir: Base directory for data files
            qbsd_work_dir: Directory for QBSD work files
            datasets_dir: Directory containing shared datasets (document collections)
            templates_dir: Directory containing template tables
            initial_schemas_dir: Directory containing initial schema files
        """
        self.sessions_dir = Path(sessions_dir)
        self.data_dir = Path(data_dir)
        self.qbsd_work_dir = Path(qbsd_work_dir)
        self.datasets_dir = Path(datasets_dir)
        self.templates_dir = Path(templates_dir)
        self.initial_schemas_dir = Path(initial_schemas_dir)

        # Ensure directories exist (except datasets which is read-only)
        self.sessions_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        self.qbsd_work_dir.mkdir(exist_ok=True)
        self.templates_dir.mkdir(exist_ok=True)
        self.initial_schemas_dir.mkdir(exist_ok=True)

    def _get_bucket_path(self, bucket: str, path: str = "") -> Path:
        """Map bucket and path to local filesystem path.

        Args:
            bucket: Logical bucket name
            path: Path within bucket

        Returns:
            Local filesystem Path
        """
        if bucket == "sessions":
            return self.sessions_dir / path
        elif bucket == "data":
            return self.data_dir / path
        elif bucket == "documents":
            # Documents are stored under data/{session_id}/documents/
            return self.data_dir / path
        elif bucket == "exports":
            # Exports are stored under data/{session_id}/exports/
            return self.data_dir / path
        elif bucket == "qbsd_work":
            return self.qbsd_work_dir / path
        else:
            # Default to data directory
            return self.data_dir / bucket / path

    # ==================
    # Session Operations
    # ==================

    async def save_session(self, session_id: str, data: Dict[str, Any]) -> bool:
        """Save session data to JSON file."""
        try:
            session_file = self.sessions_dir / f"{session_id}.json"
            async with aiofiles.open(session_file, 'w') as f:
                await f.write(json.dumps(data, indent=2, default=str))
            return True
        except Exception as e:
            print(f"Error saving session {session_id}: {e}")
            return False

    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Load session data from JSON file."""
        session_file = self.sessions_dir / f"{session_id}.json"
        if not session_file.exists():
            return None
        try:
            async with aiofiles.open(session_file, 'r') as f:
                content = await f.read()
                return json.loads(content)
        except Exception as e:
            print(f"Error loading session {session_id}: {e}")
            return None

    async def delete_session(self, session_id: str) -> bool:
        """Delete session and all associated data."""
        try:
            # Delete session file
            session_file = self.sessions_dir / f"{session_id}.json"
            if session_file.exists():
                session_file.unlink()

            # Delete data directory
            data_dir = self.data_dir / session_id
            if data_dir.exists():
                shutil.rmtree(data_dir)

            # Delete qbsd work directory
            work_dir = self.qbsd_work_dir / session_id
            if work_dir.exists():
                shutil.rmtree(work_dir)

            return True
        except Exception as e:
            print(f"Error deleting session {session_id}: {e}")
            return False

    async def list_sessions(self) -> List[str]:
        """List all session IDs from session files."""
        try:
            session_files = list(self.sessions_dir.glob("*.json"))
            return [f.stem for f in session_files]
        except Exception as e:
            print(f"Error listing sessions: {e}")
            return []

    # ================
    # File Operations
    # ================

    async def upload_file(
        self,
        bucket: str,
        path: str,
        data: bytes,
        content_type: Optional[str] = None
    ) -> str:
        """Upload file to local filesystem."""
        try:
            file_path = self._get_bucket_path(bucket, path)

            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(file_path, 'wb') as f:
                await f.write(data)

            return str(file_path)
        except Exception as e:
            print(f"Error uploading file {bucket}/{path}: {e}")
            raise

    async def download_file(self, bucket: str, path: str) -> Optional[bytes]:
        """Download file from local filesystem."""
        file_path = self._get_bucket_path(bucket, path)

        if not file_path.exists():
            return None

        try:
            async with aiofiles.open(file_path, 'rb') as f:
                return await f.read()
        except Exception as e:
            print(f"Error downloading file {bucket}/{path}: {e}")
            return None

    async def delete_file(self, bucket: str, path: str) -> bool:
        """Delete file from local filesystem."""
        try:
            file_path = self._get_bucket_path(bucket, path)
            if file_path.exists():
                file_path.unlink()
            return True
        except Exception as e:
            print(f"Error deleting file {bucket}/{path}: {e}")
            return False

    async def file_exists(self, bucket: str, path: str) -> bool:
        """Check if file exists in local filesystem."""
        file_path = self._get_bucket_path(bucket, path)
        return file_path.exists()

    async def list_folder_files(self, bucket: str, folder: str) -> set:
        """
        List all file names in a folder (optimized for batch existence checks).

        Returns a set of file names (not full paths) for efficient membership testing.

        Args:
            bucket: Storage bucket name
            folder: Folder path within the bucket

        Returns:
            Set of file names in the folder
        """
        try:
            folder_path = self._get_bucket_path(bucket, folder)
            if not folder_path.exists() or not folder_path.is_dir():
                return set()
            return {f.name for f in folder_path.iterdir() if f.is_file()}
        except Exception as e:
            print(f"Error listing folder {bucket}/{folder}: {e}")
            return set()

    async def list_files(self, bucket: str, prefix: str = "") -> List[str]:
        """List files in local directory with optional prefix filter."""
        try:
            base_path = self._get_bucket_path(bucket, prefix)

            if not base_path.exists():
                return []

            if base_path.is_file():
                return [prefix]

            # List all files recursively
            files = []
            for file_path in base_path.rglob("*"):
                if file_path.is_file():
                    # Return path relative to bucket
                    rel_path = file_path.relative_to(self._get_bucket_path(bucket, ""))
                    files.append(str(rel_path))

            return files
        except Exception as e:
            print(f"Error listing files {bucket}/{prefix}: {e}")
            return []

    # =====================
    # Directory Operations
    # =====================

    async def delete_directory(self, bucket: str, prefix: str) -> bool:
        """Delete all files under a directory prefix."""
        try:
            dir_path = self._get_bucket_path(bucket, prefix)
            if dir_path.exists() and dir_path.is_dir():
                shutil.rmtree(dir_path)
            return True
        except Exception as e:
            print(f"Error deleting directory {bucket}/{prefix}: {e}")
            return False

    # =================
    # Sync Helpers
    # =================

    def save_session_sync(self, session_id: str, data: Dict[str, Any]) -> bool:
        """Synchronous version of save_session for compatibility."""
        try:
            session_file = self.sessions_dir / f"{session_id}.json"
            with open(session_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            return True
        except Exception as e:
            print(f"Error saving session {session_id}: {e}")
            return False

    def get_session_sync(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Synchronous version of get_session for compatibility."""
        session_file = self.sessions_dir / f"{session_id}.json"
        if not session_file.exists():
            return None
        try:
            with open(session_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading session {session_id}: {e}")
            return None

    def upload_file_sync(
        self,
        bucket: str,
        path: str,
        data: bytes,
        content_type: Optional[str] = None
    ) -> str:
        """Synchronous version of upload_file for compatibility."""
        try:
            file_path = self._get_bucket_path(bucket, path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(data)
            return str(file_path)
        except Exception as e:
            print(f"Error uploading file {bucket}/{path}: {e}")
            raise

    def download_file_sync(self, bucket: str, path: str) -> Optional[bytes]:
        """Synchronous version of download_file for compatibility."""
        file_path = self._get_bucket_path(bucket, path)
        if not file_path.exists():
            return None
        try:
            with open(file_path, 'rb') as f:
                return f.read()
        except Exception as e:
            print(f"Error downloading file {bucket}/{path}: {e}")
            return None

    def file_exists_sync(self, bucket: str, path: str) -> bool:
        """Synchronous version of file_exists for compatibility."""
        file_path = self._get_bucket_path(bucket, path)
        return file_path.exists()

    def list_sessions_sync(self) -> List[str]:
        """Synchronous version of list_sessions."""
        try:
            session_files = list(self.sessions_dir.glob("*.json"))
            return [f.stem for f in session_files]
        except Exception as e:
            print(f"Error listing sessions: {e}")
            return []

    def get_local_path(self, bucket: str, path: str) -> Path:
        """Get the local filesystem path for a bucket/path.

        This is useful for operations that need direct filesystem access,
        such as passing paths to external libraries.
        """
        return self._get_bucket_path(bucket, path)

    # =======================
    # Dataset Operations
    # =======================

    def _resolve_datasets_dir(self) -> Optional[Path]:
        """Try to resolve the datasets directory from various locations."""
        # Try configured path first
        if self.datasets_dir.exists():
            return self.datasets_dir

        # Try relative to current working directory
        candidates = [
            Path("../research/data"),
            Path("../../research/data"),
            Path("research/data"),
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

        return None

    async def list_datasets(self) -> List[DatasetInfo]:
        """List available datasets from research/data directory."""
        datasets_path = self._resolve_datasets_dir()
        if not datasets_path or not datasets_path.exists():
            print(f"Datasets directory not found: {self.datasets_dir}")
            return []

        datasets = []
        try:
            for item in datasets_path.iterdir():
                if item.is_dir() and not item.name.startswith('.'):
                    # Count files in the dataset
                    file_count = sum(
                        1 for f in item.rglob("*")
                        if f.is_file() and not f.name.startswith('.')
                    )
                    if file_count > 0:
                        datasets.append(DatasetInfo(
                            name=item.name,
                            path=str(item),
                            file_count=file_count,
                            description=f"Document collection: {item.name}"
                        ))
        except Exception as e:
            print(f"Error listing datasets: {e}")

        return sorted(datasets, key=lambda d: d.name)

    async def list_dataset_files(self, dataset_name: str) -> List[FileInfo]:
        """List files in a specific dataset."""
        datasets_path = self._resolve_datasets_dir()
        if not datasets_path:
            return []

        dataset_path = datasets_path / dataset_name
        if not dataset_path.exists():
            return []

        files = []
        try:
            for file_path in dataset_path.rglob("*"):
                if file_path.is_file() and not file_path.name.startswith('.'):
                    # Get content type based on extension
                    ext = file_path.suffix.lower()
                    content_type = {
                        '.txt': 'text/plain',
                        '.md': 'text/markdown',
                        '.pdf': 'application/pdf',
                        '.json': 'application/json',
                        '.jsonl': 'application/jsonl',
                    }.get(ext, 'application/octet-stream')

                    files.append(FileInfo(
                        name=file_path.name,
                        path=str(file_path.relative_to(dataset_path)),
                        size=file_path.stat().st_size,
                        content_type=content_type
                    ))
        except Exception as e:
            print(f"Error listing dataset files: {e}")

        return sorted(files, key=lambda f: f.name)

    async def download_dataset_file(self, dataset_name: str, filename: str) -> Optional[bytes]:
        """Download a specific file from a dataset."""
        datasets_path = self._resolve_datasets_dir()
        if not datasets_path:
            return None

        file_path = datasets_path / dataset_name / filename
        if not file_path.exists():
            return None

        try:
            async with aiofiles.open(file_path, 'rb') as f:
                return await f.read()
        except Exception as e:
            print(f"Error downloading dataset file {dataset_name}/{filename}: {e}")
            return None

    async def download_dataset_to_local(self, dataset_name: str, local_dir: str) -> List[str]:
        """Download/copy all files from a dataset to a local directory.

        For local storage, this is just a copy operation.
        """
        datasets_path = self._resolve_datasets_dir()
        if not datasets_path:
            return []

        dataset_path = datasets_path / dataset_name
        if not dataset_path.exists():
            return []

        local_path = Path(local_dir)
        local_path.mkdir(parents=True, exist_ok=True)

        created_files = []
        try:
            for file_path in dataset_path.rglob("*"):
                if file_path.is_file() and not file_path.name.startswith('.'):
                    dest_path = local_path / file_path.name
                    shutil.copy2(file_path, dest_path)
                    created_files.append(str(dest_path))
        except Exception as e:
            print(f"Error copying dataset to local: {e}")

        return created_files

    # =======================
    # Template Operations
    # =======================

    async def list_templates(self) -> List[TemplateInfo]:
        """List available template tables."""
        if not self.templates_dir.exists():
            return []

        templates = []
        try:
            for file_path in self.templates_dir.iterdir():
                if file_path.is_file() and not file_path.name.startswith('.'):
                    ext = file_path.suffix.lower()
                    if ext in ['.csv', '.json', '.jsonl']:
                        # Try to get row/column count
                        row_count = None
                        column_count = None

                        try:
                            if ext == '.csv':
                                with open(file_path, 'r') as f:
                                    lines = f.readlines()
                                    # Skip comment lines (metadata) and empty lines
                                    data_lines = [l for l in lines if l.strip() and not l.strip().startswith('#')]
                                    if data_lines:
                                        row_count = len(data_lines) - 1  # Exclude header
                                        # Parse header to count actual data columns
                                        header = data_lines[0].strip().split(',')
                                        # Exclude metadata and excerpt columns
                                        metadata_cols = {'document_directory', 'papers', 'row_name'}
                                        data_columns = [
                                            col for col in header
                                            if not col.endswith('_excerpt') and col not in metadata_cols
                                        ]
                                        column_count = len(data_columns)
                            elif ext == '.jsonl':
                                with open(file_path, 'r') as f:
                                    lines = [l for l in f if l.strip()]
                                    row_count = len(lines)
                                    if lines:
                                        first_row = json.loads(lines[0])
                                        column_count = len(first_row.get('data', first_row).keys())
                            elif ext == '.json':
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                    if isinstance(data, list):
                                        row_count = len(data)
                                        if data:
                                            column_count = len(data[0].keys())
                        except Exception:
                            pass  # Ignore count errors

                        templates.append(TemplateInfo(
                            name=file_path.stem,
                            path=str(file_path),
                            file_type=ext[1:],  # Remove the dot
                            description=f"Template: {file_path.stem}",
                            row_count=row_count,
                            column_count=column_count
                        ))
        except Exception as e:
            print(f"Error listing templates: {e}")

        return sorted(templates, key=lambda t: t.name)

    async def download_template(self, template_name: str) -> Optional[bytes]:
        """Download a template file."""
        if not self.templates_dir.exists():
            return None

        # Try different extensions
        for ext in ['.csv', '.json', '.jsonl']:
            file_path = self.templates_dir / f"{template_name}{ext}"
            if file_path.exists():
                try:
                    async with aiofiles.open(file_path, 'rb') as f:
                        return await f.read()
                except Exception as e:
                    print(f"Error downloading template {template_name}: {e}")
                    return None

        # Try exact filename
        file_path = self.templates_dir / template_name
        if file_path.exists():
            try:
                async with aiofiles.open(file_path, 'rb') as f:
                    return await f.read()
            except Exception as e:
                print(f"Error downloading template {template_name}: {e}")

        return None

    # =======================
    # Initial Schema Operations
    # =======================

    async def list_initial_schemas(self) -> List[InitialSchemaInfo]:
        """List available initial schema files."""
        if not self.initial_schemas_dir.exists():
            return []

        schemas = []
        try:
            for file_path in self.initial_schemas_dir.iterdir():
                if file_path.is_file() and not file_path.name.startswith('.'):
                    ext = file_path.suffix.lower()
                    if ext == '.json':
                        # Parse the schema to get column info
                        columns = []
                        columns_count = 0
                        preview = ""

                        try:
                            with open(file_path, 'r') as f:
                                data = json.load(f)

                            # Handle both list format and object with columns key
                            if isinstance(data, list):
                                columns = data
                            elif isinstance(data, dict) and 'columns' in data:
                                columns = data['columns']

                            columns_count = len(columns)
                            # Create preview from first 3 column names
                            column_names = [col.get('name', '') for col in columns[:3]]
                            preview = ', '.join(column_names)
                            if columns_count > 3:
                                preview += f", ... (+{columns_count - 3} more)"
                        except Exception as e:
                            print(f"Error parsing schema {file_path.name}: {e}")
                            continue

                        if columns_count > 0:
                            schemas.append(InitialSchemaInfo(
                                name=file_path.stem,
                                path=str(file_path),
                                file_type='json',
                                columns_count=columns_count,
                                preview=preview,
                                columns=columns
                            ))
        except Exception as e:
            print(f"Error listing initial schemas: {e}")

        return sorted(schemas, key=lambda s: s.name)

    async def download_initial_schema(self, schema_name: str) -> Optional[bytes]:
        """Download an initial schema file."""
        if not self.initial_schemas_dir.exists():
            return None

        # Try with .json extension
        file_path = self.initial_schemas_dir / f"{schema_name}.json"
        if file_path.exists():
            try:
                async with aiofiles.open(file_path, 'rb') as f:
                    return await f.read()
            except Exception as e:
                print(f"Error downloading initial schema {schema_name}: {e}")
                return None

        # Try exact filename
        file_path = self.initial_schemas_dir / schema_name
        if file_path.exists():
            try:
                async with aiofiles.open(file_path, 'rb') as f:
                    return await f.read()
            except Exception as e:
                print(f"Error downloading initial schema {schema_name}: {e}")

        return None

    async def upload_initial_schema(
        self,
        schema_name: str,
        data: bytes,
        content_type: Optional[str] = None
    ) -> str:
        """Upload an initial schema file to local storage."""
        try:
            # Ensure filename has .json extension
            if not schema_name.endswith('.json'):
                schema_name = f"{schema_name}.json"

            file_path = self.initial_schemas_dir / schema_name

            # Ensure directory exists
            self.initial_schemas_dir.mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(file_path, 'wb') as f:
                await f.write(data)

            return str(file_path)
        except Exception as e:
            print(f"Error uploading initial schema {schema_name}: {e}")
            raise
