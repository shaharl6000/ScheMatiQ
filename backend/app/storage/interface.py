"""Abstract storage interface for backend storage operations."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class DatasetInfo:
    """Information about a dataset (document collection)."""
    name: str
    path: str
    file_count: int
    description: Optional[str] = None


@dataclass
class FileInfo:
    """Information about a file."""
    name: str
    path: str
    size: int
    content_type: Optional[str] = None


@dataclass
class TemplateInfo:
    """Information about a template (pre-made table)."""
    name: str
    path: str
    file_type: str  # csv, json, jsonl
    description: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None


@dataclass
class InitialSchemaInfo:
    """Information about an initial schema file."""
    name: str
    path: str
    file_type: str  # json
    columns_count: int
    preview: str  # First few column names as preview
    columns: List[Dict[str, Any]]  # Full column data


class StorageInterface(ABC):
    """Abstract base class defining the storage contract.

    All storage backends (local filesystem, Supabase, etc.) must implement
    this interface to ensure consistent behavior across the application.
    """

    # ==================
    # Session Operations
    # ==================

    @abstractmethod
    async def save_session(self, session_id: str, data: Dict[str, Any]) -> bool:
        """Save session data.

        Args:
            session_id: Unique session identifier
            data: Session data as dictionary

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve session data.

        Args:
            session_id: Unique session identifier

        Returns:
            Session data as dictionary, or None if not found
        """
        pass

    @abstractmethod
    async def delete_session(self, session_id: str) -> bool:
        """Delete a session and all associated data.

        Args:
            session_id: Unique session identifier

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    async def list_sessions(self) -> List[str]:
        """List all session IDs.

        Returns:
            List of session IDs
        """
        pass

    # ================
    # File Operations
    # ================

    @abstractmethod
    async def upload_file(
        self,
        bucket: str,
        path: str,
        data: bytes,
        content_type: Optional[str] = None
    ) -> str:
        """Upload a file to storage.

        Args:
            bucket: Storage bucket name (e.g., 'documents', 'data', 'exports')
            path: Path within the bucket (e.g., '{session_id}/file.pdf')
            data: File content as bytes
            content_type: MIME type of the file

        Returns:
            Full path to the uploaded file
        """
        pass

    @abstractmethod
    async def download_file(self, bucket: str, path: str) -> Optional[bytes]:
        """Download a file from storage.

        Args:
            bucket: Storage bucket name
            path: Path within the bucket

        Returns:
            File content as bytes, or None if not found
        """
        pass

    @abstractmethod
    async def delete_file(self, bucket: str, path: str) -> bool:
        """Delete a file from storage.

        Args:
            bucket: Storage bucket name
            path: Path within the bucket

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    async def file_exists(self, bucket: str, path: str) -> bool:
        """Check if a file exists in storage.

        Args:
            bucket: Storage bucket name
            path: Path within the bucket

        Returns:
            True if file exists, False otherwise
        """
        pass

    @abstractmethod
    async def list_folder_files(self, bucket: str, folder: str) -> set:
        """List all file names in a folder (optimized for batch existence checks).

        Returns a set of file names (not full paths) for efficient membership testing.
        This is much faster than calling file_exists() for each file when checking
        multiple files in the same folder.

        Args:
            bucket: Storage bucket name
            folder: Folder path within the bucket

        Returns:
            Set of file names in the folder
        """
        pass

    @abstractmethod
    async def list_files(self, bucket: str, prefix: str = "") -> List[str]:
        """List files in a bucket with optional prefix filter.

        Args:
            bucket: Storage bucket name
            prefix: Path prefix to filter files (e.g., '{session_id}/')

        Returns:
            List of file paths
        """
        pass

    # =====================
    # Directory Operations
    # =====================

    @abstractmethod
    async def delete_directory(self, bucket: str, prefix: str) -> bool:
        """Delete all files under a directory prefix.

        Args:
            bucket: Storage bucket name
            prefix: Directory prefix (e.g., '{session_id}/')

        Returns:
            True if successful, False otherwise
        """
        pass

    # ==================
    # Text File Helpers
    # ==================

    async def upload_text(
        self,
        bucket: str,
        path: str,
        text: str,
        encoding: str = "utf-8"
    ) -> str:
        """Upload text content as a file.

        Args:
            bucket: Storage bucket name
            path: Path within the bucket
            text: Text content
            encoding: Text encoding (default: utf-8)

        Returns:
            Full path to the uploaded file
        """
        return await self.upload_file(
            bucket,
            path,
            text.encode(encoding),
            content_type="text/plain"
        )

    async def download_text(
        self,
        bucket: str,
        path: str,
        encoding: str = "utf-8"
    ) -> Optional[str]:
        """Download a file as text content.

        Args:
            bucket: Storage bucket name
            path: Path within the bucket
            encoding: Text encoding (default: utf-8)

        Returns:
            Text content, or None if not found
        """
        data = await self.download_file(bucket, path)
        if data is None:
            return None
        return data.decode(encoding)

    async def upload_json(self, bucket: str, path: str, data: Dict[str, Any]) -> str:
        """Upload JSON data as a file.

        Args:
            bucket: Storage bucket name
            path: Path within the bucket
            data: Dictionary to serialize as JSON

        Returns:
            Full path to the uploaded file
        """
        import json
        json_str = json.dumps(data, indent=2, default=str)
        return await self.upload_file(
            bucket,
            path,
            json_str.encode("utf-8"),
            content_type="application/json"
        )

    async def download_json(self, bucket: str, path: str) -> Optional[Dict[str, Any]]:
        """Download a JSON file as a dictionary.

        Args:
            bucket: Storage bucket name
            path: Path within the bucket

        Returns:
            Parsed JSON as dictionary, or None if not found
        """
        import json
        data = await self.download_file(bucket, path)
        if data is None:
            return None
        return json.loads(data.decode("utf-8"))

    # =================
    # URL Generation
    # =================

    def get_public_url(self, bucket: str, path: str) -> Optional[str]:
        """Get a public URL for a file (if supported).

        Args:
            bucket: Storage bucket name
            path: Path within the bucket

        Returns:
            Public URL string, or None if not supported
        """
        return None

    # =======================
    # Dataset Operations
    # =======================

    @abstractmethod
    async def list_datasets(self) -> List[DatasetInfo]:
        """List available datasets (document collections).

        Datasets are collections of documents that can be used for ScheMatiQ processing.
        In local mode, these come from research/data/.
        In Supabase mode, these come from the datasets/ bucket.

        Returns:
            List of DatasetInfo objects
        """
        pass

    @abstractmethod
    async def list_dataset_files(self, dataset_name: str) -> List[FileInfo]:
        """List files in a specific dataset.

        Args:
            dataset_name: Name of the dataset (e.g., 'abstracts', 'full_text')

        Returns:
            List of FileInfo objects for files in the dataset
        """
        pass

    @abstractmethod
    async def download_dataset_file(self, dataset_name: str, filename: str) -> Optional[bytes]:
        """Download a specific file from a dataset.

        Args:
            dataset_name: Name of the dataset
            filename: Name of the file within the dataset

        Returns:
            File content as bytes, or None if not found
        """
        pass

    @abstractmethod
    async def download_dataset_to_local(self, dataset_name: str, local_dir: str) -> List[str]:
        """Download all files from a dataset to a local directory.

        This is used for ScheMatiQ processing, which requires local file access.

        Args:
            dataset_name: Name of the dataset
            local_dir: Local directory path to download files to

        Returns:
            List of local file paths that were created
        """
        pass

    # =======================
    # Template Operations
    # =======================

    @abstractmethod
    async def list_templates(self) -> List[TemplateInfo]:
        """List available templates (pre-made tables).

        Templates are pre-created ScheMatiQ results that users can load.
        In local mode, these come from a configured templates directory.
        In Supabase mode, these come from the templates/ bucket.

        Returns:
            List of TemplateInfo objects
        """
        pass

    @abstractmethod
    async def download_template(self, template_name: str) -> Optional[bytes]:
        """Download a template file.

        Args:
            template_name: Name of the template file

        Returns:
            Template file content as bytes, or None if not found
        """
        pass

    # =======================
    # Initial Schema Operations
    # =======================

    @abstractmethod
    async def list_initial_schemas(self) -> List["InitialSchemaInfo"]:
        """List available initial schema files.

        Initial schemas are JSON files containing column definitions
        that can be used to seed the ScheMatiQ schema discovery process.
        In local mode, these come from a configured initial_schemas directory.
        In Supabase mode, these come from the initial_schemas/ bucket.

        Returns:
            List of InitialSchemaInfo objects
        """
        pass

    @abstractmethod
    async def download_initial_schema(self, schema_name: str) -> Optional[bytes]:
        """Download an initial schema file.

        Args:
            schema_name: Name of the initial schema file

        Returns:
            Schema file content as bytes, or None if not found
        """
        pass

    @abstractmethod
    async def upload_initial_schema(
        self,
        schema_name: str,
        data: bytes,
        content_type: Optional[str] = None
    ) -> str:
        """Upload an initial schema file.

        Args:
            schema_name: Name for the schema file
            data: Schema file content as bytes
            content_type: MIME type (typically application/json)

        Returns:
            Full path to the uploaded schema file
        """
        pass
