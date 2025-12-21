"""Supabase cloud storage backend implementation."""

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from supabase import create_client, Client

from app.storage.interface import StorageInterface, DatasetInfo, FileInfo, TemplateInfo

logger = logging.getLogger(__name__)


class SupabaseStorageBackend(StorageInterface):
    """Storage backend using Supabase Storage.

    Bucket structure:
    - sessions/{session_id}.json - Session metadata
    - documents/{session_id}/... - User uploaded documents
    - data/{session_id}/... - Processed data files (JSONL, schemas)
    - exports/{session_id}/... - Generated exports
    """

    # Required buckets for the application
    REQUIRED_BUCKETS = ["sessions", "documents", "data", "exports", "datasets", "templates"]

    def __init__(self, url: str, key: str):
        """Initialize Supabase storage backend.

        Args:
            url: Supabase project URL
            key: Supabase anon/service key
        """
        logger.info(f"Initializing Supabase storage backend with URL: {url[:30]}...")
        self.client: Client = create_client(url, key)
        self._verify_buckets()
        logger.info("Supabase storage backend initialized successfully")

    def _verify_buckets(self) -> None:
        """Verify required buckets exist in Supabase."""
        try:
            existing_buckets = self.client.storage.list_buckets()
            existing_names = {b.name for b in existing_buckets}
            logger.info(f"Found Supabase buckets: {existing_names}")

            missing = set(self.REQUIRED_BUCKETS) - existing_names
            if missing:
                logger.warning(f"Missing Supabase buckets: {missing}")
                logger.warning("Please create these buckets in your Supabase dashboard.")
            else:
                logger.info("All required buckets exist")
        except Exception as e:
            logger.error(f"Could not verify Supabase buckets: {e}", exc_info=True)

    def _get_storage_path(self, bucket: str, path: str) -> str:
        """Normalize path for Supabase storage.

        Args:
            bucket: Bucket name
            path: Path within bucket

        Returns:
            Normalized path string
        """
        # Remove leading slashes
        return path.lstrip("/")

    # ==================
    # Session Operations
    # ==================

    async def save_session(self, session_id: str, data: Dict[str, Any]) -> bool:
        """Save session data to Supabase storage."""
        try:
            content = json.dumps(data, indent=2, default=str).encode("utf-8")
            path = f"{session_id}.json"

            # Check if file exists and remove it first (Supabase doesn't overwrite)
            try:
                self.client.storage.from_("sessions").remove([path])
            except Exception:
                pass  # File might not exist

            self.client.storage.from_("sessions").upload(
                path,
                content,
                {"content-type": "application/json"}
            )
            return True
        except Exception as e:
            print(f"Error saving session {session_id} to Supabase: {e}")
            return False

    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve session data from Supabase storage."""
        try:
            path = f"{session_id}.json"
            response = self.client.storage.from_("sessions").download(path)
            return json.loads(response.decode("utf-8"))
        except Exception as e:
            # Session not found is expected for new sessions
            if "not found" not in str(e).lower():
                print(f"Error loading session {session_id} from Supabase: {e}")
            return None

    async def delete_session(self, session_id: str) -> bool:
        """Delete session and all associated data from Supabase."""
        try:
            # Delete session file
            try:
                self.client.storage.from_("sessions").remove([f"{session_id}.json"])
            except Exception:
                pass

            # Delete files from all buckets for this session
            for bucket in ["documents", "data", "exports"]:
                await self.delete_directory(bucket, f"{session_id}/")

            return True
        except Exception as e:
            print(f"Error deleting session {session_id} from Supabase: {e}")
            return False

    async def list_sessions(self) -> List[str]:
        """List all session IDs from Supabase storage."""
        try:
            files = self.client.storage.from_("sessions").list()
            return [
                f["name"].replace(".json", "")
                for f in files
                if f["name"].endswith(".json")
            ]
        except Exception as e:
            print(f"Error listing sessions from Supabase: {e}")
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
        """Upload file to Supabase storage."""
        try:
            storage_path = self._get_storage_path(bucket, path)

            # Remove existing file first (Supabase doesn't overwrite by default)
            try:
                self.client.storage.from_(bucket).remove([storage_path])
            except Exception:
                pass

            options = {}
            if content_type:
                options["content-type"] = content_type

            self.client.storage.from_(bucket).upload(
                storage_path,
                data,
                options if options else None
            )

            return f"{bucket}/{storage_path}"
        except Exception as e:
            print(f"Error uploading file {bucket}/{path} to Supabase: {e}")
            raise

    async def download_file(self, bucket: str, path: str) -> Optional[bytes]:
        """Download file from Supabase storage."""
        try:
            storage_path = self._get_storage_path(bucket, path)
            return self.client.storage.from_(bucket).download(storage_path)
        except Exception as e:
            if "not found" not in str(e).lower():
                print(f"Error downloading file {bucket}/{path} from Supabase: {e}")
            return None

    async def delete_file(self, bucket: str, path: str) -> bool:
        """Delete file from Supabase storage."""
        try:
            storage_path = self._get_storage_path(bucket, path)
            self.client.storage.from_(bucket).remove([storage_path])
            return True
        except Exception as e:
            print(f"Error deleting file {bucket}/{path} from Supabase: {e}")
            return False

    async def file_exists(self, bucket: str, path: str) -> bool:
        """Check if file exists in Supabase storage."""
        try:
            storage_path = self._get_storage_path(bucket, path)

            # Try to get file info by listing with prefix
            # Supabase doesn't have a direct "exists" check
            parts = storage_path.rsplit("/", 1)
            if len(parts) == 2:
                folder, filename = parts
                files = self.client.storage.from_(bucket).list(folder)
            else:
                filename = parts[0]
                files = self.client.storage.from_(bucket).list()

            return any(f["name"] == filename for f in files)
        except Exception:
            return False

    async def list_files(self, bucket: str, prefix: str = "") -> List[str]:
        """List files in Supabase bucket with optional prefix filter."""
        try:
            storage_prefix = self._get_storage_path(bucket, prefix)

            # List files in the prefix directory
            files = self.client.storage.from_(bucket).list(storage_prefix or None)

            result = []
            for f in files:
                if f.get("id"):  # It's a file, not a folder
                    file_path = f"{storage_prefix}/{f['name']}" if storage_prefix else f["name"]
                    result.append(file_path)
                else:
                    # It's a folder, recursively list
                    subfolder = f"{storage_prefix}/{f['name']}" if storage_prefix else f["name"]
                    subfiles = await self.list_files(bucket, subfolder)
                    result.extend(subfiles)

            return result
        except Exception as e:
            print(f"Error listing files {bucket}/{prefix} from Supabase: {e}")
            return []

    # =====================
    # Directory Operations
    # =====================

    async def delete_directory(self, bucket: str, prefix: str) -> bool:
        """Delete all files under a directory prefix in Supabase."""
        try:
            # List all files with the prefix
            files = await self.list_files(bucket, prefix)

            if files:
                # Remove all files
                self.client.storage.from_(bucket).remove(files)

            return True
        except Exception as e:
            print(f"Error deleting directory {bucket}/{prefix} from Supabase: {e}")
            return False

    # =================
    # URL Generation
    # =================

    def get_public_url(self, bucket: str, path: str) -> Optional[str]:
        """Get public URL for a file in Supabase storage."""
        try:
            storage_path = self._get_storage_path(bucket, path)
            response = self.client.storage.from_(bucket).get_public_url(storage_path)
            return response
        except Exception as e:
            print(f"Error getting public URL for {bucket}/{path}: {e}")
            return None

    def get_signed_url(
        self,
        bucket: str,
        path: str,
        expires_in: int = 3600
    ) -> Optional[str]:
        """Get a signed URL for private file access.

        Args:
            bucket: Bucket name
            path: Path within bucket
            expires_in: URL expiration time in seconds (default: 1 hour)

        Returns:
            Signed URL string, or None on error
        """
        try:
            storage_path = self._get_storage_path(bucket, path)
            response = self.client.storage.from_(bucket).create_signed_url(
                storage_path,
                expires_in
            )
            return response.get("signedURL")
        except Exception as e:
            print(f"Error creating signed URL for {bucket}/{path}: {e}")
            return None

    # =================
    # Sync Helpers
    # =================

    def save_session_sync(self, session_id: str, data: Dict[str, Any]) -> bool:
        """Synchronous version of save_session."""
        import asyncio
        try:
            # Check if we're already in an async context
            loop = asyncio.get_running_loop()
            # If we get here, we're in an async context - use a new thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, self.save_session(session_id, data))
                return future.result()
        except RuntimeError:
            # No running loop, safe to use asyncio.run
            return asyncio.run(self.save_session(session_id, data))

    def get_session_sync(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Synchronous version of get_session."""
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, self.get_session(session_id))
                return future.result()
        except RuntimeError:
            return asyncio.run(self.get_session(session_id))

    def upload_file_sync(
        self,
        bucket: str,
        path: str,
        data: bytes,
        content_type: Optional[str] = None
    ) -> str:
        """Synchronous version of upload_file."""
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, self.upload_file(bucket, path, data, content_type))
                return future.result()
        except RuntimeError:
            return asyncio.run(self.upload_file(bucket, path, data, content_type))

    def download_file_sync(self, bucket: str, path: str) -> Optional[bytes]:
        """Synchronous version of download_file."""
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, self.download_file(bucket, path))
                return future.result()
        except RuntimeError:
            return asyncio.run(self.download_file(bucket, path))

    def file_exists_sync(self, bucket: str, path: str) -> bool:
        """Synchronous version of file_exists."""
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, self.file_exists(bucket, path))
                return future.result()
        except RuntimeError:
            return asyncio.run(self.file_exists(bucket, path))

    # =======================
    # Dataset Operations
    # =======================

    async def list_datasets(self) -> List[DatasetInfo]:
        """List available datasets from the datasets bucket."""
        try:
            logger.info("Fetching datasets from Supabase 'datasets' bucket...")
            # List top-level folders in datasets bucket
            items = self.client.storage.from_("datasets").list()
            logger.info(f"Supabase returned {len(items)} items from datasets bucket")
            logger.info(f"Raw items: {items}")

            datasets = []
            for item in items:
                logger.debug(f"Processing item: {item}")
                # Check if it's a folder (no id means folder)
                if not item.get("id"):
                    dataset_name = item["name"]
                    logger.info(f"Found folder (dataset): {dataset_name}")
                    # Count files in the dataset folder
                    try:
                        files = self.client.storage.from_("datasets").list(dataset_name)
                        file_count = sum(1 for f in files if f.get("id"))
                        logger.info(f"Dataset '{dataset_name}' has {file_count} files")
                    except Exception as e:
                        logger.warning(f"Error counting files in dataset '{dataset_name}': {e}")
                        file_count = 0

                    if file_count > 0:
                        datasets.append(DatasetInfo(
                            name=dataset_name,
                            path=f"datasets/{dataset_name}",
                            file_count=file_count,
                            description=f"Document collection: {dataset_name}"
                        ))
                else:
                    logger.debug(f"Skipping file at root level: {item.get('name')}")

            logger.info(f"Returning {len(datasets)} datasets: {[d.name for d in datasets]}")
            return sorted(datasets, key=lambda d: d.name)
        except Exception as e:
            logger.error(f"Error listing datasets from Supabase: {e}", exc_info=True)
            return []

    async def list_dataset_files(self, dataset_name: str) -> List[FileInfo]:
        """List files in a specific dataset."""
        try:
            files = self.client.storage.from_("datasets").list(dataset_name)

            result = []
            for f in files:
                if f.get("id"):  # It's a file
                    # Get content type based on extension
                    name = f["name"]
                    ext = Path(name).suffix.lower()
                    content_type = {
                        '.txt': 'text/plain',
                        '.md': 'text/markdown',
                        '.pdf': 'application/pdf',
                        '.json': 'application/json',
                        '.jsonl': 'application/jsonl',
                    }.get(ext, 'application/octet-stream')

                    result.append(FileInfo(
                        name=name,
                        path=f"{dataset_name}/{name}",
                        size=f.get("metadata", {}).get("size", 0),
                        content_type=content_type
                    ))

            return sorted(result, key=lambda f: f.name)
        except Exception as e:
            print(f"Error listing dataset files from Supabase: {e}")
            return []

    async def download_dataset_file(self, dataset_name: str, filename: str) -> Optional[bytes]:
        """Download a specific file from a dataset."""
        try:
            path = f"{dataset_name}/{filename}"
            return self.client.storage.from_("datasets").download(path)
        except Exception as e:
            print(f"Error downloading dataset file {dataset_name}/{filename}: {e}")
            return None

    async def download_dataset_to_local(self, dataset_name: str, local_dir: str) -> List[str]:
        """Download all files from a dataset to a local directory.

        This is required for QBSD processing which needs local file access.
        """
        local_path = Path(local_dir)
        local_path.mkdir(parents=True, exist_ok=True)

        created_files = []
        try:
            files = await self.list_dataset_files(dataset_name)

            for file_info in files:
                content = await self.download_dataset_file(dataset_name, file_info.name)
                if content:
                    dest_path = local_path / file_info.name
                    with open(dest_path, 'wb') as f:
                        f.write(content)
                    created_files.append(str(dest_path))
        except Exception as e:
            print(f"Error downloading dataset to local: {e}")

        return created_files

    # =======================
    # Template Operations
    # =======================

    async def list_templates(self) -> List[TemplateInfo]:
        """List available template tables from the templates bucket."""
        try:
            files = self.client.storage.from_("templates").list()

            templates = []
            for f in files:
                if f.get("id"):  # It's a file
                    name = f["name"]
                    ext = Path(name).suffix.lower()

                    if ext in ['.csv', '.json', '.jsonl']:
                        # Try to get row/column count by downloading and parsing
                        row_count = None
                        column_count = None

                        try:
                            content = self.client.storage.from_("templates").download(name)
                            text = content.decode('utf-8')

                            if ext == '.csv':
                                lines = text.strip().split('\n')
                                row_count = len(lines) - 1  # Exclude header
                                if lines:
                                    column_count = len(lines[0].split(','))
                            elif ext == '.jsonl':
                                lines = [l for l in text.strip().split('\n') if l.strip()]
                                row_count = len(lines)
                                if lines:
                                    first_row = json.loads(lines[0])
                                    column_count = len(first_row.get('data', first_row).keys())
                            elif ext == '.json':
                                data = json.loads(text)
                                if isinstance(data, list):
                                    row_count = len(data)
                                    if data:
                                        column_count = len(data[0].keys())
                        except Exception:
                            pass  # Ignore count errors

                        templates.append(TemplateInfo(
                            name=Path(name).stem,
                            path=f"templates/{name}",
                            file_type=ext[1:],
                            description=f"Template: {Path(name).stem}",
                            row_count=row_count,
                            column_count=column_count
                        ))

            return sorted(templates, key=lambda t: t.name)
        except Exception as e:
            print(f"Error listing templates from Supabase: {e}")
            return []

    async def download_template(self, template_name: str) -> Optional[bytes]:
        """Download a template file."""
        try:
            # Try different extensions
            for ext in ['.csv', '.json', '.jsonl']:
                try:
                    filename = f"{template_name}{ext}"
                    return self.client.storage.from_("templates").download(filename)
                except Exception:
                    continue

            # Try exact filename
            try:
                return self.client.storage.from_("templates").download(template_name)
            except Exception:
                pass

            return None
        except Exception as e:
            print(f"Error downloading template {template_name}: {e}")
            return None
