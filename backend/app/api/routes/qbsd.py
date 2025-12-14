"""QBSD API endpoints."""

import uuid
import asyncio
import csv
import io
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse

from app.models.session import VisualizationSession, SessionType, SessionMetadata
from app.models.qbsd import QBSDConfig, QBSDStatus
from app.services.qbsd_runner import QBSDRunner
from app.services import websocket_manager, session_manager

router = APIRouter()
# Create shared QBSD runner instance with shared managers
qbsd_runner = QBSDRunner(websocket_manager=websocket_manager, session_manager=session_manager)

@router.post("/configure", response_model=dict)
async def configure_qbsd(config: QBSDConfig):
    """Configure a new QBSD session."""
    try:
        print(f"DEBUG: Received QBSD config: {config}")
        
        # Validate configuration
        validation = await qbsd_runner.validate_config(config)
        
        print(f"DEBUG: Validation result: {validation}")
        
        if not validation["is_valid"]:
            raise HTTPException(status_code=400, detail=validation["errors"])
        
        # Create session
        session_id = str(uuid.uuid4())
        metadata = SessionMetadata(source=f"QBSD Query: {config.query[:50]}...")
        
        session = VisualizationSession(
            id=session_id,
            type=SessionType.QBSD,
            metadata=metadata,
            schema_query=config.query
        )
        
        # Store session and config
        session_manager.create_session(session)
        await qbsd_runner.save_config(session_id, config)
        
        return {
            "session_id": session_id,
            "message": "QBSD session configured successfully"
        }
        
    except Exception as e:
        print(f"DEBUG: Exception in configure_qbsd: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/run/{session_id}")
async def run_qbsd(session_id: str, background_tasks: BackgroundTasks):
    """Start QBSD execution."""
    try:
        session = session_manager.get_session(session_id)
        if not session or session.type != SessionType.QBSD:
            raise HTTPException(status_code=404, detail="QBSD session not found")
        
        # Start QBSD in background
        background_tasks.add_task(qbsd_runner.run_qbsd, session_id)
        
        return {"message": "QBSD execution started", "session_id": session_id}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/{session_id}", response_model=QBSDStatus)
async def get_qbsd_status(session_id: str):
    """Get QBSD execution status."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        status = await qbsd_runner.get_status(session_id)
        
        return status
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/schema/{session_id}")
async def get_qbsd_schema(session_id: str):
    """Get discovered schema."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        schema = await qbsd_runner.get_schema(session_id)
        
        return schema
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/data/{session_id}")
async def get_qbsd_data(session_id: str, page: int = 0, page_size: int = 50):
    """Get extracted data."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        data = await qbsd_runner.get_data(session_id, page, page_size)
        
        return data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stop/{session_id}")
async def stop_qbsd(session_id: str):
    """Stop QBSD execution."""
    try:
        success = await qbsd_runner.stop_execution(session_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="No running QBSD session found")
        
        return {"message": "QBSD execution stopped"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sessions", response_model=List[VisualizationSession])
async def list_qbsd_sessions():
    """List all QBSD sessions."""
    return session_manager.list_sessions(SessionType.QBSD)


@router.get("/directories")
async def list_document_directories():
    """List available document directories from research/data folder.

    Returns a list of directories that can be used as document paths for QBSD.
    Each directory includes its name and the relative path for the backend.
    """
    try:
        # Try multiple path resolution strategies
        candidates = [
            Path("../research/data"),  # When running from backend/
            Path(__file__).parent.parent.parent.parent.parent / "research" / "data",  # Absolute from file location
            Path.cwd().parent / "research" / "data",  # From current working directory
            Path.cwd() / "research" / "data",  # If running from project root
        ]

        research_data_path = None
        for candidate in candidates:
            if candidate.exists() and candidate.is_dir():
                research_data_path = candidate
                break

        if research_data_path is None:
            print(f"DEBUG: Could not find research/data directory. Tried: {[str(c) for c in candidates]}")
            return []

        directories = []
        for item in sorted(research_data_path.iterdir()):
            if item.is_dir():
                directories.append({
                    "value": f"../research/data/{item.name}",
                    "label": item.name
                })

        return directories

    except Exception as e:
        print(f"DEBUG: Error listing directories: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/{session_id}")
async def export_qbsd_data(session_id: str, column_order: Optional[str] = None):
    """Export QBSD data as CSV with excerpts in separate columns.

    Args:
        session_id: The session ID to export
        column_order: Optional comma-separated list of column names in desired order
    """
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Get all data
        data = await qbsd_runner.get_data(session_id, page=0, page_size=10000)  # Get all data
        
        if not data.rows:
            raise HTTPException(status_code=404, detail="No data to export")
        
        # Prepare CSV data with metadata
        output = io.StringIO()
        
        # Add schema metadata as CSV comments
        output.write("# QBSD Export with Schema Metadata\n")
        output.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"# Session ID: {session_id}\n")
        output.write(f"# Query: {session.schema_query or 'N/A'}\n")
        
        # Load and include LLM configuration if available
        session_dir = Path("./data") / session_id
        qbsd_config_file = session_dir / "qbsd_config.json"
        
        if qbsd_config_file.exists():
            try:
                with open(qbsd_config_file) as f:
                    qbsd_config = json.load(f)
                    if "schema_creation_backend" in qbsd_config:
                        backend = qbsd_config["schema_creation_backend"]
                        output.write(f"# Schema Creation: {backend.get('provider', 'unknown')} {backend.get('model', 'unknown')}\n")
                    if "value_extraction_backend" in qbsd_config:
                        backend = qbsd_config["value_extraction_backend"]
                        output.write(f"# Value Extraction: {backend.get('provider', 'unknown')} {backend.get('model', 'unknown')}\n")
                    elif "backend" in qbsd_config:  # Legacy support
                        backend = qbsd_config["backend"]
                        output.write(f"# AI Model: {backend.get('provider', 'unknown')} {backend.get('model', 'unknown')}\n")
            except Exception as e:
                output.write(f"# LLM Config: Error loading ({e})\n")
        
        output.write("#\n")
        output.write("# Column Definitions:\n")
        
        # Add column metadata for each schema column
        for col in session.columns:
            if col.name and not col.name.lower().endswith('_excerpt'):
                output.write(f"# {col.name}: {col.definition or 'No definition available'}\n")
                if col.rationale:
                    output.write(f"#   Rationale: {col.rationale}\n")
        
        # Add special column explanations
        output.write("# row_name: Identifier for this data row\n")
        output.write("# papers: Source documents used for extraction\n")
        output.write("# {column}_excerpt: Supporting evidence from documents for {column}\n")
        output.write("#\n")

        # Add schema evolution summary if available
        if session.statistics and session.statistics.schema_evolution:
            evolution = session.statistics.schema_evolution
            output.write("# Schema Evolution:\n")
            for snapshot in evolution.snapshots:
                if snapshot.new_columns:
                    cols_str = ", ".join(snapshot.new_columns[:5])
                    if len(snapshot.new_columns) > 5:
                        cols_str += f"... (+{len(snapshot.new_columns) - 5} more)"
                    output.write(f"# Iteration {snapshot.iteration}: +{len(snapshot.new_columns)} columns [{cols_str}]\n")
            output.write(f"# Total: {len(evolution.column_sources)} columns from {len(evolution.snapshots)} iterations\n")
            output.write("#\n")
        
        # Determine all column names including excerpt columns
        all_columns = set()
        for row in data.rows:
            if row.row_name:
                all_columns.add('row_name')
            if row.papers:
                all_columns.add('papers')
            for col_name in row.data.keys():
                all_columns.add(col_name)
                # Add excerpt column for QBSD data
                if isinstance(row.data[col_name], dict) and 'excerpts' in row.data[col_name]:
                    all_columns.add(f"{col_name}_excerpt")
        
        # Determine column order - use user-specified order if provided
        if column_order:
            # Parse user-specified column order
            requested_order = [col.strip() for col in column_order.split(',')]
            # Filter to only include columns that exist, preserving requested order
            column_names = [col for col in requested_order if col in all_columns]
            # Add any remaining columns not in the requested order
            remaining_columns = sorted([col for col in all_columns if col not in column_names])
            column_names.extend(remaining_columns)
        else:
            # Default: sorted alphabetically
            column_names = sorted(list(all_columns))

        writer = csv.DictWriter(output, fieldnames=column_names)
        writer.writeheader()
        
        # Write data rows
        for row in data.rows:
            csv_row = {}
            
            # Add standard columns
            if row.row_name:
                csv_row['row_name'] = row.row_name
            if row.papers:
                csv_row['papers'] = '; '.join(row.papers) if isinstance(row.papers, list) else str(row.papers)
            
            # Process data columns
            for col_name, value in row.data.items():
                if isinstance(value, dict) and 'answer' in value:
                    # QBSD format: extract answer and excerpts
                    csv_row[col_name] = value['answer']
                    if 'excerpts' in value and value['excerpts']:
                        excerpt_col = f"{col_name}_excerpt"
                        if isinstance(value['excerpts'], list):
                            csv_row[excerpt_col] = ' | '.join(str(ex) for ex in value['excerpts'])
                        else:
                            csv_row[excerpt_col] = str(value['excerpts'])
                else:
                    # Regular data
                    if isinstance(value, (list, dict)):
                        csv_row[col_name] = str(value)  # Convert complex types to string
                    else:
                        csv_row[col_name] = value
            
            writer.writerow(csv_row)
        
        # Prepare response
        output.seek(0)
        content = output.getvalue()
        
        # Generate filename with datetime
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"QBSD_{timestamp}.csv"
        
        return StreamingResponse(
            io.BytesIO(content.encode('utf-8')),
            media_type='text/csv',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-complete/{session_id}")
async def export_complete_qbsd_data(session_id: str, format: str = "json"):
    """Export complete QBSD data with schema metadata in multiple formats."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Get schema and data
        schema = await qbsd_runner.get_schema(session_id)
        data = await qbsd_runner.get_data(session_id, page=0, page_size=10000)
        
        # Load QBSD configuration if available
        session_dir = Path("./data") / session_id
        qbsd_config_file = session_dir / "qbsd_config.json"
        llm_configuration = None
        
        if qbsd_config_file.exists():
            try:
                with open(qbsd_config_file) as f:
                    qbsd_config = json.load(f)
                    llm_configuration = {
                        "schema_creation_backend": qbsd_config.get("schema_creation_backend"),
                        "value_extraction_backend": qbsd_config.get("value_extraction_backend")
                    }
            except Exception as e:
                print(f"DEBUG: Could not load LLM configuration for complete export: {e}")
        
        # Prepare complete export data structure
        export_data = {
            "session_id": session_id,
            "session_type": session.type.value,
            "created": session.metadata.created.isoformat(),
            "last_modified": session.metadata.last_modified.isoformat(),
            "query": session.schema_query,
            "schema": {
                "columns": [
                    {
                        "name": col.name,
                        "definition": col.definition or "",
                        "rationale": col.rationale or "",
                        "data_type": col.data_type,
                        "source_document": col.source_document,
                        "discovery_iteration": col.discovery_iteration
                    }
                    for col in session.columns
                    if col.name and not col.name.lower().endswith('_excerpt')
                ]
            },
            "metadata": {
                "total_rows": data.total_count,
                "total_columns": len([col for col in session.columns if not col.name.lower().endswith('_excerpt')]),
                "source": session.metadata.source,
                "schema_discovery_completed": session.metadata.schema_discovery_completed,
                "total_documents": session.metadata.total_documents,
                "processed_documents": session.metadata.processed_documents
            },
            "data": [
                {
                    "row_name": row.row_name,
                    "papers": row.papers,
                    "data": row.data
                }
                for row in data.rows
            ]
        }

        # Include schema evolution if available
        if session.statistics and session.statistics.schema_evolution:
            export_data["schema_evolution"] = session.statistics.schema_evolution.model_dump()
        
        # Include LLM configuration if available
        if llm_configuration and any(llm_configuration.values()):
            export_data["llm_configuration"] = llm_configuration
        
        # Handle different export formats
        if format.lower() == "json":
            # JSON format with complete metadata
            content = json.dumps(export_data, indent=2, ensure_ascii=False, default=str)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename = f"QBSD_{timestamp}_complete.json"
            
            return StreamingResponse(
                io.BytesIO(content.encode('utf-8')),
                media_type='application/json',
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
            
        elif format.lower() == "zip":
            # ZIP package with separate files
            import zipfile
            import tempfile
            
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                with zipfile.ZipFile(tmp_file.name, 'w') as zip_file:
                    # Add schema file
                    schema_data = {
                        "query": export_data["query"],
                        "schema": [
                            {
                                "name": col["name"],
                                "definition": col["definition"],
                                "rationale": col["rationale"]
                            }
                            for col in export_data["schema"]["columns"]
                        ]
                    }
                    
                    # Include LLM configuration if available
                    if llm_configuration and any(llm_configuration.values()):
                        schema_data["llm_configuration"] = llm_configuration
                    
                    zip_file.writestr("schema.json", json.dumps(schema_data, indent=2))
                    
                    # Add separate column metadata CSV for easy reference
                    metadata_output = io.StringIO()
                    metadata_writer = csv.writer(metadata_output)
                    metadata_writer.writerow(["Column Name", "Definition", "Rationale", "Data Type"])
                    
                    for col in export_data["schema"]["columns"]:
                        metadata_writer.writerow([
                            col["name"],
                            col["definition"],
                            col["rationale"],
                            col.get("data_type", "text")
                        ])
                    
                    # Add special columns documentation
                    metadata_writer.writerow(["row_name", "Identifier for this data row", "Standard QBSD metadata field", "text"])
                    metadata_writer.writerow(["papers", "Source documents used for extraction", "Standard QBSD metadata field", "text"])
                    metadata_writer.writerow(["{column}_excerpt", "Supporting evidence from documents", "Generated for each QBSD data column", "text"])
                    
                    zip_file.writestr("column_metadata.csv", metadata_output.getvalue())
                    
                    # Add data file as CSV with QBSD format handling
                    if export_data["data"]:
                        output = io.StringIO()
                        
                        # Determine all column names including excerpt columns
                        all_columns = set()
                        for row in export_data["data"]:
                            if row["row_name"]:
                                all_columns.add('row_name')
                            if row["papers"]:
                                all_columns.add('papers')
                            for col_name in row["data"].keys():
                                all_columns.add(col_name)
                                # Add excerpt column for QBSD data
                                if isinstance(row["data"][col_name], dict) and 'excerpts' in row["data"][col_name]:
                                    all_columns.add(f"{col_name}_excerpt")
                        
                        column_names = sorted(list(all_columns))
                        writer = csv.DictWriter(output, fieldnames=column_names)
                        writer.writeheader()
                        
                        # Write data rows with QBSD format handling
                        for row in export_data["data"]:
                            csv_row = {}
                            
                            # Add standard columns
                            if row["row_name"]:
                                csv_row['row_name'] = row["row_name"]
                            if row["papers"]:
                                csv_row['papers'] = '; '.join(row["papers"]) if isinstance(row["papers"], list) else str(row["papers"])
                            
                            # Process data columns
                            for col_name, value in row["data"].items():
                                if isinstance(value, dict) and 'answer' in value:
                                    # QBSD format: extract answer and excerpts
                                    csv_row[col_name] = value['answer']
                                    if 'excerpts' in value and value['excerpts']:
                                        excerpt_col = f"{col_name}_excerpt"
                                        if isinstance(value['excerpts'], list):
                                            csv_row[excerpt_col] = ' | '.join(str(ex) for ex in value['excerpts'])
                                        else:
                                            csv_row[excerpt_col] = str(value['excerpts'])
                                else:
                                    # Regular data
                                    if isinstance(value, (list, dict)):
                                        csv_row[col_name] = str(value)
                                    else:
                                        csv_row[col_name] = value
                            
                            writer.writerow(csv_row)
                        
                        zip_file.writestr("data.csv", output.getvalue())
                    
                    # Add metadata file
                    zip_file.writestr("metadata.json", json.dumps(export_data["metadata"], indent=2, default=str))
                
                # Read and return zip file
                with open(tmp_file.name, 'rb') as f:
                    zip_content = f.read()
                
                # Clean up temp file
                Path(tmp_file.name).unlink()
                
                timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                filename = f"QBSD_{timestamp}_complete.zip"
                return StreamingResponse(
                    io.BytesIO(zip_content),
                    media_type='application/zip',
                    headers={"Content-Disposition": f"attachment; filename={filename}"}
                )
                
        else:
            raise HTTPException(status_code=400, detail="Unsupported format. Use 'json' or 'zip'")
        
    except Exception as e:
        print(f"DEBUG: Exception in export_complete_qbsd_data: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-rich-csv/{session_id}")
async def export_qbsd_rich_csv(session_id: str):
    """Export QBSD data as metadata-rich CSV with definition and rationale columns."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Get all data
        data = await qbsd_runner.get_data(session_id, page=0, page_size=10000)
        
        if not data.rows:
            raise HTTPException(status_code=404, detail="No data to export")
        
        # Prepare metadata-rich CSV
        output = io.StringIO()
        
        # Create extended column set with metadata
        base_columns = set()
        for row in data.rows:
            if row.row_name:
                base_columns.add('row_name')
            if row.papers:
                base_columns.add('papers')
            for col_name in row.data.keys():
                base_columns.add(col_name)
                # Add excerpt column for QBSD data
                if isinstance(row.data[col_name], dict) and 'excerpts' in row.data[col_name]:
                    base_columns.add(f"{col_name}_excerpt")
        
        # Build enhanced column list with metadata columns
        enhanced_columns = []
        for col_name in sorted(base_columns):
            enhanced_columns.append(col_name)
            # Add metadata columns for schema columns (not for standard or excerpt columns)
            if (col_name not in ['row_name', 'papers'] and 
                not col_name.endswith('_excerpt')):
                enhanced_columns.append(f"{col_name}_definition")
                enhanced_columns.append(f"{col_name}_rationale")
        
        writer = csv.DictWriter(output, fieldnames=enhanced_columns)
        
        # Write metadata header rows first
        output.write("# Metadata-Rich CSV Export\n")
        output.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output.write(f"# Session ID: {session_id}\n")
        output.write(f"# Query: {session.schema_query or 'N/A'}\n")
        output.write("# Format: Each data column has corresponding _definition and _rationale columns\n")
        output.write("#\n")
        
        # Write CSV headers
        writer.writeheader()
        
        # Create column metadata lookup
        column_metadata = {}
        for col in session.columns:
            if col.name:
                column_metadata[col.name] = {
                    'definition': col.definition or '',
                    'rationale': col.rationale or ''
                }
        
        # Write data rows with metadata
        for row in data.rows:
            csv_row = {}
            
            # Add standard columns
            if row.row_name:
                csv_row['row_name'] = row.row_name
            if row.papers:
                csv_row['papers'] = '; '.join(row.papers) if isinstance(row.papers, list) else str(row.papers)
            
            # Process data columns with metadata
            for col_name, value in row.data.items():
                if isinstance(value, dict) and 'answer' in value:
                    # QBSD format: extract answer and excerpts
                    csv_row[col_name] = value['answer']
                    if 'excerpts' in value and value['excerpts']:
                        excerpt_col = f"{col_name}_excerpt"
                        if isinstance(value['excerpts'], list):
                            csv_row[excerpt_col] = ' | '.join(str(ex) for ex in value['excerpts'])
                        else:
                            csv_row[excerpt_col] = str(value['excerpts'])
                else:
                    # Regular data
                    if isinstance(value, (list, dict)):
                        csv_row[col_name] = str(value)
                    else:
                        csv_row[col_name] = value
                
                # Add metadata columns for this data column
                if col_name in column_metadata:
                    csv_row[f"{col_name}_definition"] = column_metadata[col_name]['definition']
                    csv_row[f"{col_name}_rationale"] = column_metadata[col_name]['rationale']
            
            writer.writerow(csv_row)
        
        # Prepare response
        output.seek(0)
        content = output.getvalue()
        
        # Generate filename with datetime
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"QBSD_{timestamp}_rich.csv"
        
        return StreamingResponse(
            io.BytesIO(content.encode('utf-8')),
            media_type='text/csv',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-schema/{session_id}")
async def export_qbsd_schema_only(session_id: str):
    """Export only the QBSD schema metadata."""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Load QBSD configuration if available
        session_dir = Path("./data") / session_id
        qbsd_config_file = session_dir / "qbsd_config.json"
        llm_configuration = None
        
        if qbsd_config_file.exists():
            try:
                with open(qbsd_config_file) as f:
                    qbsd_config = json.load(f)
                    llm_configuration = {
                        "schema_creation_backend": qbsd_config.get("schema_creation_backend"),
                        "value_extraction_backend": qbsd_config.get("value_extraction_backend")
                    }
            except Exception as e:
                print(f"DEBUG: Could not load LLM configuration: {e}")

        # Create QBSD schema export
        schema_export = {
            "query": session.schema_query or "",
            "schema": [
                {
                    "name": col.name,
                    "definition": col.definition or "",
                    "rationale": col.rationale or "",
                    "source_document": col.source_document,
                    "discovery_iteration": col.discovery_iteration
                }
                for col in session.columns
                if col.name and not col.name.lower().endswith('_excerpt')
            ],
            "metadata": {
                "session_id": session_id,
                "session_type": session.type.value,
                "created": session.metadata.created.isoformat(),
                "source": session.metadata.source,
                "total_columns": len([col for col in session.columns if not col.name.lower().endswith('_excerpt')]),
                "schema_discovery_completed": session.metadata.schema_discovery_completed,
                "export_timestamp": datetime.now().isoformat()
            }
        }

        # Include schema evolution if available
        if session.statistics and session.statistics.schema_evolution:
            schema_export["schema_evolution"] = session.statistics.schema_evolution.model_dump()

        # Include LLM configuration if available
        if llm_configuration and any(llm_configuration.values()):
            schema_export["llm_configuration"] = llm_configuration
        
        content = json.dumps(schema_export, indent=2, ensure_ascii=False, default=str)
        
        # Generate filename with datetime
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"QBSD_{timestamp}_schema.json"
        
        return StreamingResponse(
            io.BytesIO(content.encode('utf-8')),
            media_type='application/json',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        print(f"DEBUG: Exception in export_qbsd_schema_only: {e}")
        raise HTTPException(status_code=500, detail=str(e))