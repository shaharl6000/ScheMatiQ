# QBSD Visualization Module

Interactive visualization and schema editing interface for the QBSD system.

## Features

### Dual Input Options
1. **Upload Existing Data**: Import CSV/JSON files for visualization
2. **Create with QBSD**: Run live schema discovery and value extraction

### Visualization Capabilities
- Schema tree view with column details
- Data table with virtual scrolling for large datasets
- Statistics dashboard with data quality metrics
- Export functionality (CSV, JSON, filtered subsets)

### Interactive Editing (Phase 2)
- Schema editing with real-time validation
- Live re-extraction for QBSD-created schemas
- Column management (add, remove, reorder)

## Architecture

### Backend (`backend/`)
- **FastAPI** application with async support
- **RESTful API** for data access and manipulation
- **WebSocket** support for real-time updates
- **File parsing** for CSV and JSONL formats

### Frontend (`frontend/`)
- **React + TypeScript** for type safety and component reusability
- **Dual workflow** support (upload vs. QBSD creation)
- **Real-time monitoring** of QBSD execution
- **Responsive design** for various screen sizes

## Development Setup

### Backend Setup
```bash
cd backend/
pip install -r requirements.txt
uvicorn main:app --reload
```

### Frontend Setup
```bash
cd frontend/
npm install
npm start
```

## Integration with QBSD

The visualization module integrates with existing QBSD components:
- Imports `Schema` and `Column` classes from `../schema.py`
- Uses `TableBuilder` from `../value_extraction/core/`
- Leverages configuration system for LLM backends
- Supports existing file formats and structures

## API Documentation

Once running, visit `http://localhost:8000/docs` for interactive API documentation.