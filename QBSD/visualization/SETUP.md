# QBSD Visualization Setup Guide

## Quick Start

The easiest way to start the visualization module:

```bash
cd QBSD/visualization/
./start.sh
```

This will automatically:
- Set up Python virtual environment and install backend dependencies
- Install Node.js dependencies for the frontend
- Start both backend and frontend servers
- Open the application at http://localhost:3000

## Manual Setup

### Backend Setup

1. Navigate to backend directory:
```bash
cd backend/
```

2. Create virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Start the backend server:
```bash
python main.py
```

The backend will be available at: http://localhost:8000
API documentation: http://localhost:8000/docs

### Frontend Setup

1. Navigate to frontend directory:
```bash
cd frontend/
```

2. Install dependencies:
```bash
npm install
```

3. Start the development server:
```bash
npm start
```

The frontend will be available at: http://localhost:3000

## Usage

### Upload Workflow

1. Go to http://localhost:3000
2. Click "Upload Existing Data"
3. Drag & drop or select your CSV/JSON file
4. Preview and validate your data
5. Explore the interactive visualization

### QBSD Workflow

1. Go to http://localhost:3000
2. Click "Create with QBSD"
3. Configure your research query and document paths
4. Set LLM backend and retriever parameters
5. Start QBSD execution
6. Monitor progress in real-time
7. Explore discovered schema and extracted data

## Supported File Formats

### Upload Mode
- **CSV files**: Automatic header detection and type inference
- **JSON files**: Single objects or arrays of objects
- **JSONL files**: Line-delimited JSON (e.g., existing QBSD outputs)

### QBSD Mode
- **Document collections**: Text files in specified directories
- **Initial schemas**: JSON schema files (optional)
- **Multiple paths**: Comma-separated document directories

## Features

### Phase 1 (Current)
- ✅ Dual input options (Upload vs QBSD)
- ✅ File validation and parsing
- ✅ Interactive data table with virtual scrolling
- ✅ Schema visualization with column details
- ✅ Data statistics dashboard
- ✅ Real-time QBSD monitoring via WebSocket
- ✅ Responsive design

### Phase 2 (Future)
- 🔄 Schema editing with real-time validation
- 🔄 Live re-extraction for modified schemas
- 🔄 Column management (add, remove, reorder)
- 🔄 Advanced export options

## Configuration

### Environment Variables

Backend (.env file in backend/):
```env
# Optional - defaults to localhost
API_HOST=0.0.0.0
API_PORT=8000
```

Frontend (.env file in frontend/):
```env
# Optional - auto-detected in development
REACT_APP_API_BASE=http://localhost:8000/api
REACT_APP_WS_BASE=ws://localhost:8000/ws
```

### QBSD Integration

The visualization module integrates with existing QBSD components:
- Uses `Schema` and `Column` classes from `../schema.py`
- Leverages `TableBuilder` from `../value_extraction/core/`
- Supports existing configuration format
- Compatible with all LLM backends (Gemini, OpenAI, Together AI)

## Troubleshooting

### Port Conflicts
If ports 3000 or 8000 are in use:
```bash
# Check what's using the ports
lsof -i :3000
lsof -i :8000

# Kill processes if needed
kill <PID>
```

### Dependencies
Make sure you have:
- Python 3.8+
- Node.js 16+
- npm or yarn

### QBSD Integration Issues
Make sure your QBSD environment is set up with required API keys:
```bash
export OPENAI_API_KEY="your-key-here"
export TOGETHER_API_KEY="your-key-here"
export GEMINI_API_KEY="your-key-here"
```

### File Upload Issues
- Maximum file size: 100MB
- Supported formats: CSV, JSON, JSONL
- Check file permissions and encoding

## Development

### Backend Development
```bash
cd backend/
source venv/bin/activate
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend Development
```bash
cd frontend/
npm start
```

Hot reloading is enabled for both backend and frontend during development.

### Adding New Features

1. **Backend**: Add endpoints in `api/` and business logic in `services/`
2. **Frontend**: Add components in `components/` and pages in `pages/`
3. **Types**: Update TypeScript interfaces in `types/index.ts`

## Performance

### Large Datasets
- Virtual scrolling handles 10k+ rows efficiently
- Pagination reduces initial load time
- Lazy loading prevents memory issues

### Real-time Updates
- WebSocket connection for live progress monitoring
- Automatic reconnection on connection loss
- Optimistic UI updates for better responsiveness