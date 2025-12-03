#!/bin/bash

# QBSD Visualization Startup Script

echo "🚀 Starting QBSD Visualization Module..."

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if required directories exist
if [ ! -d "backend" ] || [ ! -d "frontend" ]; then
    echo "❌ Error: backend and frontend directories not found!"
    echo "Make sure you're running this script from the visualization/ directory"
    exit 1
fi

# Function to check if port is available
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null ; then
        echo "⚠️  Port $port is already in use"
        return 1
    else
        return 0
    fi
}

# Check ports
if ! check_port 8000; then
    echo "Backend port 8000 is already in use. Please stop the existing service."
    exit 1
fi

if ! check_port 3000; then
    echo "Frontend port 3000 is already in use. Please stop the existing service."
    exit 1
fi

# Install backend dependencies
echo -e "${BLUE}📦 Installing backend dependencies...${NC}"
cd backend
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
pip install -r requirements.txt

# Start backend in background
echo -e "${BLUE}🖥️  Starting backend server...${NC}"
python main.py &
BACKEND_PID=$!
echo "Backend PID: $BACKEND_PID"

# Wait a bit for backend to start
sleep 3

# Install frontend dependencies and start
echo -e "${BLUE}📦 Installing frontend dependencies...${NC}"
cd ../frontend

if [ ! -d "node_modules" ]; then
    echo "Installing npm packages..."
    npm install
fi

echo -e "${BLUE}🌐 Starting frontend development server...${NC}"
npm start &
FRONTEND_PID=$!
echo "Frontend PID: $FRONTEND_PID"

# Function to cleanup on exit
cleanup() {
    echo -e "\n${YELLOW}🛑 Shutting down services...${NC}"
    if [ ! -z "$BACKEND_PID" ]; then
        kill $BACKEND_PID 2>/dev/null
        echo "Backend server stopped"
    fi
    if [ ! -z "$FRONTEND_PID" ]; then
        kill $FRONTEND_PID 2>/dev/null
        echo "Frontend server stopped"
    fi
    exit 0
}

# Set up signal handling
trap cleanup SIGINT SIGTERM

echo -e "${GREEN}✅ Services started successfully!${NC}"
echo ""
echo "📊 QBSD Visualization is now running:"
echo "   • Backend API: http://localhost:8000"
echo "   • Frontend App: http://localhost:3000"
echo "   • API Documentation: http://localhost:8000/docs"
echo ""
echo "Press Ctrl+C to stop all services"

# Wait for user to stop
wait