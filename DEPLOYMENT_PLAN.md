# QueryDiscovery Railway Deployment

## Overview

Deploy QueryDiscovery to Railway with Docker containers. Railway builds the Docker images on their servers - no Docker Desktop required locally.

---

## Step 1: Create Services in Railway Dashboard

1. Go to [railway.app/dashboard](https://railway.app/dashboard)
2. Click **"New Project"**
3. Select **"Empty Project"**
4. Click **"Add Service"** → **"Empty Service"** → Name it `backend`
5. Click **"Add Service"** → **"Empty Service"** → Name it `frontend`

You now have a project with two empty services ready for deployment.

---

## Step 2: Install Railway CLI & Link Project

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login to Railway
railway login

# Link to your project (select the project you just created)
railway link
```

---

## Step 3: Configure & Deploy

```bash
# Configure service names
./deploy-railway setup

# Deploy both services
./deploy-railway deploy all
```

The first deployment will take several minutes (backend has ML dependencies).

---

## Step 4: Set Environment Variables

After deployment, get the service URLs from Railway dashboard, then:

```bash
# Set backend LLM API key
./deploy-railway env backend set OPENAI_API_KEY=sk-your-key

# Set frontend API URLs (replace with your actual backend URL)
./deploy-railway env frontend set REACT_APP_API_URL=https://backend-xxx.up.railway.app
./deploy-railway env frontend set REACT_APP_WS_URL=wss://backend-xxx.up.railway.app

# Set backend CORS (replace with your actual frontend URL)
./deploy-railway env backend set ALLOWED_ORIGINS=https://frontend-xxx.up.railway.app

# Redeploy frontend to apply build-time env vars
./deploy-railway deploy frontend
```

---

## Script Commands

| Command | Description |
|---------|-------------|
| `./deploy-railway setup` | Configure service names |
| `./deploy-railway deploy all` | Deploy both services |
| `./deploy-railway deploy backend` | Deploy backend only |
| `./deploy-railway deploy frontend` | Deploy frontend only |
| `./deploy-railway logs backend` | View backend logs |
| `./deploy-railway logs frontend` | View frontend logs |
| `./deploy-railway env backend list` | List backend env vars |
| `./deploy-railway env backend set KEY=VALUE` | Set backend env var |
| `./deploy-railway status` | Show deployment status |
| `./deploy-railway open` | Open Railway dashboard |
| `./deploy-railway help` | Show all commands |

---

## Environment Variables Reference

### Backend

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | Yes* | OpenAI API key |
| `TOGETHER_API_KEY` | Yes* | Together AI API key |
| `GEMINI_API_KEY` | Yes* | Google Gemini API key |
| `ALLOWED_ORIGINS` | Yes | Frontend URL for CORS |
| `DEBUG` | No | Set to `false` for production |

*At least one LLM API key is required

### Frontend

| Variable | Required | Description |
|----------|----------|-------------|
| `REACT_APP_API_URL` | Yes | Backend API URL |
| `REACT_APP_WS_URL` | Yes | Backend WebSocket URL (wss://) |

---

## Files Structure

```
QueryDiscovery/
├── deploy-railway           # Deployment script
├── railway.json             # Backend Railway config
├── .dockerignore            # Build exclusions
├── backend/
│   └── Dockerfile           # Backend container definition
└── frontend/
    ├── Dockerfile           # Frontend container definition
    └── railway.json         # Frontend Railway config
```

---

## Troubleshooting

### "Railway CLI not found"
```bash
npm install -g @railway/cli
```

### "Not linked to a Railway project"
```bash
railway link
# Then select your project from the list
```

### Build fails
```bash
# Check logs
./deploy-railway logs backend

# Verify env vars are set
./deploy-railway env backend list
```

### Frontend can't connect to backend
1. Check `REACT_APP_API_URL` is set correctly
2. Check `ALLOWED_ORIGINS` on backend includes frontend URL
3. Redeploy frontend after changing env vars
