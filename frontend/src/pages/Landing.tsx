import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Sparkles,
  Upload,
  FileUp,
  Table,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Card,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { ApiKeySection } from '@/components/ApiKeySection';
import { getConfiguredProviders, LLMProvider } from '@/utils/apiKeyStorage';
import { configAPI } from '@/services/api';

// Custom styles for the demonstration video button (matching PromptSuite shine effects)
const videoButtonStyles = `
  .video-button-shine {
    position: relative;
    overflow: hidden;
  }

  .video-button-shine::before {
    content: '';
    position: absolute;
    top: 0;
    left: -100%;
    width: 100%;
    height: 100%;
    background: linear-gradient(
      90deg,
      transparent,
      rgba(255, 255, 255, 0.4),
      transparent
    );
    animation: button-shine-initial 2s ease-out 0.5s 1 forwards, button-shine-regular 4s linear 3s infinite;
    z-index: 1;
  }

  /* Initial fast shine when page loads */
  @keyframes button-shine-initial {
    0% {
      left: -100%;
    }
    100% {
      left: 100%;
    }
  }

  /* Regular slower shine that repeats */
  @keyframes button-shine-regular {
    0% {
      left: -100%;
    }
    100% {
      left: 100%;
    }
  }

  .video-button-shine span {
    position: relative;
    z-index: 2;
  }
`;

const Landing = () => {
  const navigate = useNavigate();
  const [configuredProviders, setConfiguredProviders] = useState<LLMProvider[]>([]);
  const [isCheckingKeys, setIsCheckingKeys] = useState(true);
  const [serverHasApiKeys, setServerHasApiKeys] = useState(true);
  const [developerMode, setDeveloperMode] = useState(false);

  useEffect(() => {
    const init = async () => {
      setIsCheckingKeys(true);
      const [providers, cfg] = await Promise.all([
        getConfiguredProviders(),
        configAPI.getConfig().catch(() => ({ server_has_api_keys: false, developer_mode: false })),
      ]);
      setConfiguredProviders(providers);
      setServerHasApiKeys(cfg.server_has_api_keys);
      setDeveloperMode(cfg.developer_mode);
      setIsCheckingKeys(false);
    };
    init();
  }, []);

  const hasApiKeys = configuredProviders.length > 0 || serverHasApiKeys;
  const showApiKeySection = developerMode || !serverHasApiKeys;

  return (
    <div className="max-w-5xl mx-auto">
      <style dangerouslySetInnerHTML={{ __html: videoButtonStyles }} />
      {/* Hero Section */}
      <div className="text-center mb-8">
        <h1 className="text-4xl font-bold tracking-tight mb-4 bg-gradient-to-r from-primary via-blue-500 to-blue-400 bg-clip-text text-transparent">
          From Documents to Data
        </h1>
        <p className="text-lg text-muted-foreground max-w-2xl mx-auto mb-6">
          Turn research documents into structured datasets — automatically.
        </p>
        <div className="flex items-center justify-center gap-3 flex-wrap font-['Google_Sans',sans-serif]">
          <a
            href="https://youtube.com/watch?v=VILym_Ch0hg&feature=youtu.be"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 px-6 py-2 rounded-full bg-gray-800 hover:bg-gray-700 text-white text-[1.1rem] transition-all duration-300 shadow-sm hover:shadow-lg hover:-translate-y-1 relative overflow-hidden video-button-shine"
          >
            <span className="flex items-center justify-center w-5 h-5">
              <i className="fa-brands fa-youtube text-base"></i>
            </span>
            <span>Demonstration Video</span>
          </a>
          <a
            href="#"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 px-6 py-2 rounded-full bg-[#363636] hover:bg-[#2b2b2b] text-white text-[1.1rem] transition-colors shadow-sm"
          >
            <span className="flex items-center justify-center w-5 h-5">
              <i className="ai ai-arxiv text-xl"></i>
            </span>
            <span>arXiv</span>
          </a>
          <a
            href="https://github.com/shaharl6000/"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 px-6 py-2 rounded-full bg-[#363636] hover:bg-[#2b2b2b] text-white text-[1.1rem] transition-colors shadow-sm"
          >
            <span className="flex items-center justify-center w-5 h-5">
              <i className="fab fa-github text-xl"></i>
            </span>
            <span>Code</span>
          </a>
          <a
            href="https://x.com/"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 px-6 py-2 rounded-full bg-[#363636] hover:bg-[#2b2b2b] text-white text-[1.1rem] transition-colors shadow-sm"
          >
            <span className="flex items-center justify-center w-5 h-5">
              <i className="fa-brands fa-x-twitter text-sm"></i>
            </span>
            <span>Twitter</span>
          </a>
        </div>
      </div>

      {/* API Key Configuration Section */}
      {showApiKeySection && <ApiKeySection onConfigurationChange={setConfiguredProviders} />}

      {/* Main Cards */}
      <div className="grid md:grid-cols-2 gap-6">
        {/* New Project Card */}
        <Card className={`flex flex-col transition-all hover:-translate-y-1 hover:shadow-lg ${hasApiKeys ? 'border-primary/20' : 'opacity-60'}`}>
          <CardHeader>
            <CardTitle className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-primary/10">
                <Sparkles className="h-6 w-6 text-primary" />
              </div>
              Start a New Project
            </CardTitle>
            <CardDescription>
              Ask your research question and upload documents. The system discovers what to extract and builds your dataset.
            </CardDescription>
          </CardHeader>
          <CardFooter className="mt-auto">
            <Button
              className="w-full"
              size="lg"
              onClick={() => navigate('/schematiq')}
              disabled={!hasApiKeys}
            >
              <Sparkles className="mr-2 h-4 w-4" />
              {hasApiKeys ? 'START' : 'Configure API Keys First'}
            </Button>
          </CardFooter>
        </Card>

        {/* Load Existing Project Card */}
        <Card className="flex flex-col transition-all hover:-translate-y-1 hover:shadow-lg">
          <CardHeader>
            <CardTitle className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-secondary">
                <Upload className="h-6 w-6 text-muted-foreground" />
              </div>
              Load an Existing Project
            </CardTitle>
            <CardDescription>
              Open a previously generated dataset to explore, refine, or export.
            </CardDescription>
          </CardHeader>
          <CardFooter className="mt-auto">
            <Button
              className="w-full"
              size="lg"
              onClick={() => navigate('/load')}
            >
              <Upload className="mr-2 h-4 w-4" />
              LOAD
            </Button>
          </CardFooter>
        </Card>
      </div>

      {/* Exploration path for visitors without API keys */}
      {!hasApiKeys && (
        <p className="text-center mt-4 text-sm text-muted-foreground">
          No API key?{' '}
          <button
            onClick={() => navigate('/load')}
            className="text-primary hover:underline font-medium"
          >
            Explore an example dataset first
          </button>
        </p>
      )}

      {/* How It Works */}
      <div className="text-center mt-12">
        <h2 className="text-2xl font-semibold mb-8">How It Works</h2>
        <div className="grid sm:grid-cols-3 gap-8">
          <div className="flex flex-col items-center">
            <div className="p-4 rounded-full bg-primary/10 mb-4">
              <FileUp className="h-8 w-8 text-primary" />
            </div>
            <h3 className="font-semibold mb-2">Input</h3>
            <p className="text-sm text-muted-foreground">
              Research question + document corpus
            </p>
          </div>
          <div className="flex flex-col items-center">
            <div className="p-4 rounded-full bg-primary/10 mb-4">
              <Sparkles className="h-8 w-8 text-primary" />
            </div>
            <h3 className="font-semibold mb-2">Schema Discovery</h3>
            <p className="text-sm text-muted-foreground">
              Automatically find relevant data fields
            </p>
          </div>
          <div className="flex flex-col items-center">
            <div className="p-4 rounded-full bg-primary/10 mb-4">
              <Table className="h-8 w-8 text-primary" />
            </div>
            <h3 className="font-semibold mb-2">Structured Output</h3>
            <p className="text-sm text-muted-foreground">
              Review, edit, and export your dataset
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Landing;
