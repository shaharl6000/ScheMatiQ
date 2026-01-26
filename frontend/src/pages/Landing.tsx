import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Sparkles,
  Upload,
  CheckCircle2,
  Gauge,
  Eye,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { ApiKeySection } from '@/components/ApiKeySection';
import { getConfiguredProviders, LLMProvider } from '@/utils/apiKeyStorage';

const Landing = () => {
  const navigate = useNavigate();
  const [configuredProviders, setConfiguredProviders] = useState<LLMProvider[]>([]);
  const [isCheckingKeys, setIsCheckingKeys] = useState(true);

  useEffect(() => {
    const checkKeys = async () => {
      setIsCheckingKeys(true);
      const providers = await getConfiguredProviders();
      setConfiguredProviders(providers);
      setIsCheckingKeys(false);
    };
    checkKeys();
  }, []);

  const hasApiKeys = configuredProviders.length > 0;

  const loadFeatures = [
    'CSV & JSON support',
    'Interactive tables',
  ];

  const qbsdFeatures = [
    'Automatically discovers relevant fields',
    'Choose your LLM',
  ];

  return (
    <div className="max-w-5xl mx-auto">
      {/* Hero Section */}
      <div className="text-center mb-8">
        <h1 className="text-4xl font-bold tracking-tight mb-4 bg-gradient-to-r from-primary via-blue-500 to-blue-400 bg-clip-text text-transparent">
          From Documents to Data
        </h1>
        <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
          Given a research question and a document corpus, we automatically discover the relevant fields and extract structured data.
        </p>
      </div>

      {/* API Key Configuration Section */}
      <ApiKeySection onConfigurationChange={setConfiguredProviders} />

      {/* Main Cards */}
      <div className="grid md:grid-cols-2 gap-6 mb-12">
        {/* Create QBSD Card */}
        <Card className={`flex flex-col transition-all hover:-translate-y-1 hover:shadow-lg ${hasApiKeys ? 'border-primary/20' : 'opacity-60'}`}>
          <CardHeader>
            <CardTitle className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-primary/10">
                <Sparkles className="h-6 w-6 text-primary" />
              </div>
              New Project
            </CardTitle>
            <CardDescription>
              Ask your research question and upload documents. The system discovers what to extract and builds your dataset.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex-grow">
            <h4 className="font-medium mb-3">Features:</h4>
            <ul className="space-y-2">
              {qbsdFeatures.map((feature, index) => (
                <li key={index} className="flex items-center gap-2 text-sm text-muted-foreground">
                  <CheckCircle2 className="h-4 w-4 text-green-500 shrink-0" />
                  {feature}
                </li>
              ))}
            </ul>
            <div className="flex gap-2 mt-4">
              <Badge variant="info">Query-Based</Badge>
              <Badge variant="warning">Real-time</Badge>
            </div>
          </CardContent>
          <CardFooter>
            <Button
              className="w-full"
              size="lg"
              onClick={() => navigate('/qbsd')}
              disabled={!hasApiKeys}
            >
              <Sparkles className="mr-2 h-4 w-4" />
              {hasApiKeys ? 'Start' : 'Configure API Keys First'}
            </Button>
          </CardFooter>
        </Card>

        {/* Load Existing QBSD Card */}
        <Card className="flex flex-col transition-all hover:-translate-y-1 hover:shadow-lg">
          <CardHeader>
            <CardTitle className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-secondary">
                <Upload className="h-6 w-6 text-muted-foreground" />
              </div>
              Load Existing Project
            </CardTitle>
            <CardDescription>
              Open a previously generated dataset to explore, refine the schema, or export results.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex-grow">
            <h4 className="font-medium mb-3">Features:</h4>
            <ul className="space-y-2">
              {loadFeatures.map((feature, index) => (
                <li key={index} className="flex items-center gap-2 text-sm text-muted-foreground">
                  <CheckCircle2 className="h-4 w-4 text-green-500 shrink-0" />
                  {feature}
                </li>
              ))}
            </ul>
            <div className="flex gap-2 mt-4">
              <Badge variant="success">Quick Start</Badge>
              <Badge variant="info">CSV/JSON</Badge>
            </div>
          </CardContent>
          <CardFooter>
            <Button
              className="w-full"
              size="lg"
              onClick={() => navigate('/load')}
            >
              <Upload className="mr-2 h-4 w-4" />
              Open
            </Button>
          </CardFooter>
        </Card>
      </div>

      {/* Features Section */}
      <div className="text-center">
        <h2 className="text-2xl font-semibold mb-8">How It Works</h2>
        <div className="grid sm:grid-cols-3 gap-8">
          <div className="flex flex-col items-center">
            <div className="p-4 rounded-full bg-primary/10 mb-4">
              <Eye className="h-8 w-8 text-primary" />
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
              <Gauge className="h-8 w-8 text-primary" />
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
