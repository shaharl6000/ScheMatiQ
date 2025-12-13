import { useNavigate } from 'react-router-dom';
import {
  Sparkles,
  Upload,
  CheckCircle2,
  Gauge,
  Eye,
  Pencil,
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

const Landing = () => {
  const navigate = useNavigate();

  const loadFeatures = [
    'Support for CSV and JSON/JSONL files',
    'Automatic data type detection',
    'Data quality analysis',
    'Interactive data exploration',
  ];

  const qbsdFeatures = [
    'AI-powered schema discovery',
    'Multi-document processing',
    'Real-time progress monitoring',
    'Customizable LLM backends',
  ];

  return (
    <div className="max-w-5xl mx-auto">
      {/* Hero Section */}
      <div className="text-center mb-12">
        <h1 className="text-4xl font-bold tracking-tight mb-4 bg-gradient-to-r from-primary via-blue-500 to-blue-400 bg-clip-text text-transparent">
          QBSD Visualization
        </h1>
        <p className="text-lg text-muted-foreground mb-4 max-w-2xl mx-auto">
          Interactive visualization and schema editing for Query-Based Schema Discovery
        </p>
        <Badge variant="outline" className="text-primary border-primary">
          Dual Input Options
        </Badge>
      </div>

      {/* Main Cards */}
      <div className="grid md:grid-cols-2 gap-6 mb-12">
        {/* Create QBSD Card */}
        <Card className="flex flex-col transition-all hover:-translate-y-1 hover:shadow-lg border-primary/20">
          <CardHeader>
            <CardTitle className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-primary/10">
                <Sparkles className="h-6 w-6 text-primary" />
              </div>
              Create QBSD
            </CardTitle>
            <CardDescription>
              Run the full QBSD pipeline to discover schemas and extract structured data
              from your document collections using AI-powered analysis.
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
              <Badge variant="info">AI-Powered</Badge>
              <Badge variant="warning">Real-time</Badge>
            </div>
          </CardContent>
          <CardFooter>
            <Button
              className="w-full"
              size="lg"
              onClick={() => navigate('/qbsd')}
            >
              <Sparkles className="mr-2 h-4 w-4" />
              Create QBSD
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
              Load Existing QBSD
            </CardTitle>
            <CardDescription>
              Import your existing QBSD datasets for visualization, editing, and analysis.
              Perfect for exploring pre-processed data or continuing previous work.
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
              Load Data
            </Button>
          </CardFooter>
        </Card>
      </div>

      {/* Features Section */}
      <div className="text-center">
        <h2 className="text-2xl font-semibold mb-8">Powerful Visualization Features</h2>
        <div className="grid sm:grid-cols-3 gap-8">
          <div className="flex flex-col items-center">
            <div className="p-4 rounded-full bg-primary/10 mb-4">
              <Eye className="h-8 w-8 text-primary" />
            </div>
            <h3 className="font-semibold mb-2">Interactive Visualization</h3>
            <p className="text-sm text-muted-foreground">
              Explore your data with dynamic tables, charts, and schema views
            </p>
          </div>
          <div className="flex flex-col items-center">
            <div className="p-4 rounded-full bg-primary/10 mb-4">
              <Pencil className="h-8 w-8 text-primary" />
            </div>
            <h3 className="font-semibold mb-2">Schema Editing</h3>
            <p className="text-sm text-muted-foreground">
              Modify schemas with real-time validation and re-extraction
            </p>
          </div>
          <div className="flex flex-col items-center">
            <div className="p-4 rounded-full bg-primary/10 mb-4">
              <Gauge className="h-8 w-8 text-primary" />
            </div>
            <h3 className="font-semibold mb-2">Performance Optimized</h3>
            <p className="text-sm text-muted-foreground">
              Handle large datasets with virtual scrolling and lazy loading
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Landing;
