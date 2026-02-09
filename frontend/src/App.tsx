import { Routes, Route, Link } from 'react-router-dom';
import { Database } from 'lucide-react';

import Landing from './pages/Landing';
import Load from './pages/Load';
import QBSDConfig from './pages/QBSDConfig';
import Visualize from './pages/Visualize';
import { ThemeToggle } from '@/components/theme/theme-toggle';
import { TooltipProvider } from '@/components/ui/tooltip';
import { ViewModeProvider } from './contexts/ViewModeContext';

function App() {
  return (
    <TooltipProvider>
    <ViewModeProvider>
      <div className="min-h-screen bg-background">
        {/* Header */}
        <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
          <div className="container flex h-14 items-center">
            <Link to="/" className="flex items-center gap-2 mr-6">
              <Database className="h-6 w-6 text-primary" />
              <span className="text-xl font-semibold bg-gradient-to-r from-primary to-blue-400 bg-clip-text text-transparent">
                QBSD Visualization
              </span>
            </Link>

            <div className="flex flex-1 items-center justify-end space-x-2">
              <ThemeToggle />
            </div>
          </div>
        </header>

        {/* Main Content */}
        <main className="container py-6">
          <Routes>
            <Route path="/" element={<Landing />} />
            <Route path="/load" element={<Load />} />
            <Route path="/qbsd" element={<QBSDConfig />} />
            <Route path="/visualize/:sessionId" element={<Visualize />} />
          </Routes>
        </main>
      </div>
    </ViewModeProvider>
    </TooltipProvider>
  );
}

export default App;
