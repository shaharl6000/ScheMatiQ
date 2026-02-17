import { Routes, Route } from 'react-router-dom';
import { Mail } from 'lucide-react';

import Landing from './pages/Landing';
import Load from './pages/Load';
import ScheMatiQConfig from './pages/ScheMatiQConfig';
import Visualize from './pages/Visualize';
import { ThemeToggle } from '@/components/theme/theme-toggle';
import { TooltipProvider } from '@/components/ui/tooltip';
import { ViewModeProvider } from './contexts/ViewModeContext';
import { NavigationGuardProvider } from './contexts/NavigationGuardContext';

function AppHeader() {
  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container flex h-14 items-center">
        <div className="flex items-center gap-2 mr-6">
          <svg className="h-6 w-6" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <rect x="2" y="2" width="15" height="14" rx="2" className="text-primary" />
            <line x1="2" y1="6" x2="17" y2="6" className="text-primary" />
            <line x1="2" y1="10" x2="17" y2="10" className="text-primary" />
            <line x1="8" y1="2" x2="8" y2="16" className="text-primary" />
            <line x1="13" y1="2" x2="13" y2="16" className="text-primary" />
            <circle cx="18" cy="18" r="4" className="text-primary" />
            <line x1="21" y1="21" x2="23" y2="23" className="text-primary" strokeWidth="2" />
          </svg>
          <span className="text-xl font-semibold bg-gradient-to-r from-primary to-blue-400 bg-clip-text text-transparent">
            ScheMatiQ
          </span>
        </div>

        <div className="flex flex-1 items-center justify-end space-x-2">
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}

function App() {
  return (
    <TooltipProvider>
    <NavigationGuardProvider>
    <ViewModeProvider>
      <div className="min-h-screen flex flex-col bg-background">
        <AppHeader />

        {/* Main Content */}
        <main className="container py-6 flex-1">
          <Routes>
            <Route path="/" element={<Landing />} />
            <Route path="/load" element={<Load />} />
            <Route path="/schematiq" element={<ScheMatiQConfig />} />
            <Route path="/visualize/:sessionId" element={<Visualize />} />
          </Routes>
        </main>

        {/* Footer */}
        <footer className="border-t mt-auto bg-muted/40">
          <div className="container py-6 flex flex-col sm:flex-row items-center gap-4 sm:gap-6">
            <img
              src="/huji_icon.png"
              alt="HUJI NLP Lab"
              className="h-14 w-auto dark:invert"
            />
            <div className="flex flex-col items-center sm:items-start gap-1.5 text-muted-foreground">
              <p className="font-semibold text-foreground text-base">The Hebrew University of Jerusalem · NLP Research</p>
              <div className="flex items-center gap-4 flex-wrap text-sm">
                <a
                  href="mailto:shahar.levy2@mail.huji.ac.il"
                  className="inline-flex items-center gap-1 hover:text-primary hover:underline transition-colors"
                >
                  <Mail className="h-3.5 w-3.5" />
                  shahar.levy2@mail.huji.ac.il
                </a>
                <a
                  href="mailto:eliya.habba@mail.huji.ac.il"
                  className="inline-flex items-center gap-1 hover:text-primary hover:underline transition-colors"
                >
                  <Mail className="h-3.5 w-3.5" />
                  eliya.habba@mail.huji.ac.il
                </a>
              </div>
            </div>
          </div>
        </footer>
      </div>
    </ViewModeProvider>
    </NavigationGuardProvider>
    </TooltipProvider>
  );
}

export default App;
