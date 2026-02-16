import { Routes, Route } from 'react-router-dom';
import { Database, Mail } from 'lucide-react';

import Landing from './pages/Landing';
import Load from './pages/Load';
import QBSDConfig from './pages/QBSDConfig';
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
          <Database className="h-6 w-6 text-primary" />
          <span className="text-xl font-semibold bg-gradient-to-r from-primary to-blue-400 bg-clip-text text-transparent">
            QBSD Visualization
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
            <Route path="/qbsd" element={<QBSDConfig />} />
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
