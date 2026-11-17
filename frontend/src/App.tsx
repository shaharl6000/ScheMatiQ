import { Suspense } from 'react';
import { Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { Mail } from 'lucide-react';

import { ThemeToggle } from '@/components/theme/theme-toggle';
import { TooltipProvider } from '@/components/ui/tooltip';
import { ViewModeProvider } from './contexts/ViewModeContext';
import { NavigationGuardProvider } from './contexts/NavigationGuardContext';
import AppErrorBoundary from '@/components/shared/AppErrorBoundary';
import { lazyWithReload } from './lib/lazyWithReload';

// Each route page is its own chunk, so the first visit downloads only the
// page it opens. A chunk that vanished after a deploy reloads the app once.
const Load = lazyWithReload(() => import('./pages/Load'));
const ScheMatiQConfig = lazyWithReload(() => import('./pages/ScheMatiQConfig'));
const Visualize = lazyWithReload(() => import('./pages/Visualize'));
const Workspace = lazyWithReload(() => import('./pages/Workspace'));

function AppHeader() {
  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container flex h-14 items-center">
        <div className="flex items-center gap-2 mr-6">
          <img
            src="/icon.png"
            alt="ScheMatiQ logo"
            className="h-6 w-auto"
          />
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
  const location = useLocation();
  const isWorkspace = location.pathname === '/' || location.pathname.startsWith('/workspace');

  return (
    <TooltipProvider>
    <NavigationGuardProvider>
    <ViewModeProvider>
      <div className={isWorkspace ? 'h-screen overflow-hidden flex flex-col bg-background' : 'min-h-screen flex flex-col bg-background'}>
        {!isWorkspace && <AppHeader />}

        {/* Main Content */}
        <main className={isWorkspace ? 'flex-1 min-h-0 overflow-hidden' : 'container py-6 flex-1'}>
          <AppErrorBoundary resetKey={location.pathname}>
          <Suspense
            fallback={
              <div className="flex-1 flex items-center justify-center py-12 text-sm text-muted-foreground">
                Loading…
              </div>
            }
          >
            <Routes>
              <Route path="/" element={<Workspace />} />
              <Route path="/classic" element={<ScheMatiQConfig />} />
              <Route path="/load" element={<Load />} />
              <Route path="/schematiq" element={<Navigate to="/classic" replace />} />
              <Route path="/visualize/:sessionId" element={<Visualize />} />
              {/* Bare /workspace redirects to the new default; session URLs keep the prefix. */}
              <Route path="/workspace" element={<Navigate to="/" replace />} />
              <Route path="/workspace/:sessionId" element={<Workspace />} />
            </Routes>
          </Suspense>
          </AppErrorBoundary>
        </main>

        {/* Footer */}
        {!isWorkspace && <footer className="border-t mt-auto bg-muted/40">
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
        </footer>}
      </div>
    </ViewModeProvider>
    </NavigationGuardProvider>
    </TooltipProvider>
  );
}

export default App;
