import React from 'react';
import { AlertTriangle } from 'lucide-react';

import { Button } from '@/components/ui/button';

interface AppErrorBoundaryProps {
  children: React.ReactNode;
  /** When this value changes (e.g. the route), a caught error is cleared. */
  resetKey?: string;
}

interface AppErrorBoundaryState {
  error: Error | null;
}

/**
 * Catches render errors below it so one broken component shows a recoverable
 * message instead of unmounting the whole app to a blank page.
 */
class AppErrorBoundary extends React.Component<AppErrorBoundaryProps, AppErrorBoundaryState> {
  state: AppErrorBoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): AppErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    console.error('Unhandled render error', error, info.componentStack);
  }

  componentDidUpdate(prevProps: AppErrorBoundaryProps) {
    if (this.state.error && prevProps.resetKey !== this.props.resetKey) {
      this.setState({ error: null });
    }
  }

  render() {
    const { error } = this.state;
    if (!error) return this.props.children;

    return (
      <div role="alert" className="h-full min-h-[50vh] flex items-center justify-center p-6">
        <div className="max-w-md w-full flex flex-col items-center gap-4 text-center">
          <AlertTriangle className="h-10 w-10 text-destructive" />
          <h2 className="text-lg font-semibold">Something went wrong</h2>
          <p className="text-sm text-muted-foreground">
            This page hit an unexpected error. Reload to try again.
          </p>
          {error.message && (
            <pre className="w-full max-h-32 overflow-auto rounded-md bg-muted p-3 text-left text-xs text-muted-foreground whitespace-pre-wrap break-words">
              {error.message}
            </pre>
          )}
          <div className="flex gap-2">
            <Button onClick={() => window.location.reload()}>Reload</Button>
            <Button variant="outline" onClick={() => window.location.assign('/')}>
              Go to home
            </Button>
          </div>
        </div>
      </div>
    );
  }
}

export default AppErrorBoundary;
