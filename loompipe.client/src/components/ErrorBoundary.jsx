import React from 'react';
import { AlertCircle, RefreshCw } from 'lucide-react';

/**
 * Class-based error boundary — catches unhandled runtime errors in the
 * component tree and renders a recovery screen instead of a blank page.
 */
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error) {
    return { error };
  }

  componentDidCatch(error, info) {
    console.error('[ErrorBoundary] Uncaught error:', error, info);
  }

  render() {
    if (this.state.error) {
      return (
        <div className="flex flex-col items-center justify-center h-screen bg-slate-950 text-white px-6">
          <div className="p-4 bg-rose-500/10 rounded-full mb-6">
            <AlertCircle size={40} className="text-rose-500" />
          </div>
          <h1 className="text-xl font-bold mb-2">Something went wrong</h1>
          <p className="text-slate-400 text-sm mb-2 text-center max-w-md">
            An unexpected error occurred. The error has been logged to the console.
          </p>
          <p className="text-slate-600 font-mono text-xs mb-8 text-center max-w-md truncate">
            {this.state.error.message}
          </p>
          <button
            onClick={() => window.location.reload()}
            className="flex items-center gap-2 px-5 py-2.5 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg font-semibold text-sm transition-colors"
          >
            <RefreshCw size={15} />
            Reload Page
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}

export default ErrorBoundary;
