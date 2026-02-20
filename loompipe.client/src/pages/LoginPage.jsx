import { useState } from 'react';
import { Loader2, Workflow, AlertCircle } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';

const LoginPage = () => {
  const { login } = useAuth();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      await login(username, password);
    } catch (err) {
      setError(err.message || 'Login failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-[var(--bg-base)]">
      <div className="w-80 bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-8">
        <div className="flex items-center gap-2 mb-7">
          <Workflow size={20} className="text-[var(--accent)]" />
          <span className="text-lg font-bold text-[var(--text-primary)] tracking-wide">LoomPipe</span>
        </div>

        <h1 className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider mb-5">
          Sign in to continue
        </h1>

        {error && (
          <div className="flex items-center gap-2 bg-red-900/20 border border-[var(--red)] text-[var(--red)] rounded px-3 py-2 text-xs mb-4">
            <AlertCircle size={13} className="flex-shrink-0" />
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="flex flex-col gap-3">
          <div>
            <label className="block text-xs text-[var(--text-muted)] mb-1">Username</label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              autoComplete="username"
              autoFocus
              required
              className="w-full bg-[var(--bg-elevated)] border border-[var(--border)] rounded px-3 py-2 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors"
              placeholder="admin"
            />
          </div>

          <div>
            <label className="block text-xs text-[var(--text-muted)] mb-1">Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete="current-password"
              required
              className="w-full bg-[var(--bg-elevated)] border border-[var(--border)] rounded px-3 py-2 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors"
              placeholder="••••••••"
            />
          </div>

          <button
            type="submit"
            disabled={loading}
            className="mt-1 w-full flex items-center justify-center gap-2 bg-[var(--accent)] hover:bg-[var(--accent-dim)] disabled:opacity-60 text-white font-semibold py-2 rounded text-sm transition-colors"
          >
            {loading && <Loader2 size={15} className="animate-spin" />}
            {loading ? 'Signing in…' : 'Sign in'}
          </button>
        </form>
      </div>
    </div>
  );
};

export default LoginPage;
