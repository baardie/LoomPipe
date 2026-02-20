import { useState, useEffect, useCallback } from 'react';
import { Loader2, ChevronLeft, Play, Pencil, XCircle, ChevronDown, ChevronRight, AlertCircle } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import RoleGuard from '../components/auth/RoleGuard';

const STATUS_STYLE = {
  Success: 'bg-green-900/30 text-[var(--green)] border-green-900/50',
  Failed:  'bg-red-900/30 text-[var(--red)] border-red-900/50',
  Running: 'bg-yellow-900/30 text-[var(--yellow)] border-yellow-900/50',
};

const STAGE_LABELS = {
  SourceRead:       'Source Read',
  Mapping:          'Field Mapping / Transformation',
  DestinationWrite: 'Destination Write',
};

const StatusBadge = ({ status }) => (
  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${STATUS_STYLE[status] ?? 'bg-[var(--bg-elevated)] text-[var(--text-muted)] border-[var(--border)]'}`}>
    {status}
  </span>
);

const StatCard = ({ label, value, highlight }) => (
  <div className={`bg-[var(--bg-surface)] border rounded-lg p-4 ${highlight ? 'border-red-800/60' : 'border-[var(--border)]'}`}>
    <div className={`text-xl font-bold font-mono ${highlight ? 'text-[var(--red)]' : 'text-[var(--text-primary)]'}`}>{value ?? '–'}</div>
    <div className="text-xs text-[var(--text-secondary)] uppercase tracking-wider mt-1">{label}</div>
  </div>
);

/** Expandable error row shown beneath a failed run row. */
const ErrorDetailRow = ({ errorMessage }) => {
  // Split on the → separator the engine produces for chained messages
  const parts = errorMessage?.split(' → ') ?? [];

  return (
    <tr>
      <td colSpan={5} className="px-4 pb-3 pt-0">
        <div className="bg-red-950/40 border border-red-900/40 rounded-md p-3 text-xs">
          {parts.length > 1 ? (
            <ol className="space-y-1 list-none">
              {parts.map((p, i) => (
                <li key={i} className="flex items-start gap-2">
                  <span className="text-red-400 font-mono shrink-0">{i === parts.length - 1 ? '⤷' : String(i + 1) + '.'}</span>
                  <span className={i === parts.length - 1 ? 'text-[var(--red)] font-medium' : 'text-red-300'}>{p}</span>
                </li>
              ))}
            </ol>
          ) : (
            <span className="text-[var(--red)]">{errorMessage}</span>
          )}
        </div>
      </td>
    </tr>
  );
};

/** A single run row, with expand/collapse for error detail. */
const RunRow = ({ r }) => {
  const [expanded, setExpanded] = useState(false);
  const hasFailed = r.status === 'Failed' && r.errorMessage;

  return (
    <>
      <tr
        className={`hover:bg-[var(--bg-subtle)] ${hasFailed ? 'cursor-pointer' : ''}`}
        onClick={hasFailed ? () => setExpanded(e => !e) : undefined}
      >
        <td className="px-4 py-2 font-mono text-[var(--text-muted)]">{new Date(r.startedAt).toLocaleString()}</td>
        <td className="px-4 py-2"><StatusBadge status={r.status} /></td>
        <td className="px-4 py-2 text-[var(--text-secondary)]">{r.durationMs != null ? `${(r.durationMs / 1000).toFixed(1)}s` : '–'}</td>
        <td className="px-4 py-2 text-[var(--text-secondary)]">{r.rowsProcessed ?? '–'}</td>
        <td className="px-4 py-2">
          {hasFailed ? (
            <span className="inline-flex items-center gap-1 text-[var(--red)]">
              {expanded ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
              <span className="truncate max-w-[200px]" title={r.errorMessage}>
                {r.errorMessage.length > 50 ? r.errorMessage.slice(0, 50) + '…' : r.errorMessage}
              </span>
            </span>
          ) : (
            <span className="text-[var(--text-muted)]">–</span>
          )}
        </td>
      </tr>
      {expanded && hasFailed && <ErrorDetailRow errorMessage={r.errorMessage} />}
    </>
  );
};

const PipelineDetailPage = ({ pipelineId, onBack, onEdit }) => {
  const { authFetch } = useAuth();
  const [pipeline,  setPipeline]  = useState(null);
  const [runs,      setRuns]      = useState([]);
  const [stats,     setStats]     = useState(null);
  const [loading,   setLoading]   = useState(true);
  const [running,   setRunning]   = useState(false);
  const [runError,  setRunError]  = useState(null);   // inline error from the Run Now button
  const [runBanner, setRunBanner] = useState(null);   // success banner

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [pRes, rRes, sRes] = await Promise.all([
        authFetch(`/api/pipelines/${pipelineId}`),
        authFetch(`/api/pipelines/${pipelineId}/runs`),
        authFetch(`/api/pipelines/${pipelineId}/stats`),
      ]);
      if (pRes.ok) setPipeline(await pRes.json());
      if (rRes.ok) setRuns(await rRes.json());
      if (sRes.ok) setStats(await sRes.json());
    } finally { setLoading(false); }
  }, [pipelineId, authFetch]);

  useEffect(() => { load(); }, [load]);

  const handleRun = async () => {
    setRunning(true);
    setRunError(null);
    setRunBanner(null);
    try {
      const res = await authFetch(`/api/pipelines/${pipelineId}/run`, { method: 'POST' });
      if (res.ok) {
        const data = await res.json().catch(() => ({}));
        setRunBanner(`Pipeline executed successfully — ${data.rowsProcessed ?? 0} row(s) processed.`);
        setTimeout(load, 800);
      } else {
        const data = await res.json().catch(() => ({}));
        const stageLabel = data.stage ? ` [${STAGE_LABELS[data.stage] ?? data.stage}]` : '';
        setRunError(`Execution failed${stageLabel}: ${data.message ?? 'Unknown error'}`);
        setTimeout(load, 800);
      }
    } catch (err) {
      setRunError(`Network error: ${err.message}`);
    } finally {
      setRunning(false);
    }
  };

  if (loading) return <div className="flex items-center justify-center h-48"><Loader2 size={20} className="animate-spin text-[var(--accent)]" /></div>;
  if (!pipeline) return <div className="p-6 text-[var(--text-muted)]">Pipeline not found.</div>;

  const lastFailed = runs[0]?.status === 'Failed';

  return (
    <div className="p-6">
      <button onClick={onBack} className="flex items-center gap-1 text-xs text-[var(--text-secondary)] hover:text-[var(--text-primary)] mb-4 transition-colors">
        <ChevronLeft size={15} /> Back to Pipelines
      </button>

      <div className="flex items-start justify-between mb-5">
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-base font-semibold text-[var(--text-primary)]">{pipeline.name}</h1>
            {lastFailed && (
              <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-red-900/30 text-[var(--red)] border border-red-800/50">
                <XCircle size={10} /> Last run failed
              </span>
            )}
          </div>
          <div className="flex items-center gap-2 mt-1 text-xs text-[var(--text-muted)] font-mono">
            <span>{pipeline.source?.type ?? '–'}</span>
            <span>→</span>
            <span>{pipeline.destination?.type ?? '–'}</span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <RoleGuard roles={['Admin']}>
            <button onClick={() => onEdit && onEdit(pipeline)} className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-[var(--border)] text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:border-[var(--accent)] rounded transition-colors">
              <Pencil size={13} /> Edit
            </button>
            <button onClick={handleRun} disabled={running} className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-[var(--accent)] hover:bg-[var(--accent-dim)] text-white rounded transition-colors disabled:opacity-60">
              {running ? <Loader2 size={13} className="animate-spin" /> : <Play size={13} />} Run Now
            </button>
          </RoleGuard>
        </div>
      </div>

      {/* Run result banners */}
      {runBanner && (
        <div className="mb-4 flex items-center gap-2 px-3 py-2.5 rounded-md bg-green-900/20 border border-green-800/40 text-xs text-[var(--green)]">
          {runBanner}
          <button onClick={() => setRunBanner(null)} className="ml-auto opacity-60 hover:opacity-100">✕</button>
        </div>
      )}
      {runError && (
        <div className="mb-4 rounded-md bg-red-950/40 border border-red-900/50 p-3">
          <div className="flex items-start gap-2 text-xs text-[var(--red)]">
            <AlertCircle size={14} className="shrink-0 mt-0.5" />
            <div className="flex-1 min-w-0">
              <p className="font-semibold mb-1">Pipeline execution failed</p>
              <p className="text-red-300 break-words">{runError}</p>
            </div>
            <button onClick={() => setRunError(null)} className="shrink-0 opacity-60 hover:opacity-100 text-red-400">✕</button>
          </div>
        </div>
      )}

      {stats && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-6">
          <StatCard label="Total Runs"   value={stats.totalRuns} />
          <StatCard label="Successes"    value={stats.successCount} />
          <StatCard label="Failures"     value={stats.failCount} highlight={stats.failCount > 0} />
          <StatCard label="Avg Duration" value={stats.avgDurationMs != null ? `${(stats.avgDurationMs / 1000).toFixed(1)}s` : '–'} />
        </div>
      )}

      <h2 className="text-xs font-semibold uppercase tracking-wider text-[var(--text-secondary)] mb-3">Run History</h2>
      <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg overflow-hidden">
        <table className="w-full text-xs">
          <thead className="bg-[var(--bg-subtle)] border-b border-[var(--border)]">
            <tr>
              {['Started', 'Status', 'Duration', 'Rows', 'Error (click to expand)'].map(h => (
                <th key={h} className="px-4 py-2.5 text-left font-semibold uppercase tracking-wider text-[var(--text-secondary)]">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-[var(--border)]">
            {runs.length === 0 ? (
              <tr><td colSpan={5} className="px-4 py-6 text-center text-[var(--text-muted)]">No runs yet.</td></tr>
            ) : runs.map(r => (
              <RunRow key={r.id} r={r} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default PipelineDetailPage;
