import React, { useState, useEffect, useRef, useMemo } from 'react';
import {
  Loader2,
  Layers,
  Activity,
  CheckCircle2,
  AlertCircle,
  Clock,
  Plus,
  Play,
  MoreVertical,
  ChevronRight,
  GitMerge,
} from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
const TYPE_LABELS = {
  postgresql: 'PostgreSQL',
  sqlserver:  'SQL Server',
  mysql:      'MySQL',
  csv:        'CSV File',
  sqlite:     'SQLite',
};
const getTypeLabel = (type) =>
  TYPE_LABELS[type?.toLowerCase()] ?? type ?? 'Database';

// ---------------------------------------------------------------------------
// MetricCard
// ---------------------------------------------------------------------------
const MetricCard = ({ label, value, trend, icon: Icon, iconBg, iconColor }) => (
  <div className="bg-slate-900 border border-slate-800 p-5 rounded-xl">
    <div className="flex justify-between items-start mb-4">
      <div className={`p-2 rounded-lg ${iconBg}`}>
        <Icon size={22} className={iconColor} />
      </div>
      {trend && (
        <span
          className={`text-xs font-bold px-2 py-1 rounded-full ${
            trend.startsWith('+')
              ? 'bg-emerald-500/10 text-emerald-500'
              : 'bg-rose-500/10 text-rose-500'
          }`}
        >
          {trend}
        </span>
      )}
    </div>
    <div className="text-2xl font-bold text-white mb-1">{value ?? '–'}</div>
    <div className="text-slate-500 text-sm">{label}</div>
  </div>
);

// ---------------------------------------------------------------------------
// VisualCanvas — shows real pipelines as source → target nodes.
// Handles incomplete pipelines (missing source / destination) gracefully.
// ---------------------------------------------------------------------------
const VisualCanvas = ({ pipelines = [], onNewPipeline }) => {
  const CANVAS_H = 360;

  // Annotate each pipeline with completeness flags
  const display = pipelines.slice(0, 3).map((p) => {
    const srcType = p.source?.type ?? p.sourceType ?? null;
    const dstType = p.destination?.type ?? p.destinationType ?? null;
    return {
      ...p,
      srcType,
      dstType,
      hasSrc: !!srcType,
      hasDst: !!dstType,
      isComplete: !!srcType && !!dstType,
    };
  });

  const rowY = (i, count) => {
    if (count === 1) return CANVAS_H / 2 - 36;
    if (count === 2) return [CANVAS_H * 0.28, CANVAS_H * 0.65][i] - 36;
    return [CANVAS_H * 0.18, CANVAS_H * 0.50, CANVAS_H * 0.80][i] - 36;
  };

  const completePipes = display.filter((p) => p.isComplete).length;

  return (
    <div
      className="relative w-full rounded-xl border border-slate-800 overflow-hidden bg-slate-950"
      style={{ height: CANVAS_H }}
    >
      {/* Dot-grid background */}
      <div
        className="absolute inset-0 opacity-50"
        style={{
          backgroundImage: 'radial-gradient(#1e293b 1px, transparent 1px)',
          backgroundSize: '24px 24px',
        }}
      />

      {/* Toolbar */}
      <div className="absolute top-4 left-4 z-10">
        <div className="bg-slate-900 border border-slate-800 rounded-lg p-1 flex gap-1">
          <button
            onClick={onNewPipeline}
            title="New pipeline"
            className="p-2 text-white bg-indigo-600 rounded-md hover:bg-indigo-700 transition-colors"
          >
            <Plus size={14} />
          </button>
        </div>
      </div>

      {/* SVG bezier edges */}
      <svg
        className="absolute inset-0 w-full h-full pointer-events-none"
        viewBox={`0 0 640 ${CANVAS_H}`}
        preserveAspectRatio="none"
      >
        {display.map((p, i) => {
          const cy = rowY(i, display.length) + 36;
          // Source right edge = (5% + 24%) of 640 = 186
          // Target left edge  = 62% of 640          = 397
          const x1 = 186, x2 = 397, cx = (x1 + x2) / 2;
          const stroke = p.isComplete
            ? '#6366f1'
            : p.hasSrc || p.hasDst
              ? '#f59e0b'
              : '#334155';
          const dash = p.isComplete ? undefined : '5 4';
          return (
            <path
              key={p.id}
              d={`M ${x1} ${cy} C ${cx} ${cy}, ${cx} ${cy}, ${x2} ${cy}`}
              stroke={stroke}
              strokeWidth="1.5"
              strokeDasharray={dash}
              fill="none"
            />
          );
        })}
      </svg>

      {/* Nodes */}
      {display.map((p, i) => {
        const y = rowY(i, display.length);

        const NodeBox = ({ configured, label, badge, name }) => (
          <div
            className={`rounded-lg shadow-xl transition-colors ${
              configured
                ? 'bg-slate-900 border border-slate-700 hover:border-indigo-500'
                : 'bg-slate-900/50 border border-dashed border-slate-700 hover:border-amber-500/50'
            }`}
            style={{ padding: '10px 12px' }}
          >
            <div className="flex items-center justify-between mb-1">
              <span className="text-[9px] font-bold text-slate-500 uppercase tracking-tighter">{badge}</span>
              {configured
                ? <div className="w-1.5 h-1.5 rounded-full bg-emerald-500 shadow-[0_0_6px_rgba(16,185,129,0.5)]" />
                : <div className="w-1.5 h-1.5 rounded-full bg-amber-500/50" />
              }
            </div>
            {configured ? (
              <>
                <div className="text-xs font-semibold text-white truncate">{label}</div>
                {name && <div className="text-[10px] text-slate-400 truncate">{name}</div>}
              </>
            ) : (
              <>
                <div className="text-xs italic text-slate-600">Not configured</div>
                {name && <div className="text-[10px] text-slate-500 truncate">{name}</div>}
              </>
            )}
          </div>
        );

        return (
          <React.Fragment key={p.id}>
            {/* Node widths are 24% so right-edge = 5%+24% = 29% = SVG x 186,
                target left = 62% = SVG x 397 — matches the path endpoints above */}
            <div className="absolute" style={{ left: '5%', top: y, width: '24%' }}>
              <NodeBox
                configured={p.hasSrc}
                label={getTypeLabel(p.srcType)}
                badge="Source"
                name={p.name}
              />
            </div>
            <div className="absolute" style={{ left: '62%', top: y, width: '24%' }}>
              <NodeBox
                configured={p.hasDst}
                label={getTypeLabel(p.dstType)}
                badge="Target"
                name={null}
              />
            </div>
          </React.Fragment>
        );
      })}

      {/* Empty state */}
      {display.length === 0 && (
        <div className="absolute inset-0 flex items-center justify-center">
          <div className="text-center">
            <GitMerge size={36} className="text-slate-700 mx-auto mb-3" />
            <p className="text-slate-500 text-sm">No pipelines yet.</p>
            <button
              onClick={onNewPipeline}
              className="mt-2 text-indigo-400 text-xs hover:text-indigo-300"
            >
              Create your first pipeline →
            </button>
          </div>
        </div>
      )}

      {/* Status bar */}
      <div className="absolute bottom-3 right-4 text-slate-600 text-xs">
        LoomPipe Engine v1.0 · {completePipes} of {display.length} flow{display.length !== 1 ? 's' : ''} ready
      </div>
    </div>
  );
};

// ---------------------------------------------------------------------------
// PipelineRow — Live Pipe Monitor table row
// ---------------------------------------------------------------------------
const STATUS_CONFIG = {
  Success: { label: 'success', color: 'text-emerald-400 bg-emerald-400/10' },
  Failed:  { label: 'failed',  color: 'text-rose-400 bg-rose-400/10' },
  Running: { label: 'running', color: 'text-indigo-400 bg-indigo-400/10' },
};

const PipelineRow = ({ pipeline, latestRun, onRun, onRowClick }) => {
  const st = latestRun?.status
    ? (STATUS_CONFIG[latestRun.status] ?? { label: latestRun.status.toLowerCase(), color: 'text-slate-400 bg-slate-400/10' })
    : { label: 'idle', color: 'text-slate-400 bg-slate-400/10' };

  const lastRunTime = latestRun?.startedAt
    ? new Date(latestRun.startedAt).toLocaleString([], {
        month: 'short', day: 'numeric',
        hour: '2-digit', minute: '2-digit',
      })
    : 'Never';

  const srcType = pipeline.source?.type ?? pipeline.sourceType ?? '–';

  return (
    <div
      className="grid grid-cols-5 items-center gap-4 p-4 border-b border-slate-800 hover:bg-slate-800/50 transition-colors group cursor-pointer"
      onClick={() => onRowClick?.(pipeline.id)}
    >
      <div className="flex items-center gap-3">
        <div className="p-2 bg-slate-800 rounded-lg group-hover:bg-slate-700 transition-colors">
          <Layers size={16} className="text-indigo-400" />
        </div>
        <div>
          <div className="text-white font-medium text-sm truncate max-w-36">{pipeline.name}</div>
          <div className="text-slate-500 text-xs">{srcType}</div>
        </div>
      </div>

      <div>
        <span className={`text-[10px] uppercase tracking-wider font-bold px-2 py-1 rounded-full ${st.color}`}>
          {st.label}
        </span>
      </div>

      <div className="text-slate-300 text-sm">
        {latestRun?.rowsProcessed != null
          ? `${latestRun.rowsProcessed.toLocaleString()} rows`
          : '–'}
      </div>

      <div className="text-slate-300 text-sm flex items-center gap-2">
        <Clock size={13} className="text-slate-500 flex-shrink-0" />
        <span className="truncate text-xs">{lastRunTime}</span>
      </div>

      <div className="flex justify-end gap-1" onClick={(e) => e.stopPropagation()}>
        <button
          onClick={() => onRun?.(pipeline.id)}
          className="p-1.5 text-slate-500 hover:text-white hover:bg-slate-700 rounded-md transition-colors"
          title="Run now"
        >
          <Play size={15} />
        </button>
        <button
          onClick={() => onRowClick?.(pipeline.id)}
          className="p-1.5 text-slate-500 hover:text-white hover:bg-slate-700 rounded-md transition-colors"
          title="View details"
        >
          <MoreVertical size={15} />
        </button>
      </div>
    </div>
  );
};

// ---------------------------------------------------------------------------
// WeaveDistribution — source type breakdown from real pipelines
// ---------------------------------------------------------------------------
const WEAVE_COLORS = {
  postgresql: 'bg-indigo-500',
  sqlserver:  'bg-purple-500',
  mysql:      'bg-emerald-500',
  csv:        'bg-amber-500',
  sqlite:     'bg-sky-500',
};

const WeaveDistribution = ({ pipelines }) => {
  const data = useMemo(() => {
    const counts = {};
    pipelines.forEach((p) => {
      // Skip pipelines with no configured source type
      const t = (p.source?.type ?? p.sourceType ?? null)?.toLowerCase();
      if (!t) return;
      counts[t] = (counts[t] || 0) + 1;
    });
    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    if (total === 0) return [];
    return Object.entries(counts).map(([name, count]) => ({
      name: TYPE_LABELS[name] ?? name.charAt(0).toUpperCase() + name.slice(1),
      val: Math.round((count / total) * 100),
      color: WEAVE_COLORS[name] ?? 'bg-slate-500',
    }));
  }, [pipelines]);

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h3 className="font-bold text-white mb-4">Weave Distribution</h3>
      {data.length === 0 ? (
        <p className="text-slate-500 text-sm text-center py-6">No pipelines configured yet.</p>
      ) : (
        <div className="space-y-4">
          {data.map((s) => (
            <div key={s.name}>
              <div className="flex justify-between text-xs mb-1.5">
                <span className="text-slate-400 font-medium">{s.name}</span>
                <span className="text-white font-bold">{s.val}%</span>
              </div>
              <div className="w-full h-1.5 bg-slate-800 rounded-full overflow-hidden">
                <div
                  className={`h-full ${s.color} transition-all duration-700`}
                  style={{ width: `${s.val}%` }}
                />
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

// ---------------------------------------------------------------------------
// DashboardPage
// ---------------------------------------------------------------------------
const DashboardPage = ({ onNewPipeline, onNavigate, onPipelineClick }) => {
  const { authFetch } = useAuth();
  const [summary,      setSummary]      = useState(null);
  const [pipelines,    setPipelines]    = useState([]);
  const [pipelineRuns, setPipelineRuns] = useState({});
  const [loading,      setLoading]      = useState(true);
  const intervalRef = useRef(null);

  const fetchRuns = async (pipes) => {
    const top = (pipes ?? []).slice(0, 6);
    if (top.length === 0) return;
    const pairs = await Promise.all(
      top.map((p) =>
        authFetch(`/api/pipelines/${p.id}/runs`)
          .then((r) => (r.ok ? r.json() : []))
          .then((runs) => [p.id, (runs ?? [])[0] ?? null])
          .catch(() => [p.id, null])
      )
    );
    setPipelineRuns(Object.fromEntries(pairs));
  };

  const loadAll = async () => {
    try {
      const [sRes, pRes] = await Promise.all([
        authFetch('/api/analytics/summary'),
        authFetch('/api/pipelines'),
      ]);
      const sum   = sRes.ok ? await sRes.json() : null;
      const pipes = pRes.ok ? await pRes.json() : [];
      setSummary(sum);
      setPipelines(pipes ?? []);
      await fetchRuns(pipes ?? []);
    } catch { /* ignore */ }
  };

  useEffect(() => {
    setLoading(true);
    loadAll().finally(() => setLoading(false));
    intervalRef.current = setInterval(loadAll, 15000);
    return () => clearInterval(intervalRef.current);
  }, []);

  const handleRunNow = async (id) => {
    try {
      await authFetch(`/api/pipelines/${id}/run`, { method: 'POST' });
      setTimeout(loadAll, 2000);
    } catch { /* ignore */ }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 size={22} className="animate-spin text-indigo-500" />
      </div>
    );
  }

  const successRate =
    summary?.successRate != null
      ? `${(summary.successRate * 100).toFixed(1)}%`
      : '–';
  const avgDur =
    summary?.avgDurationMs != null
      ? `${(summary.avgDurationMs / 1000).toFixed(1)}s`
      : '–';
  const failedCount = Object.values(pipelineRuns).filter(
    (r) => r?.status === 'Failed'
  ).length;

  return (
    <div className="p-8">
      {/* Page header */}
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-white mb-1">LoomPipe Console</h2>
        <p className="text-slate-500">Managing data weave for your production environments.</p>
      </div>

      {/* Metric cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <MetricCard
          label="Active Pipelines"
          value={summary?.totalPipelines ?? 0}
          icon={Layers}
          iconBg="bg-indigo-500/10"
          iconColor="text-indigo-500"
        />
        <MetricCard
          label="Total Runs"
          value={summary?.totalRuns ?? 0}
          icon={Activity}
          iconBg="bg-purple-500/10"
          iconColor="text-purple-500"
        />
        <MetricCard
          label="Success Rate"
          value={successRate}
          icon={CheckCircle2}
          iconBg="bg-emerald-500/10"
          iconColor="text-emerald-500"
        />
        <MetricCard
          label="Pipeline Errors"
          value={failedCount}
          icon={AlertCircle}
          iconBg="bg-rose-500/10"
          iconColor="text-rose-500"
        />
      </div>

      {/* Main content grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">

        {/* Left col (2/3): canvas + monitor */}
        <div className="lg:col-span-2 space-y-6">

          {/* Visual canvas */}
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-bold text-white">Visual Loom Editor</h3>
            <button
              onClick={() => onNavigate?.('pipelines')}
              className="text-sm text-indigo-400 hover:text-indigo-300 font-medium flex items-center gap-1 transition-colors"
            >
              All Pipelines <ChevronRight size={16} />
            </button>
          </div>

          <VisualCanvas pipelines={pipelines} onNewPipeline={onNewPipeline} />

          {/* Live Pipe Monitor */}
          <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden">
            <div className="p-4 border-b border-slate-800 flex justify-between items-center">
              <h3 className="font-bold text-white">Live Pipe Monitor</h3>
              <button
                onClick={() => onNavigate?.('pipelines')}
                className="text-xs text-slate-400 hover:text-white transition-colors"
              >
                View All
              </button>
            </div>

            {pipelines.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-12 text-slate-500">
                <GitMerge size={32} className="mb-2 opacity-20" />
                <p className="text-sm">No pipelines yet.</p>
              </div>
            ) : (
              pipelines.slice(0, 5).map((p) => (
                <PipelineRow
                  key={p.id}
                  pipeline={p}
                  latestRun={pipelineRuns[p.id]}
                  onRun={handleRunNow}
                  onRowClick={onPipelineClick}
                />
              ))
            )}
          </div>
        </div>

        {/* Right col (1/3): distribution + run summary */}
        <div className="space-y-6">
          <WeaveDistribution pipelines={pipelines} />

          <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
            <h3 className="font-bold text-white mb-4">Run Summary</h3>
            <div className="space-y-3">
              {[
                { label: 'Total Runs',   value: summary?.totalRuns ?? '–',   color: 'text-white' },
                { label: 'Avg Duration', value: avgDur,                       color: 'text-amber-400' },
                { label: 'Success Rate', value: successRate,                  color: 'text-emerald-400' },
                { label: 'Pipelines',   value: summary?.totalPipelines ?? '–', color: 'text-white' },
              ].map(({ label, value, color }) => (
                <div key={label} className="flex justify-between items-center">
                  <span className="text-slate-400 text-sm">{label}</span>
                  <span className={`font-bold text-sm ${color}`}>{value}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DashboardPage;
