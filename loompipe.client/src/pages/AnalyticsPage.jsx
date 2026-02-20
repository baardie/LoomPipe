import { useState, useEffect } from 'react';
import { Loader2 } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';

const BarChart = ({ data }) => {
  if (!data || data.length === 0) return null;
  const maxCount = Math.max(...data.map(d => d.runCount), 1);
  const barW = 36, gap = 12, svgW = data.length * (barW + gap) + gap, svgH = 100;
  return (
    <svg width="100%" viewBox={`0 0 ${svgW} ${svgH + 20}`} preserveAspectRatio="xMidYMid meet">
      <line x1={0} y1={svgH} x2={svgW} y2={svgH} stroke="var(--border)" strokeWidth={1} />
      {data.map((d, i) => {
        const x = gap + i * (barW + gap);
        const totalH = Math.max((d.runCount / maxCount) * (svgH - 10), 2);
        const successH = Math.max((d.successCount / maxCount) * (svgH - 10), d.successCount > 0 ? 2 : 0);
        const label = new Date(d.date).toLocaleDateString(undefined, { weekday: 'short' });
        return (
          <g key={d.date}>
            <rect x={x} y={svgH - totalH} width={barW} height={totalH} fill="var(--accent-dim)" rx={3} />
            <rect x={x} y={svgH - successH} width={barW} height={successH} fill="var(--green)" rx={3} />
            <text x={x + barW / 2} y={svgH + 14} textAnchor="middle" fill="var(--text-muted)" fontSize={9} fontFamily="monospace">{label}</text>
          </g>
        );
      })}
    </svg>
  );
};

const StatCard = ({ label, value }) => (
  <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-4">
    <div className="text-2xl font-bold font-mono text-[var(--text-primary)]">{value ?? '–'}</div>
    <div className="text-xs text-[var(--text-secondary)] uppercase tracking-wider mt-1">{label}</div>
  </div>
);

const AnalyticsPage = () => {
  const { authFetch } = useAuth();
  const [summary, setSummary] = useState(null);
  const [byDay,   setByDay]   = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const [sRes, dRes] = await Promise.all([
          authFetch('/api/analytics/summary'),
          authFetch('/api/analytics/runs-by-day?days=7'),
        ]);
        if (sRes.ok) setSummary(await sRes.json());
        if (dRes.ok) setByDay(await dRes.json());
      } finally { setLoading(false); }
    })();
  }, [authFetch]);

  if (loading) return (
    <div className="flex items-center justify-center h-48">
      <Loader2 size={20} className="animate-spin text-[var(--accent)]" />
    </div>
  );

  return (
    <div className="p-6">
      <h1 className="text-base font-semibold text-[var(--text-primary)] mb-4">Analytics</h1>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-6">
        <StatCard label="Total Pipelines" value={summary?.totalPipelines} />
        <StatCard label="Total Runs"      value={summary?.totalRuns} />
        <StatCard label="Success Rate"    value={summary?.successRate != null ? `${(summary.successRate * 100).toFixed(1)}%` : '–'} />
        <StatCard label="Avg Duration"    value={summary?.avgDurationMs != null ? `${(summary.avgDurationMs / 1000).toFixed(1)}s` : '–'} />
      </div>
      <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-4 mb-6">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-[var(--text-secondary)]">Runs — Last 7 Days</h2>
          <div className="flex items-center gap-4 text-xs text-[var(--text-muted)]">
            <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-[var(--accent-dim)] inline-block" /> Total</span>
            <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-[var(--green)] inline-block" /> Success</span>
          </div>
        </div>
        <BarChart data={byDay} />
      </div>
      <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg overflow-hidden">
        <table className="w-full text-xs">
          <thead className="bg-[var(--bg-subtle)] border-b border-[var(--border)]">
            <tr>
              {['Date', 'Total Runs', 'Successes', 'Failures', 'Success Rate'].map(h => (
                <th key={h} className="px-4 py-2.5 text-left font-semibold uppercase tracking-wider text-[var(--text-secondary)]">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-[var(--border)]">
            {byDay.length === 0 ? (
              <tr><td colSpan={5} className="px-4 py-6 text-center text-[var(--text-muted)]">No run data yet.</td></tr>
            ) : byDay.map(d => {
              const failures = d.runCount - d.successCount;
              const rate = d.runCount > 0 ? `${((d.successCount / d.runCount) * 100).toFixed(0)}%` : '–';
              return (
                <tr key={d.date} className="hover:bg-[var(--bg-subtle)]">
                  <td className="px-4 py-2 font-mono text-[var(--text-primary)]">{new Date(d.date).toLocaleDateString()}</td>
                  <td className="px-4 py-2 text-[var(--text-primary)]">{d.runCount}</td>
                  <td className="px-4 py-2 text-[var(--green)]">{d.successCount}</td>
                  <td className="px-4 py-2 text-[var(--red)]">{failures}</td>
                  <td className="px-4 py-2 text-[var(--text-secondary)]">{rate}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default AnalyticsPage;
