import { useState } from 'react';
import { X } from 'lucide-react';

const TABS = ['Source', 'Mapped', 'Transformed'];

const DryRunResultModal = ({ open, onClose, dryRunResult }) => {
  const [tab, setTab] = useState(0);
  if (!open || !dryRunResult) return null;

  const rows = tab === 0 ? dryRunResult.sourcePreview : tab === 1 ? dryRunResult.mappedPreview : dryRunResult.transformedPreview;
  const cols = rows && rows.length > 0 ? Object.keys(rows[0]) : [];

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl w-full max-w-3xl max-h-[80vh] flex flex-col">
        <div className="flex items-center justify-between px-5 py-4 border-b border-[var(--border)]">
          <h2 className="text-sm font-semibold text-[var(--text-primary)]">Dry Run Results</h2>
          <button onClick={onClose} className="text-[var(--text-muted)] hover:text-[var(--text-primary)]"><X size={16} /></button>
        </div>

        {dryRunResult.error ? (
          <div className="flex-1 overflow-auto p-4">
            <div className="bg-red-900/20 border border-[var(--red)]/40 rounded-lg p-4">
              <p className="text-xs font-semibold text-[var(--red)] mb-1">Dry run failed</p>
              <p className="text-xs text-[var(--text-secondary)] font-mono whitespace-pre-wrap">{dryRunResult.error}</p>
            </div>
          </div>
        ) : (
          <>
            <div className="flex border-b border-[var(--border)]">
              {TABS.map((label, i) => (
                <button key={label} onClick={() => setTab(i)}
                  className={`px-4 py-2 text-xs -mb-px transition-colors ${tab === i ? 'border-b-2 border-[var(--accent)] text-[var(--text-primary)]' : 'text-[var(--text-secondary)] hover:text-[var(--text-primary)]'}`}>
                  {label}
                </button>
              ))}
            </div>

            <div className="flex-1 overflow-auto p-4">
              {!rows || rows.length === 0 ? (
                <p className="text-xs text-[var(--text-muted)] text-center py-8">No data.</p>
              ) : (
                <table className="w-full text-xs">
                  <thead className="bg-[var(--bg-subtle)] sticky top-0">
                    <tr>{cols.map(c => <th key={c} className="px-3 py-2 text-left font-semibold text-[var(--text-secondary)] font-mono">{c}</th>)}</tr>
                  </thead>
                  <tbody className="divide-y divide-[var(--border)]">
                    {rows.map((row, i) => (
                      <tr key={i} className="hover:bg-[var(--bg-subtle)]">
                        {cols.map(c => <td key={c} className="px-3 py-1.5 font-mono text-[var(--text-primary)] truncate max-w-xs">{String(row[c] ?? '')}</td>)}
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
};

export default DryRunResultModal;
