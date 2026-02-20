import { useState } from 'react';
import { Plus, GitMerge, Pencil, Trash2, ExternalLink, Clock, CheckCircle2, XCircle, Loader2 } from 'lucide-react';
import RoleGuard from '../components/auth/RoleGuard';
import ConfirmDialog from '../components/ConfirmDialog';

const TypePill = ({ type }) => (
  <span className="px-2 py-0.5 rounded font-mono text-xs bg-[var(--bg-elevated)] border border-[var(--border)] text-[var(--text-secondary)]">
    {type ?? '–'}
  </span>
);

const STATUS_CONFIG = {
  Success: {
    icon: CheckCircle2,
    className: 'text-[var(--green)]',
    label: 'Success',
  },
  Failed: {
    icon: XCircle,
    className: 'text-[var(--red)]',
    label: 'Failed',
  },
  Running: {
    icon: Loader2,
    className: 'text-[var(--yellow)] animate-spin',
    label: 'Running',
  },
};

/**
 * Shows the last-run status badge and, for failed runs, a truncated error tooltip.
 */
const LastRunBadge = ({ status, errorMessage, lastRunAt }) => {
  if (!status) return <span className="text-[var(--text-muted)]">–</span>;

  const cfg = STATUS_CONFIG[status];
  if (!cfg) return <span className="text-[var(--text-muted)]">{status}</span>;

  const Icon = cfg.icon;
  const timeLabel = lastRunAt ? new Date(lastRunAt).toLocaleString() : '';
  const tooltip = status === 'Failed' && errorMessage
    ? `${timeLabel}\n${errorMessage}`
    : timeLabel;

  return (
    <span
      className={`inline-flex items-center gap-1 font-medium ${cfg.className}`}
      title={tooltip}
    >
      <Icon size={12} />
      {cfg.label}
      {status === 'Failed' && errorMessage && (
        <span className="ml-1 text-[var(--text-muted)] font-normal truncate max-w-[140px]">
          — {errorMessage.length > 40 ? errorMessage.slice(0, 40) + '…' : errorMessage}
        </span>
      )}
    </span>
  );
};

const PipelinesPage = ({ pipelines, onEdit, onDelete, onRowClick, onCreate }) => {
  const [confirmId, setConfirmId] = useState(null);

  const confirmPipeline = pipelines.find((p) => p.id === confirmId);

  const handleConfirmDelete = () => {
    if (confirmId) onDelete?.(confirmId);
    setConfirmId(null);
  };

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-base font-semibold text-[var(--text-primary)]">Pipelines</h1>
        <RoleGuard roles={['Admin']}>
          <button
            onClick={onCreate}
            className="flex items-center gap-1.5 bg-[var(--accent)] hover:bg-[var(--accent-dim)] text-white px-3 py-1.5 rounded text-xs font-semibold transition-colors"
          >
            <Plus size={14} /> New Pipeline
          </button>
        </RoleGuard>
      </div>

      <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg overflow-hidden">
        {pipelines.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-20 text-[var(--text-muted)]">
            <GitMerge size={48} className="mb-3 opacity-30" />
            <p className="text-sm font-medium">No pipelines yet</p>
            <p className="text-xs mt-1">Click "New Pipeline" to create your first data pipeline.</p>
          </div>
        ) : (
          <table className="w-full text-xs">
            <thead className="bg-[var(--bg-subtle)] border-b border-[var(--border)]">
              <tr>
                {['Name', 'Source', 'Destination', 'Last Run', 'Schedule', 'Actions'].map((h) => (
                  <th key={h} className="px-4 py-2.5 text-left font-semibold uppercase tracking-wider text-[var(--text-secondary)]">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {pipelines.map((p) => (
                <tr
                  key={p.id}
                  onClick={() => onRowClick?.(p.id)}
                  className={`hover:bg-[var(--bg-subtle)] cursor-pointer transition-colors ${
                    p.lastRunStatus === 'Failed' ? 'border-l-2 border-l-[var(--red)]' : ''
                  }`}
                >
                  <td className="px-4 py-2.5 font-medium text-[var(--text-primary)]">{p.name}</td>
                  <td className="px-4 py-2.5"><TypePill type={p.source?.type} /></td>
                  <td className="px-4 py-2.5"><TypePill type={p.destination?.type} /></td>
                  <td className="px-4 py-2.5">
                    <LastRunBadge
                      status={p.lastRunStatus}
                      errorMessage={p.lastErrorMessage}
                      lastRunAt={p.lastRunAt}
                    />
                  </td>
                  <td className="px-4 py-2.5">
                    {p.scheduleEnabled && p.scheduleIntervalMinutes ? (
                      <span className="inline-flex items-center gap-1 text-[var(--purple)]">
                        <Clock size={11} /> every {p.scheduleIntervalMinutes}m
                      </span>
                    ) : (
                      <span className="text-[var(--text-muted)]">–</span>
                    )}
                  </td>
                  <td className="px-4 py-2.5">
                    <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
                      <button
                        onClick={() => onRowClick?.(p.id)}
                        title="View details"
                        className="p-1.5 text-[var(--text-muted)] hover:text-[var(--accent)] hover:bg-[var(--bg-elevated)] rounded transition-colors"
                      >
                        <ExternalLink size={13} />
                      </button>
                      <RoleGuard roles={['Admin']}>
                        <button
                          onClick={() => onEdit?.(p)}
                          title="Edit"
                          className="p-1.5 text-[var(--text-muted)] hover:text-[var(--accent)] hover:bg-[var(--bg-elevated)] rounded transition-colors"
                        >
                          <Pencil size={13} />
                        </button>
                        <button
                          onClick={() => setConfirmId(p.id)}
                          title="Delete"
                          className="p-1.5 text-[var(--text-muted)] hover:text-[var(--red)] hover:bg-red-900/20 rounded transition-colors"
                        >
                          <Trash2 size={13} />
                        </button>
                      </RoleGuard>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <ConfirmDialog
        open={confirmId !== null}
        title="Delete Pipeline"
        message={
          confirmPipeline
            ? `Are you sure you want to delete "${confirmPipeline.name}"? This will also remove all associated run history and cannot be undone.`
            : 'Are you sure? This action cannot be undone.'
        }
        confirmLabel="Delete Pipeline"
        onConfirm={handleConfirmDelete}
        onCancel={() => setConfirmId(null)}
      />
    </div>
  );
};

export default PipelinesPage;
