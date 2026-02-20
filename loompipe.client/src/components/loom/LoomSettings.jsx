import { useState } from 'react';
import { Settings, ChevronDown } from 'lucide-react';
import RoleGuard from '../auth/RoleGuard';

const inputCls = "bg-[var(--bg-elevated)] border border-[var(--border)] rounded px-3 py-1.5 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors w-40";

const LoomSettings = ({ scheduleEnabled, setScheduleEnabled, scheduleIntervalMinutes, setScheduleIntervalMinutes, batchEnabled, setBatchEnabled, batchSize, setBatchSize, batchDelaySeconds, setBatchDelaySeconds }) => {
  const [expanded, setExpanded] = useState(false);

  return (
    <RoleGuard roles={['Admin']}>
      <div className="bg-[var(--bg-surface)] border-t border-[var(--border)] flex-shrink-0">
        <button
          onClick={() => setExpanded(e => !e)}
          className="w-full flex items-center gap-2 px-4 py-2.5 text-xs text-[var(--text-secondary)] hover:text-[var(--text-primary)] transition-colors"
        >
          <Settings size={13} />
          Schedule &amp; Batching
          <ChevronDown size={12} className={`ml-auto transition-transform ${expanded ? 'rotate-180' : ''}`} />
        </button>

        {expanded && (
          <div className="px-4 pb-4 grid grid-cols-2 gap-6 border-t border-[var(--border)] pt-3">
            {/* Schedule */}
            <div className="flex flex-col gap-3">
              <div className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider">Schedule</div>
              <label className="flex items-center gap-2 text-xs text-[var(--text-primary)] cursor-pointer">
                <input type="checkbox" checked={scheduleEnabled} onChange={e => setScheduleEnabled(e.target.checked)}
                  style={{ accentColor: 'var(--accent)' }} />
                Enable Schedule
              </label>
              {scheduleEnabled && (
                <div className="flex items-center gap-2">
                  <label className="text-xs text-[var(--text-muted)]">Every</label>
                  <input type="number" min={1} value={scheduleIntervalMinutes} onChange={e => setScheduleIntervalMinutes(e.target.value)}
                    className={inputCls} placeholder="60" />
                  <label className="text-xs text-[var(--text-muted)]">minutes</label>
                </div>
              )}
            </div>

            {/* Batching */}
            <div className="flex flex-col gap-3">
              <div className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider">Batch Writing</div>
              <label className="flex items-center gap-2 text-xs text-[var(--text-primary)] cursor-pointer">
                <input type="checkbox" checked={batchEnabled} onChange={e => setBatchEnabled(e.target.checked)}
                  style={{ accentColor: 'var(--accent)' }} />
                Enable Batch Writing
              </label>
              {batchEnabled && (
                <div className="flex flex-col gap-2">
                  <div className="flex items-center gap-2">
                    <label className="text-xs text-[var(--text-muted)] w-20">Batch size</label>
                    <input type="number" min={1} value={batchSize} onChange={e => setBatchSize(e.target.value)}
                      className={inputCls} placeholder="1000" />
                    <label className="text-xs text-[var(--text-muted)]">rows</label>
                  </div>
                  <div className="flex items-center gap-2">
                    <label className="text-xs text-[var(--text-muted)] w-20">Delay</label>
                    <input type="number" min={0} value={batchDelaySeconds} onChange={e => setBatchDelaySeconds(e.target.value)}
                      className={inputCls} placeholder="0" />
                    <label className="text-xs text-[var(--text-muted)]">seconds</label>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </RoleGuard>
  );
};

export default LoomSettings;
