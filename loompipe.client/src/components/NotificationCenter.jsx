import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Bell, CheckCheck, CheckCircle, XCircle, Info, X } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';

// ── Type config ─────────────────────────────────────────────────────────────
// Add new notification types here — no other code changes needed.
const TYPE_CONFIG = {
  'pipeline.retry.success': {
    icon: CheckCircle,
    iconClass: 'text-indigo-400',
    dotClass: 'bg-indigo-400',
    badgeClass: 'bg-indigo-500/10 text-indigo-400 border-indigo-500/20',
    label: 'Retry OK',
  },
  'pipeline.success': {
    icon: CheckCircle,
    iconClass: 'text-emerald-400',
    dotClass: 'bg-emerald-400',
    badgeClass: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20',
    label: 'Success',
  },
  'pipeline.failed': {
    icon: XCircle,
    iconClass: 'text-red-400',
    dotClass: 'bg-red-400',
    badgeClass: 'bg-red-500/10 text-red-400 border-red-500/20',
    label: 'Failed',
  },
};

const DEFAULT_CONFIG = {
  icon: Info,
  iconClass: 'text-slate-400',
  dotClass: 'bg-slate-400',
  badgeClass: 'bg-slate-500/10 text-slate-400 border-slate-500/20',
  label: 'Info',
};

function getTypeConfig(type) {
  return TYPE_CONFIG[type] ?? DEFAULT_CONFIG;
}

function timeAgo(dateStr) {
  const diff = Date.now() - new Date(dateStr + 'Z').getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60)  return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60)  return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24)  return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

// ── Component ────────────────────────────────────────────────────────────────
const NotificationCenter = ({ onNavigateToPipeline }) => {
  const { authFetch } = useAuth();
  const [open, setOpen] = useState(false);
  const [notifications, setNotifications] = useState([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const panelRef = useRef(null);
  const buttonRef = useRef(null);
  const pollRef = useRef(null);

  const fetchNotifications = useCallback(async () => {
    try {
      const res = await authFetch('/api/notifications?limit=50');
      if (!res.ok) return;
      const data = await res.json();
      setNotifications(data);
      setUnreadCount(data.filter(n => !n.isRead).length);
    } catch { /* network error — keep stale data */ }
  }, [authFetch]);

  // Initial fetch + 30s polling
  useEffect(() => {
    fetchNotifications();
    pollRef.current = setInterval(fetchNotifications, 30_000);
    return () => clearInterval(pollRef.current);
  }, [fetchNotifications]);

  // Close panel on outside click
  useEffect(() => {
    if (!open) return;
    const handler = (e) => {
      if (
        panelRef.current && !panelRef.current.contains(e.target) &&
        buttonRef.current && !buttonRef.current.contains(e.target)
      ) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const markRead = async (id) => {
    try {
      await authFetch(`/api/notifications/${id}/read`, { method: 'PATCH' });
      setNotifications(prev => prev.map(n => n.id === id ? { ...n, isRead: true } : n));
      setUnreadCount(prev => Math.max(0, prev - 1));
    } catch { /* ignore */ }
  };

  const markAllRead = async () => {
    try {
      await authFetch('/api/notifications/mark-all-read', { method: 'POST' });
      setNotifications(prev => prev.map(n => ({ ...n, isRead: true })));
      setUnreadCount(0);
    } catch { /* ignore */ }
  };

  const handleNotificationClick = (n) => {
    if (!n.isRead) markRead(n.id);
    if (n.pipelineId && onNavigateToPipeline) {
      onNavigateToPipeline(n.pipelineId);
      setOpen(false);
    }
  };

  return (
    <div className="relative">
      {/* Bell button */}
      <button
        ref={buttonRef}
        onClick={() => setOpen(prev => !prev)}
        className="relative p-2 text-slate-400 hover:text-white transition-colors rounded-lg hover:bg-slate-800"
        aria-label="Notifications"
      >
        <Bell size={19} />
        {unreadCount > 0 && (
          <span className="absolute top-1 right-1 min-w-[16px] h-4 px-0.5 flex items-center justify-center bg-indigo-500 text-white text-[10px] font-bold rounded-full leading-none">
            {unreadCount > 99 ? '99+' : unreadCount}
          </span>
        )}
      </button>

      {/* Dropdown panel */}
      {open && (
        <div
          ref={panelRef}
          className="absolute right-0 top-full mt-2 w-96 bg-slate-900 border border-slate-700 rounded-xl shadow-2xl shadow-black/50 z-50 flex flex-col overflow-hidden"
          style={{ maxHeight: '520px' }}
        >
          {/* Header */}
          <div className="flex items-center justify-between px-4 py-3 border-b border-slate-800 flex-shrink-0">
            <div className="flex items-center gap-2">
              <h3 className="text-sm font-semibold text-white">Notifications</h3>
              {unreadCount > 0 && (
                <span className="px-1.5 py-0.5 bg-indigo-500/20 text-indigo-400 text-[10px] font-bold rounded-full border border-indigo-500/30">
                  {unreadCount} new
                </span>
              )}
            </div>
            <div className="flex items-center gap-1">
              {unreadCount > 0 && (
                <button
                  onClick={markAllRead}
                  className="flex items-center gap-1 px-2 py-1 text-xs text-slate-400 hover:text-white transition-colors rounded hover:bg-slate-800"
                  title="Mark all as read"
                >
                  <CheckCheck size={13} />
                  <span>All read</span>
                </button>
              )}
              <button
                onClick={() => setOpen(false)}
                className="p-1 text-slate-500 hover:text-white transition-colors rounded hover:bg-slate-800"
              >
                <X size={14} />
              </button>
            </div>
          </div>

          {/* List */}
          <div className="overflow-y-auto flex-1">
            {notifications.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-12 text-slate-500">
                <Bell size={28} className="mb-3 opacity-40" />
                <p className="text-sm">No notifications yet</p>
              </div>
            ) : (
              <ul>
                {notifications.map((n) => {
                  const cfg = getTypeConfig(n.type);
                  const Icon = cfg.icon;
                  return (
                    <li
                      key={n.id}
                      onClick={() => handleNotificationClick(n)}
                      className={`
                        flex gap-3 px-4 py-3 border-b border-slate-800/60 last:border-0 transition-colors
                        ${n.pipelineId ? 'cursor-pointer hover:bg-slate-800/50' : ''}
                        ${!n.isRead ? 'bg-slate-800/30' : ''}
                      `}
                    >
                      {/* Icon */}
                      <div className="flex-shrink-0 mt-0.5">
                        <Icon size={16} className={cfg.iconClass} />
                      </div>

                      {/* Content */}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-start justify-between gap-2">
                          <p className={`text-sm font-medium leading-snug ${n.isRead ? 'text-slate-300' : 'text-white'}`}>
                            {n.title}
                          </p>
                          {!n.isRead && (
                            <span className={`flex-shrink-0 w-2 h-2 rounded-full mt-1 ${cfg.dotClass}`} />
                          )}
                        </div>
                        <p className="text-xs text-slate-400 mt-0.5 leading-relaxed line-clamp-2">
                          {n.message}
                        </p>
                        <div className="flex items-center gap-2 mt-1.5">
                          <span className={`text-[10px] px-1.5 py-0.5 rounded border font-medium ${cfg.badgeClass}`}>
                            {cfg.label}
                          </span>
                          <span className="text-[11px] text-slate-500">{timeAgo(n.createdAt)}</span>
                          {n.pipelineId && (
                            <span className="text-[11px] text-indigo-400 ml-auto">View pipeline →</span>
                          )}
                        </div>
                      </div>
                    </li>
                  );
                })}
              </ul>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default NotificationCenter;
