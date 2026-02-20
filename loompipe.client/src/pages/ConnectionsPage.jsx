import { useState, useEffect, useCallback } from 'react';
import { Plus, Database, Network, GitMerge, Snowflake, Cloud, Cpu, HelpCircle, CheckCircle2, XCircle, Play, Pencil, Trash2 } from 'lucide-react';
import ConnectionProfileDialog from '../components/connections/ConnectionProfileDialog';
import ConfirmDialog from '../components/ConfirmDialog';
import RoleGuard from '../components/auth/RoleGuard';
import { useAuth } from '../contexts/AuthContext';

const PROVIDER_META = {
  sqlserver:  { label: 'SQL Server',   icon: Database,  color: '#c8373a' },
  postgresql: { label: 'PostgreSQL',   icon: Database,  color: '#336791' },
  mysql:      { label: 'MySQL',        icon: Database,  color: '#00758f' },
  oracle:     { label: 'Oracle',       icon: Database,  color: '#f00000' },
  mongodb:    { label: 'MongoDB',      icon: Network,   color: '#4caf50' },
  neo4j:      { label: 'Neo4j',        icon: GitMerge,  color: '#008cc1' },
  snowflake:  { label: 'Snowflake',    icon: Snowflake, color: '#29b5e8' },
  bigquery:   { label: 'BigQuery',     icon: Cloud,     color: '#4285f4' },
  pinecone:   { label: 'Pinecone',     icon: Cpu,       color: '#5a57fb' },
  milvus:     { label: 'Milvus',       icon: Cpu,       color: '#00a1ea' },
  csv:        { label: 'CSV File',     icon: Database,  color: '#888' },
  rest:       { label: 'REST API',     icon: Network,   color: '#888' },
  webhook:    { label: 'Webhook',      icon: Network,   color: '#888' },
};

const TestBadge = ({ profile }) => {
  if (!profile.lastTestedAt)
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs border border-[var(--border)] text-[var(--text-muted)]">
        <HelpCircle size={11} /> Not tested
      </span>
    );
  return profile.lastTestSucceeded ? (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs border border-[var(--green)]/40 text-[var(--green)] bg-green-900/20">
      <CheckCircle2 size={11} /> Connected
    </span>
  ) : (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs border border-[var(--red)]/40 text-[var(--red)] bg-red-900/20">
      <XCircle size={11} /> Failed
    </span>
  );
};

let _toastSeq = 0;
const Toast = ({ message, severity, onClose }) => {
  useEffect(() => { const t = setTimeout(onClose, 6000); return () => clearTimeout(t); }, [onClose]);
  const c = { success: 'border-[var(--green)]/50 text-[var(--green)]', error: 'border-[var(--red)]/50 text-[var(--red)]', info: 'border-[var(--accent)]/50 text-[var(--accent)]' };
  return (
    <div className={`fixed bottom-4 left-1/2 -translate-x-1/2 z-50 px-4 py-2 bg-[var(--bg-elevated)] border rounded-lg text-xs font-mono ${c[severity] ?? c.info}`}>
      {message}
    </div>
  );
};

const ConnectionsPage = ({ onProfileClick }) => {
  const { authFetch } = useAuth();
  const [profiles,   setProfiles]   = useState([]);
  const [loading,    setLoading]    = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editId,     setEditId]     = useState(null);
  const [testing,    setTesting]    = useState(null);
  const [toast,      setToast]      = useState(null);
  const [confirmId,  setConfirmId]  = useState(null);

  const showToast = (message, severity = 'info') => setToast({ id: ++_toastSeq, message, severity });

  const fetchProfiles = useCallback(async () => {
    setLoading(true);
    try {
      const resp = await authFetch('/api/connections');
      if (resp.ok) setProfiles(await resp.json());
    } finally { setLoading(false); }
  }, [authFetch]);

  useEffect(() => { fetchProfiles(); }, [fetchProfiles]);

  const handleDelete = async (id) => {
    await authFetch(`/api/connections/${id}`, { method: 'DELETE' });
    showToast('Profile deleted.', 'info');
    fetchProfiles();
  };

  const confirmProfile = profiles.find((p) => p.id === confirmId);

  const handleConfirmDelete = () => {
    if (confirmId) handleDelete(confirmId);
    setConfirmId(null);
  };

  const handleTest = async (id) => {
    setTesting(id);
    try {
      const resp = await authFetch(`/api/connections/${id}/test`, { method: 'POST' });
      const result = await resp.json();
      showToast(result.success ? `Connection OK (${result.elapsedMs}ms)` : `Connection failed: ${result.errorMessage}`, result.success ? 'success' : 'error');
      fetchProfiles();
    } finally { setTesting(null); }
  };

  const handleSaved = () => { setDialogOpen(false); setEditId(null); fetchProfiles(); };

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-base font-semibold text-[var(--text-primary)]">Sources &amp; Destinations</h1>
          <p className="text-xs text-[var(--text-muted)] mt-0.5">Credentials encrypted at rest with AES-256-CBC. Plaintext secrets are never stored.</p>
        </div>
        <RoleGuard roles={['Admin']}>
          <button onClick={() => { setEditId(null); setDialogOpen(true); }} className="flex items-center gap-1.5 bg-[var(--accent)] hover:bg-[var(--accent-dim)] text-white px-3 py-1.5 rounded text-xs font-semibold transition-colors">
            <Plus size={14} /> New Connection
          </button>
        </RoleGuard>
      </div>

      {loading ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {[1,2,3].map(i => <div key={i} className="h-44 bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg animate-pulse" />)}
        </div>
      ) : profiles.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-20 text-[var(--text-muted)]">
          <Database size={48} className="mb-3 opacity-30" />
          <p className="text-sm font-medium">No connection profiles yet</p>
          <p className="text-xs mt-1">Click "New Connection" to add your first database.</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {profiles.map(p => {
            const meta = PROVIDER_META[p.provider] ?? { label: p.provider, icon: Database, color: '#888' };
            const Icon = meta.icon;
            return (
              <div key={p.id} onClick={() => onProfileClick && onProfileClick(p.id)}
                className="bg-[var(--bg-surface)] border border-[var(--border)] hover:border-[var(--accent)]/60 rounded-lg p-4 cursor-pointer transition-colors flex flex-col">
                <div className="flex items-start gap-3 mb-2">
                  <Icon size={18} style={{ color: meta.color }} className="mt-0.5 flex-shrink-0" />
                  <div className="min-w-0">
                    <div className="text-sm font-semibold text-[var(--text-primary)] truncate">{p.name}</div>
                    <div className="text-xs text-[var(--text-secondary)]">{meta.label}</div>
                  </div>
                </div>
                {p.host && <div className="text-xs text-[var(--text-muted)] font-mono mb-1 truncate">{p.host}{p.port ? `:${p.port}` : ''}{p.databaseName ? ` / ${p.databaseName}` : ''}</div>}
                {p.username && <div className="text-xs text-[var(--text-muted)] mb-2">User: {p.username}</div>}
                <div className="mt-auto pt-3 border-t border-[var(--border)] flex items-center justify-between">
                  <TestBadge profile={p} />
                  <div className="flex items-center gap-1" onClick={e => e.stopPropagation()}>
                    <button onClick={() => handleTest(p.id)} disabled={testing === p.id} title="Test" className="p-1.5 text-[var(--green)] hover:bg-green-900/20 rounded transition-colors disabled:opacity-40"><Play size={13} /></button>
                    <RoleGuard roles={['Admin']}>
                      <button onClick={() => { setEditId(p.id); setDialogOpen(true); }} title="Edit" className="p-1.5 text-[var(--text-secondary)] hover:text-[var(--accent)] hover:bg-[var(--bg-elevated)] rounded transition-colors"><Pencil size={13} /></button>
                      <button onClick={() => setConfirmId(p.id)} title="Delete" className="p-1.5 text-[var(--text-secondary)] hover:text-[var(--red)] hover:bg-red-900/20 rounded transition-colors"><Trash2 size={13} /></button>
                    </RoleGuard>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      <ConnectionProfileDialog open={dialogOpen} onClose={() => { setDialogOpen(false); setEditId(null); }} onSaved={handleSaved} profileId={editId} />
      {toast && <Toast key={toast.id} message={toast.message} severity={toast.severity} onClose={() => setToast(null)} />}

      <ConfirmDialog
        open={confirmId !== null}
        title="Delete Connection Profile"
        message={
          confirmProfile
            ? `Are you sure you want to delete "${confirmProfile.name}"? Any pipelines using this profile will lose their connection and cannot be undone.`
            : 'Are you sure? This action cannot be undone.'
        }
        confirmLabel="Delete Profile"
        onConfirm={handleConfirmDelete}
        onCancel={() => setConfirmId(null)}
      />
    </div>
  );
};

export default ConnectionsPage;
