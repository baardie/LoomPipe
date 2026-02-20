import { useState, useEffect, useCallback } from 'react';
import { Loader2, ChevronLeft, Play, CheckCircle2, XCircle } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import RoleGuard from '../components/auth/RoleGuard';

const ProfileDetailPage = ({ profileId, onBack, pipelines }) => {
  const { authFetch, isAdmin } = useAuth();
  const [profile,          setProfile]          = useState(null);
  const [testing,          setTesting]          = useState(false);
  const [testResult,       setTestResult]       = useState(null);
  const [allUsers,         setAllUsers]         = useState([]);
  const [permittedUserIds, setPermittedUserIds] = useState(new Set());
  const [permSaving,       setPermSaving]       = useState(null);

  const load = useCallback(async () => {
    const res = await authFetch(`/api/connections/${profileId}`);
    if (res.ok) setProfile(await res.json());
  }, [profileId, authFetch]);

  const loadPermissions = useCallback(async () => {
    const [usersRes, permRes] = await Promise.all([authFetch('/api/users'), authFetch(`/api/connections/${profileId}/users`)]);
    if (usersRes.ok) setAllUsers(await usersRes.json());
    if (permRes.ok) setPermittedUserIds(new Set(await permRes.json()));
  }, [profileId, authFetch]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { if (isAdmin) loadPermissions(); }, [isAdmin, loadPermissions]);

  const handleTest = async () => {
    setTesting(true); setTestResult(null);
    try {
      const res = await authFetch(`/api/connections/${profileId}/test`, { method: 'POST' });
      if (res.ok) { setTestResult(await res.json()); await load(); }
    } finally { setTesting(false); }
  };

  const handleToggleUser = async (userId, currentlyGranted) => {
    setPermSaving(userId);
    try {
      await authFetch(`/api/connections/${profileId}/users/${userId}`, { method: currentlyGranted ? 'DELETE' : 'POST' });
      setPermittedUserIds(prev => { const n = new Set(prev); currentlyGranted ? n.delete(userId) : n.add(userId); return n; });
    } finally { setPermSaving(null); }
  };

  const linkedPipelines = (pipelines || []).filter(p =>
    String(p.source?.parameters?.connectionProfileId) === String(profileId) ||
    String(p.destination?.parameters?.connectionProfileId) === String(profileId)
  );
  const nonAdminUsers = allUsers.filter(u => u.role !== 'Admin' && u.isActive);

  if (!profile) return <div className="flex items-center justify-center h-48"><Loader2 size={20} className="animate-spin text-[var(--accent)]" /></div>;

  return (
    <div className="p-6">
      <button onClick={onBack} className="flex items-center gap-1 text-xs text-[var(--text-secondary)] hover:text-[var(--text-primary)] mb-4 transition-colors">
        <ChevronLeft size={15} /> Back to Sources &amp; Destinations
      </button>

      <div className="flex items-start justify-between mb-5">
        <div>
          <h1 className="text-base font-semibold text-[var(--text-primary)]">{profile.name}</h1>
          <span className="inline-block mt-1 px-2 py-0.5 rounded text-xs bg-[var(--bg-elevated)] border border-[var(--border)] text-[var(--text-secondary)] font-mono">{profile.provider}</span>
        </div>
        <button onClick={handleTest} disabled={testing} className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-[var(--accent)] hover:bg-[var(--accent-dim)] text-white rounded transition-colors disabled:opacity-60">
          {testing ? <Loader2 size={13} className="animate-spin" /> : <Play size={13} />} Test Connection
        </button>
      </div>

      {testResult && (
        <div className={`flex items-center gap-2 mb-4 px-3 py-2 rounded border text-xs ${testResult.success ? 'text-[var(--green)] border-[var(--green)]/40 bg-green-900/20' : 'text-[var(--red)] border-[var(--red)]/40 bg-red-900/20'}`}>
          {testResult.success ? <CheckCircle2 size={14} /> : <XCircle size={14} />}
          {testResult.success ? `Connected (${testResult.elapsedMs}ms)` : `Failed: ${testResult.errorMessage}`}
        </div>
      )}

      <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-4 mb-4">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-[var(--text-secondary)] mb-3">Details</h2>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-xs">
          {profile.host && <><dt className="text-[var(--text-muted)]">Host</dt><dd className="text-[var(--text-primary)] font-mono">{profile.host}</dd></>}
          {profile.databaseName && <><dt className="text-[var(--text-muted)]">Database</dt><dd className="text-[var(--text-primary)] font-mono">{profile.databaseName}</dd></>}
          {profile.createdAt && <><dt className="text-[var(--text-muted)]">Created</dt><dd className="text-[var(--text-secondary)]">{new Date(profile.createdAt).toLocaleDateString()}</dd></>}
          {profile.lastTestedAt && <><dt className="text-[var(--text-muted)]">Last Tested</dt><dd className="text-[var(--text-secondary)]">{new Date(profile.lastTestedAt).toLocaleString()}</dd></>}
        </dl>
      </div>

      <RoleGuard roles={['Admin']}>
        <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-4 mb-4">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-[var(--text-secondary)] mb-2">User Access</h2>
          <p className="text-xs text-[var(--text-muted)] mb-3">Admins always have full access. Grant access to User-role accounts below.</p>
          {nonAdminUsers.length === 0 ? (
            <p className="text-xs text-[var(--text-muted)]">No non-admin users found.</p>
          ) : nonAdminUsers.map(u => {
            const granted = permittedUserIds.has(u.id);
            return (
              <label key={u.id} className="flex items-center gap-2 py-1 text-xs text-[var(--text-primary)] cursor-pointer">
                <input type="checkbox" checked={granted} disabled={permSaving === u.id} onChange={() => handleToggleUser(u.id, granted)}
                  style={{ accentColor: 'var(--accent)' }} />
                <span className="font-mono">{u.username}</span>
                <span className="text-[var(--text-muted)]">{u.role}</span>
                {permSaving === u.id && <Loader2 size={11} className="animate-spin text-[var(--accent)]" />}
              </label>
            );
          })}
        </div>
      </RoleGuard>

      <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-4">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-[var(--text-secondary)] mb-3">Linked Pipelines</h2>
        {linkedPipelines.length === 0 ? (
          <p className="text-xs text-[var(--text-muted)]">No pipelines use this connection profile.</p>
        ) : (
          <ul className="space-y-1">
            {linkedPipelines.map(p => (
              <li key={p.id} className="flex items-center gap-2 text-xs">
                <span className="text-[var(--text-primary)]">{p.name}</span>
                <span className="text-[var(--text-muted)] font-mono">#{p.id}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
};

export default ProfileDetailPage;
