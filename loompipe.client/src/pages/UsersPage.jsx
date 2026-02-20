import { useState, useEffect } from 'react';
import { Plus, X, Loader2, Users } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import ConfirmDialog from '../components/ConfirmDialog';

const ROLES = ['Admin', 'User', 'Guest'];
const ROLE_STYLE = {
  Admin: 'text-[var(--purple)] bg-purple-900/20 border-purple-900/40',
  User:  'text-[var(--accent)] bg-blue-900/20 border-blue-900/40',
  Guest: 'text-[var(--text-muted)] bg-[var(--bg-elevated)] border-[var(--border)]',
};

const Modal = ({ title, onClose, children, footer }) => (
  <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
    <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl w-full max-w-sm flex flex-col">
      <div className="flex items-center justify-between px-5 py-4 border-b border-[var(--border)]">
        <h2 className="text-sm font-semibold text-[var(--text-primary)]">{title}</h2>
        <button onClick={onClose} className="text-[var(--text-muted)] hover:text-[var(--text-primary)]"><X size={16} /></button>
      </div>
      <div className="px-5 py-4 flex flex-col gap-3">{children}</div>
      <div className="flex justify-end gap-2 px-5 py-4 border-t border-[var(--border)]">{footer}</div>
    </div>
  </div>
);

const inputCls = "w-full bg-[var(--bg-elevated)] border border-[var(--border)] rounded px-3 py-2 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors";

const UsersPage = () => {
  const { authFetch } = useAuth();
  const [users,       setUsers]       = useState([]);
  const [loading,     setLoading]     = useState(true);
  const [dialogOpen,  setDialogOpen]  = useState(false);
  const [editUser,    setEditUser]    = useState(null);
  const [form,        setForm]        = useState({ username: '', password: '', role: 'User' });
  const [saving,      setSaving]      = useState(false);
  const [confirmId,   setConfirmId]   = useState(null);

  const load = async () => {
    const res = await authFetch('/api/users');
    if (res.ok) setUsers(await res.json());
    setLoading(false);
  };
  useEffect(() => { load(); }, []);

  const openNew  = () => { setEditUser(null); setForm({ username: '', password: '', role: 'User' }); setDialogOpen(true); };
  const openEdit = (u) => { setEditUser(u); setForm({ username: u.username, password: '', role: u.role }); setDialogOpen(true); };

  const handleSave = async () => {
    setSaving(true);
    try {
      if (editUser) {
        await authFetch(`/api/users/${editUser.id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ role: form.role, isActive: true }) });
      } else {
        await authFetch('/api/users', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(form) });
      }
      setDialogOpen(false); load();
    } finally { setSaving(false); }
  };

  const handleDeactivate = async (id) => {
    await authFetch(`/api/users/${id}`, { method: 'DELETE' });
    load();
  };

  const confirmUser = users.find((u) => u.id === confirmId);

  const handleConfirmDeactivate = () => {
    if (confirmId) handleDeactivate(confirmId);
    setConfirmId(null);
  };

  if (loading) return (
    <div className="flex items-center justify-center h-48">
      <Loader2 size={20} className="animate-spin text-[var(--accent)]" />
    </div>
  );

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-base font-semibold text-[var(--text-primary)]">User Management</h1>
        <button onClick={openNew} className="flex items-center gap-1.5 bg-[var(--accent)] hover:bg-[var(--accent-dim)] text-white px-3 py-1.5 rounded text-xs font-semibold transition-colors">
          <Plus size={14} /> New User
        </button>
      </div>

      <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg overflow-hidden">
        {users.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-20 text-[var(--text-muted)]">
            <Users size={48} className="mb-3 opacity-30" />
            <p className="text-sm font-medium">No users yet</p>
            <p className="text-xs mt-1">Click "New User" to create the first account.</p>
          </div>
        ) : (
          <table className="w-full text-xs">
            <thead className="bg-[var(--bg-subtle)] border-b border-[var(--border)]">
              <tr>{['Username', 'Role', 'Status', 'Created', 'Actions'].map(h => (
                <th key={h} className="px-4 py-2.5 text-left font-semibold uppercase tracking-wider text-[var(--text-secondary)]">{h}</th>
              ))}</tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {users.map(u => (
                <tr key={u.id} className="hover:bg-[var(--bg-subtle)]">
                  <td className="px-4 py-2.5 font-mono text-[var(--text-primary)]">{u.username}</td>
                  <td className="px-4 py-2.5">
                    <span className={`px-2 py-0.5 rounded text-xs font-mono border ${ROLE_STYLE[u.role] ?? ROLE_STYLE.Guest}`}>{u.role}</span>
                  </td>
                  <td className="px-4 py-2.5">
                    <span className={`px-2 py-0.5 rounded text-xs ${u.isActive ? 'text-[var(--green)] bg-green-900/20' : 'text-[var(--text-muted)]'}`}>
                      {u.isActive ? 'Active' : 'Inactive'}
                    </span>
                  </td>
                  <td className="px-4 py-2.5 text-[var(--text-muted)] font-mono">{new Date(u.createdAt).toLocaleDateString()}</td>
                  <td className="px-4 py-2.5">
                    <div className="flex items-center gap-2">
                      <button onClick={() => openEdit(u)} className="text-xs text-[var(--accent)] hover:underline">Edit Role</button>
                      {u.isActive && (
                        <button
                          onClick={() => setConfirmId(u.id)}
                          className="text-xs text-[var(--red)] hover:underline"
                        >
                          Deactivate
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {dialogOpen && (
        <Modal
          title={editUser ? 'Edit User' : 'New User'}
          onClose={() => setDialogOpen(false)}
          footer={<>
            <button onClick={() => setDialogOpen(false)} className="px-3 py-1.5 text-xs text-[var(--text-secondary)] border border-[var(--border)] rounded hover:text-[var(--text-primary)]">Cancel</button>
            <button onClick={handleSave} disabled={saving} className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-[var(--accent)] hover:bg-[var(--accent-dim)] text-white rounded disabled:opacity-60">
              {saving && <Loader2 size={12} className="animate-spin" />} Save
            </button>
          </>}
        >
          {!editUser && <>
            <div><label className="block text-xs text-[var(--text-muted)] mb-1">Username</label><input className={inputCls} value={form.username} onChange={e => setForm({ ...form, username: e.target.value })} /></div>
            <div><label className="block text-xs text-[var(--text-muted)] mb-1">Password</label><input className={inputCls} type="password" value={form.password} onChange={e => setForm({ ...form, password: e.target.value })} /></div>
          </>}
          <div>
            <label className="block text-xs text-[var(--text-muted)] mb-1">Role</label>
            <select className={inputCls} value={form.role} onChange={e => setForm({ ...form, role: e.target.value })}>
              {ROLES.map(r => <option key={r} value={r}>{r}</option>)}
            </select>
          </div>
        </Modal>
      )}

      <ConfirmDialog
        open={confirmId !== null}
        title="Deactivate User"
        message={
          confirmUser
            ? `Are you sure you want to deactivate "${confirmUser.username}"? They will no longer be able to log in.`
            : 'Are you sure? The user will no longer be able to log in.'
        }
        confirmLabel="Deactivate"
        onConfirm={handleConfirmDeactivate}
        onCancel={() => setConfirmId(null)}
      />
    </div>
  );
};

export default UsersPage;
