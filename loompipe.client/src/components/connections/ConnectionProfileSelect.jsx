import { useState, useEffect } from 'react';
import { Loader2, PlusCircle } from 'lucide-react';
import { useAuth } from '../../contexts/AuthContext';
import ConnectionProfileDialog from './ConnectionProfileDialog';

const ConnectionProfileSelect = ({ label = 'Connection Profile', profileId, onProfileChange, filterProvider }) => {
  const { authFetch } = useAuth();
  const [profiles,     setProfiles]     = useState([]);
  const [loading,      setLoading]      = useState(true);
  const [dialogOpen,   setDialogOpen]   = useState(false);

  const load = async () => {
    setLoading(true);
    try {
      const res = await authFetch('/api/connections');
      if (res.ok) {
        const all = await res.json();
        setProfiles(filterProvider ? all.filter(p => p.provider === filterProvider) : all);
      }
    } finally { setLoading(false); }
  };

  useEffect(() => { load(); }, [filterProvider]);

  const handleSaved = () => { setDialogOpen(false); load(); };

  return (
    <div>
      <label className="block text-xs text-[var(--text-muted)] mb-1">{label}</label>
      <div className="flex items-center gap-2">
        <div className="relative flex-1">
          <select
            value={profileId ?? ''}
            onChange={e => onProfileChange(e.target.value ? Number(e.target.value) : null)}
            disabled={loading}
            className="w-full bg-[var(--bg-elevated)] border border-[var(--border)] rounded px-3 py-2 text-sm text-[var(--text-primary)] focus:outline-none focus:border-[var(--accent)] transition-colors disabled:opacity-60 appearance-none pr-8"
          >
            <option value="">— none —</option>
            {profiles.map(p => <option key={p.id} value={p.id}>{p.name} ({p.provider})</option>)}
          </select>
          {loading && <Loader2 size={13} className="absolute right-2 top-1/2 -translate-y-1/2 animate-spin text-[var(--text-muted)]" />}
        </div>
        <button onClick={() => setDialogOpen(true)} title="Add new profile"
          className="p-2 text-[var(--text-muted)] hover:text-[var(--accent)] transition-colors flex-shrink-0">
          <PlusCircle size={16} />
        </button>
      </div>
      <ConnectionProfileDialog open={dialogOpen} onClose={() => setDialogOpen(false)} onSaved={handleSaved} profileId={null} />
    </div>
  );
};

export default ConnectionProfileSelect;
