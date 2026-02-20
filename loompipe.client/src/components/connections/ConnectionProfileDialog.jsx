import { useState, useEffect } from 'react';
import { X, Loader2 } from 'lucide-react';
import ConnectionProfileForm from './ConnectionProfileForm';
import { useAuth } from '../../contexts/AuthContext';

const HTTP_PROVIDERS = ['rest', 'webhook'];

const EMPTY = {
  provider: '', name: '', host: '', port: '', databaseName: '',
  username: '', password: '', notes: '',
  // HTTP-only auth fields (packed into additionalConfig before save)
  authType: 'none', customHeaders: [],
};

/** Parse saved additionalConfig JSON and restore auth fields into form values. */
const applyAdditionalConfig = (values, additionalConfigJson) => {
  try {
    const ac = JSON.parse(additionalConfigJson || '{}');
    const headers = ac.headers && typeof ac.headers === 'object'
      ? Object.entries(ac.headers).map(([key, value]) => ({ id: `${key}-${Math.random()}`, key, value: String(value) }))
      : [];
    return { ...values, authType: ac.authType ?? 'none', customHeaders: headers };
  } catch {
    return values;
  }
};

/** Build the additionalConfig JSON string and strip UI-only fields from the payload. */
const buildPayload = (values) => {
  const isHttp = HTTP_PROVIDERS.includes(values.provider);
  const { authType, customHeaders, ...rest } = values;

  const headersObj = isHttp && customHeaders?.length
    ? Object.fromEntries(customHeaders.filter(h => h.key.trim()).map(h => [h.key.trim(), h.value]))
    : {};

  const additionalConfig = isHttp
    ? JSON.stringify({
        authType: authType || 'none',
        ...(Object.keys(headersObj).length > 0 ? { headers: headersObj } : {}),
      })
    : '{}';

  return { ...rest, additionalConfig };
};

const ConnectionProfileDialog = ({ open, onClose, onSaved, profileId }) => {
  const { authFetch } = useAuth();
  const [values,  setValues]  = useState(EMPTY);
  const [saving,  setSaving]  = useState(false);
  const [testing, setTesting] = useState(false);
  const [error,   setError]   = useState('');
  const [testMsg, setTestMsg] = useState('');

  useEffect(() => {
    if (!open) { setValues(EMPTY); setError(''); setTestMsg(''); return; }
    if (profileId) {
      authFetch(`/api/connections/${profileId}`).then(r => r.ok ? r.json() : null).then(data => {
        if (!data) return;
        const base = {
          provider: data.provider || '', name: data.name || '',
          host: data.host || '', port: data.port || '',
          databaseName: data.databaseName || '', username: data.username || '',
          password: '', notes: data.notes || '',
          authType: 'none', customHeaders: [],
        };
        setValues(applyAdditionalConfig(base, data.additionalConfig));
      });
    }
  }, [open, profileId, authFetch]);

  const handleSave = async () => {
    setError(''); setSaving(true);
    try {
      const url    = profileId ? `/api/connections/${profileId}` : '/api/connections';
      const method = profileId ? 'PUT' : 'POST';
      const resp   = await authFetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildPayload(values)),
      });
      if (!resp.ok) { const e = await resp.json().catch(() => ({})); setError(e.message || 'Save failed.'); return; }
      onSaved();
    } catch (e) { setError(e.message); }
    finally { setSaving(false); }
  };

  const handleTest = async () => {
    setTestMsg(''); setTesting(true);
    try {
      const url    = profileId ? `/api/connections/${profileId}/test` : '/api/connections/test';
      const body   = profileId ? undefined : JSON.stringify(buildPayload(values));
      const resp   = await authFetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
      const result = await resp.json();
      setTestMsg(result.success ? `Connected (${result.elapsedMs}ms)` : `Failed: ${result.errorMessage}`);
    } finally { setTesting(false); }
  };

  if (!open) return null;

  const busy = saving || testing;
  const title = profileId ? 'Edit Connection' : 'New Connection';

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl w-full max-w-md max-h-[90vh] flex flex-col relative">
        {busy && <div className="absolute top-0 left-0 right-0 h-0.5 bg-[var(--accent)] animate-pulse rounded-t-xl" />}

        <div className="flex items-center justify-between px-5 py-4 border-b border-[var(--border)]">
          <h2 className="text-sm font-semibold text-[var(--text-primary)]">{title}</h2>
          <button onClick={onClose} className="text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors"><X size={16} /></button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4">
          <ConnectionProfileForm values={values} onChange={setValues} />
          {error   && <p className="mt-3 text-xs text-[var(--red)]">{error}</p>}
          {testMsg && <p className={`mt-3 text-xs ${testMsg.startsWith('Connected') ? 'text-[var(--green)]' : 'text-[var(--red)]'}`}>{testMsg}</p>}
        </div>

        <div className="flex items-center justify-between px-5 py-4 border-t border-[var(--border)]">
          <button onClick={handleTest} disabled={busy} className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-[var(--border)] text-[var(--text-secondary)] hover:border-[var(--accent)] hover:text-[var(--accent)] rounded transition-colors disabled:opacity-50">
            {testing && <Loader2 size={12} className="animate-spin" />} Test
          </button>
          <div className="flex items-center gap-2">
            <button onClick={onClose} className="px-3 py-1.5 text-xs border border-[var(--border)] text-[var(--text-secondary)] rounded hover:text-[var(--text-primary)]">Cancel</button>
            <button onClick={handleSave} disabled={busy} className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-[var(--accent)] hover:bg-[var(--accent-dim)] text-white rounded disabled:opacity-60">
              {saving && <Loader2 size={12} className="animate-spin" />} Save
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ConnectionProfileDialog;
