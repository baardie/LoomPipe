import { useState, useEffect, useCallback } from 'react';
import { Loader2, Save, Send, CheckCircle2, AlertCircle, Eye, EyeOff, Plus, Trash2, Copy, Check, KeyRound, RotateCcw } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';

const Field = ({ label, hint, children }) => (
  <div className="mb-5">
    <label className="block text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider mb-1.5">
      {label}
    </label>
    {children}
    {hint && <p className="mt-1 text-[10px] text-[var(--text-muted)]">{hint}</p>}
  </div>
);

const Input = ({ type = 'text', ...props }) => (
  <input
    type={type}
    className="w-full px-3 py-2 text-xs rounded bg-[var(--bg-elevated)] border border-[var(--border)] text-[var(--text-primary)] placeholder-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors"
    {...props}
  />
);

const Toggle = ({ checked, onChange, label, description }) => (
  <label className="flex items-start gap-3 cursor-pointer group">
    <div className="relative mt-0.5 shrink-0">
      <input
        type="checkbox"
        className="sr-only"
        checked={checked}
        onChange={e => onChange(e.target.checked)}
      />
      <div className={`w-8 h-4 rounded-full transition-colors ${checked ? 'bg-[var(--accent)]' : 'bg-[var(--bg-elevated)] border border-[var(--border)]'}`} />
      <div className={`absolute top-0.5 left-0.5 w-3 h-3 rounded-full bg-white shadow transition-transform ${checked ? 'translate-x-4' : ''}`} />
    </div>
    <div>
      <div className="text-xs font-medium text-[var(--text-primary)]">{label}</div>
      {description && <div className="text-[10px] text-[var(--text-muted)] mt-0.5">{description}</div>}
    </div>
  </label>
);

// ── API Keys section ──────────────────────────────────────────────────────────

const ApiKeysCard = () => {
  const { authFetch } = useAuth();
  const [keys, setKeys]           = useState([]);
  const [loading, setLoading]     = useState(true);
  const [creating, setCreating]   = useState(false);
  const [newName, setNewName]     = useState('');
  const [newExpiry, setNewExpiry] = useState('');
  const [showForm, setShowForm]   = useState(false);
  const [rawKey, setRawKey]       = useState(null);   // one-time display
  const [copied, setCopied]       = useState(false);
  const [revoking, setRevoking]   = useState(null);   // id being revoked

  const load = useCallback(() => {
    setLoading(true);
    authFetch('/api/apikeys')
      .then(r => r.ok ? r.json() : [])
      .then(setKeys)
      .finally(() => setLoading(false));
  }, [authFetch]);

  useEffect(() => { load(); }, [load]);

  const handleCreate = async () => {
    if (!newName.trim()) return;
    setCreating(true);
    try {
      const res  = await authFetch('/api/apikeys', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newName.trim(), expiresAt: newExpiry || null }),
      });
      const data = await res.json();
      setRawKey(data.rawKey);
      setNewName('');
      setNewExpiry('');
      setShowForm(false);
      load();
    } finally {
      setCreating(false);
    }
  };

  const handleRevoke = async (id) => {
    setRevoking(id);
    await authFetch(`/api/apikeys/${id}`, { method: 'DELETE' });
    setRevoking(null);
    load();
  };

  const handleCopy = () => {
    navigator.clipboard.writeText(rawKey);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const fmt = (iso) => iso ? new Date(iso).toLocaleDateString() : '—';

  return (
    <section className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-5 mb-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xs font-bold uppercase tracking-widest text-[var(--text-secondary)]">API Keys</h2>
        <button
          onClick={() => setShowForm(f => !f)}
          className="flex items-center gap-1 px-2.5 py-1.5 text-xs border border-[var(--border)] text-[var(--text-secondary)] hover:border-[var(--accent)] hover:text-[var(--accent)] rounded transition-colors"
        >
          <Plus size={11} /> New Key
        </button>
      </div>

      {/* New key form */}
      {showForm && (
        <div className="mb-4 p-3 rounded-md bg-[var(--bg-elevated)] border border-[var(--border)] flex flex-col gap-2">
          <input
            className="w-full px-3 py-2 text-xs rounded bg-[var(--bg-base)] border border-[var(--border)] text-[var(--text-primary)] placeholder-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors"
            placeholder="Key name (e.g. ci-pipeline)"
            value={newName}
            onChange={e => setNewName(e.target.value)}
          />
          <input
            type="date"
            className="w-full px-3 py-2 text-xs rounded bg-[var(--bg-base)] border border-[var(--border)] text-[var(--text-primary)] placeholder-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors"
            placeholder="Expires (optional)"
            value={newExpiry}
            onChange={e => setNewExpiry(e.target.value)}
          />
          <button
            onClick={handleCreate}
            disabled={creating || !newName.trim()}
            className="flex items-center justify-center gap-1.5 px-3 py-1.5 text-xs bg-[var(--accent)] hover:bg-[var(--accent-dim)] text-white rounded transition-colors disabled:opacity-60"
          >
            {creating ? <Loader2 size={11} className="animate-spin" /> : <KeyRound size={11} />}
            Generate
          </button>
        </div>
      )}

      {/* One-time key reveal */}
      {rawKey && (
        <div className="mb-4 p-3 rounded-md bg-green-900/20 border border-green-800/40">
          <p className="text-[10px] text-[var(--green)] font-semibold mb-1.5">
            Copy this key — it will not be shown again.
          </p>
          <div className="flex items-center gap-2">
            <code className="flex-1 text-[10px] font-mono text-[var(--text-primary)] bg-[var(--bg-elevated)] px-2 py-1.5 rounded break-all">
              {rawKey}
            </code>
            <button onClick={handleCopy} className="shrink-0 p-1.5 text-[var(--text-secondary)] hover:text-[var(--green)] transition-colors">
              {copied ? <Check size={13} /> : <Copy size={13} />}
            </button>
          </div>
          <button onClick={() => setRawKey(null)} className="mt-2 text-[10px] text-[var(--text-muted)] hover:text-[var(--text-secondary)] underline">
            Dismiss
          </button>
        </div>
      )}

      {/* Keys table */}
      {loading ? (
        <div className="flex justify-center py-4"><Loader2 size={16} className="animate-spin text-[var(--accent)]" /></div>
      ) : keys.length === 0 ? (
        <p className="text-xs text-[var(--text-muted)] py-2">No API keys yet.</p>
      ) : (
        <div className="divide-y divide-[var(--border)]">
          {keys.map(k => (
            <div key={k.id} className="flex items-center gap-3 py-2.5 text-xs">
              <KeyRound size={13} className="text-[var(--text-muted)] shrink-0" />
              <div className="flex-1 min-w-0">
                <span className="font-medium text-[var(--text-primary)]">{k.name}</span>
                <span className="ml-2 text-[var(--text-muted)]">Created {fmt(k.createdAt)}</span>
                {k.lastUsedAt && <span className="ml-2 text-[var(--text-muted)]">· Last used {fmt(k.lastUsedAt)}</span>}
                {k.expiresAt  && <span className="ml-2 text-[var(--text-muted)]">· Expires {fmt(k.expiresAt)}</span>}
              </div>
              <button
                onClick={() => handleRevoke(k.id)}
                disabled={revoking === k.id}
                className="shrink-0 p-1 text-[var(--text-muted)] hover:text-[var(--red)] transition-colors disabled:opacity-40"
                title="Revoke"
              >
                {revoking === k.id ? <Loader2 size={13} className="animate-spin" /> : <Trash2 size={13} />}
              </button>
            </div>
          ))}
        </div>
      )}
    </section>
  );
};

// ── Run Retention section ─────────────────────────────────────────────────────

const RetentionCard = () => {
  const { authFetch }           = useAuth();
  const [days, setDays]         = useState(7);
  const [loading, setLoading]   = useState(true);
  const [saving, setSaving]     = useState(false);
  const [feedback, setFeedback] = useState(null);

  const showFeedback = (type, message) => {
    setFeedback({ type, message });
    setTimeout(() => setFeedback(null), 4000);
  };

  useEffect(() => {
    authFetch('/api/admin/settings/system')
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data) setDays(data.failedRunRetentionDays ?? 7); })
      .finally(() => setLoading(false));
  }, [authFetch]);

  const handleSave = async () => {
    const parsed = parseInt(days, 10);
    if (isNaN(parsed) || parsed < 1 || parsed > 365) {
      showFeedback('error', 'Must be between 1 and 365 days.');
      return;
    }
    setSaving(true);
    try {
      const res = await authFetch('/api/admin/settings/system', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ failedRunRetentionDays: parsed }),
      });
      if (res.ok) showFeedback('success', 'Retention setting saved.');
      else showFeedback('error', 'Failed to save. Check server logs.');
    } catch (err) {
      showFeedback('error', `Save failed: ${err.message}`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <section className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-5 mb-6">
      <div className="flex items-center gap-2 mb-1">
        <RotateCcw size={13} className="text-[var(--text-secondary)]" />
        <h2 className="text-xs font-bold uppercase tracking-widest text-[var(--text-secondary)]">Failed Run Retention</h2>
      </div>
      <p className="text-[11px] text-[var(--text-muted)] mb-4">
        When a pipeline run fails, LoomPipe saves a snapshot of the pipeline configuration so you can retry with the exact same settings, even if the pipeline has been edited since.
        After the retention window the snapshot is deleted — retries still work but will use the current config.
      </p>

      {feedback && (
        <div className={`mb-4 flex items-center gap-2 px-3 py-2 rounded text-xs border ${
          feedback.type === 'success'
            ? 'bg-green-900/20 border-green-800/40 text-[var(--green)]'
            : 'bg-red-950/40 border-red-900/50 text-[var(--red)]'
        }`}>
          {feedback.type === 'success' ? <CheckCircle2 size={12} /> : <AlertCircle size={12} />}
          {feedback.message}
        </div>
      )}

      {loading ? (
        <div className="flex items-center gap-2 py-2"><Loader2 size={14} className="animate-spin text-[var(--accent)]" /></div>
      ) : (
        <div className="flex items-end gap-3">
          <div className="w-36">
            <label className="block text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider mb-1.5">
              Retention (days)
            </label>
            <input
              type="number"
              min={1}
              max={365}
              value={days}
              onChange={e => setDays(e.target.value)}
              className="w-full px-3 py-2 text-xs rounded bg-[var(--bg-elevated)] border border-[var(--border)] text-[var(--text-primary)] focus:outline-none focus:border-[var(--accent)] transition-colors"
            />
          </div>
          <button
            onClick={handleSave}
            disabled={saving}
            className="flex items-center gap-1.5 px-3 py-2 text-xs font-semibold bg-[var(--accent)] hover:bg-[var(--accent-dim)] text-white rounded transition-colors disabled:opacity-60"
          >
            {saving ? <Loader2 size={12} className="animate-spin" /> : <Save size={12} />}
            Save
          </button>
        </div>
      )}
    </section>
  );
};

// ─────────────────────────────────────────────────────────────────────────────

const SettingsPage = () => {
  const { authFetch } = useAuth();

  const [form, setForm]         = useState(null);
  const [loading, setLoading]   = useState(true);
  const [saving, setSaving]     = useState(false);
  const [testing, setTesting]   = useState(false);
  const [showPwd, setShowPwd]   = useState(false);
  const [feedback, setFeedback] = useState(null); // { type: 'success'|'error', message }

  const showFeedback = (type, message) => {
    setFeedback({ type, message });
    setTimeout(() => setFeedback(null), 5000);
  };

  useEffect(() => {
    authFetch('/api/admin/settings/email')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data) {
          setForm({
            enabled:         data.enabled ?? false,
            smtpHost:        data.smtpHost ?? '',
            smtpPort:        data.smtpPort ?? 587,
            enableSsl:       data.enableSsl ?? true,
            username:        data.username ?? '',
            password:        '',  // never pre-fill password; use passwordSet hint
            fromAddress:     data.fromAddress ?? '',
            fromName:        data.fromName ?? 'LoomPipe',
            adminEmail:      data.adminEmail ?? '',
            notifyOnFailure: data.notifyOnFailure ?? true,
            notifyOnSuccess: data.notifyOnSuccess ?? false,
            _passwordSet:    data.passwordSet ?? false,
          });
        }
      })
      .finally(() => setLoading(false));
  }, [authFetch]);

  const set = (key, value) => setForm(f => ({ ...f, [key]: value }));

  const handleSave = async () => {
    setSaving(true);
    try {
      const res = await authFetch('/api/admin/settings/email', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(form),
      });
      if (res.ok) {
        showFeedback('success', 'Email settings saved successfully.');
        set('_passwordSet', form.password.length > 0 || form._passwordSet);
        set('password', '');
      } else {
        showFeedback('error', 'Failed to save settings. Check the server logs.');
      }
    } catch (err) {
      showFeedback('error', `Save failed: ${err.message}`);
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async () => {
    setTesting(true);
    try {
      const res = await authFetch('/api/admin/settings/email/test', { method: 'POST' });
      const data = await res.json().catch(() => ({}));
      if (res.ok && data.success) {
        showFeedback('success', `Test email sent to ${form.adminEmail}. Check your inbox.`);
      } else {
        showFeedback('error', data.message ?? 'Test email failed. Check SMTP settings and server logs.');
      }
    } catch (err) {
      showFeedback('error', `Test failed: ${err.message}`);
    } finally {
      setTesting(false);
    }
  };

  if (loading) return (
    <div className="flex items-center justify-center h-48">
      <Loader2 size={20} className="animate-spin text-[var(--accent)]" />
    </div>
  );

  if (!form) return (
    <div className="p-6 text-[var(--text-muted)] text-sm">Could not load settings.</div>
  );

  return (
    <div className="p-6 max-w-2xl">
      <h1 className="text-base font-semibold text-[var(--text-primary)] mb-1">Settings</h1>
      <p className="text-xs text-[var(--text-muted)] mb-6">Admin-only configuration for pipeline notifications and system behaviour.</p>

      {/* Feedback banner */}
      {feedback && (
        <div className={`mb-5 flex items-center gap-2 px-3 py-2.5 rounded-md text-xs border ${
          feedback.type === 'success'
            ? 'bg-green-900/20 border-green-800/40 text-[var(--green)]'
            : 'bg-red-950/40 border-red-900/50 text-[var(--red)]'
        }`}>
          {feedback.type === 'success'
            ? <CheckCircle2 size={13} />
            : <AlertCircle size={13} />}
          {feedback.message}
        </div>
      )}

      {/* ── API Keys ────────────────────────────────────────────────────── */}
      <ApiKeysCard />

      {/* ── Failed Run Retention ────────────────────────────────────────── */}
      <RetentionCard />

      {/* ── Email Notifications ─────────────────────────────────────────── */}
      <section className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-5 mb-6">
        <h2 className="text-xs font-bold uppercase tracking-widest text-[var(--text-secondary)] mb-4">Email Notifications</h2>

        <div className="mb-5">
          <Toggle
            checked={form.enabled}
            onChange={v => set('enabled', v)}
            label="Enable email notifications"
            description="When enabled, LoomPipe will send SMTP emails for pipeline events."
          />
        </div>

        <div className={form.enabled ? '' : 'opacity-40 pointer-events-none'}>
          {/* SMTP connection */}
          <div className="grid grid-cols-3 gap-3 mb-0">
            <div className="col-span-2">
              <Field label="SMTP Host">
                <Input
                  placeholder="smtp.example.com"
                  value={form.smtpHost}
                  onChange={e => set('smtpHost', e.target.value)}
                />
              </Field>
            </div>
            <div>
              <Field label="Port">
                <Input
                  type="number"
                  placeholder="587"
                  value={form.smtpPort}
                  onChange={e => set('smtpPort', Number(e.target.value))}
                />
              </Field>
            </div>
          </div>

          <div className="mb-5">
            <Toggle
              checked={form.enableSsl}
              onChange={v => set('enableSsl', v)}
              label="Enable SSL / TLS"
              description="Recommended for ports 465 and 587 with STARTTLS."
            />
          </div>

          <div className="grid grid-cols-2 gap-3">
            <Field label="SMTP Username">
              <Input
                placeholder="notifications@example.com"
                value={form.username}
                onChange={e => set('username', e.target.value)}
              />
            </Field>
            <Field
              label="SMTP Password"
              hint={form._passwordSet ? 'A password is already saved. Leave blank to keep it.' : ''}
            >
              <div className="relative">
                <Input
                  type={showPwd ? 'text' : 'password'}
                  placeholder={form._passwordSet ? '••••••••' : 'Enter password'}
                  value={form.password}
                  onChange={e => set('password', e.target.value)}
                />
                <button
                  type="button"
                  onClick={() => setShowPwd(p => !p)}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors"
                >
                  {showPwd ? <EyeOff size={12} /> : <Eye size={12} />}
                </button>
              </div>
            </Field>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <Field label="From Address" hint="The email address shown in the 'From' field.">
              <Input
                placeholder="noreply@example.com"
                value={form.fromAddress}
                onChange={e => set('fromAddress', e.target.value)}
              />
            </Field>
            <Field label="From Name">
              <Input
                placeholder="LoomPipe"
                value={form.fromName}
                onChange={e => set('fromName', e.target.value)}
              />
            </Field>
          </div>

          <Field label="Admin Notification Email" hint="Receives all pipeline event notifications.">
            <Input
              placeholder="admin@example.com"
              value={form.adminEmail}
              onChange={e => set('adminEmail', e.target.value)}
            />
          </Field>

          {/* Event toggles */}
          <div className="border-t border-[var(--border)] pt-4 space-y-3">
            <p className="text-[10px] font-bold uppercase tracking-widest text-[var(--text-muted)] mb-2">Notify on events</p>
            <Toggle
              checked={form.notifyOnFailure}
              onChange={v => set('notifyOnFailure', v)}
              label="Pipeline failure"
              description="Send an email when any pipeline execution fails."
            />
            <Toggle
              checked={form.notifyOnSuccess}
              onChange={v => set('notifyOnSuccess', v)}
              label="Pipeline success"
              description="Send an email when a pipeline completes successfully."
            />
          </div>
        </div>
      </section>

      {/* Action buttons */}
      <div className="flex items-center gap-3">
        <button
          onClick={handleSave}
          disabled={saving}
          className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold bg-[var(--accent)] hover:bg-[var(--accent-dim)] text-white rounded transition-colors disabled:opacity-60"
        >
          {saving ? <Loader2 size={13} className="animate-spin" /> : <Save size={13} />}
          Save Settings
        </button>

        <button
          onClick={handleTest}
          disabled={testing || !form.enabled || !form.adminEmail}
          title={!form.enabled ? 'Enable email notifications first' : !form.adminEmail ? 'Enter an admin email first' : 'Send a test email'}
          className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold border border-[var(--border)] text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:border-[var(--accent)] rounded transition-colors disabled:opacity-40"
        >
          {testing ? <Loader2 size={13} className="animate-spin" /> : <Send size={13} />}
          Send Test Email
        </button>
      </div>

      <p className="mt-4 text-[10px] text-[var(--text-muted)]">
        Settings are persisted to <code className="font-mono text-[var(--text-secondary)]">email-settings.json</code> in the server root.
        The SMTP password is stored in plain text — keep this file out of source control.
      </p>
    </div>
  );
};

export default SettingsPage;
