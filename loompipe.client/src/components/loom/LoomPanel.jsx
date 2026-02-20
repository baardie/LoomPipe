import { useState, useRef } from 'react';
import { Upload, FileCheck, X, Loader2, Copy, Braces, Minimize2, Trash2, Check, Plus, Eye, EyeOff } from 'lucide-react';
import { useAuth } from '../../contexts/AuthContext';
import ConnectionProfileSelect from '../connections/ConnectionProfileSelect';
import FieldMappingForm from '../pipeline/FieldMappingForm';
import TransformationForm from '../pipeline/TransformationForm';

const DB_PROVIDERS = ['sqlserver', 'postgresql', 'mysql', 'oracle', 'mongodb', 'neo4j', 'snowflake', 'bigquery', 'pinecone', 'milvus'];

const SOURCE_TYPES = [
  { value: 'csv',        label: 'CSV File' },
  { value: 'json',       label: 'JSON' },
  { value: 'rest',       label: 'REST API' },
  { value: 'sqlserver',  label: 'SQL Server' },
  { value: 'postgresql', label: 'PostgreSQL' },
  { value: 'mysql',      label: 'MySQL / MariaDB' },
  { value: 'oracle',     label: 'Oracle Database' },
  { value: 'mongodb',    label: 'MongoDB' },
  { value: 'neo4j',      label: 'Neo4j' },
  { value: 'snowflake',  label: 'Snowflake' },
  { value: 'bigquery',   label: 'Google BigQuery' },
  { value: 'pinecone',   label: 'Pinecone (Vector DB)' },
  { value: 'milvus',     label: 'Milvus (Vector DB)' },
];

const DESTINATION_TYPES = [
  { value: 'webhook',    label: 'Webhook (HTTP POST)' },
  { value: 'sqlserver',  label: 'SQL Server' },
  { value: 'postgresql', label: 'PostgreSQL' },
  { value: 'mysql',      label: 'MySQL / MariaDB' },
  { value: 'oracle',     label: 'Oracle Database' },
  { value: 'mongodb',    label: 'MongoDB' },
  { value: 'neo4j',      label: 'Neo4j' },
  { value: 'snowflake',  label: 'Snowflake' },
  { value: 'bigquery',   label: 'Google BigQuery' },
  { value: 'pinecone',   label: 'Pinecone (Vector DB)' },
  { value: 'milvus',     label: 'Milvus (Vector DB)' },
];

const TABLE_LABEL = { mongodb: 'Collection', neo4j: 'Node Label', pinecone: 'Index Name', milvus: 'Collection' };

const inputCls = "w-full bg-[var(--bg-elevated)] border border-[var(--border)] rounded px-3 py-2 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors";
const inputSmCls = "w-full bg-[var(--bg-elevated)] border border-[var(--border)] rounded px-2.5 py-1.5 text-xs text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors";

const Field = ({ label, hint, children }) => (
  <div>
    <label className="block text-xs text-[var(--text-muted)] mb-1">{label}</label>
    {children}
    {hint && <p className="text-xs text-[var(--text-muted)] mt-1 font-mono">{hint}</p>}
  </div>
);

const SectionDivider = ({ title }) => (
  <div className="flex items-center gap-2 pt-1">
    <span className="text-[10px] font-semibold uppercase tracking-widest text-[var(--text-muted)]">{title}</span>
    <div className="flex-1 h-px bg-[var(--border)]" />
  </div>
);

const PanelHeader = ({ title }) => (
  <div className="px-4 py-3 border-b border-[var(--border)] bg-[var(--bg-subtle)]">
    <h3 className="text-xs font-semibold uppercase tracking-wider text-[var(--text-secondary)]">{title}</h3>
  </div>
);

// ── Secret input with show/hide toggle ────────────────────────────────────────
const SecretInput = ({ value, onChange, placeholder }) => {
  const [show, setShow] = useState(false);
  return (
    <div className="relative">
      <input
        type={show ? 'text' : 'password'}
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        autoComplete="new-password"
        className={inputSmCls + ' pr-8'}
      />
      <button
        type="button"
        onClick={() => setShow(s => !s)}
        className="absolute right-2 top-1/2 -translate-y-1/2 text-[var(--text-muted)] hover:text-[var(--text-secondary)] transition-colors"
      >
        {show ? <EyeOff size={12} /> : <Eye size={12} />}
      </button>
    </div>
  );
};

// ── Custom headers key-value editor ──────────────────────────────────────────
const CustomHeadersEditor = ({ headers, onChange }) => {
  const add    = () => onChange([...headers, { id: Date.now(), key: '', value: '' }]);
  const remove = (id) => onChange(headers.filter(h => h.id !== id));
  const update = (id, field, val) => onChange(headers.map(h => h.id === id ? { ...h, [field]: val } : h));

  return (
    <div>
      {headers.map(h => (
        <div key={h.id} className="flex items-center gap-1.5 mb-1.5">
          <input
            value={h.key}
            onChange={e => update(h.id, 'key', e.target.value)}
            className={inputSmCls}
            placeholder="Header-Name"
          />
          <input
            value={h.value}
            onChange={e => update(h.id, 'value', e.target.value)}
            className={inputSmCls}
            placeholder="value"
          />
          <button
            type="button"
            onClick={() => remove(h.id)}
            className="flex-shrink-0 text-[var(--text-muted)] hover:text-[var(--red)] transition-colors"
          >
            <X size={13} />
          </button>
        </div>
      ))}
      <button
        type="button"
        onClick={add}
        className="flex items-center gap-1 text-xs text-[var(--accent)] hover:text-[var(--accent-dim)] transition-colors mt-0.5"
      >
        <Plus size={11} /> Add Header
      </button>
    </div>
  );
};

// ── Auth + headers panel (shared for REST source and webhook destination) ─────
const AUTH_TYPES = [
  { value: 'none',   label: 'No Auth' },
  { value: 'bearer', label: 'Bearer Token' },
  { value: 'apikey', label: 'API Key' },
  { value: 'basic',  label: 'Basic Auth' },
];

const HttpAuthPanel = ({ auth, onChange }) => {
  const set = (field, val) => onChange({ ...auth, [field]: val });

  return (
    <div className="flex flex-col gap-3">
      <Field label="Authentication">
        <select value={auth.authType} onChange={e => set('authType', e.target.value)} className={inputSmCls}>
          {AUTH_TYPES.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
        </select>
      </Field>

      {auth.authType === 'bearer' && (
        <Field label="Token" hint="Sent as: Authorization: Bearer <token>">
          <SecretInput
            value={auth.authToken}
            onChange={e => set('authToken', e.target.value)}
            placeholder="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9…"
          />
        </Field>
      )}

      {auth.authType === 'apikey' && (
        <>
          <Field label="Header Name">
            <input value={auth.apiKeyHeader} onChange={e => set('apiKeyHeader', e.target.value)} className={inputSmCls} placeholder="X-Api-Key" />
          </Field>
          <Field label="API Key">
            <SecretInput
              value={auth.apiKeyValue}
              onChange={e => set('apiKeyValue', e.target.value)}
              placeholder="••••••••••••"
            />
          </Field>
        </>
      )}

      {auth.authType === 'basic' && (
        <>
          <Field label="Username">
            <input value={auth.basicUsername} onChange={e => set('basicUsername', e.target.value)} className={inputSmCls} placeholder="username" />
          </Field>
          <Field label="Password">
            <SecretInput
              value={auth.basicPassword}
              onChange={e => set('basicPassword', e.target.value)}
              placeholder="••••••••"
            />
          </Field>
        </>
      )}

      <Field label="Custom Headers">
        <CustomHeadersEditor headers={auth.headers} onChange={val => set('headers', val)} />
      </Field>
    </div>
  );
};

// ── CSV file uploader ─────────────────────────────────────────────────────────
const CsvUploader = ({ value, onChange }) => {
  const { authFetch } = useAuth();
  const inputRef = useRef(null);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState('');

  const fileName = value ? value.replace(/\\/g, '/').split('/').pop() : null;

  const handleFile = async (file) => {
    if (!file) return;
    setError('');
    setUploading(true);
    try {
      const form = new FormData();
      form.append('file', file);
      const res = await authFetch('/api/csv/upload', { method: 'POST', body: form });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `Upload failed (${res.status})`);
      }
      const data = await res.json();
      onChange(data.path);
    } catch (err) {
      setError(err.message);
    } finally {
      setUploading(false);
    }
  };

  const handleDrop = (e) => { e.preventDefault(); const file = e.dataTransfer.files[0]; if (file) handleFile(file); };

  return (
    <div>
      <input ref={inputRef} type="file" accept=".csv" className="hidden" onChange={e => handleFile(e.target.files[0])} />
      {fileName ? (
        <div className="flex items-center gap-2 px-3 py-2 bg-[var(--bg-elevated)] border border-[var(--green)]/50 rounded">
          <FileCheck size={14} className="text-[var(--green)] flex-shrink-0" />
          <span className="flex-1 text-xs font-mono text-[var(--text-primary)] truncate">{fileName}</span>
          <button onClick={() => { onChange(''); if (inputRef.current) inputRef.current.value = ''; }} className="text-[var(--text-muted)] hover:text-[var(--red)] transition-colors"><X size={13} /></button>
        </div>
      ) : (
        <button type="button" onClick={() => inputRef.current?.click()} onDrop={handleDrop} onDragOver={e => e.preventDefault()} disabled={uploading}
          className="w-full flex flex-col items-center gap-2 px-4 py-5 border border-dashed border-[var(--border)] rounded-lg text-[var(--text-muted)] hover:border-[var(--accent)] hover:text-[var(--accent)] transition-colors disabled:opacity-60 cursor-pointer">
          {uploading ? <Loader2 size={20} className="animate-spin" /> : <Upload size={20} />}
          <span className="text-xs">{uploading ? 'Uploading…' : 'Click or drag a .csv file here'}</span>
        </button>
      )}
      {error && <p className="mt-1.5 text-xs text-[var(--red)]">{error}</p>}
    </div>
  );
};

// ── JSON file uploader ────────────────────────────────────────────────────────
const JsonUploader = ({ value, onChange }) => {
  const { authFetch } = useAuth();
  const inputRef = useRef(null);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState('');

  const fileName = value ? value.replace(/\\/g, '/').split('/').pop() : null;

  const handleFile = async (file) => {
    if (!file) return;
    setError('');
    setUploading(true);
    try {
      const form = new FormData();
      form.append('file', file);
      const res = await authFetch('/api/json/upload', { method: 'POST', body: form });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `Upload failed (${res.status})`);
      }
      const data = await res.json();
      onChange(data.path);
    } catch (err) {
      setError(err.message);
    } finally {
      setUploading(false);
    }
  };

  const handleDrop = (e) => { e.preventDefault(); const file = e.dataTransfer.files[0]; if (file) handleFile(file); };

  return (
    <div>
      <input ref={inputRef} type="file" accept=".json" className="hidden" onChange={e => handleFile(e.target.files[0])} />
      {fileName ? (
        <div className="flex items-center gap-2 px-3 py-2 bg-[var(--bg-elevated)] border border-[var(--green)]/50 rounded">
          <FileCheck size={14} className="text-[var(--green)] flex-shrink-0" />
          <span className="flex-1 text-xs font-mono text-[var(--text-primary)] truncate">{fileName}</span>
          <button onClick={() => { onChange(''); if (inputRef.current) inputRef.current.value = ''; }} className="text-[var(--text-muted)] hover:text-[var(--red)] transition-colors"><X size={13} /></button>
        </div>
      ) : (
        <button type="button" onClick={() => inputRef.current?.click()} onDrop={handleDrop} onDragOver={e => e.preventDefault()} disabled={uploading}
          className="w-full flex flex-col items-center gap-2 px-4 py-5 border border-dashed border-[var(--border)] rounded-lg text-[var(--text-muted)] hover:border-[var(--accent)] hover:text-[var(--accent)] transition-colors disabled:opacity-60 cursor-pointer">
          {uploading ? <Loader2 size={20} className="animate-spin" /> : <Upload size={20} />}
          <span className="text-xs">{uploading ? 'Uploading…' : 'Click or drag a .json file here'}</span>
        </button>
      )}
      {error && <p className="mt-1.5 text-xs text-[var(--red)]">{error}</p>}
    </div>
  );
};

// ── Professional inline JSON editor ──────────────────────────────────────────
const JsonInlineEditor = ({ value, onChange }) => {
  const textareaRef = useRef(null);
  const [copied, setCopied] = useState(false);

  const hasContent = value.trim().length > 0;
  let parseResult = { valid: false, records: 0, error: null };
  if (hasContent) {
    try {
      const parsed = JSON.parse(value);
      parseResult = { valid: true, records: Array.isArray(parsed) ? parsed.length : 1, error: null };
    } catch (e) {
      parseResult = { valid: false, records: 0, error: e.message };
    }
  }

  const handleFormat = () => { if (!parseResult.valid) return; try { onChange(JSON.stringify(JSON.parse(value), null, 2)); } catch { /* ignore */ } };
  const handleMinify = () => { if (!parseResult.valid) return; try { onChange(JSON.stringify(JSON.parse(value))); } catch { /* ignore */ } };
  const handleCopy   = () => { if (!hasContent) return; navigator.clipboard.writeText(value).then(() => { setCopied(true); setTimeout(() => setCopied(false), 1500); }).catch(() => {}); };
  const handleClear  = () => { onChange(''); textareaRef.current?.focus(); };

  const lineCount = hasContent ? value.split('\n').length : 0;

  return (
    <div className={`flex flex-col border rounded-lg overflow-hidden transition-colors focus-within:ring-1 ${
      hasContent ? parseResult.valid ? 'border-[var(--green)]/50 focus-within:ring-[var(--green)]/30' : 'border-[var(--red)]/50 focus-within:ring-[var(--red)]/30' : 'border-[var(--border)] focus-within:ring-[var(--accent)]/30'
    }`}>
      <div className="flex items-center gap-0.5 px-2 py-1.5 bg-[var(--bg-subtle)] border-b border-[var(--border)]">
        <span className="text-[10px] font-semibold tracking-widest text-[var(--text-muted)] uppercase mr-2">JSON</span>
        <button type="button" onClick={handleFormat} disabled={!parseResult.valid} title="Pretty-print (2-space indent)"
          className="flex items-center gap-1 px-2 py-0.5 text-[11px] rounded text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] transition-colors disabled:opacity-30 disabled:cursor-not-allowed">
          <Braces size={11} /> Format
        </button>
        <button type="button" onClick={handleMinify} disabled={!parseResult.valid} title="Minify to single line"
          className="flex items-center gap-1 px-2 py-0.5 text-[11px] rounded text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] transition-colors disabled:opacity-30 disabled:cursor-not-allowed">
          <Minimize2 size={11} /> Minify
        </button>
        <div className="flex-1" />
        <button type="button" onClick={handleCopy} disabled={!hasContent} title="Copy to clipboard"
          className="flex items-center gap-1 px-2 py-0.5 text-[11px] rounded transition-colors disabled:opacity-30 disabled:cursor-not-allowed text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-elevated)]">
          {copied ? <Check size={11} className="text-[var(--green)]" /> : <Copy size={11} />}
          {copied ? 'Copied' : 'Copy'}
        </button>
        <button type="button" onClick={handleClear} disabled={!hasContent} title="Clear editor"
          className="flex items-center gap-1 px-2 py-0.5 text-[11px] rounded text-[var(--text-secondary)] hover:text-[var(--red)] hover:bg-[var(--bg-elevated)] transition-colors disabled:opacity-30 disabled:cursor-not-allowed">
          <Trash2 size={11} /> Clear
        </button>
      </div>
      <textarea ref={textareaRef} value={value} onChange={e => onChange(e.target.value)} rows={16}
        spellCheck={false} autoCorrect="off" autoCapitalize="off"
        className="w-full bg-[var(--bg-elevated)] px-4 py-3 text-xs font-mono text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none resize-none leading-[1.6]"
        placeholder={'[\n  {\n    "id": 1,\n    "name": "example",\n    "active": true\n  }\n]'}
      />
      <div className={`flex items-center gap-2 px-3 py-1.5 border-t text-[11px] font-mono select-none ${
        hasContent ? parseResult.valid ? 'border-[var(--green)]/30 bg-green-950/20' : 'border-[var(--red)]/30 bg-red-950/20' : 'border-[var(--border)] bg-[var(--bg-subtle)]'
      }`}>
        {!hasContent && <span className="text-[var(--text-muted)]">Array [ ] or object &#123; &#125; accepted</span>}
        {hasContent && parseResult.valid && (
          <>
            <span className="text-[var(--green)] font-semibold">✓ valid</span>
            <span className="text-[var(--border)]">|</span>
            <span className="text-[var(--text-secondary)]">{parseResult.records} record{parseResult.records !== 1 ? 's' : ''}</span>
            <span className="text-[var(--border)]">|</span>
            <span className="text-[var(--text-muted)]">{lineCount} ln</span>
            <span className="text-[var(--border)]">|</span>
            <span className="text-[var(--text-muted)]">{value.length.toLocaleString()} ch</span>
          </>
        )}
        {hasContent && !parseResult.valid && (
          <>
            <span className="text-[var(--red)] font-semibold">✗ invalid</span>
            <span className="text-[var(--border)]">|</span>
            <span className="text-red-400 truncate" title={parseResult.error}>{parseResult.error}</span>
          </>
        )}
      </div>
    </div>
  );
};

// ── Source panel ──────────────────────────────────────────────────────────────
const SourcePanel = (props) => {
  const {
    sourceType, setSourceType,
    sourceConnectionString, setSourceConnectionString,
    sourceProfileId, setSourceProfileId,
    sourceTable, setSourceTable,
    sourceJsonMode, setSourceJsonMode,
    sourceRestAuth, setSourceRestAuth,
  } = props;
  const isDb = DB_PROVIDERS.includes(sourceType);

  return (
    <>
      <PanelHeader title="Source Configuration" />
      <div className="p-4 flex flex-col gap-4 overflow-y-auto flex-1">
        <Field label="Source Type">
          <select value={sourceType} onChange={e => setSourceType(e.target.value)} className={inputCls}>
            <option value="">— select type —</option>
            {SOURCE_TYPES.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
          </select>
        </Field>

        {isDb ? (
          <>
            <ConnectionProfileSelect label="Connection Profile" profileId={sourceProfileId} onProfileChange={setSourceProfileId} filterProvider={sourceType} />
            <Field label={TABLE_LABEL[sourceType] ?? 'Table / View'}>
              <input value={sourceTable} onChange={e => setSourceTable(e.target.value)} className={inputCls} placeholder="my_table" />
            </Field>
          </>

        ) : sourceType === 'csv' ? (
          <Field label="CSV File">
            <CsvUploader value={sourceConnectionString} onChange={setSourceConnectionString} />
          </Field>

        ) : sourceType === 'json' ? (
          <>
            <div className="flex rounded overflow-hidden border border-[var(--border)]">
              <button type="button" onClick={() => { setSourceJsonMode('inline'); setSourceConnectionString(''); }}
                className={`flex-1 px-3 py-1.5 text-xs transition-colors ${sourceJsonMode === 'inline' ? 'bg-[var(--accent)] text-white' : 'bg-[var(--bg-elevated)] text-[var(--text-secondary)] hover:bg-[var(--bg-subtle)]'}`}>
                Paste JSON
              </button>
              <button type="button" onClick={() => { setSourceJsonMode('file'); setSourceConnectionString(''); }}
                className={`flex-1 px-3 py-1.5 text-xs transition-colors ${sourceJsonMode === 'file' ? 'bg-[var(--accent)] text-white' : 'bg-[var(--bg-elevated)] text-[var(--text-secondary)] hover:bg-[var(--bg-subtle)]'}`}>
                Upload File
              </button>
            </div>
            {sourceJsonMode === 'inline'
              ? <Field label="JSON Data"><JsonInlineEditor value={sourceConnectionString} onChange={setSourceConnectionString} /></Field>
              : <Field label="JSON File"><JsonUploader value={sourceConnectionString} onChange={setSourceConnectionString} /></Field>
            }
          </>

        ) : sourceType === 'rest' ? (
          <>
            <Field label="API URL">
              <input value={sourceConnectionString} onChange={e => setSourceConnectionString(e.target.value)} className={inputCls}
                placeholder="https://api.example.com/data" />
            </Field>
            <SectionDivider title="Authentication & Headers" />
            <HttpAuthPanel auth={sourceRestAuth} onChange={setSourceRestAuth} />
          </>

        ) : sourceType ? (
          <Field label="URL">
            <input value={sourceConnectionString} onChange={e => setSourceConnectionString(e.target.value)} className={inputCls}
              placeholder="https://api.example.com/data" />
          </Field>
        ) : null}
      </div>
    </>
  );
};

// ── Transform panel ───────────────────────────────────────────────────────────
const TransformPanel = (props) => {
  const { fieldMappings, setFieldMappings, transformations, setTransformations, handleAutomap, handleDryRun, sourceType, sourceConnectionString, destinationSchema } = props;
  return (
    <>
      <PanelHeader title="Transform Configuration" />
      <div className="p-4 flex flex-col gap-5 overflow-y-auto flex-1">
        <div>
          <div className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider mb-2">Field Mappings</div>
          <FieldMappingForm
            fieldMappings={fieldMappings}
            setFieldMappings={setFieldMappings}
            handleAutomap={handleAutomap}
            handleDryRun={handleDryRun}
            sourceType={sourceType}
            sourceConnectionString={sourceConnectionString}
            destinationSchema={destinationSchema}
          />
        </div>
        <div>
          <div className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider mb-2">Transformations</div>
          <TransformationForm transformations={transformations} setTransformations={setTransformations} />
        </div>
      </div>
    </>
  );
};

// ── Destination panel ─────────────────────────────────────────────────────────
const DestinationPanel = (props) => {
  const {
    destinationType, setDestinationType,
    destinationConnectionString, setDestinationConnectionString,
    destinationProfileId, setDestinationProfileId,
    destinationTable, setDestinationTable,
    destinationSchema, setDestinationSchema,
    destWebhookAuth, setDestWebhookAuth,
  } = props;
  const isDb = DB_PROVIDERS.includes(destinationType);

  return (
    <>
      <PanelHeader title="Destination Configuration" />
      <div className="p-4 flex flex-col gap-4 overflow-y-auto flex-1">
        <Field label="Destination Type">
          <select value={destinationType} onChange={e => setDestinationType(e.target.value)} className={inputCls}>
            <option value="">— select type —</option>
            {DESTINATION_TYPES.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
          </select>
        </Field>

        {isDb ? (
          <>
            <ConnectionProfileSelect label="Connection Profile" profileId={destinationProfileId} onProfileChange={setDestinationProfileId} filterProvider={destinationType} />
            <Field label={TABLE_LABEL[destinationType] ?? 'Table / View'}>
              <input value={destinationTable} onChange={e => setDestinationTable(e.target.value)} className={inputCls} placeholder="my_table" />
            </Field>
          </>

        ) : destinationType === 'webhook' ? (
          <>
            <Field label="Webhook URL">
              <input value={destinationConnectionString} onChange={e => setDestinationConnectionString(e.target.value)} className={inputCls}
                placeholder="https://hooks.example.com/..." />
            </Field>
            <SectionDivider title="Authentication & Headers" />
            <HttpAuthPanel auth={destWebhookAuth} onChange={setDestWebhookAuth} />
          </>

        ) : destinationType ? (
          <Field label="Connection String">
            <input value={destinationConnectionString} onChange={e => setDestinationConnectionString(e.target.value)} className={inputCls} />
          </Field>
        ) : null}

        <SectionDivider title="Schema" />
        <Field label="Comma-separated field names">
          <input value={destinationSchema} onChange={e => setDestinationSchema(e.target.value)} className={inputCls}
            placeholder="id, name, email, created_at" />
          <p className="text-xs text-[var(--text-muted)] mt-1">Target field names used for field mapping.</p>
        </Field>
      </div>
    </>
  );
};

const LoomPanel = ({ activePanel, ...props }) => {
  if (activePanel === 'source')      return <SourcePanel {...props} />;
  if (activePanel === 'transform')   return <TransformPanel {...props} />;
  if (activePanel === 'destination') return <DestinationPanel {...props} />;
  return null;
};

export default LoomPanel;
