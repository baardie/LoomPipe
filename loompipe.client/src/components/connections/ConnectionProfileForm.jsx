import { useState } from 'react';
import { Eye, EyeOff, Plus, X } from 'lucide-react';

const PROVIDERS = [
  { value: 'csv',        label: 'CSV File' },
  { value: 'rest',       label: 'REST API' },
  { value: 'webhook',    label: 'Webhook (destination)' },
  { value: 'sqlserver',  label: 'Microsoft SQL Server' },
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

const AUTH_TYPES = [
  { value: 'none',   label: 'No Auth' },
  { value: 'bearer', label: 'Bearer Token' },
  { value: 'apikey', label: 'API Key' },
  { value: 'basic',  label: 'Basic Auth' },
];

const inputCls = "w-full bg-[var(--bg-elevated)] border border-[var(--border)] rounded px-3 py-2 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors";
const inputSmCls = "w-full bg-[var(--bg-elevated)] border border-[var(--border)] rounded px-2.5 py-1.5 text-xs text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors";

const Field = ({ label, hint, children }) => (
  <div>
    <label className="block text-xs text-[var(--text-muted)] mb-1">{label}</label>
    {children}
    {hint && <p className="text-xs text-[var(--text-muted)] mt-1">{hint}</p>}
  </div>
);

const SectionDivider = ({ title }) => (
  <div className="flex items-center gap-2 pt-1">
    <span className="text-[10px] font-semibold uppercase tracking-widest text-[var(--text-muted)]">{title}</span>
    <div className="flex-1 h-px bg-[var(--border)]" />
  </div>
);

const PasswordInput = ({ value, onChange, placeholder }) => {
  const [show, setShow] = useState(false);
  return (
    <div className="relative">
      <input type={show ? 'text' : 'password'} value={value} onChange={onChange} placeholder={placeholder}
        autoComplete="new-password" className={inputCls + ' pr-9'} />
      <button type="button" onClick={() => setShow(s => !s)}
        className="absolute right-2 top-1/2 -translate-y-1/2 text-[var(--text-muted)] hover:text-[var(--text-secondary)]">
        {show ? <EyeOff size={14} /> : <Eye size={14} />}
      </button>
    </div>
  );
};

const SecretInputSm = ({ value, onChange, placeholder }) => {
  const [show, setShow] = useState(false);
  return (
    <div className="relative">
      <input type={show ? 'text' : 'password'} value={value} onChange={onChange} placeholder={placeholder}
        autoComplete="new-password" className={inputSmCls + ' pr-8'} />
      <button type="button" onClick={() => setShow(s => !s)}
        className="absolute right-2 top-1/2 -translate-y-1/2 text-[var(--text-muted)] hover:text-[var(--text-secondary)]">
        {show ? <EyeOff size={12} /> : <Eye size={12} />}
      </button>
    </div>
  );
};

const CustomHeadersEditor = ({ headers, onChange }) => {
  const add    = () => onChange([...headers, { id: Date.now(), key: '', value: '' }]);
  const remove = (id) => onChange(headers.filter(h => h.id !== id));
  const update = (id, field, val) => onChange(headers.map(h => h.id === id ? { ...h, [field]: val } : h));

  return (
    <div>
      {headers.map(h => (
        <div key={h.id} className="flex items-center gap-1.5 mb-1.5">
          <input value={h.key} onChange={e => update(h.id, 'key', e.target.value)} className={inputSmCls} placeholder="Header-Name" />
          <input value={h.value} onChange={e => update(h.id, 'value', e.target.value)} className={inputSmCls} placeholder="value" />
          <button type="button" onClick={() => remove(h.id)} className="flex-shrink-0 text-[var(--text-muted)] hover:text-[var(--red)] transition-colors">
            <X size={13} />
          </button>
        </div>
      ))}
      <button type="button" onClick={add}
        className="flex items-center gap-1 text-xs text-[var(--accent)] hover:text-[var(--accent-dim)] transition-colors mt-0.5">
        <Plus size={11} /> Add Header
      </button>
    </div>
  );
};

/** Auth + custom headers for REST and webhook profiles. */
const HttpAuthSection = ({ values, onChange }) => {
  const handle = (field) => (e) => onChange({ ...values, [field]: e.target.value });
  const authType = values.authType || 'none';

  return (
    <>
      <SectionDivider title="Authentication" />

      <Field label="Auth Type">
        <select value={authType} onChange={handle('authType')} className={inputSmCls}>
          {AUTH_TYPES.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
        </select>
      </Field>

      {authType === 'bearer' && (
        <Field label="Bearer Token" hint="Stored encrypted · sent as: Authorization: Bearer <token>">
          <SecretInputSm value={values.password || ''} onChange={handle('password')} placeholder="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9…" />
        </Field>
      )}

      {authType === 'apikey' && (
        <>
          <Field label="Header Name">
            <input value={values.username || ''} onChange={handle('username')} className={inputSmCls} placeholder="X-Api-Key" />
          </Field>
          <Field label="API Key" hint="Stored encrypted">
            <SecretInputSm value={values.password || ''} onChange={handle('password')} placeholder="••••••••••••" />
          </Field>
        </>
      )}

      {authType === 'basic' && (
        <>
          <Field label="Username">
            <input value={values.username || ''} onChange={handle('username')} className={inputSmCls} placeholder="username" />
          </Field>
          <Field label="Password" hint="Stored encrypted">
            <SecretInputSm value={values.password || ''} onChange={handle('password')} placeholder="••••••••" />
          </Field>
        </>
      )}

      <SectionDivider title="Custom Headers" />
      <Field label="">
        <CustomHeadersEditor
          headers={values.customHeaders || []}
          onChange={(headers) => onChange({ ...values, customHeaders: headers })}
        />
      </Field>
    </>
  );
};

const ConnectionProfileForm = ({ values, onChange }) => {
  const handle = (field) => (e) => onChange({ ...values, [field]: e.target.value });

  const p = values.provider || '';
  const isSql       = ['sqlserver', 'postgresql', 'mysql', 'oracle'].includes(p);
  const isMongo     = p === 'mongodb';
  const isNeo4j     = p === 'neo4j';
  const isSnowflake = p === 'snowflake';
  const isBigQuery  = p === 'bigquery';
  const isVector    = ['pinecone', 'milvus'].includes(p);
  const isCsv       = p === 'csv';
  const isRest      = p === 'rest';
  const isWebhook   = p === 'webhook';
  const needsHostPort = isSql || isMongo || isNeo4j || isVector;

  return (
    <div className="flex flex-col gap-4">
      <Field label="Provider *">
        <select value={values.provider || ''} onChange={handle('provider')} className={inputCls}>
          <option value="">Select provider…</option>
          {PROVIDERS.map(pr => <option key={pr.value} value={pr.value}>{pr.label}</option>)}
        </select>
      </Field>

      <Field label="Profile Name *">
        <input value={values.name || ''} onChange={handle('name')} placeholder="My Connection" className={inputCls} />
      </Field>

      {isCsv && (
        <Field label="File Path *" hint="Absolute path to the CSV file on the server.">
          <input value={values.host || ''} onChange={handle('host')} placeholder="C:\path\to\file.csv" className={inputCls} />
        </Field>
      )}

      {isRest && (
        <Field label="API URL *" hint="Base URL for the REST API endpoint.">
          <input value={values.host || ''} onChange={handle('host')} placeholder="https://api.example.com/data" className={inputCls} />
        </Field>
      )}

      {isWebhook && (
        <Field label="Webhook URL *" hint="HTTP endpoint that will receive POST requests.">
          <input value={values.host || ''} onChange={handle('host')} placeholder="https://hooks.example.com/..." className={inputCls} />
        </Field>
      )}

      {needsHostPort && (
        <div className="grid grid-cols-3 gap-3">
          <div className="col-span-2">
            <Field label="Host *">
              <input value={values.host || ''} onChange={handle('host')} placeholder="localhost" className={inputCls} />
            </Field>
          </div>
          <Field label="Port">
            <input type="number" value={values.port || ''} onChange={handle('port')} placeholder={isSql ? '1433' : isMongo ? '27017' : isNeo4j ? '7474' : ''} className={inputCls} />
          </Field>
        </div>
      )}

      {isSnowflake && (
        <Field label="Account *" hint="e.g. myorg-myaccount">
          <input value={values.host || ''} onChange={handle('host')} placeholder="myorg-myaccount" className={inputCls} />
        </Field>
      )}

      {isBigQuery && (
        <Field label="Project ID *">
          <input value={values.host || ''} onChange={handle('host')} placeholder="my-gcp-project" className={inputCls} />
        </Field>
      )}

      {(isSql || isMongo || isNeo4j) && (
        <Field label={isMongo ? 'Database (optional)' : 'Database Name *'}>
          <input value={values.databaseName || ''} onChange={handle('databaseName')} placeholder={isMongo ? 'mydb' : 'my_database'} className={inputCls} />
        </Field>
      )}

      {(isSql || isMongo || isNeo4j || isSnowflake || isVector) && (
        <>
          <Field label={isVector ? 'API Key *' : 'Username *'}>
            <input value={values.username || ''} onChange={handle('username')} placeholder={isVector ? 'pk-...' : 'sa'} className={inputCls} />
          </Field>
          {!isVector && (
            <Field label="Password *">
              <PasswordInput value={values.password || ''} onChange={handle('password')} placeholder="••••••••" />
            </Field>
          )}
        </>
      )}

      {isBigQuery && (
        <Field label="Service Account JSON *" hint="Paste the full contents of your service account key JSON.">
          <textarea value={values.password || ''} onChange={handle('password')} rows={4}
            placeholder='{"type":"service_account","project_id":"..."}'
            className={inputCls + ' font-mono text-xs resize-y min-h-24'} />
        </Field>
      )}

      {isSnowflake && (
        <>
          <Field label="Warehouse">
            <input value={values.databaseName || ''} onChange={handle('databaseName')} placeholder="COMPUTE_WH" className={inputCls} />
          </Field>
          <Field label="Password *">
            <PasswordInput value={values.password || ''} onChange={handle('password')} placeholder="••••••••" />
          </Field>
        </>
      )}

      {isVector && (
        <Field label="Environment / Region" hint="e.g. us-east-1-aws or gcp-starter">
          <input value={values.databaseName || ''} onChange={handle('databaseName')} placeholder="us-east-1-aws" className={inputCls} />
        </Field>
      )}

      {/* HTTP auth + custom headers — REST and webhook only */}
      {(isRest || isWebhook) && (
        <HttpAuthSection values={values} onChange={onChange} />
      )}

      <Field label="Notes (optional)">
        <textarea value={values.notes || ''} onChange={handle('notes')} rows={2}
          placeholder="Any notes about this connection…"
          className={inputCls + ' resize-none'} />
      </Field>
    </div>
  );
};

export default ConnectionProfileForm;
