import { useState } from 'react';
import { ChevronLeft, Play, Zap, Save, Loader2 } from 'lucide-react';
import { useAuth } from '../../contexts/AuthContext';
import LoomCanvas from './LoomCanvas';
import LoomPanel from './LoomPanel';
import LoomSettings from './LoomSettings';
import DryRunResultModal from '../pipeline/DryRunResultModal';
import RoleGuard from '../auth/RoleGuard';

const DB_PROVIDERS = ['sqlserver', 'postgresql', 'mysql', 'oracle', 'mongodb', 'neo4j', 'snowflake', 'bigquery', 'pinecone', 'milvus'];

// ── Auth state helpers ────────────────────────────────────────────────────────

const DEFAULT_AUTH = {
  authType: 'none',
  authToken: '',
  apiKeyHeader: 'X-Api-Key',
  apiKeyValue: '',
  basicUsername: '',
  basicPassword: '',
  headers: [],   // [{id, key, value}]
};

/** Load auth from saved Parameters (headers object → array for the editor). */
const loadAuth = (params) => {
  if (!params) return DEFAULT_AUTH;
  const savedHeaders = params.headers && typeof params.headers === 'object' && !Array.isArray(params.headers)
    ? Object.entries(params.headers).map(([key, value]) => ({ id: `${key}-${Math.random()}`, key, value: String(value) }))
    : [];
  return {
    authType:      params.authType      ?? 'none',
    authToken:     params.authToken     ?? '',
    apiKeyHeader:  params.apiKeyHeader  ?? 'X-Api-Key',
    apiKeyValue:   params.apiKeyValue   ?? '',
    basicUsername: params.basicUsername ?? '',
    basicPassword: params.basicPassword ?? '',
    headers: savedHeaders,
  };
};

/** Serialise auth state back to a Parameters-compatible object. */
const buildAuthParams = (auth) => {
  const { authType, authToken, apiKeyHeader, apiKeyValue, basicUsername, basicPassword, headers } = auth;
  const headersObj = Object.fromEntries(headers.filter(h => h.key.trim()).map(h => [h.key.trim(), h.value]));
  const params = { authType };
  if (authType === 'bearer' && authToken)   params.authToken     = authToken;
  if (authType === 'apikey')                { params.apiKeyHeader = apiKeyHeader; if (apiKeyValue) params.apiKeyValue = apiKeyValue; }
  if (authType === 'basic')                 { if (basicUsername) params.basicUsername = basicUsername; if (basicPassword) params.basicPassword = basicPassword; }
  if (Object.keys(headersObj).length > 0)  params.headers = headersObj;
  return params;
};

// ── Component ─────────────────────────────────────────────────────────────────

const LoomEditor = ({ onSave, onCancel, pipeline = {} }) => {
  const { authFetch } = useAuth();

  // Pipeline identity
  const [name,                      setName]                      = useState(pipeline.name || '');

  // Source state
  const [sourceType,                setSourceType]                = useState(pipeline.source?.type || '');
  const [sourceConnectionString,    setSourceConnectionString]    = useState(pipeline.source?.connectionString || '');
  const [sourceProfileId,           setSourceProfileId]           = useState(pipeline.source?.parameters?.connectionProfileId ?? null);
  const [sourceTable,               setSourceTable]               = useState(pipeline.source?.parameters?.table || '');
  const [sourceJsonMode,            setSourceJsonMode]            = useState(pipeline.source?.parameters?.jsonMode ?? 'inline');
  const [sourceRestAuth,            setSourceRestAuth]            = useState(() => loadAuth(pipeline.source?.parameters));

  // Destination state
  const [destinationType,           setDestinationType]           = useState(pipeline.destination?.type || '');
  const [destinationConnectionString, setDestinationConnectionString] = useState(pipeline.destination?.connectionString || '');
  const [destinationProfileId,      setDestinationProfileId]      = useState(pipeline.destination?.parameters?.connectionProfileId ?? null);
  const [destinationTable,          setDestinationTable]          = useState(pipeline.destination?.parameters?.table || '');
  const [destinationSchema,         setDestinationSchema]         = useState(pipeline.destination?.schema || '');
  const [destWebhookAuth,           setDestWebhookAuth]           = useState(() => loadAuth(pipeline.destination?.parameters));

  // Mappings / transforms
  const [fieldMappings,             setFieldMappings]             = useState(pipeline.fieldMappings || []);
  const [transformations,           setTransformations]           = useState(pipeline.transformations || []);

  // Schedule / batch / incremental
  const [scheduleEnabled,           setScheduleEnabled]           = useState(pipeline.scheduleEnabled || false);
  const [cronExpression,            setCronExpression]            = useState(pipeline.cronExpression ?? '');
  const [batchEnabled,              setBatchEnabled]              = useState(!!(pipeline.batchSize));
  const [batchSize,                 setBatchSize]                 = useState(pipeline.batchSize ?? '');
  const [batchDelaySeconds,         setBatchDelaySeconds]         = useState(pipeline.batchDelaySeconds ?? '');
  const [incrementalEnabled,        setIncrementalEnabled]        = useState(!!(pipeline.incrementalField));
  const [incrementalField,          setIncrementalField]          = useState(pipeline.incrementalField ?? '');

  // UI state
  const [activePanel,   setActivePanel]   = useState(null);
  const [saving,        setSaving]        = useState(false);
  const [saveError,     setSaveError]     = useState(null);
  const [running,       setRunning]       = useState(false);
  const [dryRunResult,  setDryRunResult]  = useState(null);
  const [dryRunOpen,    setDryRunOpen]    = useState(false);

  const buildSourceConfig = () => {
    if (DB_PROVIDERS.includes(sourceType))
      return { id: pipeline.source?.id ?? 0, type: sourceType, parameters: { connectionProfileId: sourceProfileId, table: sourceTable } };
    if (sourceType === 'json')
      return { id: pipeline.source?.id ?? 0, type: sourceType, connectionString: sourceConnectionString, parameters: { jsonMode: sourceJsonMode } };
    if (sourceType === 'rest')
      return { id: pipeline.source?.id ?? 0, type: sourceType, connectionString: sourceConnectionString, parameters: buildAuthParams(sourceRestAuth) };
    return { id: pipeline.source?.id ?? 0, type: sourceType, connectionString: sourceConnectionString };
  };

  const buildDestConfig = () => {
    if (DB_PROVIDERS.includes(destinationType))
      return { id: pipeline.destination?.id ?? 0, type: destinationType, schema: destinationSchema, parameters: { connectionProfileId: destinationProfileId, table: destinationTable } };
    if (destinationType === 'webhook')
      return { id: pipeline.destination?.id ?? 0, type: destinationType, connectionString: destinationConnectionString, schema: destinationSchema, parameters: buildAuthParams(destWebhookAuth) };
    return { id: pipeline.destination?.id ?? 0, type: destinationType, connectionString: destinationConnectionString, schema: destinationSchema };
  };

  const buildPayload = () => ({
    ...pipeline,
    name,
    source: buildSourceConfig(),
    destination: buildDestConfig(),
    fieldMappings,
    transformations,
    scheduleEnabled,
    cronExpression: scheduleEnabled && cronExpression ? cronExpression : null,
    nextRunAt: pipeline.nextRunAt ?? null,
    batchSize:        batchEnabled && batchSize        ? Number(batchSize)        : null,
    batchDelaySeconds: batchEnabled && batchDelaySeconds ? Number(batchDelaySeconds) : null,
    incrementalField: incrementalEnabled && incrementalField ? incrementalField : null,
    lastIncrementalValue: pipeline.lastIncrementalValue ?? null,
  });

  const handleSave = async () => {
    setSaving(true);
    setSaveError(null);
    try {
      await onSave(buildPayload());
    } catch (e) {
      setSaveError(e.message);
    } finally {
      setSaving(false);
    }
  };

  const handleRun = async () => {
    if (!pipeline.id) return;
    setRunning(true);
    try { await authFetch(`/api/pipelines/${pipeline.id}/run`, { method: 'POST' }); }
    finally { setRunning(false); }
  };

  const handleAutomap = async () => {
    const response = await authFetch('/api/pipelines/automap', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source: buildSourceConfig(), destination: { schema: destinationSchema } }),
    });
    const data = await response.json();
    setFieldMappings(data);
  };

  const handleDryRun = async () => {
    try {
      const response = await authFetch('/api/pipelines/dryrun', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, source: buildSourceConfig(), destination: buildDestConfig(), fieldMappings, transformations }),
      });
      if (!response.ok) {
        const text = await response.text();
        setDryRunResult({ error: `Server error ${response.status}: ${text}`, sourcePreview: [], mappedPreview: [], transformedPreview: [] });
        setDryRunOpen(true);
        return;
      }
      const data = await response.json();
      setDryRunResult(data);
      setDryRunOpen(true);
    } catch (e) {
      setDryRunResult({ error: e.message, sourcePreview: [], mappedPreview: [], transformedPreview: [] });
      setDryRunOpen(true);
    }
  };

  const handleNodeClick = (panel) => setActivePanel(prev => prev === panel ? null : panel);

  const panelWide = (activePanel === 'source' && (sourceType === 'json' || sourceType === 'rest'))
                 || (activePanel === 'destination' && destinationType === 'webhook');

  return (
    <div className="fixed inset-0 z-40 flex flex-col bg-[var(--bg-base)]">
      {/* Editor topbar */}
      <div className="flex-shrink-0 flex items-center gap-3 px-4 py-2.5 bg-[var(--bg-surface)] border-b border-[var(--border)]">
        <button onClick={onCancel} className="flex items-center gap-1 text-xs text-[var(--text-secondary)] hover:text-[var(--text-primary)] transition-colors">
          <ChevronLeft size={15} /> Back
        </button>
        <div className="w-px h-4 bg-[var(--border)]" />
        <input
          value={name}
          onChange={e => setName(e.target.value)}
          placeholder="Pipeline name…"
          className="flex-1 bg-transparent text-sm font-semibold text-[var(--text-primary)] placeholder:text-[var(--text-muted)] outline-none border-b border-transparent focus:border-[var(--accent)] transition-colors pb-0.5"
        />
        <div className="flex items-center gap-2 ml-auto">
          {saveError && (
            <span className="text-xs text-[var(--red)] font-mono max-w-sm truncate" title={saveError}>
              {saveError}
            </span>
          )}
          <button onClick={handleDryRun} className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-[var(--border)] text-[var(--text-secondary)] hover:border-[var(--yellow)] hover:text-[var(--yellow)] rounded transition-colors">
            <Zap size={13} /> Dry Run
          </button>
          {pipeline.id && (
            <RoleGuard roles={['Admin']}>
              <button onClick={handleRun} disabled={running} className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-[var(--green)]/50 text-[var(--green)] hover:bg-green-900/20 rounded transition-colors disabled:opacity-60">
                {running ? <Loader2 size={13} className="animate-spin" /> : <Play size={13} />} Run Now
              </button>
            </RoleGuard>
          )}
          <button onClick={handleSave} disabled={saving} className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-[var(--accent)] hover:bg-[var(--accent-dim)] text-white rounded transition-colors disabled:opacity-60">
            {saving ? <Loader2 size={13} className="animate-spin" /> : <Save size={13} />} Save
          </button>
        </div>
      </div>

      {/* Main area: canvas + side panel */}
      <div className="flex-1 flex overflow-hidden">
        {/* SVG Canvas */}
        <div className="flex-1 overflow-hidden relative">
          <LoomCanvas
            sourceType={sourceType}
            destinationType={destinationType}
            fieldMappings={fieldMappings}
            activePanel={activePanel}
            onNodeClick={handleNodeClick}
          />
          {!activePanel && (
            <div className="absolute bottom-4 left-1/2 -translate-x-1/2 text-xs text-[var(--text-muted)] font-mono">
              Click a node to configure it
            </div>
          )}
        </div>

        {/* Side panel — wider for JSON editor, REST auth, and webhook auth */}
        {activePanel && (
          <div className={`flex-shrink-0 bg-[var(--bg-surface)] border-l border-[var(--border)] flex flex-col overflow-hidden transition-all ${panelWide ? 'w-[460px]' : 'w-80'}`}>
            <LoomPanel
              activePanel={activePanel}
              sourceType={sourceType}                   setSourceType={setSourceType}
              sourceConnectionString={sourceConnectionString} setSourceConnectionString={setSourceConnectionString}
              sourceProfileId={sourceProfileId}         setSourceProfileId={setSourceProfileId}
              sourceTable={sourceTable}                 setSourceTable={setSourceTable}
              sourceJsonMode={sourceJsonMode}           setSourceJsonMode={setSourceJsonMode}
              sourceRestAuth={sourceRestAuth}           setSourceRestAuth={setSourceRestAuth}
              destinationType={destinationType}         setDestinationType={setDestinationType}
              destinationConnectionString={destinationConnectionString} setDestinationConnectionString={setDestinationConnectionString}
              destinationProfileId={destinationProfileId} setDestinationProfileId={setDestinationProfileId}
              destinationTable={destinationTable}       setDestinationTable={setDestinationTable}
              destinationSchema={destinationSchema}     setDestinationSchema={setDestinationSchema}
              destWebhookAuth={destWebhookAuth}         setDestWebhookAuth={setDestWebhookAuth}
              fieldMappings={fieldMappings}             setFieldMappings={setFieldMappings}
              transformations={transformations}         setTransformations={setTransformations}
              handleAutomap={handleAutomap}
              handleDryRun={handleDryRun}
            />
          </div>
        )}
      </div>

      {/* Schedule, Batching & Incremental strip */}
      <LoomSettings
        scheduleEnabled={scheduleEnabled}         setScheduleEnabled={setScheduleEnabled}
        cronExpression={cronExpression}           setCronExpression={setCronExpression}
        batchEnabled={batchEnabled}               setBatchEnabled={setBatchEnabled}
        batchSize={batchSize}                     setBatchSize={setBatchSize}
        batchDelaySeconds={batchDelaySeconds}     setBatchDelaySeconds={setBatchDelaySeconds}
        incrementalEnabled={incrementalEnabled}   setIncrementalEnabled={setIncrementalEnabled}
        incrementalField={incrementalField}       setIncrementalField={setIncrementalField}
        lastIncrementalValue={pipeline.lastIncrementalValue}
      />

      <DryRunResultModal open={dryRunOpen} onClose={() => setDryRunOpen(false)} dryRunResult={dryRunResult} />
    </div>
  );
};

export default LoomEditor;
