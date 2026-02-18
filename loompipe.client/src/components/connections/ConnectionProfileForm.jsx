import React, { useState } from 'react';
import {
  Stack, TextField, FormControl, InputLabel, Select, MenuItem,
  InputAdornment, IconButton, Typography, Divider
} from '@mui/material';
import { Visibility, VisibilityOff } from '@mui/icons-material';

// Providers that use host/port/db/user/password fields
const RELATIONAL = ['sqlserver', 'postgresql', 'mysql', 'oracle'];

const PROVIDERS = [
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

const DEFAULT_PORTS = {
  sqlserver: 1433, postgresql: 5432, mysql: 3306, oracle: 1521,
  mongodb: 27017, neo4j: 7687, snowflake: 443, milvus: 19530,
};

/**
 * Dynamic connection profile form.
 * Fields adapt based on the selected provider.
 *
 * Props:
 *   values       - current form values object
 *   onChange(field, value) - change handler
 */
const ConnectionProfileForm = ({ values = {}, onChange }) => {
  const [showPassword, setShowPassword] = useState(false);
  const [showApiKey,   setShowApiKey]   = useState(false);
  const [showSa,       setShowSa]       = useState(false);

  const provider = values.provider || '';
  const isRelational = RELATIONAL.includes(provider);
  const isMongo      = provider === 'mongodb';
  const isNeo4j      = provider === 'neo4j';
  const isSnowflake  = provider === 'snowflake';
  const isBigQuery   = provider === 'bigquery';
  const isPinecone   = provider === 'pinecone';
  const isMilvus     = provider === 'milvus';

  const extra = (() => {
    try { return JSON.parse(values.additionalConfig || '{}'); } catch { return {}; }
  })();

  const setExtra = (key, val) => {
    const next = { ...extra, [key]: val };
    onChange('additionalConfig', JSON.stringify(next));
  };

  const handle = (field) => (e) => onChange(field, e.target.value);

  const PasswordField = ({ field, label, show, setShow }) => (
    <TextField
      label={label}
      type={show ? 'text' : 'password'}
      value={values[field] || ''}
      onChange={handle(field)}
      fullWidth
      autoComplete="new-password"
      InputProps={{
        endAdornment: (
          <InputAdornment position="end">
            <IconButton size="small" onClick={() => setShow(s => !s)} edge="end">
              {show ? <VisibilityOff fontSize="small" /> : <Visibility fontSize="small" />}
            </IconButton>
          </InputAdornment>
        )
      }}
    />
  );

  return (
    <Stack spacing={2}>
      {/* Common fields */}
      <TextField label="Profile Name *" value={values.name || ''} onChange={handle('name')} fullWidth />

      <FormControl fullWidth>
        <InputLabel>Provider *</InputLabel>
        <Select
          value={provider}
          onChange={(e) => {
            onChange('provider', e.target.value);
            onChange('port', DEFAULT_PORTS[e.target.value] ?? '');
          }}
          label="Provider *"
        >
          {PROVIDERS.map(p => (
            <MenuItem key={p.value} value={p.value}>{p.label}</MenuItem>
          ))}
        </Select>
      </FormControl>

      {/* Relational: SQL Server / PostgreSQL / MySQL / Oracle */}
      {isRelational && (
        <>
          <Typography variant="caption" color="text.secondary">Connection Details</Typography>
          <Divider />
          <Stack direction="row" spacing={2}>
            <TextField label="Host *" value={values.host || ''} onChange={handle('host')} fullWidth />
            <TextField label="Port *" type="number" value={values.port || ''} onChange={handle('port')} sx={{ width: 120 }} />
          </Stack>
          <TextField label="Database *" value={values.databaseName || ''} onChange={handle('databaseName')} fullWidth />
          <TextField label="Username *" value={values.username || ''} onChange={handle('username')} fullWidth autoComplete="username" />
          <PasswordField field="password" label="Password *" show={showPassword} setShow={setShowPassword} />
        </>
      )}

      {/* MongoDB */}
      {isMongo && (
        <>
          <Typography variant="caption" color="text.secondary">Connection Details</Typography>
          <Divider />
          <Stack direction="row" spacing={2}>
            <TextField label="Host *" value={values.host || ''} onChange={handle('host')} fullWidth />
            <TextField label="Port" type="number" value={values.port || 27017} onChange={handle('port')} sx={{ width: 120 }} />
          </Stack>
          <TextField label="Database *" value={values.databaseName || ''} onChange={handle('databaseName')} fullWidth />
          <TextField label="Username" value={values.username || ''} onChange={handle('username')} fullWidth autoComplete="username" />
          <PasswordField field="password" label="Password" show={showPassword} setShow={setShowPassword} />
        </>
      )}

      {/* Neo4j */}
      {isNeo4j && (
        <>
          <Typography variant="caption" color="text.secondary">Connection Details</Typography>
          <Divider />
          <Stack direction="row" spacing={2}>
            <TextField label="Host *" value={values.host || ''} onChange={handle('host')} fullWidth />
            <TextField label="Bolt Port" type="number" value={values.port || 7687} onChange={handle('port')} sx={{ width: 140 }} />
          </Stack>
          <TextField label="Username *" value={values.username || ''} onChange={handle('username')} fullWidth autoComplete="username" />
          <PasswordField field="password" label="Password *" show={showPassword} setShow={setShowPassword} />
        </>
      )}

      {/* Snowflake */}
      {isSnowflake && (
        <>
          <Typography variant="caption" color="text.secondary">Connection Details</Typography>
          <Divider />
          <TextField
            label="Account Identifier *"
            placeholder="orgname-accountname"
            value={extra.account || ''}
            onChange={(e) => setExtra('account', e.target.value)}
            fullWidth
          />
          <Stack direction="row" spacing={2}>
            <TextField label="Warehouse *" value={extra.warehouse || ''} onChange={(e) => setExtra('warehouse', e.target.value)} fullWidth />
            <TextField label="Database *" value={values.databaseName || ''} onChange={handle('databaseName')} fullWidth />
          </Stack>
          <TextField label="Schema" placeholder="PUBLIC" value={extra.schema || ''} onChange={(e) => setExtra('schema', e.target.value)} fullWidth />
          <TextField label="Username *" value={values.username || ''} onChange={handle('username')} fullWidth autoComplete="username" />
          <PasswordField field="password" label="Password *" show={showPassword} setShow={setShowPassword} />
        </>
      )}

      {/* BigQuery */}
      {isBigQuery && (
        <>
          <Typography variant="caption" color="text.secondary">Connection Details</Typography>
          <Divider />
          <Stack direction="row" spacing={2}>
            <TextField label="Project ID *" value={extra.projectId || ''} onChange={(e) => setExtra('projectId', e.target.value)} fullWidth />
            <TextField label="Dataset *" value={extra.dataset || ''} onChange={(e) => setExtra('dataset', e.target.value)} fullWidth />
          </Stack>
          <PasswordField field="serviceAccountJson" label="Service Account JSON *" show={showSa} setShow={setShowSa} />
          {!showSa && (
            <Typography variant="caption" color="text.secondary">
              Paste your service account JSON key — it will be encrypted at rest.
            </Typography>
          )}
        </>
      )}

      {/* Pinecone */}
      {isPinecone && (
        <>
          <Typography variant="caption" color="text.secondary">Connection Details</Typography>
          <Divider />
          <PasswordField field="apiKey" label="API Key *" show={showApiKey} setShow={setShowApiKey} />
          <Stack direction="row" spacing={2}>
            <TextField label="Index Name *" value={extra.indexName || ''} onChange={(e) => setExtra('indexName', e.target.value)} fullWidth />
            <TextField label="Environment" placeholder="us-east1-gcp" value={extra.environment || ''} onChange={(e) => setExtra('environment', e.target.value)} fullWidth />
          </Stack>
        </>
      )}

      {/* Milvus */}
      {isMilvus && (
        <>
          <Typography variant="caption" color="text.secondary">Connection Details</Typography>
          <Divider />
          <Stack direction="row" spacing={2}>
            <TextField label="Host *" value={values.host || ''} onChange={handle('host')} fullWidth />
            <TextField label="Port" type="number" value={values.port || 19530} onChange={handle('port')} sx={{ width: 120 }} />
          </Stack>
          <TextField label="Collection *" value={extra.collection || ''} onChange={(e) => setExtra('collection', e.target.value)} fullWidth />
          <TextField label="Username" value={values.username || ''} onChange={handle('username')} fullWidth autoComplete="username" />
          <PasswordField field="password" label="Password" show={showPassword} setShow={setShowPassword} />
        </>
      )}
    </Stack>
  );
};

export default ConnectionProfileForm;
