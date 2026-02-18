import React from 'react';
import { Grid, Typography, FormControl, InputLabel, Select, MenuItem, TextField, Stack } from '@mui/material';
import ConnectionProfileSelect from '../connections/ConnectionProfileSelect';

// Providers that use saved connection profiles (not a raw connection string)
const DB_PROVIDERS = [
  'sqlserver', 'postgresql', 'mysql', 'oracle',
  'mongodb', 'neo4j', 'snowflake', 'bigquery', 'pinecone', 'milvus'
];

const SOURCE_TYPES = [
  { value: 'csv',        label: 'CSV File' },
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

const TABLE_LABEL = {
    mongodb: 'Collection',
    neo4j:   'Node Label',
    pinecone: 'Index Name',
    milvus:  'Collection',
};

const getTableLabel = (type) => TABLE_LABEL[type] || 'Table / View';

const SourceDestinationForm = ({
    sourceType,
    setSourceType,
    sourceConnectionString,
    setSourceConnectionString,
    sourceProfileId,
    setSourceProfileId,
    sourceTable,
    setSourceTable,
    destinationType,
    setDestinationType,
    destinationConnectionString,
    setDestinationConnectionString,
    destinationProfileId,
    setDestinationProfileId,
    destinationTable,
    setDestinationTable,
    destinationSchema,
    setDestinationSchema
}) => {
    const sourceIsDb = DB_PROVIDERS.includes(sourceType);
    const destIsDb   = DB_PROVIDERS.includes(destinationType);

    return (
        <Grid container spacing={3}>
            {/* ── Source ── */}
            <Grid item xs={6}>
                <Stack spacing={2}>
                    <Typography variant="subtitle1" fontWeight={600} gutterBottom>Source</Typography>

                    <FormControl fullWidth>
                        <InputLabel>Type</InputLabel>
                        <Select value={sourceType} onChange={(e) => setSourceType(e.target.value)} label="Type">
                            {SOURCE_TYPES.map(t => (
                                <MenuItem key={t.value} value={t.value}>{t.label}</MenuItem>
                            ))}
                        </Select>
                    </FormControl>

                    {sourceIsDb ? (
                        <>
                            <ConnectionProfileSelect
                                label="Connection Profile"
                                profileId={sourceProfileId}
                                onProfileChange={setSourceProfileId}
                                filterProvider={sourceType}
                            />
                            <TextField
                                label={getTableLabel(sourceType)}
                                value={sourceTable}
                                onChange={(e) => setSourceTable(e.target.value)}
                                fullWidth
                                placeholder={sourceType === 'neo4j' ? 'Person' : 'my_table'}
                            />
                        </>
                    ) : (
                        <TextField
                            label={sourceType === 'csv' ? 'File Path' : 'URL'}
                            value={sourceConnectionString}
                            onChange={(e) => setSourceConnectionString(e.target.value)}
                            fullWidth
                            placeholder={sourceType === 'csv' ? '/data/myfile.csv' : 'https://api.example.com/data'}
                        />
                    )}
                </Stack>
            </Grid>

            {/* ── Destination ── */}
            <Grid item xs={6}>
                <Stack spacing={2}>
                    <Typography variant="subtitle1" fontWeight={600} gutterBottom>Destination</Typography>

                    <FormControl fullWidth>
                        <InputLabel>Type</InputLabel>
                        <Select value={destinationType} onChange={(e) => setDestinationType(e.target.value)} label="Type">
                            {DESTINATION_TYPES.map(t => (
                                <MenuItem key={t.value} value={t.value}>{t.label}</MenuItem>
                            ))}
                        </Select>
                    </FormControl>

                    {destIsDb ? (
                        <>
                            <ConnectionProfileSelect
                                label="Connection Profile"
                                profileId={destinationProfileId}
                                onProfileChange={setDestinationProfileId}
                                filterProvider={destinationType}
                            />
                            <TextField
                                label={getTableLabel(destinationType)}
                                value={destinationTable}
                                onChange={(e) => setDestinationTable(e.target.value)}
                                fullWidth
                                placeholder={destinationType === 'neo4j' ? 'Person' : 'my_table'}
                            />
                        </>
                    ) : (
                        <TextField
                            label={destinationType === 'webhook' ? 'Webhook URL' : 'Connection String'}
                            value={destinationConnectionString}
                            onChange={(e) => setDestinationConnectionString(e.target.value)}
                            fullWidth
                            placeholder={destinationType === 'webhook' ? 'https://hooks.example.com/...' : ''}
                        />
                    )}

                    <TextField
                        label="Schema (comma-separated field names)"
                        value={destinationSchema}
                        onChange={(e) => setDestinationSchema(e.target.value)}
                        fullWidth
                        placeholder="id, name, email, created_at"
                        helperText="Target field names used for field mapping"
                    />
                </Stack>
            </Grid>
        </Grid>
    );
};

export default SourceDestinationForm;
