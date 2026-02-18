import React, { useState } from 'react';
import {
    Card, CardContent, Divider, FormControlLabel, Paper,
    Stack, Switch, TextField, Typography,
} from '@mui/material';
import SourceDestinationForm from './pipeline/SourceDestinationForm';
import FieldMappingForm from './pipeline/FieldMappingForm';
import TransformationForm from './pipeline/TransformationForm';
import ActionButtons from './pipeline/ActionButtons';
import DryRunResultModal from './pipeline/DryRunResultModal';
import RoleGuard from './auth/RoleGuard';
import { useAuth } from '../contexts/AuthContext';

const DB_PROVIDERS = ['sqlserver', 'postgresql', 'mysql', 'oracle', 'mongodb', 'neo4j', 'snowflake', 'bigquery', 'pinecone', 'milvus'];

const PipelineForm = ({ onSave, onCancel, pipeline = {} }) => {
    const { authFetch } = useAuth();
    const [name, setName] = useState(pipeline.name || '');
    const [sourceType, setSourceType] = useState(pipeline.source?.type || '');
    const [sourceConnectionString, setSourceConnectionString] = useState(pipeline.source?.connectionString || '');
    const [sourceProfileId, setSourceProfileId] = useState(pipeline.source?.parameters?.connectionProfileId ?? null);
    const [sourceTable, setSourceTable] = useState(pipeline.source?.parameters?.table || '');
    const [destinationType, setDestinationType] = useState(pipeline.destination?.type || '');
    const [destinationConnectionString, setDestinationConnectionString] = useState(pipeline.destination?.connectionString || '');
    const [destinationProfileId, setDestinationProfileId] = useState(pipeline.destination?.parameters?.connectionProfileId ?? null);
    const [destinationTable, setDestinationTable] = useState(pipeline.destination?.parameters?.table || '');
    const [destinationSchema, setDestinationSchema] = useState(pipeline.destination?.schema || '');
    const [fieldMappings, setFieldMappings] = useState(pipeline.fieldMappings || []);
    const [transformations, setTransformations] = useState(pipeline.transformations || []);
    const [dryRunResult, setDryRunResult] = useState(null);
    const [isDryRunModalOpen, setIsDryRunModalOpen] = useState(false);

    // Scheduling fields
    const [scheduleEnabled, setScheduleEnabled] = useState(pipeline.scheduleEnabled || false);
    const [scheduleIntervalMinutes, setScheduleIntervalMinutes] = useState(pipeline.scheduleIntervalMinutes ?? '');

    // Batch writing fields
    const [batchEnabled, setBatchEnabled] = useState(!!(pipeline.batchSize));
    const [batchSize, setBatchSize] = useState(pipeline.batchSize ?? '');
    const [batchDelaySeconds, setBatchDelaySeconds] = useState(pipeline.batchDelaySeconds ?? '');

    const buildSourceConfig = () => {
        if (DB_PROVIDERS.includes(sourceType)) {
            return { type: sourceType, parameters: { connectionProfileId: sourceProfileId, table: sourceTable } };
        }
        return { type: sourceType, connectionString: sourceConnectionString };
    };

    const buildDestConfig = () => {
        if (DB_PROVIDERS.includes(destinationType)) {
            return { type: destinationType, schema: destinationSchema, parameters: { connectionProfileId: destinationProfileId, table: destinationTable } };
        }
        return { type: destinationType, connectionString: destinationConnectionString, schema: destinationSchema };
    };

    const buildPayload = () => ({
        ...pipeline,
        name,
        source: buildSourceConfig(),
        destination: buildDestConfig(),
        fieldMappings,
        transformations,
        scheduleEnabled,
        scheduleIntervalMinutes: scheduleEnabled && scheduleIntervalMinutes ? Number(scheduleIntervalMinutes) : null,
        nextRunAt: scheduleEnabled && scheduleIntervalMinutes && !pipeline.nextRunAt
            ? new Date(Date.now() + Number(scheduleIntervalMinutes) * 60000).toISOString()
            : pipeline.nextRunAt ?? null,
        batchSize: batchEnabled && batchSize ? Number(batchSize) : null,
        batchDelaySeconds: batchEnabled && batchDelaySeconds ? Number(batchDelaySeconds) : null,
    });

    const handleSave = () => onSave(buildPayload());

    const handleRun = async () => {
        await authFetch(`/api/pipelines/${pipeline.id}/run`, { method: 'POST' });
    };

    const handleAutomap = async () => {
        const response = await authFetch('/api/pipelines/automap', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                source: buildSourceConfig(),
                destination: { schema: destinationSchema },
            }),
        });
        const data = await response.json();
        setFieldMappings(data);
    };

    const handleDryRun = async () => {
        const response = await authFetch('/api/pipelines/dryrun', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name,
                source: buildSourceConfig(),
                destination: buildDestConfig(),
                fieldMappings,
                transformations,
            }),
        });
        const data = await response.json();
        setDryRunResult(data);
        setIsDryRunModalOpen(true);
    };

    return (
        <Paper sx={{ p: 2, mt: 4 }}>
            <Typography variant="h6" gutterBottom>
                {pipeline.id ? 'Edit Pipeline' : 'Create Pipeline'}
            </Typography>
            <Stack spacing={2}>
                <TextField
                    label="Pipeline Name"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    fullWidth
                    margin="normal"
                />

                <Card>
                    <CardContent>
                        <Typography variant="h5" component="div" gutterBottom>
                            Source & Destination
                        </Typography>
                        <Divider sx={{ mb: 2 }} />
                        <SourceDestinationForm
                            sourceType={sourceType}
                            setSourceType={setSourceType}
                            sourceConnectionString={sourceConnectionString}
                            setSourceConnectionString={setSourceConnectionString}
                            sourceProfileId={sourceProfileId}
                            setSourceProfileId={setSourceProfileId}
                            sourceTable={sourceTable}
                            setSourceTable={setSourceTable}
                            destinationType={destinationType}
                            setDestinationType={setDestinationType}
                            destinationConnectionString={destinationConnectionString}
                            setDestinationConnectionString={setDestinationConnectionString}
                            destinationProfileId={destinationProfileId}
                            setDestinationProfileId={setDestinationProfileId}
                            destinationTable={destinationTable}
                            setDestinationTable={setDestinationTable}
                            destinationSchema={destinationSchema}
                            setDestinationSchema={setDestinationSchema}
                        />
                    </CardContent>
                </Card>

                <Card>
                    <CardContent>
                        <Typography variant="h5" component="div" gutterBottom>
                            Field Mappings
                        </Typography>
                        <Divider sx={{ mb: 2 }} />
                        <FieldMappingForm
                            fieldMappings={fieldMappings}
                            setFieldMappings={setFieldMappings}
                            handleAutomap={handleAutomap}
                            handleDryRun={handleDryRun}
                            sourceType={sourceType}
                            sourceConnectionString={sourceConnectionString}
                            destinationSchema={destinationSchema}
                        />
                    </CardContent>
                </Card>

                <Card>
                    <CardContent>
                        <Typography variant="h5" component="div" gutterBottom>
                            Transformations
                        </Typography>
                        <Divider sx={{ mb: 2 }} />
                        <TransformationForm
                            transformations={transformations}
                            setTransformations={setTransformations}
                        />
                    </CardContent>
                </Card>

                <RoleGuard roles={['Admin']}>
                    <Card>
                        <CardContent>
                            <Typography variant="h5" component="div" gutterBottom>
                                Schedule & Batching
                            </Typography>
                            <Divider sx={{ mb: 2 }} />
                            <Stack spacing={2}>
                                <FormControlLabel
                                    control={<Switch checked={scheduleEnabled} onChange={(e) => setScheduleEnabled(e.target.checked)} />}
                                    label="Enable Schedule"
                                />
                                {scheduleEnabled && (
                                    <TextField
                                        label="Run Every (minutes)"
                                        type="number"
                                        value={scheduleIntervalMinutes}
                                        onChange={(e) => setScheduleIntervalMinutes(e.target.value)}
                                        inputProps={{ min: 1 }}
                                        sx={{ maxWidth: 220 }}
                                    />
                                )}
                                <Divider />
                                <FormControlLabel
                                    control={<Switch checked={batchEnabled} onChange={(e) => setBatchEnabled(e.target.checked)} />}
                                    label="Enable Batch Writing"
                                />
                                {batchEnabled && (
                                    <Stack direction="row" spacing={2}>
                                        <TextField
                                            label="Batch Size (rows)"
                                            type="number"
                                            value={batchSize}
                                            onChange={(e) => setBatchSize(e.target.value)}
                                            inputProps={{ min: 1 }}
                                            sx={{ maxWidth: 200 }}
                                        />
                                        <TextField
                                            label="Delay Between Batches (seconds)"
                                            type="number"
                                            value={batchDelaySeconds}
                                            onChange={(e) => setBatchDelaySeconds(e.target.value)}
                                            inputProps={{ min: 0 }}
                                            sx={{ maxWidth: 280 }}
                                        />
                                    </Stack>
                                )}
                            </Stack>
                        </CardContent>
                    </Card>
                </RoleGuard>

                <ActionButtons
                    onCancel={onCancel}
                    onSave={handleSave}
                    onRun={handleRun}
                    pipelineId={pipeline.id}
                />
            </Stack>
            <DryRunResultModal
                open={isDryRunModalOpen}
                onClose={() => setIsDryRunModalOpen(false)}
                dryRunResult={dryRunResult}
            />
        </Paper>
    );
};

export default PipelineForm;
