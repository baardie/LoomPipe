import React, { useState, useEffect, useCallback } from 'react';
import {
    Box, Button, Chip, CircularProgress, Container, Paper,
    Table, TableBody, TableCell, TableHead, TableRow,
    Tooltip, Typography,
} from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import { useAuth } from '../contexts/AuthContext';
import RoleGuard from '../components/auth/RoleGuard';

const statusColor = (s) => s === 'Success' ? 'success' : s === 'Failed' ? 'error' : 'warning';

const fmtDuration = (ms) => {
    if (ms == null) return '–';
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(1)}s`;
};

const fmtDate = (iso) => iso ? new Date(iso).toLocaleString() : '–';

const PipelineDetailPage = ({ pipelineId, onBack, onEdit }) => {
    const { authFetch, isUser } = useAuth();
    const [pipeline, setPipeline] = useState(null);
    const [runs, setRuns] = useState([]);
    const [stats, setStats] = useState(null);
    const [running, setRunning] = useState(false);

    const load = useCallback(async () => {
        const [pRes, rRes, sRes] = await Promise.all([
            authFetch(`/api/pipelines/${pipelineId}`),
            authFetch(`/api/pipelines/${pipelineId}/runs?limit=10`),
            authFetch(`/api/pipelines/${pipelineId}/stats`),
        ]);
        if (pRes.ok) setPipeline(await pRes.json());
        if (rRes.ok) setRuns(await rRes.json());
        if (sRes.ok) setStats(await sRes.json());
    }, [pipelineId, authFetch]);

    useEffect(() => { load(); }, [load]);

    const handleRun = async () => {
        setRunning(true);
        try {
            await authFetch(`/api/pipelines/${pipelineId}/run`, { method: 'POST' });
            await load();
        } finally {
            setRunning(false);
        }
    };

    if (!pipeline) {
        return (
            <Box sx={{ display: 'flex', justifyContent: 'center', mt: 8 }}>
                <CircularProgress />
            </Box>
        );
    }

    return (
        <Container sx={{ my: 4 }}>
            <Button startIcon={<ArrowBackIcon />} onClick={onBack} sx={{ mb: 2 }}>
                Back to Pipelines
            </Button>

            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
                <Typography variant="h4">{pipeline.name}</Typography>
                <Box sx={{ display: 'flex', gap: 1 }}>
                    {isUser && (
                        <Button variant="contained" onClick={handleRun} disabled={running}>
                            {running ? <CircularProgress size={20} color="inherit" /> : 'Run Now'}
                        </Button>
                    )}
                    <RoleGuard roles={['Admin']}>
                        <Button variant="outlined" onClick={() => onEdit(pipeline)}>Edit</Button>
                    </RoleGuard>
                </Box>
            </Box>

            {/* Stat cards */}
            {stats && (
                <Box sx={{ display: 'flex', gap: 2, mb: 4, flexWrap: 'wrap' }}>
                    {[
                        { label: 'Created',     value: pipeline.createdAt ? new Date(pipeline.createdAt).toLocaleDateString() : '–' },
                        { label: 'Total Runs',  value: stats.totalRuns },
                        { label: 'Successful',  value: stats.successCount },
                        { label: 'Failed',      value: stats.failCount },
                        { label: 'Avg Duration', value: fmtDuration(stats.avgDurationMs ? Math.round(stats.avgDurationMs) : null) },
                        { label: 'Last Run',    value: fmtDate(stats.lastRunAt) },
                    ].map((c) => (
                        <Paper key={c.label} sx={{ p: 2, minWidth: 140, textAlign: 'center' }} elevation={2}>
                            <Typography variant="h5" fontWeight={700}>{c.value ?? '–'}</Typography>
                            <Typography variant="caption" color="text.secondary">{c.label}</Typography>
                        </Paper>
                    ))}
                </Box>
            )}

            {/* Run log table */}
            <Typography variant="h6" sx={{ mb: 1 }}>Run History</Typography>
            <Paper elevation={1}>
                <Table size="small">
                    <TableHead>
                        <TableRow>
                            <TableCell>Started</TableCell>
                            <TableCell>Duration</TableCell>
                            <TableCell>Rows</TableCell>
                            <TableCell>Status</TableCell>
                            <TableCell>Triggered By</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {runs.length === 0 && (
                            <TableRow>
                                <TableCell colSpan={5} align="center">No runs yet</TableCell>
                            </TableRow>
                        )}
                        {runs.map((r) => (
                            <TableRow key={r.id}>
                                <TableCell>{fmtDate(r.startedAt)}</TableCell>
                                <TableCell>{fmtDuration(r.durationMs)}</TableCell>
                                <TableCell>{r.rowsProcessed}</TableCell>
                                <TableCell>
                                    <Tooltip title={r.errorMessage || ''} disableHoverListener={!r.errorMessage}>
                                        <Chip label={r.status} color={statusColor(r.status)} size="small" />
                                    </Tooltip>
                                </TableCell>
                                <TableCell>{r.triggeredBy}</TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </Paper>
        </Container>
    );
};

export default PipelineDetailPage;
