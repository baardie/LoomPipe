import React, { useState, useEffect } from 'react';
import {
    Box, CircularProgress, Container, Paper, Table, TableBody,
    TableCell, TableHead, TableRow, Typography,
} from '@mui/material';
import { useAuth } from '../contexts/AuthContext';

const AnalyticsPage = () => {
    const { authFetch } = useAuth();
    const [summary, setSummary] = useState(null);
    const [byDay, setByDay] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        Promise.all([
            authFetch('/api/analytics/summary').then((r) => r.ok ? r.json() : null),
            authFetch('/api/analytics/runs-by-day?days=7').then((r) => r.ok ? r.json() : []),
        ]).then(([s, d]) => {
            setSummary(s);
            setByDay(d);
            setLoading(false);
        });
    }, [authFetch]);

    if (loading) {
        return (
            <Box sx={{ display: 'flex', justifyContent: 'center', mt: 8 }}>
                <CircularProgress />
            </Box>
        );
    }

    const cards = summary ? [
        { label: 'Total Pipelines', value: summary.totalPipelines },
        { label: 'Total Runs',      value: summary.totalRuns },
        { label: 'Success Rate',    value: `${summary.successRate}%` },
        { label: 'Last 24h',        value: summary.runsLast24h },
        { label: 'Last 7 Days',     value: summary.runsLast7d },
    ] : [];

    return (
        <Container sx={{ my: 4 }}>
            <Typography variant="h4" sx={{ mb: 3 }}>Analytics</Typography>

            <Box sx={{ display: 'flex', gap: 2, mb: 4, flexWrap: 'wrap' }}>
                {cards.map((c) => (
                    <Paper key={c.label} sx={{ p: 2, minWidth: 140, textAlign: 'center' }} elevation={2}>
                        <Typography variant="h5" fontWeight={700}>{c.value}</Typography>
                        <Typography variant="caption" color="text.secondary">{c.label}</Typography>
                    </Paper>
                ))}
            </Box>

            <Typography variant="h6" sx={{ mb: 1 }}>Runs by Day (Last 7 Days)</Typography>
            <Paper elevation={1}>
                <Table size="small">
                    <TableHead>
                        <TableRow>
                            <TableCell>Date</TableCell>
                            <TableCell align="right">Total Runs</TableCell>
                            <TableCell align="right">Successful</TableCell>
                            <TableCell align="right">Failed</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {byDay.map((d) => (
                            <TableRow key={d.date}>
                                <TableCell>{d.date}</TableCell>
                                <TableCell align="right">{d.runCount}</TableCell>
                                <TableCell align="right">{d.successCount}</TableCell>
                                <TableCell align="right">{d.runCount - d.successCount}</TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </Paper>
        </Container>
    );
};

export default AnalyticsPage;
