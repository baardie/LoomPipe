import React, { useState, useEffect, useCallback } from 'react';
import {
    Container, Typography, Button, Grid, Card, CardContent, CardActions,
    Chip, IconButton, Tooltip, Snackbar, Alert, Box, Stack, Skeleton
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import HelpOutlineIcon from '@mui/icons-material/HelpOutline';
import StorageIcon from '@mui/icons-material/Storage';
import CloudIcon from '@mui/icons-material/Cloud';
import HubIcon from '@mui/icons-material/Hub';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import AcUnitIcon from '@mui/icons-material/AcUnit';
import BlurOnIcon from '@mui/icons-material/BlurOn';
import ConnectionProfileDialog from '../components/connections/ConnectionProfileDialog';
import RoleGuard from '../components/auth/RoleGuard';
import { useAuth } from '../contexts/AuthContext';

const PROVIDER_META = {
    sqlserver:  { label: 'SQL Server',   icon: <StorageIcon />,     color: '#c8373a' },
    postgresql: { label: 'PostgreSQL',   icon: <StorageIcon />,     color: '#336791' },
    mysql:      { label: 'MySQL',        icon: <StorageIcon />,     color: '#00758f' },
    oracle:     { label: 'Oracle',       icon: <StorageIcon />,     color: '#f00000' },
    mongodb:    { label: 'MongoDB',      icon: <HubIcon />,         color: '#4caf50' },
    neo4j:      { label: 'Neo4j',        icon: <AccountTreeIcon />, color: '#008cc1' },
    snowflake:  { label: 'Snowflake',    icon: <AcUnitIcon />,      color: '#29b5e8' },
    bigquery:   { label: 'BigQuery',     icon: <CloudIcon />,       color: '#4285f4' },
    pinecone:   { label: 'Pinecone',     icon: <BlurOnIcon />,      color: '#5a57fb' },
    milvus:     { label: 'Milvus',       icon: <BlurOnIcon />,      color: '#00a1ea' },
};

const TestBadge = ({ profile }) => {
    if (!profile.lastTestedAt)
        return <Chip icon={<HelpOutlineIcon />} label="Not tested" size="small" variant="outlined" />;
    return profile.lastTestSucceeded
        ? <Chip icon={<CheckCircleIcon />} label="Connected" size="small" color="success" variant="outlined" />
        : <Chip icon={<ErrorIcon />}       label="Failed"    size="small" color="error"   variant="outlined" />;
};

const ConnectionsPage = ({ onProfileClick }) => {
    const { authFetch } = useAuth();
    const [profiles,    setProfiles]    = useState([]);
    const [loading,     setLoading]     = useState(true);
    const [dialogOpen,  setDialogOpen]  = useState(false);
    const [editId,      setEditId]      = useState(null);
    const [testing,     setTesting]     = useState(null);
    const [snack,       setSnack]       = useState({ open: false, message: '', severity: 'success' });

    const fetchProfiles = useCallback(async () => {
        setLoading(true);
        try {
            const resp = await authFetch('/api/connections');
            if (resp.ok) setProfiles(await resp.json());
        } finally {
            setLoading(false);
        }
    }, [authFetch]);

    useEffect(() => { fetchProfiles(); }, [fetchProfiles]);

    const handleDelete = async (id) => {
        if (!window.confirm('Delete this connection profile?')) return;
        await authFetch(`/api/connections/${id}`, { method: 'DELETE' });
        setSnack({ open: true, message: 'Profile deleted.', severity: 'info' });
        fetchProfiles();
    };

    const handleTest = async (id) => {
        setTesting(id);
        try {
            const resp = await authFetch(`/api/connections/${id}/test`, { method: 'POST' });
            const result = await resp.json();
            setSnack({
                open: true,
                message: result.success
                    ? `Connection OK (${result.elapsedMs}ms)`
                    : `Connection failed: ${result.errorMessage}`,
                severity: result.success ? 'success' : 'error',
            });
            fetchProfiles();
        } finally {
            setTesting(null);
        }
    };

    const handleSaved = () => {
        setDialogOpen(false);
        setEditId(null);
        fetchProfiles();
    };

    const openCreate = () => { setEditId(null); setDialogOpen(true); };
    const openEdit   = (id) => { setEditId(id);   setDialogOpen(true); };

    return (
        <Container maxWidth="lg" sx={{ mt: 4, mb: 8 }}>
            <Stack direction="row" justifyContent="space-between" alignItems="center" mb={3}>
                <Typography variant="h4" fontWeight={600}>Connection Profiles</Typography>
                <RoleGuard roles={['Admin']}>
                    <Button variant="contained" startIcon={<AddIcon />} onClick={openCreate}>
                        New Connection
                    </Button>
                </RoleGuard>
            </Stack>

            <Typography variant="body2" color="text.secondary" mb={3}>
                Credentials are encrypted at rest with AES-256-CBC (ASP.NET Core Data Protection).
                Plaintext secrets are never stored.
            </Typography>

            {loading ? (
                <Grid container spacing={2}>
                    {[1, 2, 3].map(i => (
                        <Grid item xs={12} sm={6} md={4} key={i}>
                            <Skeleton variant="rectangular" height={180} sx={{ borderRadius: 2 }} />
                        </Grid>
                    ))}
                </Grid>
            ) : profiles.length === 0 ? (
                <Box sx={{ textAlign: 'center', py: 8, color: 'text.secondary' }}>
                    <StorageIcon sx={{ fontSize: 64, mb: 2, opacity: 0.3 }} />
                    <Typography variant="h6">No connection profiles yet</Typography>
                    <Typography variant="body2" mt={1}>Click "New Connection" to add your first database.</Typography>
                </Box>
            ) : (
                <Grid container spacing={2}>
                    {profiles.map(p => {
                        const meta = PROVIDER_META[p.provider] || { label: p.provider, icon: <StorageIcon />, color: '#888' };
                        return (
                            <Grid item xs={12} sm={6} md={4} key={p.id}>
                                <Card
                                    variant="outlined"
                                    sx={{ height: '100%', display: 'flex', flexDirection: 'column', cursor: 'pointer' }}
                                    onClick={() => onProfileClick && onProfileClick(p.id)}
                                >
                                    <CardContent sx={{ flexGrow: 1 }}>
                                        <Stack direction="row" spacing={1.5} alignItems="flex-start" mb={1}>
                                            <Box sx={{ color: meta.color, mt: 0.3 }}>{meta.icon}</Box>
                                            <Box>
                                                <Typography variant="subtitle1" fontWeight={600} lineHeight={1.2}>{p.name}</Typography>
                                                <Typography variant="caption" color="text.secondary">{meta.label}</Typography>
                                            </Box>
                                        </Stack>

                                        {p.host && (
                                            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                                                {p.host}{p.port ? `:${p.port}` : ''}{p.databaseName ? ` / ${p.databaseName}` : ''}
                                            </Typography>
                                        )}
                                        {p.username && (
                                            <Typography variant="body2" color="text.secondary">User: {p.username}</Typography>
                                        )}

                                        <Box mt={1.5}>
                                            <TestBadge profile={p} />
                                        </Box>
                                    </CardContent>

                                    <CardActions sx={{ px: 2, pb: 1.5, justifyContent: 'flex-end' }} onClick={(e) => e.stopPropagation()}>
                                        <Tooltip title="Test connection">
                                            <span>
                                                <IconButton
                                                    size="small"
                                                    onClick={() => handleTest(p.id)}
                                                    disabled={testing === p.id}
                                                    color="success"
                                                >
                                                    <PlayArrowIcon fontSize="small" />
                                                </IconButton>
                                            </span>
                                        </Tooltip>
                                        <RoleGuard roles={['Admin']}>
                                            <Tooltip title="Edit">
                                                <IconButton size="small" onClick={() => openEdit(p.id)} color="primary">
                                                    <EditIcon fontSize="small" />
                                                </IconButton>
                                            </Tooltip>
                                            <Tooltip title="Delete">
                                                <IconButton size="small" onClick={() => handleDelete(p.id)} color="error">
                                                    <DeleteIcon fontSize="small" />
                                                </IconButton>
                                            </Tooltip>
                                        </RoleGuard>
                                    </CardActions>
                                </Card>
                            </Grid>
                        );
                    })}
                </Grid>
            )}

            <ConnectionProfileDialog
                open={dialogOpen}
                onClose={() => { setDialogOpen(false); setEditId(null); }}
                onSaved={handleSaved}
                profileId={editId}
            />

            <Snackbar
                open={snack.open}
                autoHideDuration={6000}
                onClose={() => setSnack(s => ({ ...s, open: false }))}
                anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
            >
                <Alert severity={snack.severity} variant="filled" onClose={() => setSnack(s => ({ ...s, open: false }))}>
                    {snack.message}
                </Alert>
            </Snackbar>
        </Container>
    );
};

export default ConnectionsPage;
