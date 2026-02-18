import React, { useState, useEffect, useCallback } from 'react';
import {
    Box, Button, Chip, CircularProgress, Container, Divider,
    List, ListItem, ListItemText, Paper, Typography,
    Checkbox, FormControlLabel, Stack, Alert,
} from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import { useAuth } from '../contexts/AuthContext';
import RoleGuard from '../components/auth/RoleGuard';

const ProfileDetailPage = ({ profileId, onBack, pipelines }) => {
    const { authFetch, isAdmin } = useAuth();
    const [profile, setProfile] = useState(null);
    const [testing, setTesting] = useState(false);
    const [testResult, setTestResult] = useState(null);

    // Permission management state (Admin only)
    const [allUsers, setAllUsers] = useState([]);
    const [permittedUserIds, setPermittedUserIds] = useState(new Set());
    const [permSaving, setPermSaving] = useState(null); // userId being toggled

    const load = useCallback(async () => {
        const res = await authFetch(`/api/connections/${profileId}`);
        if (res.ok) setProfile(await res.json());
    }, [profileId, authFetch]);

    const loadPermissions = useCallback(async () => {
        const [usersRes, permRes] = await Promise.all([
            authFetch('/api/users'),
            authFetch(`/api/connections/${profileId}/users`),
        ]);
        if (usersRes.ok) setAllUsers(await usersRes.json());
        if (permRes.ok) setPermittedUserIds(new Set(await permRes.json()));
    }, [profileId, authFetch]);

    useEffect(() => { load(); }, [load]);
    useEffect(() => { if (isAdmin) loadPermissions(); }, [isAdmin, loadPermissions]);

    const handleTest = async () => {
        setTesting(true);
        setTestResult(null);
        try {
            const res = await authFetch(`/api/connections/${profileId}/test`, { method: 'POST' });
            if (res.ok) {
                const data = await res.json();
                setTestResult(data);
                await load();
            }
        } finally {
            setTesting(false);
        }
    };

    const handleToggleUser = async (userId, currentlyGranted) => {
        setPermSaving(userId);
        try {
            const method = currentlyGranted ? 'DELETE' : 'POST';
            await authFetch(`/api/connections/${profileId}/users/${userId}`, { method });
            setPermittedUserIds(prev => {
                const next = new Set(prev);
                if (currentlyGranted) next.delete(userId); else next.add(userId);
                return next;
            });
        } finally {
            setPermSaving(null);
        }
    };

    const linkedPipelines = (pipelines || []).filter((p) => {
        const sid = p.source?.parameters?.connectionProfileId;
        const did = p.destination?.parameters?.connectionProfileId;
        return String(sid) === String(profileId) || String(did) === String(profileId);
    });

    // Only show non-Admin users in the permissions list (Admins always have access)
    const nonAdminUsers = allUsers.filter(u => u.role !== 'Admin' && u.isActive);

    if (!profile) {
        return (
            <Box sx={{ display: 'flex', justifyContent: 'center', mt: 8 }}>
                <CircularProgress />
            </Box>
        );
    }

    return (
        <Container sx={{ my: 4 }}>
            <Button startIcon={<ArrowBackIcon />} onClick={onBack} sx={{ mb: 2 }}>
                Back to Connections
            </Button>

            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
                <Box>
                    <Typography variant="h4">{profile.name}</Typography>
                    <Chip label={profile.provider} size="small" sx={{ mt: 0.5 }} />
                </Box>
                <Box sx={{ display: 'flex', gap: 1 }}>
                    <Button variant="contained" onClick={handleTest} disabled={testing}>
                        {testing ? <CircularProgress size={20} color="inherit" /> : 'Test Connection'}
                    </Button>
                </Box>
            </Box>

            {testResult && (
                <Box sx={{ mb: 2 }}>
                    <Chip
                        label={testResult.success ? `Connected (${testResult.elapsedMs}ms)` : `Failed: ${testResult.errorMessage}`}
                        color={testResult.success ? 'success' : 'error'}
                    />
                </Box>
            )}

            <Paper sx={{ p: 2, mb: 3 }} elevation={1}>
                <Typography variant="subtitle2" color="text.secondary">Details</Typography>
                <Divider sx={{ my: 1 }} />
                <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 1 }}>
                    {profile.host && <><Typography variant="caption" color="text.secondary">Host</Typography><Typography>{profile.host}</Typography></>}
                    {profile.databaseName && <><Typography variant="caption" color="text.secondary">Database</Typography><Typography>{profile.databaseName}</Typography></>}
                    {profile.createdAt && <><Typography variant="caption" color="text.secondary">Created</Typography><Typography>{new Date(profile.createdAt).toLocaleDateString()}</Typography></>}
                    {profile.lastTestedAt && <><Typography variant="caption" color="text.secondary">Last Tested</Typography><Typography>{new Date(profile.lastTestedAt).toLocaleString()}</Typography></>}
                    {profile.lastTestSucceeded != null && <><Typography variant="caption" color="text.secondary">Last Test</Typography><Chip label={profile.lastTestSucceeded ? 'Passed' : 'Failed'} color={profile.lastTestSucceeded ? 'success' : 'error'} size="small" /></>}
                </Box>
            </Paper>

            {/* Permitted Users — Admin only */}
            <RoleGuard roles={['Admin']}>
                <Typography variant="h6" sx={{ mb: 1 }}>User Access</Typography>
                <Paper sx={{ p: 2, mb: 3 }} elevation={1}>
                    <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                        Admins always have full access. Grant access to specific User-role accounts below.
                    </Typography>
                    {nonAdminUsers.length === 0 ? (
                        <Typography variant="body2" color="text.secondary">No non-admin users found.</Typography>
                    ) : (
                        <Stack spacing={0.5}>
                            {nonAdminUsers.map(u => {
                                const granted = permittedUserIds.has(u.id);
                                return (
                                    <FormControlLabel
                                        key={u.id}
                                        control={
                                            <Checkbox
                                                checked={granted}
                                                disabled={permSaving === u.id}
                                                onChange={() => handleToggleUser(u.id, granted)}
                                                size="small"
                                            />
                                        }
                                        label={
                                            <Stack direction="row" spacing={1} alignItems="center">
                                                <span>{u.username}</span>
                                                <Chip label={u.role} size="small" variant="outlined" />
                                                {permSaving === u.id && <CircularProgress size={14} />}
                                            </Stack>
                                        }
                                    />
                                );
                            })}
                        </Stack>
                    )}
                </Paper>
            </RoleGuard>

            <Typography variant="h6" sx={{ mb: 1 }}>Linked Pipelines</Typography>
            <Paper elevation={1}>
                {linkedPipelines.length === 0 ? (
                    <Box sx={{ p: 2, color: 'text.secondary' }}>No pipelines use this connection profile.</Box>
                ) : (
                    <List dense>
                        {linkedPipelines.map((p) => (
                            <ListItem key={p.id}>
                                <ListItemText primary={p.name} secondary={`ID: ${p.id}`} />
                            </ListItem>
                        ))}
                    </List>
                )}
            </Paper>
        </Container>
    );
};

export default ProfileDetailPage;
