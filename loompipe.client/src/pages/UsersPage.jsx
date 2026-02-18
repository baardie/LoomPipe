import React, { useState, useEffect } from 'react';
import {
    Box, Button, Chip, CircularProgress, Container, Dialog, DialogActions,
    DialogContent, DialogTitle, MenuItem, Paper, Select, Table, TableBody,
    TableCell, TableHead, TableRow, TextField, Typography,
} from '@mui/material';
import { useAuth } from '../contexts/AuthContext';

const ROLES = ['Admin', 'User', 'Guest'];

const UsersPage = () => {
    const { authFetch } = useAuth();
    const [users, setUsers] = useState([]);
    const [loading, setLoading] = useState(true);
    const [dialogOpen, setDialogOpen] = useState(false);
    const [editUser, setEditUser] = useState(null); // null = new user
    const [form, setForm] = useState({ username: '', password: '', role: 'User' });

    const load = async () => {
        const res = await authFetch('/api/users');
        if (res.ok) setUsers(await res.json());
        setLoading(false);
    };

    useEffect(() => { load(); }, []);

    const openNew = () => {
        setEditUser(null);
        setForm({ username: '', password: '', role: 'User' });
        setDialogOpen(true);
    };

    const openEdit = (u) => {
        setEditUser(u);
        setForm({ username: u.username, password: '', role: u.role });
        setDialogOpen(true);
    };

    const handleSave = async () => {
        if (editUser) {
            await authFetch(`/api/users/${editUser.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ role: form.role, isActive: true }),
            });
        } else {
            await authFetch('/api/users', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(form),
            });
        }
        setDialogOpen(false);
        load();
    };

    const handleDeactivate = async (id) => {
        await authFetch(`/api/users/${id}`, { method: 'DELETE' });
        load();
    };

    if (loading) {
        return (
            <Box sx={{ display: 'flex', justifyContent: 'center', mt: 8 }}>
                <CircularProgress />
            </Box>
        );
    }

    return (
        <Container sx={{ my: 4 }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
                <Typography variant="h4">User Management</Typography>
                <Button variant="contained" onClick={openNew}>New User</Button>
            </Box>

            <Paper elevation={1}>
                <Table>
                    <TableHead>
                        <TableRow>
                            <TableCell>Username</TableCell>
                            <TableCell>Role</TableCell>
                            <TableCell>Status</TableCell>
                            <TableCell>Created</TableCell>
                            <TableCell>Actions</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {users.map((u) => (
                            <TableRow key={u.id}>
                                <TableCell>{u.username}</TableCell>
                                <TableCell>
                                    <Chip
                                        label={u.role}
                                        color={u.role === 'Admin' ? 'error' : u.role === 'User' ? 'primary' : 'default'}
                                        size="small"
                                    />
                                </TableCell>
                                <TableCell>
                                    <Chip
                                        label={u.isActive ? 'Active' : 'Inactive'}
                                        color={u.isActive ? 'success' : 'default'}
                                        size="small"
                                    />
                                </TableCell>
                                <TableCell>{new Date(u.createdAt).toLocaleDateString()}</TableCell>
                                <TableCell>
                                    <Box sx={{ display: 'flex', gap: 1 }}>
                                        <Button size="small" onClick={() => openEdit(u)}>Edit Role</Button>
                                        {u.isActive && (
                                            <Button size="small" color="error" onClick={() => handleDeactivate(u.id)}>
                                                Deactivate
                                            </Button>
                                        )}
                                    </Box>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </Paper>

            <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} maxWidth="xs" fullWidth>
                <DialogTitle>{editUser ? 'Edit User' : 'New User'}</DialogTitle>
                <DialogContent sx={{ display: 'flex', flexDirection: 'column', gap: 2, pt: 2 }}>
                    {!editUser && (
                        <>
                            <TextField
                                label="Username"
                                value={form.username}
                                onChange={(e) => setForm({ ...form, username: e.target.value })}
                                fullWidth
                            />
                            <TextField
                                label="Password"
                                type="password"
                                value={form.password}
                                onChange={(e) => setForm({ ...form, password: e.target.value })}
                                fullWidth
                            />
                        </>
                    )}
                    <Select
                        value={form.role}
                        onChange={(e) => setForm({ ...form, role: e.target.value })}
                        fullWidth
                    >
                        {ROLES.map((r) => <MenuItem key={r} value={r}>{r}</MenuItem>)}
                    </Select>
                </DialogContent>
                <DialogActions>
                    <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
                    <Button variant="contained" onClick={handleSave}>Save</Button>
                </DialogActions>
            </Dialog>
        </Container>
    );
};

export default UsersPage;
