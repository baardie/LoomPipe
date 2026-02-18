import React, { useState, useEffect } from 'react';
import {
  Dialog, DialogTitle, DialogContent, DialogActions,
  Button, CircularProgress, Snackbar, Alert, LinearProgress
} from '@mui/material';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import ConnectionProfileForm from './ConnectionProfileForm';
import { useAuth } from '../../contexts/AuthContext';

/**
 * Dialog for creating or editing a connection profile.
 *
 * Props:
 *   open          - boolean
 *   onClose()     - called when dialog closes
 *   onSaved(summary) - called after successful save
 *   profileId     - if set, load existing profile for editing
 */
const ConnectionProfileDialog = ({ open, onClose, onSaved, profileId = null }) => {
  const { authFetch } = useAuth();
  const isEditing = !!profileId;

  const emptyValues = {
    name: '', provider: '', host: '', port: '', databaseName: '',
    username: '', additionalConfig: '{}',
    password: '', apiKey: '', serviceAccountJson: '',
  };

  const [values,     setValues]     = useState(emptyValues);
  const [saving,     setSaving]     = useState(false);
  const [testing,    setTesting]    = useState(false);
  const [snack,      setSnack]      = useState({ open: false, message: '', severity: 'success' });

  // Load existing profile when editing
  useEffect(() => {
    if (open && isEditing) {
      authFetch(`/api/connections/${profileId}`)
        .then(r => r.json())
        .then(data => setValues({
          name:             data.name             || '',
          provider:         data.provider         || '',
          host:             data.host             || '',
          port:             data.port             || '',
          databaseName:     data.databaseName     || '',
          username:         data.username         || '',
          additionalConfig: data.additionalConfig || '{}',
          // Secrets not returned by API — leave blank (null = keep existing on save)
          password: '', apiKey: '', serviceAccountJson: '',
        }));
    } else if (open && !isEditing) {
      setValues(emptyValues);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, profileId]);

  const handleChange = (field, value) =>
    setValues(prev => ({ ...prev, [field]: value }));

  const handleSave = async () => {
    setSaving(true);
    try {
      const payload = {
        name:             values.name,
        provider:         values.provider,
        host:             values.host,
        port:             values.port ? Number(values.port) : null,
        databaseName:     values.databaseName,
        username:         values.username,
        additionalConfig: values.additionalConfig,
        // Only include secret fields if non-empty (null = keep existing)
        password:           values.password           || null,
        apiKey:             values.apiKey             || null,
        serviceAccountJson: values.serviceAccountJson || null,
      };

      const url     = isEditing ? `/api/connections/${profileId}` : '/api/connections';
      const method  = isEditing ? 'PUT' : 'POST';
      const resp    = await authFetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || resp.statusText);
      }

      const saved = method === 'POST' ? await resp.json() : null;
      setSnack({ open: true, message: 'Saved successfully.', severity: 'success' });
      onSaved(saved);
      onClose();
    } catch (err) {
      setSnack({ open: true, message: `Save failed: ${err.message}`, severity: 'error' });
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async () => {
    if (!isEditing) {
      setSnack({ open: true, message: 'Save the profile first, then test it.', severity: 'info' });
      return;
    }
    setTesting(true);
    try {
      const resp = await authFetch(`/api/connections/${profileId}/test`, { method: 'POST' });
      const result = await resp.json();
      setSnack({
        open: true,
        message: result.success
          ? `Connection successful (${result.elapsedMs}ms)`
          : `Connection failed: ${result.errorMessage}`,
        severity: result.success ? 'success' : 'error',
      });
    } catch (err) {
      setSnack({ open: true, message: `Test error: ${err.message}`, severity: 'error' });
    } finally {
      setTesting(false);
    }
  };

  return (
    <>
      <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
        {(saving || testing) && <LinearProgress />}
        <DialogTitle>{isEditing ? 'Edit Connection Profile' : 'New Connection Profile'}</DialogTitle>
        <DialogContent dividers>
          <ConnectionProfileForm values={values} onChange={handleChange} />
        </DialogContent>
        <DialogActions>
          {isEditing && (
            <Button
              onClick={handleTest}
              disabled={testing}
              startIcon={testing ? <CircularProgress size={16} /> : null}
              color="info"
            >
              Test Connection
            </Button>
          )}
          <Button onClick={onClose} disabled={saving}>Cancel</Button>
          <Button onClick={handleSave} variant="contained" disabled={saving}>
            {saving ? <CircularProgress size={18} /> : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={snack.open}
        autoHideDuration={5000}
        onClose={() => setSnack(s => ({ ...s, open: false }))}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert severity={snack.severity} onClose={() => setSnack(s => ({ ...s, open: false }))} variant="filled">
          {snack.message}
        </Alert>
      </Snackbar>
    </>
  );
};

export default ConnectionProfileDialog;
