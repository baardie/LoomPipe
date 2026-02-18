import React, { useState, useEffect } from 'react';
import { FormControl, InputLabel, Select, MenuItem, IconButton, Tooltip, Stack, CircularProgress } from '@mui/material';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import ConnectionProfileDialog from './ConnectionProfileDialog';
import { useAuth } from '../../contexts/AuthContext';

const PROVIDER_LABELS = {
  sqlserver: 'SQL Server', postgresql: 'PostgreSQL', mysql: 'MySQL',
  oracle: 'Oracle', mongodb: 'MongoDB', neo4j: 'Neo4j',
  snowflake: 'Snowflake', bigquery: 'BigQuery', pinecone: 'Pinecone', milvus: 'Milvus',
};

/**
 * A profile-picker Select + "New Profile" button.
 * Used in SourceDestinationForm for DB-type connections.
 *
 * Props:
 *   label           - "Source Profile" | "Destination Profile"
 *   profileId       - selected profile ID (number|null)
 *   onProfileChange(id) - callback when selection changes
 *   filterProvider  - if set, only show profiles for this provider type
 */
const ConnectionProfileSelect = ({ label, profileId, onProfileChange, filterProvider }) => {
  const { authFetch } = useAuth();
  const [profiles,    setProfiles]    = useState([]);
  const [loading,     setLoading]     = useState(false);
  const [dialogOpen,  setDialogOpen]  = useState(false);

  const fetchProfiles = async () => {
    setLoading(true);
    try {
      const resp = await authFetch('/api/connections');
      if (resp.ok) {
        const data = await resp.json();
        setProfiles(filterProvider ? data.filter(p => p.provider === filterProvider) : data);
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchProfiles(); }, [filterProvider]);

  const handleSaved = (newProfile) => {
    fetchProfiles();
    if (newProfile) onProfileChange(newProfile.id);
  };

  return (
    <>
      <Stack direction="row" alignItems="center" spacing={1}>
        <FormControl fullWidth>
          <InputLabel>{label}</InputLabel>
          <Select
            value={profileId || ''}
            onChange={(e) => onProfileChange(e.target.value || null)}
            label={label}
            endAdornment={loading ? <CircularProgress size={18} sx={{ mr: 2 }} /> : null}
          >
            <MenuItem value=""><em>None</em></MenuItem>
            {profiles.map(p => (
              <MenuItem key={p.id} value={p.id}>
                <Stack direction="row" spacing={1} alignItems="center">
                  <span style={{ fontWeight: 500 }}>{p.name}</span>
                  <span style={{ fontSize: 12, color: '#888' }}>
                    ({PROVIDER_LABELS[p.provider] || p.provider} — {p.host || p.provider}{p.databaseName ? `/${p.databaseName}` : ''})
                  </span>
                  {p.lastTestedAt && (
                    <span style={{ fontSize: 11, color: p.lastTestSucceeded ? '#4caf50' : '#f44336' }}>
                      {p.lastTestSucceeded ? '✓' : '✗'}
                    </span>
                  )}
                </Stack>
              </MenuItem>
            ))}
          </Select>
        </FormControl>

        <Tooltip title="New Profile">
          <IconButton onClick={() => setDialogOpen(true)} color="primary">
            <AddCircleOutlineIcon />
          </IconButton>
        </Tooltip>
      </Stack>

      <ConnectionProfileDialog
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
        onSaved={handleSaved}
      />
    </>
  );
};

export default ConnectionProfileSelect;
