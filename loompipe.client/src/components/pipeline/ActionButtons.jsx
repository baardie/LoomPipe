import React from 'react';
import { Box, Button } from '@mui/material';

const ActionButtons = ({ onCancel, onSave, onRun, pipelineId }) => {
    return (
        <Box sx={{ mt: 2, display: 'flex', justifyContent: 'flex-end' }}>
            <Button onClick={onCancel} sx={{ mr: 1 }}>Cancel</Button>
            <Button variant="contained" color="primary" onClick={onSave} sx={{ mr: 1 }}>Save</Button>
            {pipelineId && <Button variant="contained" color="secondary" onClick={onRun}>Run</Button>}
        </Box>
    );
};

export default ActionButtons;
