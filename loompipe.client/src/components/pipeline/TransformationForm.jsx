import React from 'react';
import { Box, Typography, TextField } from '@mui/material';

const TransformationForm = ({ transformations, setTransformations }) => {
    return (
        <Box sx={{ my: 2 }}>
            <Typography variant="subtitle1" gutterBottom>Transformations</Typography>
            <TextField
                label="Transformations (one per line)"
                multiline
                rows={4}
                value={transformations.join('\n')}
                onChange={(e) => setTransformations(e.target.value.split('\n'))}
                fullWidth
            />
        </Box>
    );
};

export default TransformationForm;
