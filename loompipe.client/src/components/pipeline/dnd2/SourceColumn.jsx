import React from 'react';
import { Paper, Typography, Box } from '@mui/material';
import SourceField from './SourceField';

const SourceColumn = ({ column, fields, mappedFields }) => {
    return (
        <Paper sx={{ p: 2, display: 'flex', flexDirection: 'column', height: '100%' }}>
            <Typography variant="h6" sx={{ mb: 2 }}>{column.title}</Typography>
            <Box sx={{ flexGrow: 1, p: 1, borderRadius: '4px' }}>
                {fields.map(field => {
                    const mappedField = mappedFields.find(mf => mf.sourceFieldId === field.id);
                    return <SourceField key={field.id} field={field} mappedField={mappedField} />;
                })}
            </Box>
        </Paper>
    );
};

export default SourceColumn;
