import React from 'react';
import { Typography, Button, Box, Stack } from '@mui/material';
import DraggableFieldMapping from './DraggableFieldMapping';
import useSchema from '../../hooks/useSchema';

const FieldMappingForm = ({
    fieldMappings,
    setFieldMappings,
    handleAutomap,
    handleDryRun,
    sourceType,
    sourceConnectionString,
    destinationSchema: destinationSchemaText,
}) => {
    const { sourceSchema, destinationSchema, loading, error } = useSchema(
        sourceType,
        sourceConnectionString,
        destinationSchemaText
    );

    return (
        <Stack spacing={2}>
            <Box>
                <Button variant="outlined" sx={{ mr: 1 }} onClick={handleAutomap}>Automap Fields</Button>
                <Button variant="outlined" onClick={handleDryRun}>Dry Run</Button>
            </Box>
            {loading && <Typography>Loading schema...</Typography>}
            {error && <Typography color="error">Error loading schema.</Typography>}
            {!loading && !error && (
                <DraggableFieldMapping
                    sourceSchema={sourceSchema}
                    destinationSchema={destinationSchema}
                    fieldMappings={fieldMappings}
                    setFieldMappings={setFieldMappings}
                />
            )}
        </Stack>
    );
};

export default FieldMappingForm;
