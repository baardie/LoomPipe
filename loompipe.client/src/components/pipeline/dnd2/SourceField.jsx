import React from 'react';
import { Droppable } from '@hello-pangea/dnd';
import { Paper, Typography, Box, Grid } from '@mui/material';
import MappedField from './MappedField';

const SourceField = ({ field, mappedField }) => {
    return (
        <Paper sx={{ p: 1, mb: 1, border: '1px solid lightgrey', borderRadius: '4px' }}>
            <Grid container spacing={2} alignItems="center">
                <Grid item xs={6}>
                    <Typography>{field.content}</Typography>
                </Grid>
                <Grid item xs={6}>
                    <Droppable droppableId={field.id} direction="horizontal">
                        {(provided, snapshot) => (
                            <Box
                                ref={provided.innerRef}
                                {...provided.droppableProps}
                                sx={{
                                    backgroundColor: snapshot.isDraggingOver ? 'lightblue' : 'lightgrey',
                                    p: 1,
                                    borderRadius: '4px',
                                    minHeight: '50px',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center'
                                }}
                            >
                                {mappedField ? (
                                    <MappedField field={mappedField} index={0} />
                                ) : (
                                    <Typography variant="body2" color="textSecondary">Drop here</Typography>
                                )}
                                {provided.placeholder}
                            </Box>
                        )}
                    </Droppable>
                </Grid>
            </Grid>
        </Paper>
    );
};

export default SourceField;
