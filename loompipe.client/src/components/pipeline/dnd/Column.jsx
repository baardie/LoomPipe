import React from 'react';
import { Droppable } from '@hello-pangea/dnd';
import { Paper, Typography, Box } from '@mui/material';
import Field from './Field';

const Column = ({ column, fields }) => {
    return (
        <Paper sx={{ p: 2, display: 'flex', flexDirection: 'column', height: '100%' }}>
            <Typography variant="h6" sx={{ mb: 2 }}>{column.title}</Typography>
            <Droppable droppableId={column.id}>
                {(provided, snapshot) => (
                    <Box
                        ref={provided.innerRef}
                        {...provided.droppableProps}
                        sx={{
                            backgroundColor: snapshot.isDraggingOver ? 'lightblue' : 'lightgrey',
                            flexGrow: 1,
                            p: 1,
                            borderRadius: '4px'
                        }}
                    >
                        {fields.map((field, index) => (
                            <Field key={field.id} field={field.content} index={index} fieldRef={field.ref} />
                        ))}
                        {provided.placeholder}
                    </Box>
                )}
            </Droppable>
        </Paper>
    );
};

export default Column;