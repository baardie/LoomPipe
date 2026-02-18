import React from 'react';
import { Draggable } from '@hello-pangea/dnd';
import { Paper, Typography } from '@mui/material';

const DestinationField = ({ field, index }) => {
    return (
        <Draggable draggableId={field.id} index={index}>
            {(provided, snapshot) => (
                <Paper
                    ref={provided.innerRef}
                    {...provided.draggableProps}
                    {...provided.dragHandleProps}
                    sx={{
                        p: 1,
                        mb: 1,
                        backgroundColor: snapshot.isDragging ? 'lightgreen' : 'white',
                        border: '1px solid lightgrey',
                        borderRadius: '4px'
                    }}
                >
                    <Typography>{field.content}</Typography>
                </Paper>
            )}
        </Draggable>
    );
};

export default DestinationField;
