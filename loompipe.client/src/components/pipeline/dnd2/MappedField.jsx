import React from 'react';
import { Draggable } from '@hello-pangea/dnd';
import { Paper, Typography } from '@mui/material';

const MappedField = ({ field, index }) => {
    return (
        <Draggable draggableId={field.id} index={index}>
            {(provided, snapshot) => (
                <Paper
                    ref={provided.innerRef}
                    {...provided.draggableProps}
                    {...provided.dragHandleProps}
                    sx={{
                        p: 1,
                        backgroundColor: snapshot.isDragging ? 'lightcoral' : 'white',
                        border: '1px solid lightgrey',
                        borderRadius: '4px',
                        width: '100%'
                    }}
                >
                    <Typography>{field.content}</Typography>
                </Paper>
            )}
        </Draggable>
    );
};

export default MappedField;
