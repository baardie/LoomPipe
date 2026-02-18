import React from 'react';
import { Draggable } from '@hello-pangea/dnd';
import { Paper, Typography } from '@mui/material';

const Field = ({ field, index, fieldRef }) => {
    return (
        <Draggable draggableId={field} index={index}>
            {(provided, snapshot) => (
                <Paper
                    ref={node => {
                        provided.innerRef(node);
                        if (fieldRef) {
                            fieldRef.current = node;
                        }
                    }}
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
                    <Typography>{field}</Typography>
                </Paper>
            )}
        </Draggable>
    );
};

export default Field;