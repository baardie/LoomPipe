import React, { useState, useEffect } from 'react';
import { DragDropContext } from '@hello-pangea/dnd';
import { Grid } from '@mui/material';
import SourceColumn from './dnd2/SourceColumn';
import DestinationColumn from './dnd2/DestinationColumn';

const DraggableFieldMapping = ({ sourceSchema, destinationSchema, fieldMappings, setFieldMappings }) => {
    const [sourceFields, setSourceFields] = useState([]);
    const [destinationFields, setDestinationFields] = useState([]);
    const [mappedFields, setMappedFields] = useState([]);

    useEffect(() => {
        // Initialize the state based on the schemas and existing mappings
        const sFields = sourceSchema.map((field, index) => ({ id: `s-${index}`, content: field }));
        setSourceFields(sFields);
        
        const mappedDestinationContents = fieldMappings.map(m => m.destinationField);
        const availableDestinationFields = destinationSchema
            .filter(d => !mappedDestinationContents.includes(d))
            .map((field, index) => ({ id: `d-${destinationSchema.indexOf(field)}`, content: field }));

        setDestinationFields(availableDestinationFields);

        const newMappedFields = fieldMappings.map(m => {
            const sourceField = sFields.find(s => s.content === m.sourceField);
            const sourceFieldId = sourceField ? sourceField.id : null;
            const destFieldIndex = destinationSchema.indexOf(m.destinationField);
            return {
                id: `md-${destFieldIndex}`,
                content: m.destinationField,
                sourceFieldId: sourceFieldId,
            };
        }).filter(mf => mf.sourceFieldId);
        setMappedFields(newMappedFields);

    }, [sourceSchema, destinationSchema, fieldMappings]);

    const onDragEnd = (result) => {
        const { destination, source, draggableId } = result;

        if (!destination) return;

        // UNMAPPING: Move a mapped field back to the available destination fields list
        if (source.droppableId.startsWith('s-') && destination.droppableId === 'destination') {
            const fieldToUnmap = mappedFields.find(f => f.id === draggableId);
            if (!fieldToUnmap) return;
            
            // Add to available destination fields
            const newDestinationFields = [...destinationFields, {id: fieldToUnmap.id, content: fieldToUnmap.content }];
            setDestinationFields(newDestinationFields);

            // Remove from mapped fields
            const newMappedFields = mappedFields.filter(f => f.id !== draggableId);
            setMappedFields(newMappedFields);

            // Update the main fieldMappings state
            const sourceFieldContent = sourceFields.find(f => f.id === source.droppableId).content;
            setFieldMappings(fieldMappings.filter(m => m.sourceField !== sourceFieldContent));
        }

        // MAPPING: Move a destination field to a source field's droppable area
        if (source.droppableId === 'destination' && destination.droppableId.startsWith('s-')) {
            const fieldToMap = destinationFields.find(f => f.id === draggableId);
            if (!fieldToMap) return;

            const sourceFieldId = destination.droppableId;
            const sourceFieldContent = sourceFields.find(f => f.id === sourceFieldId).content;
            
            // Remove from available destination fields
            const newDestinationFields = destinationFields.filter(f => f.id !== draggableId);

            // If the source field already has a mapping, move the old mapped field back to the available list
            const existingMapping = mappedFields.find(mf => mf.sourceFieldId === sourceFieldId);
            if (existingMapping) {
                newDestinationFields.push({ id: existingMapping.id, content: existingMapping.content });
            }

            setDestinationFields(newDestinationFields);

            // Add to mapped fields
            const newMappedField = { id: draggableId, content: fieldToMap.content, sourceFieldId };
            const newMappedFields = [...mappedFields.filter(mf => mf.sourceFieldId !== sourceFieldId), newMappedField];
            setMappedFields(newMappedFields);

            // Update the main fieldMappings state
            const newFieldMapping = { sourceField: sourceFieldContent, destinationField: fieldToMap.content };
            const otherMappings = fieldMappings.filter(m => m.sourceField !== sourceFieldContent);
            setFieldMappings([...otherMappings, newFieldMapping]);
        }
    };

    return (
        <DragDropContext onDragEnd={onDragEnd}>
            <Grid container spacing={2}>
                <Grid item xs={6}>
                    <SourceColumn
                        column={{ id: 'source', title: 'Source Fields' }}
                        fields={sourceFields}
                        mappedFields={mappedFields}
                    />
                </Grid>
                <Grid item xs={6}>
                    <DestinationColumn
                        column={{ id: 'destination', title: 'Available Destination Fields' }}
                        fields={destinationFields}
                    />
                </Grid>
            </Grid>
        </DragDropContext>
    );
};

export default DraggableFieldMapping;