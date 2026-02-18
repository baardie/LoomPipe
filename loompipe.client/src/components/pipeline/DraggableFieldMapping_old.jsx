import React, { useState, useEffect, createRef } from 'react';
import { DragDropContext } from '@hello-pangea/dnd';
import { Grid, Box } from '@mui/material';
import Column from './dnd/Column';
import MappingLine from './dnd/MappingLine';

const DraggableFieldMapping = ({ sourceSchema, destinationSchema, fieldMappings, setFieldMappings }) => {
    const [data, setData] = useState({ columns: {}, fields: {}, columnOrder: [] });
    const [fieldRefs, setFieldRefs] = useState({});
    const [lineCoordinates, setLineCoordinates] = useState([]);

    useEffect(() => {
        const fields = {};
        const refs = {};
        sourceSchema.forEach((field, index) => {
            const id = `s-${index}`;
            fields[id] = { id, content: field };
            refs[id] = createRef();
        });
        destinationSchema.forEach((field, index) => {
            const id = `d-${index}`;
            fields[id] = { id, content: field };
            refs[id] = createRef();
        });

        const sourceColumn = {
            id: 'source',
            title: 'Source Fields',
            fieldIds: sourceSchema.map((_, index) => `s-${index}`),
        };

        const destinationColumn = {
            id: 'destination',
            title: 'Destination Fields',
            fieldIds: destinationSchema.map((_, index) => `d-${index}`),
        };

        setData({
            fields,
            columns: {
                'source': sourceColumn,
                'destination': destinationColumn,
            },
            columnOrder: ['source', 'destination'],
        });
        setFieldRefs(refs);
    }, [sourceSchema, destinationSchema]);

    useEffect(() => {
        const calculateLineCoordinates = () => {
            const coordinates = fieldMappings.map(mapping => {
                const sourceFieldId = Object.keys(data.fields).find(key => data.fields[key].content === mapping.sourceField);
                const destFieldId = Object.keys(data.fields).find(key => data.fields[key].content === mapping.destinationField);

                const sourceRef = fieldRefs[sourceFieldId];
                const destRef = fieldRefs[destFieldId];

                if (sourceRef && sourceRef.current && destRef && destRef.current) {
                    const sourceRect = sourceRef.current.getBoundingClientRect();
                    const destRect = destRef.current.getBoundingClientRect();

                    return {
                        from: { x: sourceRect.right, y: sourceRect.top + sourceRect.height / 2 },
                        to: { x: destRect.left, y: destRect.top + destRect.height / 2 },
                    };
                }
                return null;
            }).filter(Boolean);
            setLineCoordinates(coordinates);
        };

        calculateLineCoordinates();
    }, [data, fieldMappings, fieldRefs]);


    const onDragEnd = (result) => {
        // ... (same as before)
    };

    if (!data.columnOrder.length) {
        return null;
    }

    return (
        <Box sx={{ position: 'relative' }}>
            <DragDropContext onDragEnd={onDragEnd}>
                <Grid container spacing={2}>
                    {data.columnOrder.map(columnId => {
                        const column = data.columns[columnId];
                        const fields = column.fieldIds.map(fieldId => ({
                            id: data.fields[fieldId].id,
                            content: data.fields[fieldId].content,
                            ref: fieldRefs[fieldId]
                        }));

                        return (
                            <Grid item xs={6} key={column.id}>
                                <Column column={column} fields={fields} />
                            </Grid>
                        );
                    })}
                </Grid>
            </DragDropContext>
            {lineCoordinates.map((coords, index) => (
                <MappingLine key={index} from={coords.from} to={coords.to} />
            ))}
        </Box>
    );
};

export default DraggableFieldMapping;