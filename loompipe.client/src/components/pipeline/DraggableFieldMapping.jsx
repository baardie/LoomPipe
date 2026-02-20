import { useState, useEffect } from 'react';
import { DragDropContext } from '@hello-pangea/dnd';
import SourceColumn from './dnd2/SourceColumn';
import DestinationColumn from './dnd2/DestinationColumn';

const DraggableFieldMapping = ({ sourceSchema, destinationSchema, fieldMappings, setFieldMappings }) => {
  const [sourceFields,      setSourceFields]      = useState([]);
  const [destinationFields, setDestinationFields] = useState([]);
  const [mappedFields,      setMappedFields]      = useState([]);

  useEffect(() => {
    const sFields = sourceSchema.map((field, index) => ({ id: `s-${index}`, content: field }));
    setSourceFields(sFields);
    const mappedDestinationContents = fieldMappings.map(m => m.destinationField);
    const availableDestinationFields = destinationSchema
      .filter(d => !mappedDestinationContents.includes(d))
      .map((field) => ({ id: `d-${destinationSchema.indexOf(field)}`, content: field }));
    setDestinationFields(availableDestinationFields);
    const newMappedFields = fieldMappings.map(m => {
      const sourceField = sFields.find(s => s.content === m.sourceField);
      const sourceFieldId = sourceField ? sourceField.id : null;
      const destFieldIndex = destinationSchema.indexOf(m.destinationField);
      return { id: `md-${destFieldIndex}`, content: m.destinationField, sourceFieldId };
    }).filter(mf => mf.sourceFieldId);
    setMappedFields(newMappedFields);
  }, [sourceSchema, destinationSchema, fieldMappings]);

  const onDragEnd = (result) => {
    const { destination, source, draggableId } = result;
    if (!destination) return;
    if (source.droppableId.startsWith('s-') && destination.droppableId === 'destination') {
      const fieldToUnmap = mappedFields.find(f => f.id === draggableId);
      if (!fieldToUnmap) return;
      setDestinationFields([...destinationFields, { id: fieldToUnmap.id, content: fieldToUnmap.content }]);
      setMappedFields(mappedFields.filter(f => f.id !== draggableId));
      const sourceFieldContent = sourceFields.find(f => f.id === source.droppableId).content;
      setFieldMappings(fieldMappings.filter(m => m.sourceField !== sourceFieldContent));
    }
    if (source.droppableId === 'destination' && destination.droppableId.startsWith('s-')) {
      const fieldToMap = destinationFields.find(f => f.id === draggableId);
      if (!fieldToMap) return;
      const sourceFieldId = destination.droppableId;
      const sourceFieldContent = sourceFields.find(f => f.id === sourceFieldId).content;
      const newDestinationFields = destinationFields.filter(f => f.id !== draggableId);
      const existingMapping = mappedFields.find(mf => mf.sourceFieldId === sourceFieldId);
      if (existingMapping) newDestinationFields.push({ id: existingMapping.id, content: existingMapping.content });
      setDestinationFields(newDestinationFields);
      const newMappedField = { id: draggableId, content: fieldToMap.content, sourceFieldId };
      setMappedFields([...mappedFields.filter(mf => mf.sourceFieldId !== sourceFieldId), newMappedField]);
      const newFieldMapping = { sourceField: sourceFieldContent, destinationField: fieldToMap.content };
      setFieldMappings([...fieldMappings.filter(m => m.sourceField !== sourceFieldContent), newFieldMapping]);
    }
  };

  return (
    <DragDropContext onDragEnd={onDragEnd}>
      <div className="grid grid-cols-2 gap-4">
        <SourceColumn column={{ id: 'source', title: 'Source Fields' }} fields={sourceFields} mappedFields={mappedFields} />
        <DestinationColumn column={{ id: 'destination', title: 'Available Destination Fields' }} fields={destinationFields} />
      </div>
    </DragDropContext>
  );
};

export default DraggableFieldMapping;
