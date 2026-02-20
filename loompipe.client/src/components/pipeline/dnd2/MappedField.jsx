import { Draggable } from '@hello-pangea/dnd';

const MappedField = ({ field, sourceFieldId }) => (
  <Draggable draggableId={field.id} index={0}>
    {(provided, snapshot) => (
      <div
        ref={provided.innerRef}
        {...provided.draggableProps}
        {...provided.dragHandleProps}
        className={`px-2 py-0.5 rounded text-xs font-mono border cursor-grab transition-colors ${snapshot.isDragging ? 'bg-[var(--yellow)]/20 border-[var(--yellow)] text-[var(--yellow)]' : 'bg-[var(--accent)]/10 border-[var(--accent)]/40 text-[var(--accent)]'}`}
      >
        {field.content}
      </div>
    )}
  </Draggable>
);

export default MappedField;
