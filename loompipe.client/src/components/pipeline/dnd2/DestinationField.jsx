import { Draggable } from '@hello-pangea/dnd';

const DestinationField = ({ field, index }) => (
  <Draggable draggableId={field.id} index={index}>
    {(provided, snapshot) => (
      <div
        ref={provided.innerRef}
        {...provided.draggableProps}
        {...provided.dragHandleProps}
        className={`px-3 py-1.5 mb-1.5 rounded text-xs font-mono border cursor-grab transition-colors ${snapshot.isDragging ? 'bg-[var(--accent)]/20 border-[var(--accent)] text-[var(--accent)]' : 'bg-[var(--bg-elevated)] border-[var(--border)] text-[var(--text-primary)]'}`}
      >
        {field.content}
      </div>
    )}
  </Draggable>
);

export default DestinationField;
