import { Droppable, Draggable } from '@hello-pangea/dnd';
import MappedField from './MappedField';

const SourceField = ({ field, mappedField }) => (
  <div className="flex items-center gap-2 mb-2 p-2 bg-[var(--bg-subtle)] border border-[var(--border)] rounded">
    <div className="flex-1 text-xs font-mono text-[var(--text-primary)] truncate">{field.content}</div>
    <Droppable droppableId={field.id} isDropDisabled={!!mappedField}>
      {(provided, snapshot) => (
        <div
          ref={provided.innerRef}
          {...provided.droppableProps}
          className={`flex-1 min-h-8 rounded border px-2 py-1 flex items-center transition-colors ${snapshot.isDraggingOver ? 'bg-[var(--accent)]/10 border-[var(--accent)]/50' : 'bg-[var(--bg-elevated)] border-[var(--border)] border-dashed'}`}
        >
          {mappedField
            ? <MappedField field={mappedField} sourceFieldId={field.id} />
            : <span className="text-xs text-[var(--text-muted)]">Drop here</span>
          }
          {provided.placeholder}
        </div>
      )}
    </Droppable>
  </div>
);

export default SourceField;
