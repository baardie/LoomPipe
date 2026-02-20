import { Droppable, Draggable } from '@hello-pangea/dnd';

const DestinationColumn = ({ column, fields }) => (
  <div className="flex flex-col h-full">
    <h3 className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider mb-2">{column.title}</h3>
    <Droppable droppableId={column.id}>
      {(provided, snapshot) => (
        <div
          ref={provided.innerRef}
          {...provided.droppableProps}
          className={`flex-1 min-h-32 rounded-lg border p-2 transition-colors ${snapshot.isDraggingOver ? 'bg-[var(--accent)]/10 border-[var(--accent)]/50' : 'bg-[var(--bg-surface)] border-[var(--border)]'}`}
        >
          {fields.map((field, index) => (
            <Draggable key={field.id} draggableId={field.id} index={index}>
              {(prov, snap) => (
                <div
                  ref={prov.innerRef}
                  {...prov.draggableProps}
                  {...prov.dragHandleProps}
                  className={`px-3 py-1.5 mb-1.5 rounded text-xs font-mono border cursor-grab transition-colors ${snap.isDragging ? 'bg-[var(--accent)]/20 border-[var(--accent)] text-[var(--accent)]' : 'bg-[var(--bg-elevated)] border-[var(--border)] text-[var(--text-primary)]'}`}
                >
                  {field.content}
                </div>
              )}
            </Draggable>
          ))}
          {provided.placeholder}
        </div>
      )}
    </Droppable>
  </div>
);

export default DestinationColumn;
