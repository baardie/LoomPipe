import { Droppable } from '@hello-pangea/dnd';
import SourceField from './SourceField';

const SourceColumn = ({ column, fields, mappedFields }) => (
  <div className="flex flex-col h-full">
    <h3 className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider mb-2">{column.title}</h3>
    <div className="flex-1 bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-2 overflow-y-auto">
      {fields.map(field => (
        <SourceField
          key={field.id}
          field={field}
          mappedField={mappedFields.find(mf => mf.sourceFieldId === field.id) || null}
        />
      ))}
    </div>
  </div>
);

export default SourceColumn;
