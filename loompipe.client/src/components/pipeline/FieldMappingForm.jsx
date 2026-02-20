import { useState } from 'react';
import { Loader2 } from 'lucide-react';
import useSchema from '../../hooks/useSchema';
import DraggableFieldMapping from './DraggableFieldMapping';

const FieldMappingForm = ({ fieldMappings, setFieldMappings, handleAutomap, handleDryRun, sourceType, sourceConnectionString, destinationSchema }) => {
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState('');
  const { sourceSchema } = useSchema(sourceType, sourceConnectionString);
  const destSchema = destinationSchema ? destinationSchema.split(',').map(s => s.trim()).filter(Boolean) : [];

  const onAutomap = async () => {
    setLoading(true); setError('');
    try { await handleAutomap(); }
    catch (e) { setError(e.message); }
    finally { setLoading(false); }
  };

  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <button
          onClick={onAutomap}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-[var(--border)] text-[var(--text-secondary)] hover:border-[var(--accent)] hover:text-[var(--accent)] rounded transition-colors disabled:opacity-50"
        >
          {loading && <Loader2 size={12} className="animate-spin" />} Automap Fields
        </button>
        <button
          onClick={handleDryRun}
          className="px-3 py-1.5 text-xs border border-[var(--border)] text-[var(--text-secondary)] hover:border-[var(--yellow)] hover:text-[var(--yellow)] rounded transition-colors"
        >
          Dry Run
        </button>
      </div>
      {error && <p className="text-xs text-[var(--red)] mb-2">{error}</p>}
      {sourceSchema && destSchema.length > 0 ? (
        <DraggableFieldMapping
          sourceSchema={sourceSchema}
          destinationSchema={destSchema}
          fieldMappings={fieldMappings}
          setFieldMappings={setFieldMappings}
        />
      ) : (
        <p className="text-xs text-[var(--text-muted)]">
          {!sourceSchema ? 'Select a source to see available fields.' : 'Enter a destination schema to map fields.'}
        </p>
      )}
    </div>
  );
};

export default FieldMappingForm;
