import { useState } from 'react';
import { ChevronDown } from 'lucide-react';
import ConnectorPicker from './ConnectorPicker';
import { getConnectorMeta } from '../../data/connectorRegistry';

const ConnectorPickerButton = ({ value, onChange, mode = 'all', placeholder = 'Select connector...', title }) => {
  const [open, setOpen] = useState(false);
  const connector = value ? getConnectorMeta(value) : null;
  const Icon = connector?.icon;

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="w-full flex items-center gap-3 bg-[var(--bg-elevated)] border border-[var(--border)] rounded px-3 py-2 text-sm text-left hover:border-[var(--accent)] transition-colors cursor-pointer"
      >
        {connector && value ? (
          <>
            <div
              className="w-6 h-6 rounded flex items-center justify-center flex-shrink-0"
              style={{ backgroundColor: connector.color + '20' }}
            >
              <Icon size={14} style={{ color: connector.color }} />
            </div>
            <span className="text-[var(--text-primary)] truncate">{connector.label}</span>
          </>
        ) : (
          <span className="text-[var(--text-muted)]">{placeholder}</span>
        )}
        <ChevronDown size={14} className="ml-auto text-[var(--text-muted)] flex-shrink-0" />
      </button>

      <ConnectorPicker
        open={open}
        onClose={() => setOpen(false)}
        onSelect={onChange}
        value={value}
        mode={mode}
        title={title}
      />
    </>
  );
};

export default ConnectorPickerButton;
