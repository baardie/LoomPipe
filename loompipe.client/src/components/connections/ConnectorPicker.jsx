import { useState, useMemo, useEffect, useRef } from 'react';
import { Search, X } from 'lucide-react';
import { ALL_CONNECTORS, CONNECTOR_CATEGORIES, getConnectorsByCategory } from '../../data/connectorRegistry';

// ── Connector card ───────────────────────────────────────────────────────────
const ConnectorCard = ({ connector, selected, onClick }) => {
  const Icon = connector.icon;
  return (
    <button
      type="button"
      onClick={onClick}
      className={`flex items-center gap-3 px-3 py-2.5 rounded-lg border text-left transition-colors cursor-pointer
        ${selected
          ? 'border-[var(--accent)] bg-[var(--accent)]/10'
          : 'border-[var(--border)] hover:border-[var(--accent)]/60 bg-[var(--bg-elevated)]'
        }`}
    >
      <div
        className="w-8 h-8 rounded-md flex items-center justify-center flex-shrink-0"
        style={{ backgroundColor: connector.color + '20' }}
      >
        <Icon size={16} style={{ color: connector.color }} />
      </div>
      <span className="text-sm text-[var(--text-primary)] truncate">{connector.label}</span>
    </button>
  );
};

// ── Main picker modal ────────────────────────────────────────────────────────
const ConnectorPicker = ({ open, onClose, onSelect, value, mode = 'all', title = 'Select Connector' }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [activeCategory, setActiveCategory] = useState('popular');
  const searchRef = useRef(null);

  // Reset state when modal opens
  useEffect(() => {
    if (open) {
      setSearchQuery('');
      setActiveCategory('popular');
      setTimeout(() => searchRef.current?.focus(), 50);
    }
  }, [open]);

  // Close on Escape
  useEffect(() => {
    if (!open) return;
    const handler = (e) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [open, onClose]);

  // Filter categories that have at least one connector for this mode
  const visibleCategories = useMemo(() =>
    CONNECTOR_CATEGORIES.filter(cat => {
      if (cat.id === 'all' || cat.id === 'popular') return true;
      return ALL_CONNECTORS.some(c =>
        c.category === cat.id &&
        (mode === 'all' || c.capabilities[mode])
      );
    }),
    [mode]
  );

  // Filter connectors by mode, category, and search query
  const filteredConnectors = useMemo(() => {
    const effectiveCategory = searchQuery.trim() ? 'all' : activeCategory;
    let connectors = getConnectorsByCategory(effectiveCategory, mode);

    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase().trim();
      connectors = connectors.filter(c =>
        c.label.toLowerCase().includes(q) ||
        c.value.toLowerCase().includes(q) ||
        c.tags.some(t => t.includes(q))
      );
    }

    return connectors;
  }, [mode, activeCategory, searchQuery]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={onClose}>
      <div
        className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl w-full max-w-2xl max-h-[85vh] flex flex-col shadow-2xl"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-[var(--border)]">
          <h2 className="text-base font-semibold text-[var(--text-primary)]">{title}</h2>
          <button onClick={onClose} className="text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors">
            <X size={18} />
          </button>
        </div>

        {/* Search bar */}
        <div className="px-5 pt-4">
          <div className="relative">
            <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-[var(--text-muted)]" />
            <input
              ref={searchRef}
              type="text"
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
              placeholder="Search connectors..."
              className="w-full bg-[var(--bg-elevated)] border border-[var(--border)] rounded-lg pl-9 pr-3 py-2 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors"
            />
          </div>
        </div>

        {/* Category chips */}
        <div className="px-5 pt-3 pb-2 flex gap-1.5 overflow-x-auto scrollbar-none">
          {visibleCategories.map(cat => (
            <button
              key={cat.id}
              type="button"
              onClick={() => { setActiveCategory(cat.id); setSearchQuery(''); }}
              className={`px-3 py-1 rounded-full text-xs font-medium whitespace-nowrap transition-colors flex-shrink-0
                ${(searchQuery.trim() ? 'all' : activeCategory) === cat.id
                  ? 'bg-[var(--accent)] text-white'
                  : 'bg-[var(--bg-elevated)] text-[var(--text-muted)] hover:text-[var(--text-primary)] border border-[var(--border)]'
                }`}
            >
              {cat.label}
            </button>
          ))}
        </div>

        {/* Connector grid */}
        <div className="flex-1 overflow-y-auto px-5 pb-5 pt-2">
          {filteredConnectors.length > 0 ? (
            <div className="grid grid-cols-3 gap-2.5">
              {filteredConnectors.map(c => (
                <ConnectorCard
                  key={c.value}
                  connector={c}
                  selected={c.value === value}
                  onClick={() => { onSelect(c.value); onClose(); }}
                />
              ))}
            </div>
          ) : (
            <div className="text-center py-16 text-[var(--text-muted)] text-sm">
              No connectors found{searchQuery.trim() ? ` matching "${searchQuery}"` : ''}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ConnectorPicker;
