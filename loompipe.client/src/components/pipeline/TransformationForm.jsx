const TransformationForm = ({ transformations, setTransformations }) => {
  const value = transformations?.map(t => t.expression ?? t).join('\n') ?? '';
  const handleChange = (e) => {
    const lines = e.target.value.split('\n').filter(l => l.trim());
    setTransformations(lines.map(l => ({ expression: l })));
  };
  return (
    <div>
      <label className="block text-xs text-[var(--text-muted)] mb-1">Transformation Expressions</label>
      <textarea
        value={value}
        onChange={handleChange}
        rows={5}
        placeholder="One expression per line, e.g. UPPER(name)"
        className="w-full bg-[var(--bg-elevated)] border border-[var(--border)] rounded px-3 py-2 text-sm text-[var(--text-primary)] font-mono placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors resize-y min-h-24"
      />
      <p className="text-xs text-[var(--text-muted)] mt-1">Enter one transformation expression per line.</p>
    </div>
  );
};

export default TransformationForm;
