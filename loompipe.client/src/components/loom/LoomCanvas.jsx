// SVG icon paths copied from Lucide source (to avoid foreignObject cross-browser issues)
const DatabasePath = "M12 2C6.48 2 2 3.79 2 6v12c0 2.21 4.48 4 10 4s10-1.79 10-4V6c0-2.21-4.48-4-10-4zm0 2c4.42 0 8 1.34 8 3s-3.58 3-8 3-8-1.34-8-3 3.58-3 8-3zm0 16c-4.42 0-8-1.34-8-3v-2.23C5.61 16.12 8.62 17 12 17s6.39-.88 8-2.23V17c0 1.66-3.58 3-8 3zm0-6c-4.42 0-8-1.34-8-3V9.77C5.61 11.12 8.62 12 12 12s6.39-.88 8-2.23V11c0 1.66-3.58 3-8 3z";
const ShufflePath  = "M16 3h5v5l-1.5-1.5-4.5 4.5-4-4L5 13m14 0v5h-5l1.5-1.5-4.5-4.5 4 4 5.5-6";

const Node = ({ cx, cy, iconPath, label, sublabel, isSelected, onClick, badgeCount }) => (
  <g onClick={onClick} style={{ cursor: 'pointer' }}>
    <rect
      x={cx - 65} y={cy - 45} width={130} height={90} rx={8}
      fill="var(--bg-elevated)"
      stroke={isSelected ? 'var(--accent)' : 'var(--border)'}
      strokeWidth={isSelected ? 2 : 1}
    />
    {isSelected && (
      <rect x={cx - 65} y={cy - 45} width={130} height={90} rx={8}
        fill="var(--accent)" fillOpacity={0.04} />
    )}
    {/* Icon */}
    {iconPath && (
      <g transform={`translate(${cx - 10}, ${cy - 36})`}>
        <path d={iconPath} fill="none" stroke={isSelected ? 'var(--accent)' : 'var(--text-muted)'} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" transform="scale(0.85)" />
      </g>
    )}
    {/* Label */}
    <text x={cx} y={cy + 8} textAnchor="middle" fill={isSelected ? 'var(--text-primary)' : 'var(--text-secondary)'} fontSize={11} fontFamily="monospace" fontWeight={isSelected ? '600' : '400'}>
      {label ? (label.length > 12 ? label.slice(0, 12) + '…' : label) : '–'}
    </text>
    {/* Sublabel */}
    <text x={cx} y={cy + 24} textAnchor="middle" fill="var(--text-muted)" fontSize={9} fontFamily="monospace">
      {sublabel}
    </text>
    {/* Badge */}
    {badgeCount > 0 && (
      <>
        <circle cx={cx + 55} cy={cy - 37} r={10} fill="var(--accent)" />
        <text x={cx + 55} y={cy - 33} textAnchor="middle" fill="white" fontSize={8} fontWeight="bold">{badgeCount}</text>
      </>
    )}
  </g>
);

const LoomCanvas = ({ sourceType, destinationType, fieldMappings, activePanel, onNodeClick }) => {
  const mappingCount = fieldMappings?.length ?? 0;

  return (
    <svg
      width="100%"
      height="100%"
      viewBox="0 0 900 300"
      preserveAspectRatio="xMidYMid meet"
    >
      <defs>
        {/* Dot grid background */}
        <pattern id="loom-grid" width={24} height={24} patternUnits="userSpaceOnUse">
          <circle cx={1} cy={1} r={1} fill="var(--border-muted)" />
        </pattern>
        {/* Arrow marker */}
        <marker id="loom-arrow" viewBox="0 0 10 10" refX={9} refY={5} markerWidth={6} markerHeight={6} orient="auto">
          <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--accent)" fillOpacity={0.7} />
        </marker>
        {/* Glow filter */}
        <filter id="wire-glow">
          <feGaussianBlur in="SourceGraphic" stdDeviation="2" result="blur" />
          <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
        </filter>
      </defs>

      {/* Background */}
      <rect width={900} height={300} fill="var(--bg-base)" />
      <rect width={900} height={300} fill="url(#loom-grid)" />

      {/* Source → Transform connector */}
      <path
        d="M 215,150 C 300,150 360,150 385,150"
        stroke="var(--accent)" strokeWidth={1.5} fill="none"
        markerEnd="url(#loom-arrow)"
        filter="url(#wire-glow)"
        strokeOpacity={0.6}
      />

      {/* Transform → Destination connector */}
      <path
        d="M 515,150 C 600,150 660,150 685,150"
        stroke="var(--accent)" strokeWidth={1.5} fill="none"
        markerEnd="url(#loom-arrow)"
        filter="url(#wire-glow)"
        strokeOpacity={0.6}
      />

      {/* Source node */}
      <Node
        cx={150} cy={150}
        iconPath={DatabasePath}
        label={sourceType || 'Source'}
        sublabel={sourceType ? 'click to configure' : 'not set'}
        isSelected={activePanel === 'source'}
        onClick={() => onNodeClick('source')}
      />

      {/* Transform node */}
      <Node
        cx={450} cy={150}
        iconPath={ShufflePath}
        label="Transform"
        sublabel={mappingCount > 0 ? `${mappingCount} field mapping${mappingCount !== 1 ? 's' : ''}` : 'no mappings'}
        isSelected={activePanel === 'transform'}
        onClick={() => onNodeClick('transform')}
        badgeCount={mappingCount}
      />

      {/* Destination node */}
      <Node
        cx={750} cy={150}
        iconPath={DatabasePath}
        label={destinationType || 'Destination'}
        sublabel={destinationType ? 'click to configure' : 'not set'}
        isSelected={activePanel === 'destination'}
        onClick={() => onNodeClick('destination')}
      />

      {/* Label hints at bottom */}
      <text x={150} y={270} textAnchor="middle" fill="var(--text-muted)" fontSize={9} fontFamily="monospace">SOURCE</text>
      <text x={450} y={270} textAnchor="middle" fill="var(--text-muted)" fontSize={9} fontFamily="monospace">TRANSFORM</text>
      <text x={750} y={270} textAnchor="middle" fill="var(--text-muted)" fontSize={9} fontFamily="monospace">DESTINATION</text>
    </svg>
  );
};

export default LoomCanvas;
