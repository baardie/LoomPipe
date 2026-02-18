import React from 'react';

const MappingLine = ({ from, to }) => {
    if (!from || !to) {
        return null;
    }

    return (
        <svg style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: '100%', pointerEvents: 'none' }}>
            <line x1={from.x} y1={from.y} x2={to.x} y2={to.y} stroke="black" strokeWidth="2" />
        </svg>
    );
};

export default MappingLine;
