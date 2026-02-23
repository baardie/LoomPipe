import React from 'react';
import { Search, Plus } from 'lucide-react';
import NotificationCenter from './NotificationCenter';

const Topbar = ({ onNewPipeline, onNavigateToPipeline }) => {
  return (
    <header className="h-16 flex-shrink-0 flex items-center justify-between px-8 bg-slate-950/80 backdrop-blur-md border-b border-slate-800 sticky top-0 z-10">
      {/* Search bar */}
      <div className="flex items-center gap-3 bg-slate-900 border border-slate-800 px-3 py-2 rounded-lg w-80">
        <Search size={15} className="text-slate-500 flex-shrink-0" />
        <input
          type="text"
          placeholder="Search LoomPipe assets..."
          className="bg-transparent border-none outline-none text-sm w-full text-slate-300 placeholder-slate-600"
        />
      </div>

      {/* Right actions */}
      <div className="flex items-center gap-4">
        {/* Engine status */}
        <div className="flex items-center gap-1.5 px-3 py-1 bg-emerald-500/10 text-emerald-500 rounded-full border border-emerald-500/20">
          <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
          <span className="text-xs font-bold">Loom Engine: Stable</span>
        </div>

        {/* Notification bell */}
        <NotificationCenter onNavigateToPipeline={onNavigateToPipeline} />

        {/* New Pipe button */}
        {onNewPipeline && (
          <button
            onClick={onNewPipeline}
            className="bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded-lg text-sm font-semibold flex items-center gap-2 transition-all shadow-lg shadow-indigo-600/20"
          >
            <Plus size={16} />
            New Pipe
          </button>
        )}
      </div>
    </header>
  );
};

export default Topbar;
