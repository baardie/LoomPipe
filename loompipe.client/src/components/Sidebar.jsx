import React from 'react';
import {
  Activity,
  Layers,
  Database,
  BarChart2,
  Users,
  Settings,
  LogOut,
  Zap,
} from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import RoleGuard from './auth/RoleGuard';

const NAV_ITEMS = [
  { key: 'dashboard',   label: 'Dashboard',   icon: Activity },
  { key: 'pipelines',   label: 'Pipelines',   icon: Layers },
  { key: 'connections', label: 'Connections', icon: Database },
  { key: 'analytics',  label: 'Analytics',   icon: BarChart2 },
];

const SidebarItem = ({ icon: Icon, label, active, onClick }) => (
  <button
    onClick={onClick}
    className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-all duration-200 text-left ${
      active
        ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/20'
        : 'text-slate-400 hover:bg-slate-800 hover:text-white'
    }`}
  >
    <Icon size={19} />
    <span className="font-medium text-sm">{label}</span>
  </button>
);

const Sidebar = ({ currentPage, onNavigate }) => {
  const { user, logout } = useAuth();
  const initials = user?.username?.slice(0, 2)?.toUpperCase() ?? 'LP';

  return (
    <aside className="w-64 flex-shrink-0 flex flex-col h-screen bg-slate-950 border-r border-slate-800">
      {/* Logo */}
      <div className="flex items-center gap-3 px-5 py-5 mb-4">
        <div className="bg-indigo-600 p-2 rounded-lg flex-shrink-0">
          <Zap size={20} className="text-white" />
        </div>
        <h1 className="text-lg font-bold tracking-tight text-white italic">LoomPipe</h1>
      </div>

      {/* Nav items */}
      <nav className="flex-1 px-3 space-y-1 overflow-y-auto">
        {NAV_ITEMS.map(({ key, label, icon }) => {
          const isActive =
            currentPage === key ||
            (key === 'connections' && currentPage === 'profile-detail') ||
            (key === 'pipelines' && currentPage === 'pipeline-detail');
          return (
            <SidebarItem
              key={key}
              icon={icon}
              label={label}
              active={isActive}
              onClick={() => onNavigate(key)}
            />
          );
        })}

        <div className="pt-6 pb-2 px-4">
          <span className="text-xs font-bold text-slate-700 uppercase tracking-widest">Admin</span>
        </div>

        <RoleGuard roles={['Admin']}>
          <SidebarItem
            icon={Users}
            label="Users"
            active={currentPage === 'users'}
            onClick={() => onNavigate('users')}
          />
          <SidebarItem
            icon={Settings}
            label="Settings"
            active={currentPage === 'settings'}
            onClick={() => onNavigate('settings')}
          />
        </RoleGuard>
      </nav>

      {/* User footer card */}
      <div className="m-3 p-3 bg-slate-900/60 rounded-xl border border-slate-800">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-full bg-gradient-to-tr from-indigo-500 to-purple-600 flex items-center justify-center font-bold text-white text-xs flex-shrink-0">
            {initials}
          </div>
          <div className="flex-1 min-w-0">
            <div className="text-sm font-medium text-white truncate">{user?.username}</div>
            <div className="text-xs text-slate-500 truncate">{user?.role}</div>
          </div>
          <button
            onClick={logout}
            title="Logout"
            className="text-slate-500 hover:text-rose-400 transition-colors flex-shrink-0"
          >
            <LogOut size={14} />
          </button>
        </div>
      </div>
    </aside>
  );
};

export default Sidebar;
