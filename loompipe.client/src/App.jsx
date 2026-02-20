import React, { useState, useEffect } from 'react';
import { Loader2 } from 'lucide-react';
import Sidebar from './components/Sidebar';
import Topbar from './components/Topbar';
import ErrorBoundary from './components/ErrorBoundary';
import ConnectionsPage from './pages/ConnectionsPage';
import LoginPage from './pages/LoginPage';
import PipelineDetailPage from './pages/PipelineDetailPage';
import ProfileDetailPage from './pages/ProfileDetailPage';
import UsersPage from './pages/UsersPage';
import AnalyticsPage from './pages/AnalyticsPage';
import { useAuth } from './contexts/AuthContext';

// Pages that will be progressively migrated — placeholders until their phase
const DashboardPage   = React.lazy(() => import('./pages/DashboardPage'));
const PipelinesPage   = React.lazy(() => import('./pages/PipelinesPage'));
const SettingsPage    = React.lazy(() => import('./pages/SettingsPage'));
const LoomEditor      = React.lazy(() => import('./components/loom/LoomEditor'));

// Fallback for lazy-loaded pages not yet created
const PageFallback = ({ name }) => (
  <div className="flex items-center justify-center h-64">
    <Loader2 size={20} className="animate-spin text-[var(--accent)] mr-2" />
    <span className="text-[var(--text-secondary)] text-sm">Loading {name}…</span>
  </div>
);

const SafeLazy = ({ component: Component, fallbackName, ...props }) => (
  <React.Suspense fallback={<PageFallback name={fallbackName} />}>
    <Component {...props} />
  </React.Suspense>
);

function App() {
  const { user, loading, authFetch } = useAuth();
  const [currentPage, setCurrentPage] = useState('dashboard');
  const [pipelines, setPipelines] = useState([]);
  const [showForm, setShowForm] = useState(false);
  const [editingPipeline, setEditingPipeline] = useState(null);
  const [selectedPipelineId, setSelectedPipelineId] = useState(null);
  const [selectedProfileId, setSelectedProfileId] = useState(null);

  const fetchPipelines = async () => {
    try {
      const response = await authFetch('/api/pipelines');
      if (response.ok) {
        const data = await response.json();
        setPipelines(data);
      }
    } catch { /* ignore */ }
  };

  useEffect(() => {
    if (user) fetchPipelines();
  }, [user]);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-[var(--bg-base)]">
        <Loader2 size={24} className="animate-spin text-[var(--accent)]" />
      </div>
    );
  }

  if (!user) return <LoginPage />;

  const handleSavePipeline = async (pipeline) => {
    const resp = pipeline.id
      ? await authFetch(`/api/pipelines/${pipeline.id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(pipeline),
        })
      : await authFetch('/api/pipelines', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(pipeline),
        });

    if (!resp.ok) {
      const text = await resp.text().catch(() => '');
      throw new Error(`Save failed (${resp.status}): ${text || resp.statusText}`);
    }

    fetchPipelines();
    setShowForm(false);
    setEditingPipeline(null);
  };

  const handleEdit = (pipeline) => {
    setEditingPipeline(pipeline);
    setShowForm(true);
  };

  const handleDelete = async (id) => {
    await authFetch(`/api/pipelines/${id}`, { method: 'DELETE' });
    fetchPipelines();
  };

  const handleCancel = () => {
    setShowForm(false);
    setEditingPipeline(null);
  };

  const handleNavigate = (page) => {
    setCurrentPage(page);
    setShowForm(false);
    setEditingPipeline(null);
    setSelectedPipelineId(null);
    setSelectedProfileId(null);
  };

  const handlePipelineRowClick = (id) => {
    setSelectedPipelineId(id);
    setCurrentPage('pipeline-detail');
  };

  const handleProfileClick = (id) => {
    setSelectedProfileId(id);
    setCurrentPage('profile-detail');
  };

  return (
    <ErrorBoundary>
    <div className="flex h-screen overflow-hidden bg-[var(--bg-base)]">
      <Sidebar currentPage={currentPage} onNavigate={handleNavigate} />

      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        <Topbar
          currentPage={currentPage}
          onNewPipeline={() => { setEditingPipeline(null); setShowForm(true); }}
        />

        <main className="flex-1 overflow-y-auto">
          {currentPage === 'dashboard' && (
            <SafeLazy
              component={DashboardPage}
              fallbackName="Dashboard"
              onNewPipeline={() => { setEditingPipeline(null); setShowForm(true); }}
              onNavigate={handleNavigate}
              onPipelineClick={handlePipelineRowClick}
            />
          )}

          {currentPage === 'pipelines' && (
            <SafeLazy
              component={PipelinesPage}
              fallbackName="Pipelines"
              pipelines={pipelines}
              onEdit={handleEdit}
              onDelete={handleDelete}
              onRowClick={handlePipelineRowClick}
              onCreate={() => { setEditingPipeline(null); setShowForm(true); }}
            />
          )}

          {currentPage === 'connections' && (
            <ConnectionsPage onProfileClick={handleProfileClick} />
          )}

          {currentPage === 'analytics' && <AnalyticsPage />}

          {currentPage === 'users' && <UsersPage />}

          {currentPage === 'settings' && (
            <SafeLazy component={SettingsPage} fallbackName="Settings" />
          )}

          {currentPage === 'pipeline-detail' && selectedPipelineId && (
            <PipelineDetailPage
              pipelineId={selectedPipelineId}
              onBack={() => handleNavigate('pipelines')}
              onEdit={(p) => {
                setEditingPipeline(p);
                setShowForm(true);
                setCurrentPage('pipelines');
              }}
            />
          )}

          {currentPage === 'profile-detail' && selectedProfileId && (
            <ProfileDetailPage
              profileId={selectedProfileId}
              onBack={() => handleNavigate('connections')}
              pipelines={pipelines}
            />
          )}
        </main>

      </div>

      {/* Loom Editor — full-screen overlay */}
      {showForm && (
        <SafeLazy
          component={LoomEditor}
          fallbackName="Pipeline Editor"
          onSave={handleSavePipeline}
          onCancel={handleCancel}
          pipeline={editingPipeline ?? undefined}
        />
      )}
    </div>
    </ErrorBoundary>
  );
}

export default App;
