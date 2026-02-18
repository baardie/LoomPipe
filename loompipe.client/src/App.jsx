import React, { useState, useEffect } from 'react';
import { CircularProgress, Container, Box, Typography, Button } from '@mui/material';
import Header from './components/Header';
import PipelineList from './components/PipelineList';
import PipelineForm from './components/PipelineForm';
import ConnectionsPage from './pages/ConnectionsPage';
import LoginPage from './pages/LoginPage';
import PipelineDetailPage from './pages/PipelineDetailPage';
import ProfileDetailPage from './pages/ProfileDetailPage';
import UsersPage from './pages/UsersPage';
import AnalyticsPage from './pages/AnalyticsPage';
import RoleGuard from './components/auth/RoleGuard';
import Footer from './components/Footer';
import { useAuth } from './contexts/AuthContext';

function App() {
    const { user, loading, authFetch } = useAuth();
    const [currentPage, setCurrentPage] = useState('pipelines');
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
            <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '100vh' }}>
                <CircularProgress />
            </Box>
        );
    }

    if (!user) return <LoginPage />;

    const handleSavePipeline = async (pipeline) => {
        if (pipeline.id) {
            await authFetch(`/api/pipelines/${pipeline.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(pipeline),
            });
        } else {
            await authFetch('/api/pipelines', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(pipeline),
            });
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
        <div>
            <Header currentPage={currentPage} onNavigate={handleNavigate} />

            {currentPage === 'connections' && (
                <ConnectionsPage onProfileClick={handleProfileClick} />
            )}

            {currentPage === 'analytics' && <AnalyticsPage />}

            {currentPage === 'users' && <UsersPage />}

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

            {currentPage === 'pipelines' && (
                <Container>
                    <Box sx={{ my: 4 }}>
                        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                            <Typography variant="h4" component="h1" gutterBottom>
                                Pipelines
                            </Typography>
                            <RoleGuard roles={['Admin']}>
                                <Button
                                    variant="contained"
                                    color="primary"
                                    onClick={() => { setEditingPipeline(null); setShowForm(true); }}
                                >
                                    Create Pipeline
                                </Button>
                            </RoleGuard>
                        </Box>

                        {!showForm && (
                            <PipelineList
                                pipelines={pipelines}
                                onEdit={handleEdit}
                                onDelete={handleDelete}
                                onRowClick={handlePipelineRowClick}
                            />
                        )}

                        {showForm && (
                            <PipelineForm
                                onSave={handleSavePipeline}
                                onCancel={handleCancel}
                                pipeline={editingPipeline}
                            />
                        )}
                    </Box>
                </Container>
            )}
            <Footer />
        </div>
    );
}

export default App;
