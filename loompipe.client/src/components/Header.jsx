import React from 'react';
import { AppBar, Toolbar, Typography, Button, Box, Chip } from '@mui/material';
import { useAuth } from '../contexts/AuthContext';

const Header = ({ currentPage, onNavigate }) => {
    const { user, logout, isAdmin, isUser } = useAuth();

    return (
        <AppBar position="static">
            <Toolbar>
                <Typography variant="h6" component="div" sx={{ flexGrow: 0, mr: 3 }}>
                    LoomPipe
                </Typography>

                <Box sx={{ display: 'flex', gap: 1, flexGrow: 1 }}>
                    <Button
                        color="inherit"
                        variant={currentPage === 'pipelines' ? 'outlined' : 'text'}
                        onClick={() => onNavigate('pipelines')}
                    >
                        Pipelines
                    </Button>
                    {isUser && (
                        <Button
                            color="inherit"
                            variant={currentPage === 'connections' ? 'outlined' : 'text'}
                            onClick={() => onNavigate('connections')}
                        >
                            Connections
                        </Button>
                    )}
                    {isUser && (
                        <Button
                            color="inherit"
                            variant={currentPage === 'analytics' ? 'outlined' : 'text'}
                            onClick={() => onNavigate('analytics')}
                        >
                            Analytics
                        </Button>
                    )}
                    {isAdmin && (
                        <Button
                            color="inherit"
                            variant={currentPage === 'users' ? 'outlined' : 'text'}
                            onClick={() => onNavigate('users')}
                        >
                            Users
                        </Button>
                    )}
                </Box>

                {user && (
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Chip
                            label={`${user.username} \u00b7 ${user.role}`}
                            size="small"
                            sx={{ color: 'white', borderColor: 'rgba(255,255,255,0.5)', border: '1px solid' }}
                        />
                        <Button color="inherit" size="small" onClick={logout}>
                            Logout
                        </Button>
                    </Box>
                )}
            </Toolbar>
        </AppBar>
    );
};

export default Header;
