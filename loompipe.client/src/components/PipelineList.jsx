import React from 'react';
import { List, ListItemButton, ListItemText, IconButton, Paper } from '@mui/material';
import { Edit, Delete } from '@mui/icons-material';
import RoleGuard from './auth/RoleGuard';

const PipelineList = ({ pipelines, onEdit, onDelete, onRowClick }) => {
    return (
        <Paper>
            <List>
                {pipelines.map((pipeline) => (
                    <ListItemButton
                        key={pipeline.id}
                        onClick={() => onRowClick && onRowClick(pipeline.id)}
                        sx={{ pr: 10 }}
                    >
                        <ListItemText primary={pipeline.name} />
                        <RoleGuard roles={['Admin']}>
                            <IconButton
                                edge="end"
                                aria-label="edit"
                                onClick={(e) => { e.stopPropagation(); onEdit(pipeline); }}
                            >
                                <Edit />
                            </IconButton>
                            <IconButton
                                edge="end"
                                aria-label="delete"
                                onClick={(e) => { e.stopPropagation(); onDelete(pipeline.id); }}
                            >
                                <Delete />
                            </IconButton>
                        </RoleGuard>
                    </ListItemButton>
                ))}
            </List>
        </Paper>
    );
};

export default PipelineList;
