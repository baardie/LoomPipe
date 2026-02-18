import React from 'react';
import { Box, Divider, Link, Typography } from '@mui/material';
import FavoriteIcon from '@mui/icons-material/Favorite';

// Update GITHUB_URL after the repository is published
const GITHUB_URL = 'https://github.com/baardie/LoomPipe';

const Footer = () => (
    <Box
        component="footer"
        sx={{
            mt: 8,
            py: 3,
            px: 2,
            borderTop: '1px solid',
            borderColor: 'divider',
            textAlign: 'center',
        }}
    >
        <Typography variant="body2" color="text.secondary">
            Built by{' '}
            <Link href={`mailto:lukebaard@outlook.com`} underline="hover" color="inherit">
                Luke Baard
            </Link>
            {' · '}
            <Link href={GITHUB_URL} target="_blank" rel="noopener noreferrer" underline="hover" color="inherit">
                GitHub
            </Link>
            {' · '}
            <Link
                href="https://www.paypal.com/paypalme/baardie"
                target="_blank"
                rel="noopener noreferrer"
                underline="hover"
                color="inherit"
                sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.4 }}
            >
                <FavoriteIcon sx={{ fontSize: 13, color: '#e53935', verticalAlign: 'middle' }} />
                Buy me a coffee
            </Link>
        </Typography>
    </Box>
);

export default Footer;
