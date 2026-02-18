import React, { useState } from 'react';
import { Modal, Box, Typography, Table, TableBody, TableCell, TableHead, TableRow, Paper, Tabs, Tab } from '@mui/material';

const style = {
    position: 'absolute',
    top: '50%',
    left: '50%',
    transform: 'translate(-50%, -50%)',
    width: '80%',
    bgcolor: 'background.paper',
    boxShadow: 24,
    p: 4,
};

const TabPanel = (props) => {
    const { children, value, index, ...other } = props;

    return (
        <div
            role="tabpanel"
            hidden={value !== index}
            id={`simple-tabpanel-${index}`}
            aria-labelledby={`simple-tab-${index}`}
            {...other}
        >
            {value === index && (
                <Box sx={{ p: 3 }}>
                    {children}
                </Box>
            )}
        </div>
    );
}

const DataTable = ({ data }) => {
    if (!data || data.length === 0) {
        return <Typography>No data to display.</Typography>;
    }

    return (
        <Paper>
            <Table size="small">
                <TableHead>
                    <TableRow>
                        {Object.keys(data[0] || {}).map((key) => (
                            <TableCell key={key}>{key}</TableCell>
                        ))}
                    </TableRow>
                </TableHead>
                <TableBody>
                    {data.map((row, i) => (
                        <TableRow key={i}>
                            {Object.values(row).map((value, j) => (
                                <TableCell key={j}>{String(value)}</TableCell>
                            ))}
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
        </Paper>
    );
};


const DryRunResultModal = ({ open, onClose, dryRunResult }) => {
    const [tabIndex, setTabIndex] = useState(0);

    const handleChange = (event, newValue) => {
        setTabIndex(newValue);
    };

    return (
        <Modal
            open={open}
            onClose={onClose}
            aria-labelledby="dry-run-result-title"
            aria-describedby="dry-run-result-description"
        >
            <Box sx={style}>
                <Typography id="dry-run-result-title" variant="h6" component="h2">
                    Dry Run Result
                </Typography>
                <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
                    <Tabs value={tabIndex} onChange={handleChange} aria-label="dry run result tabs">
                        <Tab label="Source" />
                        <Tab label="Mapped" />
                        <Tab label="Transformed" />
                    </Tabs>
                </Box>
                <TabPanel value={tabIndex} index={0}>
                    <DataTable data={dryRunResult?.sourcePreview} />
                </TabPanel>
                <TabPanel value={tabIndex} index={1}>
                    <DataTable data={dryRunResult?.mappedPreview} />
                </TabPanel>
                <TabPanel value={tabIndex} index={2}>
                    <DataTable data={dryRunResult?.transformedPreview} />
                </TabPanel>
            </Box>
        </Modal>
    );
};

export default DryRunResultModal;