import React from 'react';
import { Routes, Route } from 'react-router-dom';
import { Container, AppBar, Toolbar, Typography, Box } from '@mui/material';

import Landing from './pages/Landing';
import Upload from './pages/Upload';
import QBSDConfig from './pages/QBSDConfig';
import Visualize from './pages/Visualize';

function App() {
  return (
    <Box sx={{ flexGrow: 1 }}>
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            QBSD Visualization
          </Typography>
        </Toolbar>
      </AppBar>
      
      <Container maxWidth="xl" sx={{ mt: 3 }}>
        <Routes>
          <Route path="/" element={<Landing />} />
          <Route path="/upload" element={<Upload />} />
          <Route path="/qbsd" element={<QBSDConfig />} />
          <Route path="/visualize/:sessionId" element={<Visualize />} />
        </Routes>
      </Container>
    </Box>
  );
}

export default App;