import React from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Typography,
  Card,
  CardContent,
  CardActions,
  Button,
  Grid,
  Container,
  Chip,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
} from '@mui/material';
import {
  CloudUpload,
  AutoAwesome,
  CheckCircle,
  Speed,
  Visibility,
  Edit,
} from '@mui/icons-material';

const Landing: React.FC = () => {
  const navigate = useNavigate();

  const uploadFeatures = [
    'Support for CSV and JSON/JSONL files',
    'Automatic data type detection',
    'Data quality analysis',
    'Interactive data exploration',
  ];

  const qbsdFeatures = [
    'AI-powered schema discovery',
    'Multi-document processing',
    'Real-time progress monitoring',
    'Customizable LLM backends',
  ];

  return (
    <Container maxWidth="lg">
      <Box sx={{ textAlign: 'center', mb: 6 }}>
        <Typography variant="h3" component="h1" gutterBottom>
          QBSD Visualization
        </Typography>
        <Typography variant="h6" color="text.secondary" sx={{ mb: 3 }}>
          Interactive visualization and schema editing for Query-Based Schema Discovery
        </Typography>
        <Chip label="Dual Input Options" color="primary" variant="outlined" />
      </Box>

      <Grid container spacing={4} justifyContent="center">
        {/* Upload Option */}
        <Grid item xs={12} md={6}>
          <Card 
            sx={{ 
              height: '100%',
              display: 'flex',
              flexDirection: 'column',
              transition: 'transform 0.2s',
              '&:hover': {
                transform: 'translateY(-4px)',
                boxShadow: 3,
              }
            }}
          >
            <CardContent sx={{ flexGrow: 1 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <CloudUpload color="primary" sx={{ fontSize: 32, mr: 1 }} />
                <Typography variant="h5" component="h2">
                  Upload Existing Data
                </Typography>
              </Box>
              
              <Typography variant="body1" color="text.secondary" paragraph>
                Import your existing datasets for immediate visualization and analysis.
                Perfect for exploring pre-processed data or QBSD outputs.
              </Typography>

              <Typography variant="h6" sx={{ mt: 3, mb: 1 }}>
                Features:
              </Typography>
              <List dense>
                {uploadFeatures.map((feature, index) => (
                  <ListItem key={index} sx={{ py: 0 }}>
                    <ListItemIcon sx={{ minWidth: 36 }}>
                      <CheckCircle color="success" fontSize="small" />
                    </ListItemIcon>
                    <ListItemText 
                      primary={feature}
                      primaryTypographyProps={{ variant: 'body2' }}
                    />
                  </ListItem>
                ))}
              </List>

              <Box sx={{ mt: 2 }}>
                <Chip label="Quick Start" size="small" color="success" variant="outlined" />
                <Chip label="CSV/JSON" size="small" color="info" variant="outlined" sx={{ ml: 1 }} />
              </Box>
            </CardContent>
            
            <CardActions sx={{ p: 3, pt: 0 }}>
              <Button
                variant="contained"
                fullWidth
                size="large"
                startIcon={<CloudUpload />}
                onClick={() => navigate('/upload')}
              >
                Upload Data
              </Button>
            </CardActions>
          </Card>
        </Grid>

        {/* QBSD Option */}
        <Grid item xs={12} md={6}>
          <Card 
            sx={{ 
              height: '100%',
              display: 'flex',
              flexDirection: 'column',
              transition: 'transform 0.2s',
              '&:hover': {
                transform: 'translateY(-4px)',
                boxShadow: 3,
              }
            }}
          >
            <CardContent sx={{ flexGrow: 1 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <AutoAwesome color="secondary" sx={{ fontSize: 32, mr: 1 }} />
                <Typography variant="h5" component="h2">
                  Create with QBSD
                </Typography>
              </Box>
              
              <Typography variant="body1" color="text.secondary" paragraph>
                Run the full QBSD pipeline to discover schemas and extract structured data
                from your document collections using AI-powered analysis.
              </Typography>

              <Typography variant="h6" sx={{ mt: 3, mb: 1 }}>
                Features:
              </Typography>
              <List dense>
                {qbsdFeatures.map((feature, index) => (
                  <ListItem key={index} sx={{ py: 0 }}>
                    <ListItemIcon sx={{ minWidth: 36 }}>
                      <CheckCircle color="success" fontSize="small" />
                    </ListItemIcon>
                    <ListItemText 
                      primary={feature}
                      primaryTypographyProps={{ variant: 'body2' }}
                    />
                  </ListItem>
                ))}
              </List>

              <Box sx={{ mt: 2 }}>
                <Chip label="AI-Powered" size="small" color="secondary" variant="outlined" />
                <Chip label="Real-time" size="small" color="warning" variant="outlined" sx={{ ml: 1 }} />
              </Box>
            </CardContent>
            
            <CardActions sx={{ p: 3, pt: 0 }}>
              <Button
                variant="contained"
                color="secondary"
                fullWidth
                size="large"
                startIcon={<AutoAwesome />}
                onClick={() => navigate('/qbsd')}
              >
                Configure QBSD
              </Button>
            </CardActions>
          </Card>
        </Grid>
      </Grid>

      {/* Common Features */}
      <Box sx={{ mt: 6, textAlign: 'center' }}>
        <Typography variant="h5" gutterBottom>
          Powerful Visualization Features
        </Typography>
        <Grid container spacing={3} justifyContent="center" sx={{ mt: 2 }}>
          <Grid item xs={12} sm={4}>
            <Box sx={{ textAlign: 'center' }}>
              <Visibility color="primary" sx={{ fontSize: 48, mb: 1 }} />
              <Typography variant="h6">Interactive Visualization</Typography>
              <Typography variant="body2" color="text.secondary">
                Explore your data with dynamic tables, charts, and schema views
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Box sx={{ textAlign: 'center' }}>
              <Edit color="primary" sx={{ fontSize: 48, mb: 1 }} />
              <Typography variant="h6">Schema Editing</Typography>
              <Typography variant="body2" color="text.secondary">
                Modify schemas with real-time validation and re-extraction
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={12} sm={4}>
            <Box sx={{ textAlign: 'center' }}>
              <Speed color="primary" sx={{ fontSize: 48, mb: 1 }} />
              <Typography variant="h6">Performance Optimized</Typography>
              <Typography variant="body2" color="text.secondary">
                Handle large datasets with virtual scrolling and lazy loading
              </Typography>
            </Box>
          </Grid>
        </Grid>
      </Box>
    </Container>
  );
};

export default Landing;