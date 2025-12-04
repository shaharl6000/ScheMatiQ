import React from 'react';
import {
  Box,
  Typography,
  Alert,
  Chip,
  Paper,
  Grid,
  LinearProgress,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
} from '@mui/material';
import {
  CheckCircle,
  Error,
  Warning,
  Info,
  Schema as SchemaIcon,
  TableView,
} from '@mui/icons-material';

import { CompatibilityCheck, SchemaValidationResult, FileValidationResult } from '../../types';

interface CompatibilityDisplayProps {
  schemaValidation: SchemaValidationResult;
  dataValidation: FileValidationResult;
  compatibility: CompatibilityCheck;
}

const CompatibilityDisplay: React.FC<CompatibilityDisplayProps> = ({
  schemaValidation,
  dataValidation,
  compatibility,
}) => {
  const getCompatibilityColor = (score: number) => {
    if (score >= 100) return 'success';
    if (score >= 75) return 'warning';
    return 'error';
  };

  const getCompatibilityLabel = (score: number, isCompatible: boolean) => {
    if (isCompatible) return 'Perfect Match';
    if (score >= 75) return 'Partial Match';
    if (score >= 50) return 'Poor Match';
    return 'No Match';
  };

  return (
    <Box>
      {/* Overall Compatibility Status */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 2 }}>
          {compatibility.is_compatible ? (
            <CheckCircle color="success" sx={{ fontSize: 32 }} />
          ) : (
            <Error color="error" sx={{ fontSize: 32 }} />
          )}
          <Box sx={{ flexGrow: 1 }}>
            <Typography variant="h6">
              Schema Compatibility: {getCompatibilityLabel(compatibility.compatibility_score, compatibility.is_compatible)}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {compatibility.matching_columns.length} of {compatibility.schema_count} schema columns found in data
            </Typography>
          </Box>
        </Box>

        {/* Compatibility Score Bar */}
        <Box sx={{ mb: 2 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}>
            <Typography variant="body2">Compatibility Score</Typography>
            <Typography variant="body2">{compatibility.compatibility_score.toFixed(1)}%</Typography>
          </Box>
          <LinearProgress
            variant="determinate"
            value={compatibility.compatibility_score}
            color={getCompatibilityColor(compatibility.compatibility_score)}
            sx={{ height: 8, borderRadius: 1 }}
          />
        </Box>

        {/* Statistics */}
        <Grid container spacing={2}>
          <Grid item xs={4}>
            <Chip
              icon={<SchemaIcon />}
              label={`${compatibility.schema_count} Schema Columns`}
              color="primary"
              variant="outlined"
              size="small"
            />
          </Grid>
          <Grid item xs={4}>
            <Chip
              icon={<TableView />}
              label={`${compatibility.data_count} Data Columns`}
              color="secondary"
              variant="outlined"
              size="small"
            />
          </Grid>
          <Grid item xs={4}>
            <Chip
              icon={<CheckCircle />}
              label={`${compatibility.matching_columns.length} Matches`}
              color="success"
              variant="outlined"
              size="small"
            />
          </Grid>
        </Grid>
      </Paper>

      {/* Detailed Analysis */}
      <Grid container spacing={3}>
        {/* Matching Columns */}
        {compatibility.matching_columns.length > 0 && (
          <Grid item xs={12} md={4}>
            <Paper sx={{ p: 2, height: '100%' }}>
              <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <CheckCircle color="success" />
                Matching Columns ({compatibility.matching_columns.length})
              </Typography>
              <List dense>
                {compatibility.matching_columns.map((col, index) => (
                  <ListItem key={index} sx={{ py: 0.5 }}>
                    <ListItemIcon sx={{ minWidth: 24 }}>
                      <CheckCircle color="success" fontSize="small" />
                    </ListItemIcon>
                    <ListItemText primary={col} />
                  </ListItem>
                ))}
              </List>
            </Paper>
          </Grid>
        )}

        {/* Missing Columns */}
        {compatibility.missing_in_data.length > 0 && (
          <Grid item xs={12} md={4}>
            <Paper sx={{ p: 2, height: '100%' }}>
              <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Error color="error" />
                Missing in Data ({compatibility.missing_in_data.length})
              </Typography>
              <List dense>
                {compatibility.missing_in_data.map((col, index) => (
                  <ListItem key={index} sx={{ py: 0.5 }}>
                    <ListItemIcon sx={{ minWidth: 24 }}>
                      <Error color="error" fontSize="small" />
                    </ListItemIcon>
                    <ListItemText primary={col} />
                  </ListItem>
                ))}
              </List>
            </Paper>
          </Grid>
        )}

        {/* Extra Columns */}
        {compatibility.extra_in_data.length > 0 && (
          <Grid item xs={12} md={4}>
            <Paper sx={{ p: 2, height: '100%' }}>
              <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Warning color="warning" />
                Extra in Data ({compatibility.extra_in_data.length})
              </Typography>
              <List dense>
                {compatibility.extra_in_data.slice(0, 10).map((col, index) => (
                  <ListItem key={index} sx={{ py: 0.5 }}>
                    <ListItemIcon sx={{ minWidth: 24 }}>
                      <Warning color="warning" fontSize="small" />
                    </ListItemIcon>
                    <ListItemText primary={col} />
                  </ListItem>
                ))}
                {compatibility.extra_in_data.length > 10 && (
                  <ListItem sx={{ py: 0.5 }}>
                    <ListItemText 
                      primary={`... and ${compatibility.extra_in_data.length - 10} more columns`}
                      sx={{ fontStyle: 'italic' }}
                    />
                  </ListItem>
                )}
              </List>
            </Paper>
          </Grid>
        )}
      </Grid>

      {/* Detailed Errors and Suggestions */}
      {compatibility.detailed_errors.length > 0 && (
        <Alert severity="error" sx={{ mt: 3 }}>
          <Typography variant="subtitle2" gutterBottom>Issues Found:</Typography>
          <ul style={{ margin: 0, paddingLeft: '20px' }}>
            {compatibility.detailed_errors.map((error, index) => (
              <li key={index}>{error}</li>
            ))}
          </ul>
        </Alert>
      )}

      {compatibility.suggestions.length > 0 && (
        <Alert severity="info" sx={{ mt: 2 }}>
          <Typography variant="subtitle2" gutterBottom>Suggestions:</Typography>
          <ul style={{ margin: 0, paddingLeft: '20px' }}>
            {compatibility.suggestions.map((suggestion, index) => (
              <li key={index}>{suggestion}</li>
            ))}
          </ul>
        </Alert>
      )}

      {/* Schema Query Display */}
      {schemaValidation.query && (
        <Paper sx={{ p: 3, mt: 3, bgcolor: 'primary.light', color: 'primary.contrastText' }}>
          <Typography variant="h6" gutterBottom>
            Research Query from Schema:
          </Typography>
          <Typography variant="body1" sx={{ fontStyle: 'italic' }}>
            "{schemaValidation.query}"
          </Typography>
        </Paper>
      )}
    </Box>
  );
};

export default CompatibilityDisplay;