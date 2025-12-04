import React, { useState } from 'react';
import {
  Box,
  Typography,
  Paper,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Chip,
  IconButton,
  TextField,
  Button,
  Alert,
  Grid,
  Card,
  CardContent,
} from '@mui/material';
import {
  ExpandMore,
  Edit,
  Save,
  Cancel,
  Add,
  Delete,
  Schema as SchemaIcon,
} from '@mui/icons-material';

import { ColumnInfo } from '../../types';
import { formatColumnName } from '../../utils/formatting';

interface SchemaViewerProps {
  columns: ColumnInfo[];
  query?: string;
  sessionId: string;
  readonly?: boolean;
}

const SchemaViewer: React.FC<SchemaViewerProps> = ({ 
  columns, 
  query, 
  sessionId, 
  readonly = false 
}) => {
  const [editingColumn, setEditingColumn] = useState<string | null>(null);
  const [editedColumn, setEditedColumn] = useState<Partial<ColumnInfo>>({});

  const handleEditColumn = (column: ColumnInfo) => {
    setEditingColumn(column.name);
    setEditedColumn({ ...column });
  };

  const handleSaveColumn = () => {
    // Implementation would save the edited column
    console.log('Save column:', editedColumn);
    setEditingColumn(null);
    setEditedColumn({});
  };

  const handleCancelEdit = () => {
    setEditingColumn(null);
    setEditedColumn({});
  };

  const handleAddColumn = () => {
    // Implementation would add a new column
    console.log('Add new column');
  };

  const handleDeleteColumn = (columnName: string) => {
    // Implementation would delete the column
    console.log('Delete column:', columnName);
  };

  return (
    <Box>
      {/* Query Display */}
      {query && (
        <Paper sx={{ p: 3, mb: 3 }}>
          <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center' }}>
            <SchemaIcon sx={{ mr: 1 }} />
            Research Query
          </Typography>
          <Typography variant="body1" color="text.secondary">
            {query}
          </Typography>
        </Paper>
      )}

      {/* Schema Overview */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="h6">
            Schema Overview ({columns.length} columns)
          </Typography>
          
          {!readonly && (
            <Button
              startIcon={<Add />}
              variant="outlined"
              onClick={handleAddColumn}
            >
              Add Column
            </Button>
          )}
        </Box>

        <Grid container spacing={2}>
          {columns.map((column, index) => (
            <Grid item xs={12} sm={6} md={4} key={column.name}>
              <Card variant="outlined" sx={{ height: '100%' }}>
                <CardContent>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1 }}>
                    <Typography variant="h6" component="div" sx={{ fontSize: '1rem' }}>
                      {formatColumnName(column.name)}
                    </Typography>
                    
                    {!readonly && (
                      <Box>
                        <IconButton 
                          size="small" 
                          onClick={() => handleEditColumn(column)}
                          title="Edit column"
                        >
                          <Edit fontSize="small" />
                        </IconButton>
                        <IconButton 
                          size="small" 
                          color="error"
                          onClick={() => handleDeleteColumn(column.name)}
                          title="Delete column"
                        >
                          <Delete fontSize="small" />
                        </IconButton>
                      </Box>
                    )}
                  </Box>

                  {column.data_type && (
                    <Chip label={column.data_type} size="small" color="primary" sx={{ mb: 1 }} />
                  )}

                  {column.definition && (
                    <Typography variant="body2" color="text.secondary" paragraph>
                      <strong>Definition:</strong> {column.definition}
                    </Typography>
                  )}

                  {column.rationale && (
                    <Typography variant="body2" color="text.secondary" paragraph>
                      <strong>Rationale:</strong> {column.rationale}
                    </Typography>
                  )}

                  {(column.non_null_count !== undefined || column.unique_count !== undefined) && (
                    <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                      {column.non_null_count !== undefined && (
                        <Chip 
                          label={`${column.non_null_count} non-null`} 
                          size="small" 
                          variant="outlined" 
                        />
                      )}
                      {column.unique_count !== undefined && (
                        <Chip 
                          label={`${column.unique_count} unique`} 
                          size="small" 
                          variant="outlined" 
                        />
                      )}
                    </Box>
                  )}
                </CardContent>
              </Card>
            </Grid>
          ))}
        </Grid>
      </Paper>

      {/* Detailed Schema */}
      <Paper sx={{ p: 3 }}>
        <Typography variant="h6" gutterBottom>
          Detailed Schema
        </Typography>

        {columns.map((column, index) => (
          <Accordion key={column.name}>
            <AccordionSummary expandIcon={<ExpandMore />}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, width: '100%' }}>
                <Typography variant="subtitle1" sx={{ fontWeight: 500 }}>
                  {formatColumnName(column.name)}
                </Typography>
                
                {column.data_type && (
                  <Chip label={column.data_type} size="small" color="primary" />
                )}
                
                {(column.non_null_count !== undefined || column.unique_count !== undefined) && (
                  <Box sx={{ display: 'flex', gap: 0.5, ml: 'auto', mr: 2 }}>
                    {column.non_null_count !== undefined && (
                      <Chip 
                        label={`${column.non_null_count} values`} 
                        size="small" 
                        variant="outlined" 
                      />
                    )}
                    {column.unique_count !== undefined && (
                      <Chip 
                        label={`${column.unique_count} unique`} 
                        size="small" 
                        variant="outlined" 
                      />
                    )}
                  </Box>
                )}
              </Box>
            </AccordionSummary>
            
            <AccordionDetails>
              {editingColumn === column.name ? (
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <TextField
                    label="Column Name"
                    value={editedColumn.name || ''}
                    onChange={(e) => setEditedColumn(prev => ({ ...prev, name: e.target.value }))}
                    fullWidth
                  />
                  
                  <TextField
                    label="Definition"
                    value={editedColumn.definition || ''}
                    onChange={(e) => setEditedColumn(prev => ({ ...prev, definition: e.target.value }))}
                    multiline
                    rows={2}
                    fullWidth
                  />
                  
                  <TextField
                    label="Rationale"
                    value={editedColumn.rationale || ''}
                    onChange={(e) => setEditedColumn(prev => ({ ...prev, rationale: e.target.value }))}
                    multiline
                    rows={3}
                    fullWidth
                  />

                  <Box sx={{ display: 'flex', gap: 1, justifyContent: 'flex-end' }}>
                    <Button 
                      onClick={handleCancelEdit}
                      startIcon={<Cancel />}
                    >
                      Cancel
                    </Button>
                    <Button 
                      onClick={handleSaveColumn}
                      variant="contained"
                      startIcon={<Save />}
                    >
                      Save
                    </Button>
                  </Box>
                </Box>
              ) : (
                <Box>
                  {column.definition && (
                    <Box sx={{ mb: 2 }}>
                      <Typography variant="subtitle2" color="primary">
                        Definition:
                      </Typography>
                      <Typography variant="body2">
                        {column.definition}
                      </Typography>
                    </Box>
                  )}

                  {column.rationale && (
                    <Box sx={{ mb: 2 }}>
                      <Typography variant="subtitle2" color="primary">
                        Rationale:
                      </Typography>
                      <Typography variant="body2">
                        {column.rationale}
                      </Typography>
                    </Box>
                  )}

                  {!readonly && (
                    <Box sx={{ display: 'flex', justifyContent: 'flex-end' }}>
                      <Button
                        startIcon={<Edit />}
                        onClick={() => handleEditColumn(column)}
                        size="small"
                      >
                        Edit Column
                      </Button>
                    </Box>
                  )}
                </Box>
              )}
            </AccordionDetails>
          </Accordion>
        ))}
      </Paper>

      {!readonly && (
        <Alert severity="info" sx={{ mt: 2 }}>
          Schema editing is available in Phase 2. Changes will trigger re-extraction of values.
        </Alert>
      )}
    </Box>
  );
};

export default SchemaViewer;