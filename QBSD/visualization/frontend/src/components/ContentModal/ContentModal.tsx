import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  Chip,
} from '@mui/material';
import { Close } from '@mui/icons-material';

interface ContentModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  content: any;
}

const ContentModal: React.FC<ContentModalProps> = ({ open, onClose, title, content }) => {
  const formatContent = (value: any): React.ReactNode => {
    if (value === null || value === undefined) {
      return <Chip label="null" size="small" variant="outlined" color="default" />;
    }
    
    if (Array.isArray(value)) {
      return (
        <Box>
          <Typography variant="subtitle2" gutterBottom>
            Array ({value.length} items):
          </Typography>
          {value.map((item, index) => (
            <Box key={index} sx={{ mb: 1, pl: 2, borderLeft: '2px solid #e0e0e0' }}>
              <Typography variant="body2">
                <strong>{index}:</strong> {typeof item === 'object' ? JSON.stringify(item, null, 2) : String(item)}
              </Typography>
            </Box>
          ))}
        </Box>
      );
    }
    
    if (typeof value === 'object') {
      return (
        <Box>
          <Typography variant="subtitle2" gutterBottom>
            Object:
          </Typography>
          <Box
            component="pre"
            sx={{
              backgroundColor: '#f5f5f5',
              padding: 2,
              borderRadius: 1,
              overflow: 'auto',
              fontSize: '0.875rem',
              fontFamily: 'monospace',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
            }}
          >
            {JSON.stringify(value, null, 2)}
          </Box>
        </Box>
      );
    }
    
    // For long text content
    return (
      <Typography variant="body1" sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
        {String(value)}
      </Typography>
    );
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
      scroll="paper"
    >
      <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Typography variant="h6">
          {title}
        </Typography>
        <Button onClick={onClose} size="small" startIcon={<Close />}>
          Close
        </Button>
      </DialogTitle>
      
      <DialogContent dividers>
        <Box sx={{ minHeight: 200 }}>
          {formatContent(content)}
        </Box>
      </DialogContent>
      
      <DialogActions>
        <Button onClick={onClose} variant="contained">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ContentModal;