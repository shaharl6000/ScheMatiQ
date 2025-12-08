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
import { CellValue, QBSDAnswerWithExcerpts } from '../../types';

interface ContentModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  content: CellValue;
}

const ContentModal: React.FC<ContentModalProps> = ({ open, onClose, title, content }) => {
  const formatContent = (value: CellValue): React.ReactNode => {
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
    
    if (typeof value === 'object' && value !== null) {
      // Check if this is QBSD format or integrated format: {answer: "...", excerpts: [...]}
      if ('answer' in value && 'excerpts' in value) {
        const qbsdValue = value as QBSDAnswerWithExcerpts;
        const answer = qbsdValue.answer;
        const excerpts = qbsdValue.excerpts || [];
        
        return (
          <Box>
            <Typography variant="h6" gutterBottom color="primary">
              Content:
            </Typography>
            <Box sx={{ mb: 3, p: 2, backgroundColor: '#f0f7ff', borderRadius: 1, border: '1px solid #e3f2fd' }}>
              <Typography variant="body1" sx={{ fontSize: '1.1rem', fontWeight: 500, lineHeight: 1.6 }}>
                {String(answer)}
              </Typography>
            </Box>
            
            {excerpts.length > 0 && (
              <>
                <Typography variant="h6" gutterBottom color="secondary">
                  {excerpts.length === 1 ? 'Supporting Evidence:' : `Supporting Evidence (${excerpts.length} sources):`}
                </Typography>
                {excerpts.map((excerpt: string, index: number) => (
                  <Box key={index} sx={{ mb: 2, p: 2, backgroundColor: '#f9f9f9', borderRadius: 1, borderLeft: '4px solid #2196f3' }}>
                    {excerpts.length > 1 && (
                      <Typography variant="body2" sx={{ fontStyle: 'italic', mb: 1 }}>
                        <strong>Source {index + 1}:</strong>
                      </Typography>
                    )}
                    <Typography variant="body2" sx={{ lineHeight: 1.6 }}>
                      {String(excerpt)}
                    </Typography>
                  </Box>
                ))}
              </>
            )}
          </Box>
        );
      }
      
      // Regular object handling
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
    
    // For long text content - check if it looks like an excerpt
    const textValue = String(value);
    const isLikelyExcerpt = textValue.length > 200 || 
                           title.toLowerCase().includes('excerpt') ||
                           title.toLowerCase().includes('evidence') ||
                           title.toLowerCase().includes('source');
    
    if (isLikelyExcerpt) {
      // Format as excerpt-like content
      return (
        <Box>
          <Typography variant="h6" gutterBottom color="primary">
            Content:
          </Typography>
          <Box sx={{ 
            p: 2, 
            backgroundColor: '#f9f9f9', 
            borderRadius: 1, 
            borderLeft: '4px solid #2196f3',
            maxHeight: '400px',
            overflow: 'auto'
          }}>
            <Typography variant="body1" sx={{ 
              whiteSpace: 'pre-wrap', 
              wordBreak: 'break-word',
              lineHeight: 1.6,
              fontSize: '1rem'
            }}>
              {textValue}
            </Typography>
          </Box>
        </Box>
      );
    }
    
    // Regular text content
    return (
      <Typography variant="body1" sx={{ 
        whiteSpace: 'pre-wrap', 
        wordBreak: 'break-word',
        lineHeight: 1.6,
        fontSize: '1rem'
      }}>
        {textValue}
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