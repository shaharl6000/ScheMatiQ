import React, { useState, useMemo } from 'react';
import {
  Box,
  Typography,
  Paper,
  TableContainer,
  Table,
  TableHead,
  TableRow,
  TableCell,
  TableBody,
  TablePagination,
  Chip,
  IconButton,
  TextField,
  InputAdornment,
} from '@mui/material';
import { Search, Visibility } from '@mui/icons-material';
import { useQuery } from 'react-query';

import { PaginatedData } from '../../types';
import { sessionAPI } from '../../services/api';
import { formatColumnName, needsTruncation, truncateText } from '../../utils/formatting';
import ContentModal from '../ContentModal/ContentModal';

interface DataTableProps {
  data: PaginatedData;
  sessionId: string;
  sessionType: 'upload' | 'qbsd';
}

const DataTable: React.FC<DataTableProps> = ({ data: initialData, sessionId, sessionType }) => {
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [searchTerm, setSearchTerm] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [modalContent, setModalContent] = useState<{ title: string; content: any }>({ title: '', content: null });

  // Fetch data with pagination
  const { data = initialData } = useQuery(
    ['data', sessionId, sessionType, page, pageSize],
    () => sessionAPI.getData(sessionId, sessionType, page, pageSize),
    {
      keepPreviousData: true,
      initialData,
    }
  );

  // Filter data based on search term
  const filteredRows = useMemo(() => {
    if (!searchTerm.trim()) return data.rows;
    
    return data.rows.filter(row => {
      const searchLower = searchTerm.toLowerCase();
      
      // Search in row name
      if (row.row_name?.toLowerCase().includes(searchLower)) return true;
      
      // Search in data values
      return Object.values(row.data).some(value => {
        if (value === null || value === undefined) return false;
        return String(value).toLowerCase().includes(searchLower);
      });
    });
  }, [data.rows, searchTerm]);

  // Get all column names
  const columns = useMemo(() => {
    const columnSet = new Set<string>();
    
    // Add standard columns
    if (data.rows.some(row => row.row_name)) {
      columnSet.add('_row_name');
    }
    if (data.rows.some(row => row.papers?.length)) {
      columnSet.add('_papers');
    }
    
    // Add data columns
    data.rows.forEach(row => {
      Object.keys(row.data).forEach(key => columnSet.add(key));
    });
    
    return Array.from(columnSet);
  }, [data.rows]);

  const handleChangePage = (event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setPageSize(parseInt(event.target.value, 10));
    setPage(0);
  };

  const handleViewContent = (columnName: string, content: any) => {
    setModalContent({
      title: `${formatColumnName(columnName)} - Full Content`,
      content: content
    });
    setModalOpen(true);
  };

  const formatCellValue = (value: any, columnName: string): React.ReactNode => {
    if (value === null || value === undefined) {
      return <Chip label="null" size="small" variant="outlined" color="default" />;
    }
    
    if (Array.isArray(value)) {
      if (value.length > 3) {
        return (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            {value.slice(0, 2).map((item, index) => (
              <Chip key={index} label={String(item)} size="small" sx={{ mr: 0.5 }} />
            ))}
            <Typography variant="caption">
              +{value.length - 2} more
            </Typography>
            <IconButton 
              size="small" 
              title="View full array"
              onClick={() => handleViewContent(columnName, value)}
            >
              <Visibility fontSize="small" />
            </IconButton>
          </Box>
        );
      }
      return (
        <Box>
          {value.map((item, index) => (
            <Chip key={index} label={String(item)} size="small" sx={{ mr: 0.5, mb: 0.5 }} />
          ))}
        </Box>
      );
    }
    
    if (typeof value === 'object') {
      return (
        <IconButton 
          size="small" 
          title="View object"
          onClick={() => handleViewContent(columnName, value)}
        >
          <Visibility fontSize="small" />
        </IconButton>
      );
    }
    
    const stringValue = String(value);
    if (needsTruncation(stringValue)) {
      return (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
          <Typography variant="body2" component="span">
            {truncateText(stringValue)}
          </Typography>
          <IconButton 
            size="small" 
            title="View full content"
            onClick={() => handleViewContent(columnName, value)}
          >
            <Visibility fontSize="small" />
          </IconButton>
        </Box>
      );
    }
    
    return stringValue;
  };

  return (
    <Paper>
      <Box sx={{ p: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="h6">
            Data Table ({data.total_count.toLocaleString()} rows)
          </Typography>
          
          <TextField
            size="small"
            placeholder="Search data..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Search />
                </InputAdornment>
              ),
            }}
            sx={{ width: 300 }}
          />
        </Box>

        <TableContainer sx={{ maxHeight: 600 }}>
          <Table stickyHeader size="small">
            <TableHead>
              <TableRow>
                {columns.map(column => (
                  <TableCell key={column} sx={{ fontWeight: 'bold', minWidth: 150 }}>
                    {column.startsWith('_') ? (
                      <Chip 
                        label={formatColumnName(column)} 
                        size="small" 
                        color="primary" 
                        variant="outlined" 
                      />
                    ) : (
                      formatColumnName(column)
                    )}
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {filteredRows.slice(page * pageSize, (page + 1) * pageSize).map((row, rowIndex) => (
                <TableRow key={rowIndex} hover>
                  {columns.map(column => {
                    let cellValue;
                    
                    if (column === '_row_name') {
                      cellValue = row.row_name;
                    } else if (column === '_papers') {
                      cellValue = row.papers;
                    } else {
                      cellValue = row.data[column];
                    }
                    
                    return (
                      <TableCell key={column}>
                        {formatCellValue(cellValue, column)}
                      </TableCell>
                    );
                  })}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        <TablePagination
          rowsPerPageOptions={[25, 50, 100, 200]}
          component="div"
          count={searchTerm ? filteredRows.length : data.total_count}
          rowsPerPage={pageSize}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
        />
      </Box>

      {/* Content Modal */}
      <ContentModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        title={modalContent.title}
        content={modalContent.content}
      />
    </Paper>
  );
};

export default DataTable;