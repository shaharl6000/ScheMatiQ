import React from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Card,
  CardContent,
  LinearProgress,
} from '@mui/material';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts';

import { DataStatistics } from '../../types';

interface StatsDashboardProps {
  statistics: DataStatistics;
}

const StatsDashboard: React.FC<StatsDashboardProps> = ({ statistics }) => {
  // Prepare data for charts
  const completenessData = statistics.column_stats.map(col => ({
    name: col.name,
    completeness: col.non_null_count && statistics.total_rows 
      ? (col.non_null_count / statistics.total_rows) * 100 
      : 0,
    non_null_count: col.non_null_count || 0,
    unique_count: col.unique_count || 0,
  }));

  const dataTypeDistribution = statistics.column_stats.reduce((acc, col) => {
    const type = col.data_type || 'unknown';
    acc[type] = (acc[type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  const pieData = Object.entries(dataTypeDistribution).map(([type, count]) => ({
    name: type,
    value: count,
  }));

  const colors = ['#8884d8', '#82ca9d', '#ffc658', '#ff7300', '#8dd1e1', '#d084d0'];

  return (
    <Box>
      {/* Overview Cards */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                Total Rows
              </Typography>
              <Typography variant="h4">
                {statistics.total_rows.toLocaleString()}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                Total Columns
              </Typography>
              <Typography variant="h4">
                {statistics.total_columns}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                Overall Completeness
              </Typography>
              <Typography variant="h4">
                {statistics.completeness.toFixed(1)}%
              </Typography>
              <LinearProgress 
                variant="determinate" 
                value={statistics.completeness} 
                sx={{ mt: 1 }}
              />
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                Data Types
              </Typography>
              <Typography variant="h4">
                {Object.keys(dataTypeDistribution).length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Charts */}
      <Grid container spacing={3}>
        {/* Column Completeness Chart */}
        <Grid item xs={12} lg={8}>
          <Paper sx={{ p: 3, height: 400 }}>
            <Typography variant="h6" gutterBottom>
              Column Completeness
            </Typography>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={completenessData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis 
                  dataKey="name" 
                  angle={-45}
                  textAnchor="end"
                  height={80}
                />
                <YAxis />
                <Tooltip 
                  formatter={(value: number, name: string) => [
                    `${value.toFixed(1)}%`,
                    'Completeness'
                  ]}
                  labelFormatter={(label) => `Column: ${label}`}
                />
                <Bar dataKey="completeness" fill="#8884d8" />
              </BarChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>

        {/* Data Type Distribution */}
        <Grid item xs={12} lg={4}>
          <Paper sx={{ p: 3, height: 400 }}>
            <Typography variant="h6" gutterBottom>
              Data Type Distribution
            </Typography>
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={pieData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percent }) => 
                    `${name}: ${(percent * 100).toFixed(0)}%`
                  }
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {pieData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>

        {/* Column Details Table */}
        <Grid item xs={12}>
          <Paper sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Column Statistics
            </Typography>
            <Box sx={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ borderBottom: '2px solid #ddd' }}>
                    <th style={{ padding: '12px', textAlign: 'left' }}>Column Name</th>
                    <th style={{ padding: '12px', textAlign: 'left' }}>Data Type</th>
                    <th style={{ padding: '12px', textAlign: 'right' }}>Non-Null Count</th>
                    <th style={{ padding: '12px', textAlign: 'right' }}>Unique Count</th>
                    <th style={{ padding: '12px', textAlign: 'right' }}>Completeness</th>
                  </tr>
                </thead>
                <tbody>
                  {completenessData.map((col, index) => (
                    <tr key={col.name} style={{ 
                      borderBottom: '1px solid #eee',
                      backgroundColor: index % 2 === 0 ? '#f9f9f9' : 'white'
                    }}>
                      <td style={{ padding: '12px' }}>
                        <strong>{col.name}</strong>
                      </td>
                      <td style={{ padding: '12px' }}>
                        {statistics.column_stats[index]?.data_type || 'unknown'}
                      </td>
                      <td style={{ padding: '12px', textAlign: 'right' }}>
                        {col.non_null_count.toLocaleString()}
                      </td>
                      <td style={{ padding: '12px', textAlign: 'right' }}>
                        {col.unique_count.toLocaleString()}
                      </td>
                      <td style={{ padding: '12px', textAlign: 'right' }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end' }}>
                          <Typography variant="body2" sx={{ mr: 1, minWidth: 45 }}>
                            {col.completeness.toFixed(1)}%
                          </Typography>
                          <LinearProgress 
                            variant="determinate" 
                            value={col.completeness} 
                            sx={{ width: 100 }}
                          />
                        </Box>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Box>
          </Paper>
        </Grid>
      </Grid>
    </Box>
  );
};

export default StatsDashboard;