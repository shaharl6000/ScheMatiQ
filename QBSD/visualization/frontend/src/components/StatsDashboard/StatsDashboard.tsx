import React from 'react';
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

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Progress } from '@/components/ui/progress';

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

  const colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899'];

  return (
    <div className="space-y-6">
      {/* Overview Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground mb-1">Total Rows</p>
            <p className="text-3xl font-bold">
              {statistics.total_rows.toLocaleString()}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground mb-1">Total Columns</p>
            <p className="text-3xl font-bold">
              {statistics.total_columns}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground mb-1">Overall Completeness</p>
            <p className="text-3xl font-bold">
              {statistics.completeness.toFixed(1)}%
            </p>
            <Progress value={statistics.completeness} className="mt-2" />
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground mb-1">Data Types</p>
            <p className="text-3xl font-bold">
              {Object.keys(dataTypeDistribution).length}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Column Completeness Chart */}
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Column Completeness</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-[350px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={completenessData}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis
                    dataKey="name"
                    angle={-45}
                    textAnchor="end"
                    height={80}
                    tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12 }}
                  />
                  <YAxis tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12 }} />
                  <Tooltip
                    formatter={(value: number) => [`${value.toFixed(1)}%`, 'Completeness']}
                    labelFormatter={(label) => `Column: ${label}`}
                    contentStyle={{
                      backgroundColor: 'hsl(var(--background))',
                      border: '1px solid hsl(var(--border))',
                      borderRadius: '8px',
                    }}
                  />
                  <Bar dataKey="completeness" fill="hsl(var(--primary))" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        {/* Data Type Distribution */}
        <Card>
          <CardHeader>
            <CardTitle>Data Type Distribution</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-[350px]">
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
                    {pieData.map((_, index) => (
                      <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'hsl(var(--background))',
                      border: '1px solid hsl(var(--border))',
                      borderRadius: '8px',
                    }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Column Details Table */}
      <Card>
        <CardHeader>
          <CardTitle>Column Statistics</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b-2 border-border">
                  <th className="p-3 text-left font-semibold">Column Name</th>
                  <th className="p-3 text-left font-semibold">Data Type</th>
                  <th className="p-3 text-right font-semibold">Non-Null Count</th>
                  <th className="p-3 text-right font-semibold">Unique Count</th>
                  <th className="p-3 text-right font-semibold">Completeness</th>
                </tr>
              </thead>
              <tbody>
                {completenessData.map((col, index) => (
                  <tr
                    key={col.name}
                    className="border-b border-border odd:bg-muted/50"
                  >
                    <td className="p-3 font-medium">{col.name}</td>
                    <td className="p-3 text-muted-foreground">
                      {statistics.column_stats[index]?.data_type || 'unknown'}
                    </td>
                    <td className="p-3 text-right">
                      {col.non_null_count.toLocaleString()}
                    </td>
                    <td className="p-3 text-right">
                      {col.unique_count.toLocaleString()}
                    </td>
                    <td className="p-3">
                      <div className="flex items-center justify-end gap-2">
                        <span className="text-sm min-w-[45px] text-right">
                          {col.completeness.toFixed(1)}%
                        </span>
                        <Progress value={col.completeness} className="w-24" />
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default StatsDashboard;
