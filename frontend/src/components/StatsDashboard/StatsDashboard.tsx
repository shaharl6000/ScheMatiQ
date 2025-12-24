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
  LineChart,
  Line,
} from 'recharts';

import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Progress } from '@/components/ui/progress';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { TrendingUp, FileText, Info, Plus, Edit, Trash2 } from 'lucide-react';

import { DataStatistics, SchemaEvolution, CreationMetadata, ModificationAction } from '../../types';
import { CollapsibleSection, InfoCard } from '../shared';

interface StatsDashboardProps {
  statistics: DataStatistics;
  creationMetadata?: CreationMetadata;
  modificationHistory?: ModificationAction[];
}

const StatsDashboard: React.FC<StatsDashboardProps> = ({
  statistics,
  creationMetadata,
  modificationHistory = []
}) => {
  // Filter out excerpt columns from statistics
  const filteredColumnStats = statistics.column_stats.filter(
    col => !col.name.endsWith('_excerpt')
  );

  // Count modifications by type
  const modificationCounts = modificationHistory.reduce((acc, mod) => {
    acc[mod.action_type] = (acc[mod.action_type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'Unknown';
    try {
      return new Date(dateString).toLocaleString();
    } catch {
      return 'Unknown';
    }
  };

  // Prepare data for charts (excluding excerpt columns)
  const completenessData = filteredColumnStats.map(col => ({
    name: col.name,
    completeness: col.non_null_count && statistics.total_rows
      ? (col.non_null_count / statistics.total_rows) * 100
      : 0,
    non_null_count: col.non_null_count || 0,
    unique_count: col.unique_count || 0,
  }));

  const dataTypeDistribution = filteredColumnStats.reduce((acc, col) => {
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
              {filteredColumnStats.length}
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
          <div className="overflow-x-auto overscroll-x-contain">
            <table className="w-full min-w-[500px]">
              <thead>
                <tr className="border-b-2 border-border">
                  <th className="p-3 text-left font-semibold whitespace-nowrap">Column Name</th>
                  <th className="p-3 text-left font-semibold whitespace-nowrap">Data Type</th>
                  <th className="p-3 text-right font-semibold whitespace-nowrap">Non-Null Count</th>
                  <th className="p-3 text-right font-semibold whitespace-nowrap">Unique Count</th>
                  <th className="p-3 text-right font-semibold whitespace-nowrap">Completeness</th>
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
                      {filteredColumnStats[index]?.data_type || 'unknown'}
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

      {/* Advanced Statistics Section - Collapsible */}
      <CollapsibleSection title="Advanced Statistics" defaultExpanded={false}>
        <div className="space-y-6">
          {/* Creation Information */}
          {creationMetadata && (
            <div className="bg-blue-50 dark:bg-blue-950 p-4 rounded-lg border border-blue-200 dark:border-blue-800">
              <h4 className="text-sm font-semibold text-blue-900 dark:text-blue-100 mb-3 flex items-center gap-2">
                <Info className="h-4 w-4" />
                Creation Information
              </h4>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
                <InfoCard
                  title="Created"
                  value={formatDate(creationMetadata.created_at)}
                  size="small"
                />
                <InfoCard
                  title="LLM Model"
                  value={creationMetadata.llm_model || 'Unknown'}
                  size="small"
                />
                <InfoCard
                  title="Iterations"
                  value={creationMetadata.iterations_count || 0}
                  size="small"
                />
                <InfoCard
                  title="Convergence"
                  value={creationMetadata.convergence_achieved ? 'Yes' : 'No'}
                  size="small"
                />
              </div>
              {creationMetadata.creation_query && (
                <div className="mt-3 p-3 bg-white dark:bg-blue-900 rounded border border-blue-200 dark:border-blue-700">
                  <div className="text-xs text-blue-700 dark:text-blue-300 font-medium mb-1">Creation Query</div>
                  <div className="text-sm text-blue-900 dark:text-blue-100 italic">
                    "{creationMetadata.creation_query}"
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Modification Summary */}
          {modificationHistory.length > 0 && (
            <div className="bg-amber-50 dark:bg-amber-950 p-4 rounded-lg border border-amber-200 dark:border-amber-800">
              <h4 className="text-sm font-semibold text-amber-900 dark:text-amber-100 mb-3 flex items-center gap-2">
                <Edit className="h-4 w-4" />
                Modifications Summary
              </h4>
              <div className="grid grid-cols-3 gap-3">
                <div className="flex items-center gap-2 p-2 bg-white dark:bg-amber-900 rounded border border-amber-200 dark:border-amber-700">
                  <Plus className="h-4 w-4 text-green-600" />
                  <div>
                    <div className="text-xs text-amber-700 dark:text-amber-300">Columns Added</div>
                    <div className="text-lg font-bold text-amber-900 dark:text-amber-100">
                      {modificationCounts['column_added'] || 0}
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-2 p-2 bg-white dark:bg-amber-900 rounded border border-amber-200 dark:border-amber-700">
                  <Edit className="h-4 w-4 text-blue-600" />
                  <div>
                    <div className="text-xs text-amber-700 dark:text-amber-300">Columns Edited</div>
                    <div className="text-lg font-bold text-amber-900 dark:text-amber-100">
                      {modificationCounts['column_edited'] || 0}
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-2 p-2 bg-white dark:bg-amber-900 rounded border border-amber-200 dark:border-amber-700">
                  <Trash2 className="h-4 w-4 text-red-600" />
                  <div>
                    <div className="text-xs text-amber-700 dark:text-amber-300">Columns Deleted</div>
                    <div className="text-lg font-bold text-amber-900 dark:text-amber-100">
                      {modificationCounts['column_deleted'] || 0}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Schema Evolution Section */}
          <SchemaEvolutionSection evolution={statistics.schema_evolution} columnStats={statistics.column_stats} />
        </div>
      </CollapsibleSection>
    </div>
  );
};

// Schema Evolution Section Component
interface SchemaEvolutionSectionProps {
  evolution?: SchemaEvolution;
  columnStats: DataStatistics['column_stats'];
}

const SchemaEvolutionSection: React.FC<SchemaEvolutionSectionProps> = ({ evolution, columnStats }) => {
  // If no evolution data, show info message
  if (!evolution || !evolution.snapshots || evolution.snapshots.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5" />
            Schema Evolution
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Alert>
            <Info className="h-4 w-4" />
            <AlertDescription>
              Schema evolution data is not available. This feature tracks how the schema
              grew during QBSD discovery and is available for new QBSD sessions.
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  // Check if we need to add initial schema point to the graph
  // This handles cases where initial schema exists but iteration 0 snapshot is missing
  const hasInitialSchema = evolution.column_sources &&
    Object.values(evolution.column_sources).includes('initial_schema');
  const firstSnapshotIsInitial = evolution.snapshots[0]?.iteration === 0;

  let snapshotsToUse = evolution.snapshots;
  if (hasInitialSchema && !firstSnapshotIsInitial) {
    const initialCols = Object.entries(evolution.column_sources)
      .filter(([_, source]) => source === 'initial_schema')
      .map(([name, _]) => name);

    snapshotsToUse = [{
      iteration: 0,
      documents_processed: ['Initial Schema'],
      total_columns: initialCols.length,
      new_columns: initialCols,
      cumulative_documents: 0
    }, ...evolution.snapshots];
  }

  // Prepare data for schema growth chart
  const growthData = snapshotsToUse.map((snapshot, index) => ({
    name: snapshot.documents_processed[0] || `Iteration ${snapshot.iteration}`,
    iteration: snapshot.iteration,
    totalColumns: snapshot.total_columns,
    newColumns: snapshot.new_columns.length,
    cumulativeDocs: snapshot.cumulative_documents,
  }));

  // Build column origins data from column_sources
  const columnOriginsData = Object.entries(evolution.column_sources).map(([columnName, source]) => {
    const colStat = columnStats.find(c => c.name === columnName);
    return {
      columnName,
      source,
      definition: colStat?.definition || '',
      iteration: colStat?.discovery_iteration,
    };
  });

  return (
    <>
      {/* Schema Growth Chart */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5" />
            Schema Growth Over Iterations
          </CardTitle>
          <CardDescription>
            How the schema evolved during discovery - total columns after each iteration
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={growthData}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis
                  dataKey="name"
                  angle={-45}
                  textAnchor="end"
                  height={80}
                  tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 11 }}
                />
                <YAxis
                  tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12 }}
                  label={{ value: 'Columns', angle: -90, position: 'insideLeft', fill: 'hsl(var(--muted-foreground))' }}
                />
                <Tooltip
                  formatter={(value: number, name: string) => {
                    if (name === 'totalColumns') return [value, 'Total Columns'];
                    if (name === 'newColumns') return [value, 'New Columns'];
                    return [value, name];
                  }}
                  labelFormatter={(label) => `${label}`}
                  contentStyle={{
                    backgroundColor: 'hsl(var(--background))',
                    border: '1px solid hsl(var(--border))',
                    borderRadius: '8px',
                  }}
                />
                <Line
                  type="linear"
                  dataKey="totalColumns"
                  stroke="hsl(var(--primary))"
                  strokeWidth={2}
                  dot={{ fill: 'hsl(var(--primary))', strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 6 }}
                />
                <Line
                  type="linear"
                  dataKey="newColumns"
                  stroke="hsl(var(--chart-2))"
                  strokeWidth={2}
                  strokeDasharray="5 5"
                  dot={{ fill: 'hsl(var(--chart-2))', strokeWidth: 2, r: 4 }}
                  activeDot={{ r: 6 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="flex justify-center gap-6 mt-4 text-sm">
            <div className="flex items-center gap-2">
              <div className="w-4 h-1 bg-primary rounded" />
              <span className="text-muted-foreground">Total Columns</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-1 border-t-2 border-dashed border-[hsl(var(--chart-2))]" />
              <span className="text-muted-foreground">New Columns</span>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Column Origins Table */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <FileText className="h-5 w-5" />
            Column Origins
          </CardTitle>
          <CardDescription>
            Which iteration or document contributed each column to the schema
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto overscroll-x-contain">
            <table className="w-full min-w-[400px]">
              <thead>
                <tr className="border-b-2 border-border">
                  <th className="p-3 text-left font-semibold whitespace-nowrap">Column Name</th>
                  <th className="p-3 text-left font-semibold whitespace-nowrap">Source</th>
                  <th className="p-3 text-left font-semibold whitespace-nowrap">Definition</th>
                </tr>
              </thead>
              <tbody>
                {columnOriginsData.map((col) => (
                  <tr
                    key={col.columnName}
                    className="border-b border-border odd:bg-muted/50"
                  >
                    <td className="p-3 font-medium whitespace-nowrap">{col.columnName}</td>
                    <td className="p-3">
                      <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-primary/10 text-primary whitespace-nowrap">
                        {col.source}
                      </span>
                    </td>
                    <td className="p-3 text-muted-foreground text-sm max-w-md truncate">
                      {col.definition || '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </>
  );
};

export default StatsDashboard;
