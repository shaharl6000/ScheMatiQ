/**
 * Schema Column Clustering
 *
 * Implements hierarchical agglomerative clustering for grouping
 * schema columns by semantic similarity.
 *
 * Algorithm: HAC with average linkage
 * Features: TF-IDF + name tokens + data type + domain keywords
 * Performance: < 300ms for 50 columns
 */

import { ColumnInfo, ColumnCluster, ClusteringConfig } from '../../types';

// ==================== TYPES ====================

export interface ClusteringOptions {
  similarityThreshold?: number;  // Default: 0.5
  minClusterSize?: number;       // Default: 1
  maxClusters?: number;          // Default: 10
  respectUserClusters?: boolean; // Default: true
}

export interface ClusteringResult {
  clusters: ColumnCluster[];
  config: ClusteringConfig;
}

// ==================== DOMAIN KEYWORDS ====================

const DOMAIN_KEYWORDS: Record<string, string[]> = {
  'Demographics': [
    'age', 'gender', 'sex', 'race', 'ethnicity', 'education', 'income',
    'occupation', 'employment', 'marital', 'nationality', 'birth', 'population'
  ],
  'Temporal': [
    'date', 'time', 'duration', 'period', 'year', 'month', 'day', 'week',
    'timestamp', 'start', 'end', 'interval', 'frequency', 'timeline'
  ],
  'Outcomes': [
    'outcome', 'result', 'effect', 'response', 'success', 'failure',
    'mortality', 'survival', 'improvement', 'recovery', 'efficacy', 'performance'
  ],
  'Methodology': [
    'method', 'protocol', 'procedure', 'technique', 'approach', 'design',
    'sample', 'experiment', 'trial', 'study', 'analysis', 'measurement'
  ],
  'Identification': [
    'id', 'identifier', 'name', 'title', 'label', 'code', 'key',
    'reference', 'index', 'number', 'serial'
  ],
  'Quantitative': [
    'count', 'amount', 'quantity', 'total', 'sum', 'average', 'mean',
    'score', 'rate', 'ratio', 'percentage', 'value', 'measure'
  ],
  'Location': [
    'location', 'place', 'site', 'region', 'country', 'city', 'address',
    'geographic', 'area', 'zone', 'territory'
  ],
  'Classification': [
    'type', 'category', 'class', 'group', 'status', 'level', 'stage',
    'grade', 'rank', 'tier'
  ]
};

// Cluster colors (from Tailwind palette)
const CLUSTER_COLORS = [
  '#3B82F6', // blue
  '#10B981', // emerald
  '#F59E0B', // amber
  '#EF4444', // red
  '#8B5CF6', // violet
  '#EC4899', // pink
  '#06B6D4', // cyan
  '#84CC16', // lime
  '#F97316', // orange
  '#6366F1', // indigo
];

// ==================== FEATURE EXTRACTION ====================

/**
 * Tokenize a column name by splitting on underscores, camelCase, and hyphens
 */
function tokenizeName(name: string): string[] {
  return name
    .replace(/([a-z])([A-Z])/g, '$1 $2')  // camelCase
    .replace(/[_-]/g, ' ')                 // underscores and hyphens
    .toLowerCase()
    .split(/\s+/)
    .filter(token => token.length > 1);    // Remove single chars
}

/**
 * Extract tokens from text (definition, rationale)
 */
function tokenizeText(text: string): string[] {
  if (!text) return [];
  return text
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')  // Remove punctuation
    .split(/\s+/)
    .filter(token => token.length > 2 && !STOP_WORDS.has(token));
}

// Common stop words to ignore
const STOP_WORDS = new Set([
  'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
  'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
  'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
  'should', 'may', 'might', 'must', 'shall', 'can', 'need', 'dare', 'ought',
  'used', 'this', 'that', 'these', 'those', 'which', 'who', 'whom', 'whose',
  'what', 'where', 'when', 'why', 'how', 'all', 'each', 'every', 'both',
  'few', 'more', 'most', 'other', 'some', 'such', 'than', 'too', 'very',
  'just', 'also', 'now', 'here', 'there', 'then', 'once', 'only', 'own',
  'same', 'so', 'not', 'nor', 'any', 'into', 'over', 'after', 'before'
]);

/**
 * Compute TF-IDF scores for all columns
 */
function computeTFIDF(columns: ColumnInfo[]): Map<string, Map<string, number>> {
  // Document frequency for each term
  const df = new Map<string, number>();

  // Term frequency per column
  const columnTokens = columns.map(col => {
    const tokens = [
      ...tokenizeName(col.name),
      ...tokenizeText(col.definition || ''),
      ...tokenizeText(col.rationale || '')
    ];

    // Count term frequency
    const tf = new Map<string, number>();
    tokens.forEach(token => {
      tf.set(token, (tf.get(token) || 0) + 1);
    });

    // Update document frequency
    tf.forEach((_, token) => {
      df.set(token, (df.get(token) || 0) + 1);
    });

    return { column: col, tf, totalTokens: tokens.length };
  });

  // Compute TF-IDF
  const n = columns.length;
  const tfidf = new Map<string, Map<string, number>>();

  columnTokens.forEach(({ column, tf, totalTokens }) => {
    const scores = new Map<string, number>();

    tf.forEach((freq, token) => {
      const termFreq = freq / Math.max(totalTokens, 1);
      const docFreq = df.get(token) || 1;
      const idf = Math.log(n / docFreq) + 1;
      scores.set(token, termFreq * idf);
    });

    tfidf.set(column.name, scores);
  });

  return tfidf;
}

/**
 * Check if column matches a domain category
 */
function matchesDomain(column: ColumnInfo, keywords: string[]): boolean {
  const text = `${column.name} ${column.definition || ''} ${column.rationale || ''}`.toLowerCase();
  return keywords.some(kw => text.includes(kw));
}

/**
 * Get domain category for a column (if any)
 */
function getDomainCategory(column: ColumnInfo): string | null {
  for (const [category, keywords] of Object.entries(DOMAIN_KEYWORDS)) {
    if (matchesDomain(column, keywords)) {
      return category;
    }
  }
  return null;
}

/**
 * Extract feature vector for a column
 */
function extractFeatures(
  column: ColumnInfo,
  tfidf: Map<string, Map<string, number>>,
  vocabulary: string[]
): number[] {
  const features: number[] = [];

  // 1. TF-IDF features (normalized)
  const columnTfidf = tfidf.get(column.name) || new Map();
  vocabulary.forEach(term => {
    features.push(columnTfidf.get(term) || 0);
  });

  // 2. Data type features (one-hot)
  const dataTypes = ['string', 'number', 'boolean', 'date', 'array'];
  dataTypes.forEach(dt => {
    features.push(column.data_type?.toLowerCase() === dt ? 1 : 0);
  });

  // 3. Has allowed values (categorical indicator)
  features.push(column.allowed_values && column.allowed_values.length > 0 ? 2 : 0);

  // 4. Domain category features
  Object.keys(DOMAIN_KEYWORDS).forEach(category => {
    features.push(matchesDomain(column, DOMAIN_KEYWORDS[category]) ? 2 : 0);
  });

  return features;
}

// ==================== SIMILARITY ====================

/**
 * Compute cosine similarity between two vectors
 */
function cosineSimilarity(a: number[], b: number[]): number {
  if (a.length !== b.length || a.length === 0) return 0;

  let dotProduct = 0;
  let normA = 0;
  let normB = 0;

  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }

  const denominator = Math.sqrt(normA) * Math.sqrt(normB);
  return denominator === 0 ? 0 : dotProduct / denominator;
}

/**
 * Compute similarity matrix for all columns
 */
function computeSimilarityMatrix(
  columns: ColumnInfo[],
  features: Map<string, number[]>
): number[][] {
  const n = columns.length;
  const matrix: number[][] = Array(n).fill(null).map(() => Array(n).fill(0));

  for (let i = 0; i < n; i++) {
    matrix[i][i] = 1; // Self-similarity
    for (let j = i + 1; j < n; j++) {
      const featA = features.get(columns[i].name) || [];
      const featB = features.get(columns[j].name) || [];
      const sim = cosineSimilarity(featA, featB);
      matrix[i][j] = sim;
      matrix[j][i] = sim;
    }
  }

  return matrix;
}

// ==================== HIERARCHICAL CLUSTERING ====================

interface ClusterNode {
  members: number[];  // Indices into columns array
  similarity: number; // Intra-cluster similarity
}

/**
 * Compute average linkage distance between two clusters
 */
function averageLinkage(
  clusterA: ClusterNode,
  clusterB: ClusterNode,
  simMatrix: number[][]
): number {
  let totalSim = 0;
  let count = 0;

  for (const i of clusterA.members) {
    for (const j of clusterB.members) {
      totalSim += simMatrix[i][j];
      count++;
    }
  }

  return count > 0 ? totalSim / count : 0;
}

/**
 * Hierarchical agglomerative clustering with average linkage
 */
function hierarchicalClustering(
  columns: ColumnInfo[],
  simMatrix: number[][],
  threshold: number
): ClusterNode[] {
  // Initialize: each column is its own cluster
  let clusters: ClusterNode[] = columns.map((_, i) => ({
    members: [i],
    similarity: 1
  }));

  // Merge until threshold is reached
  while (clusters.length > 1) {
    // Find most similar cluster pair
    let maxSim = -1;
    let mergeI = -1;
    let mergeJ = -1;

    for (let i = 0; i < clusters.length; i++) {
      for (let j = i + 1; j < clusters.length; j++) {
        const sim = averageLinkage(clusters[i], clusters[j], simMatrix);
        if (sim > maxSim) {
          maxSim = sim;
          mergeI = i;
          mergeJ = j;
        }
      }
    }

    // Stop if best similarity is below threshold
    if (maxSim < threshold) {
      break;
    }

    // Merge clusters i and j
    const merged: ClusterNode = {
      members: [...clusters[mergeI].members, ...clusters[mergeJ].members],
      similarity: maxSim
    };

    // Remove merged clusters and add new one
    clusters = clusters.filter((_, idx) => idx !== mergeI && idx !== mergeJ);
    clusters.push(merged);
  }

  return clusters;
}

// ==================== CLUSTER LABELING ====================

/**
 * Capitalize first letter of each word
 */
function titleCase(str: string): string {
  return str.split(' ')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

/**
 * Generate a label for a cluster based on centroid of its members
 * Uses the most representative terms from actual column content
 */
function generateClusterLabel(columns: ColumnInfo[], memberIndices: number[]): string {
  const members = memberIndices.map(i => columns[i]);

  if (members.length === 1) {
    // Single column - use shortened name
    const name = members[0].name;
    const tokens = tokenizeName(name);
    return titleCase(tokens.slice(0, 3).join(' '));
  }

  // Extract all tokens from column names with their frequencies
  const tokenFreq = new Map<string, number>();
  const tokenInColumns = new Map<string, Set<number>>(); // Track which columns contain each token

  members.forEach((member, idx) => {
    const tokens = tokenizeName(member.name);
    const seenInThisColumn = new Set<string>();

    tokens.forEach(token => {
      if (token.length > 2) {
        tokenFreq.set(token, (tokenFreq.get(token) || 0) + 1);
        if (!seenInThisColumn.has(token)) {
          seenInThisColumn.add(token);
          if (!tokenInColumns.has(token)) {
            tokenInColumns.set(token, new Set());
          }
          tokenInColumns.get(token)!.add(idx);
        }
      }
    });
  });

  // Score tokens by how many columns contain them (not raw frequency)
  // This finds terms that appear across multiple columns = centroid
  const tokenScores: Array<{ token: string; score: number; coverage: number }> = [];

  tokenInColumns.forEach((columnSet, token) => {
    const coverage = columnSet.size / members.length;
    // Prefer tokens that appear in many columns but aren't too generic
    const score = coverage * (tokenFreq.get(token) || 0);
    tokenScores.push({ token, score, coverage });
  });

  // Sort by coverage first, then by score
  tokenScores.sort((a, b) => {
    if (Math.abs(a.coverage - b.coverage) > 0.1) {
      return b.coverage - a.coverage;
    }
    return b.score - a.score;
  });

  // Get top 2 distinct tokens for the label
  const topTokens: string[] = [];
  for (const { token, coverage } of tokenScores) {
    if (topTokens.length >= 2) break;
    // Only include if it appears in at least 30% of columns
    if (coverage >= 0.3 || topTokens.length === 0) {
      topTokens.push(token);
    }
  }

  if (topTokens.length > 0) {
    return titleCase(topTokens.join(' & '));
  }

  // Fallback: use first two words from the first column name
  const firstTokens = tokenizeName(members[0].name);
  if (firstTokens.length > 0) {
    return titleCase(firstTokens.slice(0, 2).join(' '));
  }

  return `Cluster ${memberIndices[0] + 1}`;
}

// ==================== MAIN CLUSTERING FUNCTION ====================

/**
 * Cluster schema columns using hierarchical agglomerative clustering
 */
export function clusterColumns(
  columns: ColumnInfo[],
  existingClusters?: ColumnCluster[],
  options: ClusteringOptions = {}
): ClusteringResult {
  const {
    similarityThreshold = 0.5,
    minClusterSize = 1,
    maxClusters = 10,
    respectUserClusters = true
  } = options;

  if (columns.length === 0) {
    return {
      clusters: [],
      config: {
        enabled: true,
        clusters: [],
        unclustered_behavior: 'show_at_end'
      }
    };
  }

  // Separate user-assigned columns from algorithm-assigned
  const userClusters = existingClusters?.filter(c => c.id.startsWith('user_')) || [];
  const userAssignedColumns = new Set(
    respectUserClusters ? userClusters.flatMap(c => c.column_names) : []
  );

  const columnsToCluster = columns.filter(c => !userAssignedColumns.has(c.name));

  if (columnsToCluster.length === 0) {
    return {
      clusters: userClusters,
      config: {
        enabled: true,
        clusters: userClusters,
        unclustered_behavior: 'show_at_end'
      }
    };
  }

  // Compute TF-IDF
  const tfidf = computeTFIDF(columnsToCluster);

  // Build vocabulary (union of all terms)
  const vocabulary = new Set<string>();
  tfidf.forEach(scores => {
    scores.forEach((_, term) => vocabulary.add(term));
  });
  const vocabArray = Array.from(vocabulary);

  // Extract features for each column
  const features = new Map<string, number[]>();
  columnsToCluster.forEach(col => {
    features.set(col.name, extractFeatures(col, tfidf, vocabArray));
  });

  // Compute similarity matrix
  const simMatrix = computeSimilarityMatrix(columnsToCluster, features);

  // Perform hierarchical clustering
  const clusterNodes = hierarchicalClustering(columnsToCluster, simMatrix, similarityThreshold);

  // Convert to ColumnCluster format
  const algorithmClusters: ColumnCluster[] = clusterNodes
    .filter(node => node.members.length >= minClusterSize)
    .slice(0, maxClusters)
    .map((node, idx) => ({
      id: `algo_${Date.now()}_${idx}`,
      name: generateClusterLabel(columnsToCluster, node.members),
      description: `${node.members.length} columns (similarity: ${(node.similarity * 100).toFixed(0)}%)`,
      color: CLUSTER_COLORS[idx % CLUSTER_COLORS.length],
      collapsed: false,
      column_names: node.members.map(i => columnsToCluster[i].name)
    }));

  // Handle unclustered columns (singleton clusters below threshold)
  const clusteredColumns = new Set(algorithmClusters.flatMap(c => c.column_names));
  const unclustered = columnsToCluster.filter(c => !clusteredColumns.has(c.name));

  if (unclustered.length > 0) {
    // Put all unclustered columns in a single "Other" cluster
    // Name it based on the content of the columns
    const unclusteredIndices = unclustered.map(col =>
      columnsToCluster.findIndex(c => c.name === col.name)
    ).filter(idx => idx !== -1);

    const clusterName = unclustered.length === 1
      ? titleCase(tokenizeName(unclustered[0].name).slice(0, 3).join(' '))
      : generateClusterLabel(columnsToCluster, unclusteredIndices);

    algorithmClusters.push({
      id: `algo_${Date.now()}_other`,
      name: clusterName || 'Other',
      description: `${unclustered.length} columns`,
      color: '#6B7280', // gray
      collapsed: false,
      column_names: unclustered.map(c => c.name)
    });
  }

  // Merge with user clusters
  const allClusters = [...userClusters, ...algorithmClusters];

  return {
    clusters: allClusters,
    config: {
      enabled: true,
      clusters: allClusters,
      unclustered_behavior: 'show_at_end'
    }
  };
}

/**
 * Suggest a cluster for a new/edited column
 */
export function suggestClusterForColumn(
  column: ColumnInfo,
  existingClusters: ColumnCluster[],
  allColumns: ColumnInfo[],
  threshold: number = 0.4
): ColumnCluster | null {
  if (existingClusters.length === 0) return null;

  // Compute TF-IDF including the new column
  const columnsWithNew = [...allColumns, column];
  const tfidf = computeTFIDF(columnsWithNew);

  // Build vocabulary
  const vocabulary = new Set<string>();
  tfidf.forEach(scores => {
    scores.forEach((_, term) => vocabulary.add(term));
  });
  const vocabArray = Array.from(vocabulary);

  // Extract features for new column
  const newFeatures = extractFeatures(column, tfidf, vocabArray);

  // Find most similar cluster
  let bestCluster: ColumnCluster | null = null;
  let bestSimilarity = threshold;

  existingClusters.forEach(cluster => {
    // Compute average similarity to cluster members
    const memberColumns = allColumns.filter(c => cluster.column_names.includes(c.name));
    if (memberColumns.length === 0) return;

    let totalSim = 0;
    memberColumns.forEach(member => {
      const memberFeatures = extractFeatures(member, tfidf, vocabArray);
      totalSim += cosineSimilarity(newFeatures, memberFeatures);
    });

    const avgSim = totalSim / memberColumns.length;
    if (avgSim > bestSimilarity) {
      bestSimilarity = avgSim;
      bestCluster = cluster;
    }
  });

  return bestCluster;
}

/**
 * Create a user-defined cluster
 */
export function createUserCluster(
  name: string,
  columnNames: string[],
  color?: string
): ColumnCluster {
  return {
    id: `user_${Date.now()}`,
    name,
    description: `${columnNames.length} columns (user-defined)`,
    color: color || CLUSTER_COLORS[Math.floor(Math.random() * CLUSTER_COLORS.length)],
    collapsed: false,
    column_names: columnNames
  };
}

/**
 * Assign a column to a cluster (user override)
 */
export function assignColumnToCluster(
  columnName: string,
  targetClusterId: string | null,
  currentClusters: ColumnCluster[]
): ColumnCluster[] {
  // Remove column from all clusters first
  let updatedClusters = currentClusters.map(cluster => ({
    ...cluster,
    column_names: cluster.column_names.filter(name => name !== columnName)
  })).filter(cluster => cluster.column_names.length > 0);

  if (targetClusterId === null) {
    // Create new user cluster for this column
    updatedClusters.push(createUserCluster(`Custom: ${columnName}`, [columnName]));
  } else {
    // Add to existing cluster
    updatedClusters = updatedClusters.map(cluster =>
      cluster.id === targetClusterId
        ? {
            ...cluster,
            column_names: [...cluster.column_names, columnName],
            // Mark as user-modified if it was algorithm-generated
            id: cluster.id.startsWith('algo_') ? `user_${cluster.id}` : cluster.id
          }
        : cluster
    );
  }

  return updatedClusters;
}

/**
 * Re-cluster when schema changes, preserving user clusters
 */
export function reclusterOnSchemaChange(
  columns: ColumnInfo[],
  existingClusters: ColumnCluster[],
  changedColumnNames: string[],
  options?: ClusteringOptions
): ClusteringResult {
  // User clusters are preserved
  const userClusters = existingClusters.filter(c => c.id.startsWith('user_'));

  // Re-cluster all algorithm-assigned columns
  return clusterColumns(columns, userClusters, {
    ...options,
    respectUserClusters: true
  });
}
