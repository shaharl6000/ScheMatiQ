"""Semantic column clustering for batched value extraction.

Groups schema columns so that semantically related columns land in the same
LLM call, improving extraction quality compared to arbitrary sequential chunking.

Algorithm
---------
1. Embed each column as  "<name>: <rationale>"  using the shared all-MiniLM-L6-v2
   model already loaded in schematiq.core.schema.
2. Compute the number of clusters  k = ceil(n_columns / batch_size).
3. Run greedy cosine-similarity clustering (same pattern as _deduplicate_units):
   - For each unassigned column, start a new cluster.
   - Assign subsequent unassigned columns to the most similar existing cluster
     centroid, provided cosine similarity ≥ SIM_THRESHOLD, and the cluster has
     room (< batch_size columns).
   - Recompute the cluster centroid as the mean of member embeddings after each
     assignment.
4. Return the clusters as a list-of-lists of Column objects, each list ≤ batch_size.

The greedy approach is O(n²) in the number of columns — fine for schemas with
up to a few hundred columns.
"""

import logging
import math
from typing import List

import numpy as np
from sentence_transformers import util as st_util

from schematiq.core.schema import EMB_MODEL

logger = logging.getLogger(__name__)


def cluster_columns_for_extraction(
    columns: list,
    batch_size: int,
) -> List[List]:
    """Partition *columns* into semantically coherent batches of ≤ *batch_size*.

    Args:
        columns:    List of Column objects (must have .name and .rationale attrs).
        batch_size: Maximum columns per cluster (typically
                    _MAX_COLUMNS_FOR_CONTROLLED_GENERATION = 40).

    Returns:
        List of Column lists.  All columns appear exactly once.  Each inner list
        has 1 ≤ len ≤ batch_size.  Order within each cluster reflects insertion
        order (most-similar columns first after the seed).
    """
    n = len(columns)
    if n == 0:
        return []
    if n <= batch_size:
        return [list(columns)]

    k = math.ceil(n / batch_size)

    logger.info(
        "Semantic column clustering: %d columns → %d clusters of ≤%d",
        n, k, batch_size,
    )

    # --- batch-embed all columns at once ("name: rationale") -------
    texts = [
        f"{col.name}: {col.rationale}" if getattr(col, "rationale", None)
        else col.name
        for col in columns
    ]
    emb_matrix = EMB_MODEL.encode(texts, normalize_embeddings=True, show_progress_bar=False)
    embeddings = [emb_matrix[i] for i in range(n)]

    # --- greedy clustering -----------------------------------------------
    # clusters[i] = list of column indices in cluster i
    clusters: List[List[int]] = []
    # centroids[i] = mean embedding of cluster i (numpy array)
    centroids: List[np.ndarray] = []
    unassigned = list(range(n))

    SIM_THRESHOLD = 0.35  # low threshold — we just want thematic grouping,
    # not strict dedup; columns are intentionally diverse

    while unassigned:
        # Start a new cluster with the first unassigned column
        seed_idx = unassigned.pop(0)
        cluster = [seed_idx]
        centroid = embeddings[seed_idx].copy()

        # Try to fill the cluster up to batch_size
        still_unassigned = []
        for idx in unassigned:
            if len(cluster) >= batch_size:
                still_unassigned.append(idx)
                continue
            sim = st_util.cos_sim(embeddings[idx], centroid).item()
            if sim >= SIM_THRESHOLD:
                cluster.append(idx)
                # Update centroid to mean of members
                centroid = np.mean([embeddings[i] for i in cluster], axis=0)
            else:
                still_unassigned.append(idx)

        clusters.append(cluster)
        centroids.append(centroid)
        unassigned = still_unassigned

        # Safety: if we've created k clusters and still have unassigned columns,
        # distribute them across existing clusters (fill least-full first).
        if len(clusters) == k and unassigned:
            for idx in unassigned:
                # Pick the cluster with fewest members that still has room
                target = min(
                    range(len(clusters)),
                    key=lambda ci: (
                        len(clusters[ci]) >= batch_size,  # prefer clusters with room
                        len(clusters[ci]),
                    ),
                )
                clusters[target].append(idx)
                centroids[target] = np.mean(
                    [embeddings[i] for i in clusters[target]], axis=0
                )
            unassigned = []

    # --- convert index clusters back to Column objects -------------------
    result = [[columns[i] for i in cluster] for cluster in clusters]

    if logger.isEnabledFor(logging.DEBUG):
        for ci, cl in enumerate(result):
            logger.debug(
                "  Cluster %d/%d (%d cols): %s",
                ci + 1, len(result), len(cl),
                [c.name for c in cl],
            )

    return result
