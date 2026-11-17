// Banner shown over the Data sheet when the project has more rows (or units)
// than the Workspace loads into the grid. Filtering, sorting, search and edits
// in the grid only see the loaded rows, while CSV/project export is
// server-side and always complete, so the user needs to know the grid is
// partial.
// Parent: Workspace (index.tsx).

export function RowLimitBanner({
  shown,
  total,
  unit,
}: {
  shown: number;
  total: number;
  unit: 'rows' | 'units';
}) {
  return (
    <div className="workspace-followup-banner" role="status" data-testid="row-limit-banner">
      <div className="workspace-followup-banner-copy">
        <strong>
          Showing the first {shown.toLocaleString()} of {total.toLocaleString()} {unit}
        </strong>
        <span>
          Filtering, sorting, search and edits in the grid apply to the loaded {unit} only.
          Exported CSV and project files include all {unit}.
        </span>
      </div>
    </div>
  );
}
