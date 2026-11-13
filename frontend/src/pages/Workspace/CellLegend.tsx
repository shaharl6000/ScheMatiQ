// Toolbar popover that explains the Data sheet's cell markers. The samples
// reuse the grid's own marker styles (see Workspace.css) so the legend cannot
// drift from what the table shows.
// Parent: SpreadsheetChrome.

import type { ReactNode } from 'react';
import { Info } from 'lucide-react';

import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';

const LEGEND_ITEMS: Array<{ sample: ReactNode; text: string }> = [
  {
    sample: <span className="workspace-legend-sample workspace-legend-grounded">value</span>,
    text: 'Supported by a quote in the document. Click the corner mark to see it.',
  },
  {
    sample: <span className="workspace-legend-sample workspace-legend-ungrounded">value</span>,
    text: 'No supporting quote found. The value may come from inference or general knowledge. Worth checking.',
  },
  {
    sample: <span className="workspace-legend-sample workspace-legend-empty">Not found</span>,
    text: 'The model checked the document and found no value.',
  },
  {
    sample: <span className="workspace-legend-sample workspace-legend-off-list">value</span>,
    text: "Not in the column's allowed values. Add it in the Schema sheet, or edit the cell.",
  },
];

export function CellLegend() {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button className="workspace-toolbar-icon" type="button" title="What the cell marks mean" aria-label="Cell legend">
          <Info className="h-3.5 w-3.5" />
        </button>
      </PopoverTrigger>
      <PopoverContent align="end" className="workspace-legend w-80 p-3">
        <div className="workspace-legend-title">Cell marks in the Data sheet</div>
        <ul className="workspace-legend-list">
          {LEGEND_ITEMS.map((item) => (
            <li key={item.text} className="workspace-legend-item">
              {item.sample}
              <span>{item.text}</span>
            </li>
          ))}
        </ul>
        <p className="workspace-legend-note">
          Bold, italic and other formatting come only from you (toolbar or Ctrl/Cmd+B/I/U). They are saved in this browser, for this project.
        </p>
      </PopoverContent>
    </Popover>
  );
}
