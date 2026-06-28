import { ChevronDown, Loader2, AlertTriangle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
  DropdownMenuLabel,
} from '@/components/ui/dropdown-menu';

export interface CloudDataset {
  name: string;
  path: string;
  file_count: number;
  description?: string;
}

interface CloudDatasetPickerProps {
  datasets: CloudDataset[];
  loading: boolean;
  selected: string[];
  onChange: (names: string[]) => void;
  maxDocuments?: number;
  bypassLimit?: boolean;
  className?: string;
}

/** Total number of files across the selected datasets. */
export function countCloudFiles(datasets: CloudDataset[], selected: string[]): number {
  return selected.reduce((total, name) => {
    const dataset = datasets.find((d) => d.name === name);
    return total + (dataset?.file_count ?? 0);
  }, 0);
}

/**
 * Multi-select picker for cloud datasets, with an over-limit warning. Presentational:
 * the parent owns the dataset list and selection. Shared so both the classic config
 * screen and the Workspace dialog can offer cloud datasets from one place.
 */
export function CloudDatasetPicker({
  datasets,
  loading,
  selected,
  onChange,
  maxDocuments,
  bypassLimit = false,
  className,
}: CloudDatasetPickerProps) {
  const fileCount = countCloudFiles(datasets, selected);
  const isOverLimit = typeof maxDocuments === 'number' && fileCount > maxDocuments;

  const triggerLabel = loading
    ? 'Loading datasets...'
    : selected.length === 0
      ? 'Select datasets...'
      : selected.length <= 3
        ? selected.join(', ')
        : `${selected.slice(0, 2).join(', ')} +${selected.length - 2} more`;

  return (
    <div className={className ?? 'space-y-2'}>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" className="w-full justify-between" disabled={loading}>
            {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            <span className="truncate">{triggerLabel}</span>
            <ChevronDown className="ml-2 h-4 w-4 flex-shrink-0" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent className="z-[140] w-full min-w-[300px] max-h-[300px] overflow-y-auto">
          <DropdownMenuLabel>Select Datasets</DropdownMenuLabel>
          {datasets.length === 0 ? (
            <div className="px-2 py-1.5 text-sm text-muted-foreground">No datasets available</div>
          ) : (
            datasets.map((dataset) => (
              <DropdownMenuCheckboxItem
                key={dataset.name}
                checked={selected.includes(dataset.name)}
                onSelect={(e) => e.preventDefault()}
                onCheckedChange={(checked) => {
                  const next = checked
                    ? [...selected, dataset.name]
                    : selected.filter((p) => p !== dataset.name);
                  onChange(next);
                }}
              >
                <span className="flex items-center justify-between w-full">
                  <span>{dataset.name}</span>
                  <Badge variant="secondary" className="ml-2 text-xs">{dataset.file_count} files</Badge>
                </span>
              </DropdownMenuCheckboxItem>
            ))
          )}
        </DropdownMenuContent>
      </DropdownMenu>

      {!bypassLimit && isOverLimit && (
        <Alert className="border-amber-500 bg-amber-50 dark:bg-amber-950/20">
          <AlertTriangle className="h-4 w-4 text-amber-600" />
          <AlertDescription className="text-amber-700 dark:text-amber-400">
            Your selection contains {fileCount} documents, but analysis is limited to {maxDocuments} to
            ensure fast results and reasonable costs. A representative sample will be used.
          </AlertDescription>
        </Alert>
      )}
    </div>
  );
}

export default CloudDatasetPicker;
