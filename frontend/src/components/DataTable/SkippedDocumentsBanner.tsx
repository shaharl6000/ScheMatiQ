import React, { useState } from 'react';
import { AlertTriangle, ChevronDown, ChevronUp, FileText, Search, X } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { SkippedDocument } from '../../types';

interface SkippedDocumentsBannerProps {
  skippedDocuments: SkippedDocument[];
  totalDocuments: number;
  observationUnitName?: string;
}

const SkippedDocumentsBanner: React.FC<SkippedDocumentsBannerProps> = ({
  skippedDocuments,
  totalDocuments,
  observationUnitName,
}) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');

  const count = skippedDocuments.length;
  if (count === 0 || totalDocuments === 0) return null;

  const percentage = Math.round((count / totalDocuments) * 100);

  const filtered = searchQuery
    ? skippedDocuments.filter(
        (d) =>
          d.document.toLowerCase().includes(searchQuery.toLowerCase()) ||
          (d.reason || '').toLowerCase().includes(searchQuery.toLowerCase()),
      )
    : skippedDocuments;

  return (
    <div className="mb-4 rounded-lg border border-amber-200 dark:border-amber-800 bg-amber-50/60 dark:bg-amber-950/40">
      {/* Collapsed bar */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center gap-2.5 px-3.5 py-2.5 text-left group"
      >
        <AlertTriangle className="h-4 w-4 text-amber-500 dark:text-amber-400 flex-shrink-0" />
        <span className="text-sm text-amber-900 dark:text-amber-100 flex-1">
          <strong>{count}</strong> of {totalDocuments} document{totalDocuments !== 1 ? 's' : ''} skipped
          <span className="text-amber-600 dark:text-amber-400 ml-1.5">({percentage}%)</span>
          {!isExpanded && (
            <span className="ml-2 text-xs text-amber-500 dark:text-amber-500">
              No matching {observationUnitName ? `"${observationUnitName}"` : 'observation units'} found
            </span>
          )}
        </span>
        {isExpanded ? (
          <ChevronUp className="h-4 w-4 text-amber-400 group-hover:text-amber-600 dark:group-hover:text-amber-300 transition-colors" />
        ) : (
          <ChevronDown className="h-4 w-4 text-amber-400 group-hover:text-amber-600 dark:group-hover:text-amber-300 transition-colors" />
        )}
      </button>

      {/* Expanded detail */}
      {isExpanded && (
        <div className="px-3.5 pb-3.5 space-y-2.5">
          {/* Search (only for many skips) */}
          {count > 5 && (
            <div className="relative">
              <Search className="absolute left-2.5 top-2 h-3.5 w-3.5 text-amber-500/50" />
              <Input
                placeholder="Filter documents..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="h-8 pl-8 pr-8 text-xs bg-white/60 dark:bg-amber-900/30 border-amber-200 dark:border-amber-700"
              />
              {searchQuery && (
                <button
                  onClick={() => setSearchQuery('')}
                  className="absolute right-2 top-2 text-amber-400 hover:text-amber-600"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              )}
            </div>
          )}

          {/* Document list */}
          <div className="rounded-md border border-amber-200/80 dark:border-amber-800/80 overflow-hidden max-h-64 overflow-y-auto">
            <table className="w-full text-xs">
              <thead className="sticky top-0">
                <tr className="bg-amber-100/70 dark:bg-amber-900/50 border-b border-amber-200 dark:border-amber-800">
                  <th className="px-2.5 py-1.5 text-left font-semibold text-amber-900 dark:text-amber-100 w-2/5">
                    Document
                  </th>
                  <th className="px-2.5 py-1.5 text-left font-semibold text-amber-900 dark:text-amber-100">
                    Reason
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-amber-100 dark:divide-amber-800/50">
                {filtered.length > 0 ? (
                  filtered.map((doc, i) => (
                    <tr
                      key={i}
                      className="hover:bg-amber-50 dark:hover:bg-amber-900/20 transition-colors"
                    >
                      <td className="px-2.5 py-1.5 text-amber-900 dark:text-amber-100 font-medium">
                        <div className="flex items-center gap-1.5">
                          <FileText className="h-3 w-3 flex-shrink-0 opacity-50" />
                          <span className="truncate">{doc.document}</span>
                        </div>
                      </td>
                      <td className="px-2.5 py-1.5 text-amber-700 dark:text-amber-300">
                        {doc.reason || (
                          <span className="text-amber-400 dark:text-amber-600 italic">
                            No reason provided
                          </span>
                        )}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td
                      colSpan={2}
                      className="px-2.5 py-3 text-center text-amber-500 italic"
                    >
                      No documents match "{searchQuery}"
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {percentage > 20 && (
            <p className="text-xs text-amber-600 dark:text-amber-400 italic">
              Consider reviewing your observation unit definition if too many documents are skipped.
            </p>
          )}
        </div>
      )}
    </div>
  );
};

export default SkippedDocumentsBanner;
