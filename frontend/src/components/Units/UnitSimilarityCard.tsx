/**
 * Card component showing a merge suggestion for similar observation units.
 */

import React from 'react';
import { Merge, X, Lightbulb } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { UnitSimilarity } from '../../types/unit';

interface UnitSimilarityCardProps {
  /** The similarity suggestion data */
  suggestion: UnitSimilarity;
  /** Callback when user clicks to merge */
  onMerge: (suggestion: UnitSimilarity) => void;
  /** Callback when user dismisses the suggestion */
  onDismiss: (suggestion: UnitSimilarity) => void;
  /** Whether a merge operation is in progress */
  loading?: boolean;
}

export const UnitSimilarityCard: React.FC<UnitSimilarityCardProps> = ({
  suggestion,
  onMerge,
  onDismiss,
  loading = false,
}) => {
  const similarityPercent = Math.round(suggestion.similarity * 100);

  return (
    <Card className="relative overflow-hidden">
      {/* Dismiss button */}
      <Button
        variant="ghost"
        size="icon"
        className="absolute top-2 right-2 h-6 w-6 text-muted-foreground hover:text-foreground"
        onClick={() => onDismiss(suggestion)}
        disabled={loading}
      >
        <X className="h-4 w-4" />
      </Button>

      <CardContent className="pt-4 space-y-3">
        {/* Header with lightbulb icon */}
        <div className="flex items-start gap-2">
          <Lightbulb className="h-4 w-4 text-yellow-500 mt-0.5 shrink-0" />
          <div className="text-sm">
            <p className="font-medium">Potential Duplicate</p>
            <p className="text-muted-foreground text-xs">{suggestion.reason}</p>
          </div>
        </div>

        {/* Units being compared */}
        <div className="flex flex-wrap gap-2">
          {suggestion.units.map((unit, index) => (
            <React.Fragment key={unit}>
              <Badge variant="outline" className="max-w-[140px]">
                <span className="truncate">{unit}</span>
              </Badge>
              {index < suggestion.units.length - 1 && (
                <span className="text-muted-foreground self-center">≈</span>
              )}
            </React.Fragment>
          ))}
        </div>

        {/* Similarity score */}
        <div className="space-y-1">
          <div className="flex items-center justify-between text-xs">
            <span className="text-muted-foreground">Similarity</span>
            <span className="font-medium">{similarityPercent}%</span>
          </div>
          <Progress value={similarityPercent} className="h-1.5" />
        </div>

        {/* Suggested merged name */}
        <div className="text-xs">
          <span className="text-muted-foreground">Suggested name: </span>
          <span className="font-medium">{suggestion.suggestedName}</span>
        </div>

        {/* Action button */}
        <Button
          size="sm"
          className="w-full"
          onClick={() => onMerge(suggestion)}
          disabled={loading}
        >
          <Merge className="h-4 w-4 mr-2" />
          Merge These Units
        </Button>
      </CardContent>
    </Card>
  );
};

export default UnitSimilarityCard;
