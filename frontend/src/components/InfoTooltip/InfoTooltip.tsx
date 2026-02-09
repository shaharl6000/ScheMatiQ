import { useState } from 'react';
import { HelpCircle } from 'lucide-react';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

interface InfoTooltipProps {
  text: string;
  side?: 'top' | 'bottom' | 'left' | 'right';
}

export function InfoTooltip({ text, side = 'top' }: InfoTooltipProps) {
  const [open, setOpen] = useState(false);

  return (
    <Tooltip open={open} onOpenChange={setOpen}>
      <TooltipTrigger
        type="button"
        onClick={(e) => {
          e.preventDefault();
          setOpen((prev) => !prev);
        }}
        className="inline-flex items-center justify-center text-muted-foreground hover:text-foreground transition-colors"
        aria-label="More info"
      >
        <HelpCircle className="h-4 w-4" />
      </TooltipTrigger>
      <TooltipContent side={side} className="max-w-[280px]">
        <p>{text}</p>
      </TooltipContent>
    </Tooltip>
  );
}
