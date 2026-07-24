import { HelpCircle } from 'lucide-react';
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover';

interface InfoTooltipProps {
  text: string;
  side?: 'top' | 'bottom' | 'left' | 'right';
}

export function InfoTooltip({ text, side = 'top' }: InfoTooltipProps) {
  return (
    <Popover>
      <PopoverTrigger
        type="button"
        onClick={(e) => e.preventDefault()}
        className="inline-flex items-center justify-center text-muted-foreground hover:text-foreground transition-colors"
        aria-label="More info"
      >
        <HelpCircle className="h-4 w-4" />
      </PopoverTrigger>
      <PopoverContent
        side={side}
        align="center"
        collisionPadding={8}
        className="w-auto max-w-[280px] p-3 text-sm font-normal"
      >
        {text}
      </PopoverContent>
    </Popover>
  );
}
