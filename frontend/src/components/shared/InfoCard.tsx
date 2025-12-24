import React from 'react';
import { cn } from '@/lib/utils';

interface InfoCardProps {
  title: string;
  value: string | number;
  description?: string;
  size?: 'small' | 'medium';
  className?: string;
}

const InfoCard: React.FC<InfoCardProps> = ({
  title,
  value,
  description,
  size = 'medium',
  className,
}) => {
  return (
    <div className={cn(
      'bg-background rounded-lg border border-border',
      size === 'small' ? 'p-3' : 'p-4',
      className
    )}>
      <div className={cn(
        'text-muted-foreground font-medium',
        size === 'small' ? 'text-xs' : 'text-sm'
      )}>
        {title}
      </div>
      <div className={cn(
        'font-bold text-foreground mt-1',
        size === 'small' ? 'text-lg' : 'text-2xl'
      )}>
        {value}
      </div>
      {description && (
        <div className="text-xs text-muted-foreground mt-1">
          {description}
        </div>
      )}
    </div>
  );
};

export default InfoCard;
