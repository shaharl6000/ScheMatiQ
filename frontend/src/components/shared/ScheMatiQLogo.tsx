import { useId } from 'react';

export interface ScheMatiQLogoProps {
  className?: string;
  /** Accessible label. Omit (or pass empty string) when decorative / paired with text. */
  label?: string;
}

/**
 * ScheMatiQ "Grid Q" brand mark — geometric Q-lens enclosing structured schema cells.
 * Uses `useId()` so multiple instances on the same page never share gradient IDs.
 */
export function ScheMatiQLogo({ className, label }: ScheMatiQLogoProps) {
  const uid = useId();
  const gradId = `schematiq-grid-q-${uid}`;
  const isDecorative = !label;

  return (
    <svg
      viewBox="0 0 32 32"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      role={isDecorative ? undefined : 'img'}
      aria-label={isDecorative ? undefined : label}
      aria-hidden={isDecorative ? 'true' : undefined}
    >
      <defs>
        <linearGradient id={gradId} x1="3" y1="3" x2="29" y2="29" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#3b5bdb" />
          <stop offset="100%" stopColor="#60a5fa" />
        </linearGradient>
      </defs>
      <rect
        x="4"
        y="4"
        width="20"
        height="20"
        rx="5.5"
        stroke={`url(#${gradId})`}
        strokeWidth="2.6"
      />
      <path
        d="M22.4 22.4L27.8 27.8"
        stroke={`url(#${gradId})`}
        strokeWidth="2.6"
        strokeLinecap="round"
      />
      <rect x="8.2" y="8.2" width="11.6" height="3.5" rx="1.2" fill="#3b5bdb" />
      <rect x="8.2" y="13.5" width="5" height="6.1" rx="1.2" fill="#3b5bdb" fillOpacity="0.34" />
      <rect x="14.8" y="13.5" width="5" height="6.1" rx="1.2" fill="#4f46e5" fillOpacity="0.88" />
    </svg>
  );
}
