export const ClockIcon: React.FC<{ className?: string; viewBox?: string }> = ({ className, viewBox = "0 0 24 24" }) => (
  <svg width="24" height="24" viewBox={viewBox} fill="none" className={className}>
    <path
      fill="currentColor"
      d="M12 19.5a7.5 7.5 0 1 1 0-15 7.5 7.5 0 0 1 0 15Zm.704-7V7h-1.5l.005 5.784a1 1 0 0 0 .4.798l1.386 1.04 2.086 1.476.83-1.198-3.207-2.4Z"
    />
  </svg>
);
