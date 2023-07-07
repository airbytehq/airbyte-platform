interface SimpleCircleIconProps {
  className?: string;
  viewBox?: string;
}

export const SimpleCircleIcon: React.FC<SimpleCircleIconProps> = ({ className, viewBox = "0 0 24 24" }) => (
  <svg width="24" height="24" viewBox={viewBox} fill="none" className={className}>
    <path
      d="M12 19.5C7.85775 19.5 4.5 16.1423 4.5 12C4.5 7.85775 7.85775 4.5 12 4.5C16.1423 4.5 19.5 7.85775 19.5 12C19.5 16.1423 16.1423 19.5 12 19.5Z"
      fill="currentColor"
    />
  </svg>
);
