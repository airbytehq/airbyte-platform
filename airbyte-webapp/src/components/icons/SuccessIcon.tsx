interface SuccessIconProps {
  className?: string;
}

export const SuccessIcon: React.FC<SuccessIconProps> = ({ className }) => (
  <svg width="16" height="16" viewBox="0 0 16 16" fill="none" className={className}>
    <path
      d="M8 15.5C3.85775 15.5 0.5 12.1423 0.5 8C0.5 3.85775 3.85775 0.5 8 0.5C12.1423 0.5 15.5 3.85775 15.5 8C15.5 12.1423 12.1423 15.5 8 15.5ZM7.25225 11L12.5548 5.69675L11.4943 4.63625L7.25225 8.879L5.1305 6.75725L4.07 7.81775L7.25225 11Z"
      fill="currentColor"
    />
  </svg>
);
