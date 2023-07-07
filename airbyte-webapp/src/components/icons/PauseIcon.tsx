interface Props {
  color?: string;
  title?: string;
}

export const PauseIcon = ({ color = "currentColor", title }: Props): JSX.Element => (
  <svg viewBox="0 0 6 10" fill="none" role="img" data-icon="pause">
    {title && <title>{title}</title>}
    <line x1="1.5" y1="2" x2="1.5" y2="8" stroke={color} strokeWidth="1.5" />
    <line x1="4.5" y1="2" x2="4.5" y2="8" stroke={color} strokeWidth="1.5" />
  </svg>
);
