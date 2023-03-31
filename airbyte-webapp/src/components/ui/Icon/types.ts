export type IconType =
  | "arrowRight"
  | "credits"
  | "cross"
  | "docs"
  | "ga"
  | "info"
  | "minus"
  | "modification"
  | "moon"
  | "pause"
  | "pencil"
  | "play"
  | "plus"
  | "rotate";

export type IconColor = "primary" | "disabled" | "action" | "success" | "error" | "warning";

export interface IconProps {
  type: IconType;
  className?: string;
  size?: "xs" | "sm" | "md" | "lg" | "xl";
  color?: IconColor;
  withBackground?: boolean;
  title?: string;
}
