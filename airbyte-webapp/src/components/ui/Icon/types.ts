export type IconType =
  | "arrowRight"
  | "credits"
  | "cross"
  | "chevronRight"
  | "docs"
  | "ga"
  | "info"
  | "lens"
  | "minus"
  | "modification"
  | "moon"
  | "pause"
  | "pencil"
  | "play"
  | "plus"
  | "rotate"
  | "nested";

export type IconColor = "primary" | "disabled" | "action" | "success" | "error" | "warning" | "affordance";

export interface IconProps {
  type: IconType;
  className?: string;
  size?: "xs" | "sm" | "md" | "lg" | "xl";
  color?: IconColor;
  withBackground?: boolean;
  title?: string;
}
