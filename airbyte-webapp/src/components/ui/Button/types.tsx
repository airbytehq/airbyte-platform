import React from "react";

import { IconProps } from "../Icon";

type ButtonSize = "xs" | "sm" | "lg";
export type ButtonVariant =
  | "primary"
  | "secondary"
  | "danger"
  | "magic"
  | "light"
  | "clear"
  | "clearDark"
  | "primaryDark"
  | "secondaryDark"
  | "link";

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  full?: boolean;
  narrow?: boolean;
  isLoading?: boolean;
  size?: ButtonSize;
  variant?: ButtonVariant;
  width?: number;
  icon?: IconProps["type"];
  iconSize?: IconProps["size"];
  iconColor?: IconProps["color"];
  iconClassName?: IconProps["className"];
  iconPosition?: "left" | "right";
  "data-testid"?: string;
}
