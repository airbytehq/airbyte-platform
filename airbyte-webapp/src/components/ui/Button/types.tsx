import React from "react";

type ButtonSize = "xs" | "sm" | "lg";
export type ButtonVariant = "primary" | "secondary" | "danger" | "light" | "clear" | "dark" | "link";

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  full?: boolean;
  narrow?: boolean;
  icon?: React.ReactElement;
  iconPosition?: "left" | "right";
  isLoading?: boolean;
  size?: ButtonSize;
  variant?: ButtonVariant;
  width?: number;
  "data-testid"?: string;
}
