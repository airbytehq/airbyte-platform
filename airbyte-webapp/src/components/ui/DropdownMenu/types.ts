import { Placement } from "@floating-ui/react-dom";
import React from "react";

export type DisplacementType = 5 | 10; // $spacing-sm, $spacing-md

export type DropdownMenuItemElementType = "a" | "button";

export type DropdownMenuItemIconPositionType = "left" | "right";

export interface DropdownMenuOptionBaseType {
  icon?: React.ReactNode;
  iconPosition?: DropdownMenuItemIconPositionType;
  displayName: string;
  value?: unknown;
  className?: string;
  disabled?: boolean;
  "data-testid"?: string;
}

interface DropdownMenuSeparator {
  as: "separator";
}

export type DropdownMenuOptionType = DropdownMenuOptionAnchorType | DropdownMenuOptionButtonType;
export type DropdownMenuOptions = Array<DropdownMenuOptionType | DropdownMenuSeparator>;

type DropdownMenuOptionButtonType = DropdownMenuOptionBaseType & { as?: "button" };

export type DropdownMenuOptionAnchorType = DropdownMenuOptionBaseType & {
  as: "a";
  href: string;
  internal?: boolean;
};

export interface MenuItemContentProps {
  data: DropdownMenuOptionType;
  active?: boolean;
}

export interface DropdownMenuProps {
  options: DropdownMenuOptions;
  children: ({ open }: { open: boolean }) => React.ReactNode;
  onChange?: (data: DropdownMenuOptionType) => void;
  placement?: Placement;
  displacement?: DisplacementType;
  "data-testid"?: string;
}
