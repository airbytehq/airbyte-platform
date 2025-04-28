import { OffsetOptions } from "@floating-ui/core";
import { Placement } from "@floating-ui/react-dom";
import React from "react";

import { TextSize } from "../Text";

export type DisplacementType = OffsetOptions;

export type DropdownMenuItemElementType = "a" | "button";

export type DropdownMenuItemIconPositionType = "left" | "right";

export interface DropdownMenuOptionBaseType {
  icon?: React.ReactNode;
  iconPosition?: DropdownMenuItemIconPositionType;
  displayName: string;
  value?: unknown;
  className?: string;
  disabled?: boolean;
  tooltipContent?: React.ReactNode;
  "data-testid"?: string;
}

interface DropdownMenuSeparator {
  as: "separator";
}

interface DropdownMenuDiv {
  as: "div";
  className?: string;
  children?: React.ReactNode;
}

export type DropdownMenuOptionType = DropdownMenuOptionAnchorType | DropdownMenuOptionButtonType;
export type DropdownMenuOptions = Array<DropdownMenuOptionType | DropdownMenuSeparator | DropdownMenuDiv>;

type DropdownMenuOptionButtonType = DropdownMenuOptionBaseType & { as?: "button" };

export type DropdownMenuOptionAnchorType = DropdownMenuOptionBaseType & {
  as: "a";
  href: string;
  internal?: boolean;
};

export interface MenuItemContentProps {
  data: DropdownMenuOptionType;
  active?: boolean;
  textSize?: TextSize;
}

export interface DropdownMenuProps {
  options: DropdownMenuOptions;
  children: ({ open }: { open: boolean }) => React.ReactNode;
  onChange?: (data: DropdownMenuOptionType) => void;
  placement?: Placement;
  displacement?: DisplacementType;
  "data-testid"?: string;
  textSize?: TextSize;
  style?: React.CSSProperties;
}
