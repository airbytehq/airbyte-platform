import React from "react";

import { IconProps, Icons } from "./types";

const sizeMap: Record<Exclude<IconProps["size"], undefined>, number> = {
  xs: 0.25,
  sm: 0.5,
  md: 1,
  lg: 2,
  xl: 4,
} as const;

export const Icon: React.FC<IconProps> = React.memo(({ type, color, size = "md", ...props }) => {
  return React.createElement(Icons[type], {
    ...props,
    style: {
      transform: `scale(${sizeMap[size]})`,
      color,
    },
  });
});
