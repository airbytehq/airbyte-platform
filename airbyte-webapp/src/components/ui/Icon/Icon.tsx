import classNames from "classnames";
import React from "react";

import styles from "./Icon.module.scss";
import { IconColor, IconProps, Icons } from "./types";

const sizeMap: Record<Exclude<IconProps["size"], undefined>, number> = {
  xs: 0.25,
  sm: 0.5,
  md: 1,
  lg: 2,
  xl: 4,
} as const;

const colorMap: Record<IconColor, string> = {
  action: styles.action,
  warning: styles.warning,
  success: styles.success,
  primary: styles.primary,
  error: styles.error,
  grey: styles.grey,
  darkBlue: styles.darkBlue,
};

export const Icon: React.FC<IconProps> = React.memo(({ type, color, size = "md", className, ...props }) => {
  const classes = classNames(className, color ? colorMap[color] : undefined);
  return React.createElement(Icons[type], {
    ...props,
    className: classes,
    style: {
      transform: `scale(${sizeMap[size]})`,
    },
  });
});
