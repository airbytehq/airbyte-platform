import classnames from "classnames";
import React from "react";

import { Text } from "components/ui/Text";

import styles from "./NumberBadge.module.scss";

interface NumberBadgeProps {
  value: number;
  color?: "green" | "red" | "blue" | "default" | "grey" | "yellow";
  outline?: boolean;
  className?: string;
  "aria-label"?: string;
  small?: boolean;
  inverse?: boolean;
}

export const NumberBadge: React.FC<NumberBadgeProps> = ({
  value,
  color,
  className,
  outline = false,
  "aria-label": ariaLabel,
  small = false,
  inverse = false,
}) => {
  const numberBadgeClassnames = classnames(styles.circle, className, {
    [styles.small]: small,
    [styles.inverse]: inverse,
    [styles.default]: !color || color === "default",
    [styles["grey--outline"]]: outline === true && color === "grey",
    [styles["blue--outline"]]: outline === true && color === "blue",
    [styles.green]: color === "green",
    [styles.red]: color === "red",
    [styles.blue]: color === "blue",
    [styles.yellow]: color === "yellow",
  });

  const inverseColor = inverse || (color === "blue" && !outline);

  return (
    <div className={numberBadgeClassnames} aria-label={ariaLabel}>
      <Text
        size="xs"
        color={color === "blue" && outline ? "blue" : color === "grey" && outline ? "grey300" : undefined}
        inverseColor={inverseColor}
        align="center"
      >
        {value}
      </Text>
    </div>
  );
};
