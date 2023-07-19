import classNames from "classnames";
import React from "react";

import styles from "./Heading.module.scss";

type HeadingSize = "sm" | "md" | "lg" | "xl";
type HeadingColor = "darkBlue" | "blue";
type HeadingElementType = "h1" | "h2" | "h3" | "h4" | "h5" | "h6";

interface HeadingProps {
  className?: string;
  centered?: boolean;
  as: HeadingElementType;
  size?: HeadingSize;
  color?: HeadingColor;
  inverseColor?: boolean;
}

const sizes: Record<HeadingSize, string> = {
  sm: styles.sm,
  md: styles.md,
  lg: styles.lg,
  xl: styles.xl,
};

const colors: Record<HeadingColor, string> = {
  darkBlue: styles.darkBlue,
  blue: styles.blue,
};

const getHeadingClassNames = ({
  size,
  color,
  centered,
  inverseColor,
}: Required<Pick<HeadingProps, "size" | "color" | "centered" | "inverseColor">>) =>
  classNames(styles.heading, sizes[size], colors[color], {
    [styles.centered]: centered,
    [styles.inverse]: inverseColor,
  });

export const Heading: React.FC<React.PropsWithChildren<HeadingProps>> = React.memo(
  ({
    as,
    centered = false,
    children,
    className: classNameProp,
    size = "md",
    color = "darkBlue",
    inverseColor = false,
    ...remainingProps
  }) => {
    const className = classNames(getHeadingClassNames({ centered, size, color, inverseColor }), classNameProp);

    return React.createElement(as, {
      ...remainingProps,
      className,
      children,
    });
  }
);
Heading.displayName = "Heading";
