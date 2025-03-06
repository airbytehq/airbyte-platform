import classNames from "classnames";
import React, { HTMLAttributes } from "react";

import styles from "./Heading.module.scss";

type HeadingSize = "sm" | "md" | "lg" | "xl";
type HeadingColor = "darkBlue" | "blue";
type HeadingElementType = "h1" | "h2" | "h3" | "h4" | "h5" | "h6";

type HeadingProps = HTMLAttributes<HTMLHeadingElement> & {
  className?: string;
  centered?: boolean;
  as: HeadingElementType;
  size?: HeadingSize;
  color?: HeadingColor;
};

const sizes: Record<HeadingSize, string> = {
  sm: styles["heading--sm"],
  md: styles["heading--md"],
  lg: styles["heading--lg"],
  xl: styles["heading--xl"],
};

const colors: Record<HeadingColor, string> = {
  darkBlue: styles["heading--darkBlue"],
  blue: styles["heading--blue"],
};

const getHeadingClassNames = ({ size, color }: Required<Pick<HeadingProps, "size" | "color" | "centered">>) =>
  classNames(styles.heading, sizes[size], colors[color]);

export const Heading: React.FC<React.PropsWithChildren<HeadingProps>> = React.memo(
  ({
    as,
    centered = false,
    children,
    className: classNameProp,
    size = "md",
    color = "darkBlue",
    ...remainingProps
  }) => {
    const className = classNames(getHeadingClassNames({ centered, size, color }), classNameProp);

    return React.createElement(as, {
      ...remainingProps,
      className,
      children,
    });
  }
);
Heading.displayName = "Heading";
