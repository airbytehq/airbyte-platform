import classNames from "classnames";
import React from "react";

import styles from "./CellText.module.scss";

type Sizes = "xsmall" | "small" | "medium" | "large" | "fixed";

export interface CellTextProps {
  size?: Sizes;
  className?: string;
  withOverflow?: boolean;
}

// This lets us avoid the eslint complaint about unused styles
const STYLES_BY_SIZE: Readonly<Record<Sizes, string>> = {
  xsmall: styles.xsmall,
  small: styles.small,
  medium: styles.medium,
  large: styles.large,
  fixed: styles.fixed,
};

export const CellText: React.FC<React.PropsWithChildren<CellTextProps>> = ({
  size = "medium",
  withOverflow,
  className,
  children,
  ...props
}) => {
  return (
    <div
      className={classNames(styles.container, className, STYLES_BY_SIZE[size], { [styles.withOverflow]: withOverflow })}
      {...props}
    >
      {children}
    </div>
  );
};
