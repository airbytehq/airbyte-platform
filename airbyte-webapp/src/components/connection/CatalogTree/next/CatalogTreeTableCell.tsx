import classNames from "classnames";
import React from "react";

import styles from "./CatalogTreeTableCell.module.scss";

type Sizes = "xsmall" | "small" | "medium" | "large" | "fixed";

export interface CatalogTreeTableCellProps {
  size?: Sizes;
  className?: string;
  withTooltip?: boolean;
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

export const CatalogTreeTableCell: React.FC<React.PropsWithChildren<CatalogTreeTableCellProps>> = ({
  size = "medium",
  className,
  children,
}) => {
  return <div className={classNames(styles.tableCell, className, STYLES_BY_SIZE[size])}>{children}</div>;
};
