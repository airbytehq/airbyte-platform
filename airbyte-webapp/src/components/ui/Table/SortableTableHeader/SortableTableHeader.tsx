import classNames from "classnames";
import React, { PropsWithChildren } from "react";

import { Icon, IconSize } from "components/ui/Icon";

import styles from "./SortableTableHeader.module.scss";

interface SortableTableHeaderProps {
  onClick: () => void;
  isActive: boolean;
  isAscending: boolean;
  className?: string;
  activeClassName?: string;
  iconSize?: IconSize;
}

export const SortableTableHeader: React.FC<PropsWithChildren<SortableTableHeaderProps>> = ({
  onClick,
  isActive,
  isAscending,
  children,
  className,
  activeClassName,
  iconSize,
}) => (
  <button
    className={classNames(styles.sortButton, className, {
      ...(activeClassName ? { [activeClassName]: isActive } : {}),
    })}
    onClick={onClick}
    type="button"
  >
    {children}
    <Icon
      className={styles.sortIcon}
      type={!isActive ? "unsorted" : isAscending ? "chevronUp" : "chevronDown"}
      size={iconSize}
    />
  </button>
);
