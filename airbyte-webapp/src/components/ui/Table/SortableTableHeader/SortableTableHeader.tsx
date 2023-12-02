import React, { PropsWithChildren } from "react";

import { Icon } from "components/ui/Icon";

import styles from "./SortableTableHeader.module.scss";

interface SortableTableHeaderProps {
  onClick: () => void;
  isActive: boolean;
  isAscending: boolean;
}

export const SortableTableHeader: React.FC<PropsWithChildren<SortableTableHeaderProps>> = ({
  onClick,
  isActive,
  isAscending,
  children,
}) => (
  <button className={styles.sortButton} onClick={onClick}>
    {children}
    <Icon className={styles.sortIcon} type={isAscending || !isActive ? "chevronUp" : "chevronDown"} />
  </button>
);
