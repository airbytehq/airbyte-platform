import { PropsWithChildren } from "react";

import styles from "./RowItem.module.scss";

interface RowItemProps {
  width?: number;
}

export const RowItem: React.FC<PropsWithChildren<RowItemProps>> = ({ children, width }) => {
  return (
    <div className={styles.rowItem} style={{ width }}>
      {children}
    </div>
  );
};
