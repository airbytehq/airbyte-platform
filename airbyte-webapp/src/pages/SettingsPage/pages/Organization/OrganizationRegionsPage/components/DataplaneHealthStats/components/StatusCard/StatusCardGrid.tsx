import React from "react";

import styles from "./StatusCardGrid.module.scss";

export const StatusCardGrid: React.FC<React.PropsWithChildren> = ({ children }) => (
  <div className={styles.grid}>{children}</div>
);
