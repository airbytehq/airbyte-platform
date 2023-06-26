import { PropsWithChildren } from "react";

import styles from "./PageContainer.module.scss";

export const PageContainer: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  return <div className={styles.pageContainer}>{children}</div>;
};
