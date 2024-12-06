import classNames from "classnames";
import { PropsWithChildren } from "react";

import styles from "./PageGridContainer.module.scss";

export const PageGridContainer: React.FC<PropsWithChildren<{ className?: string }>> = ({ children, className }) => {
  return <div className={classNames(className, styles.container)}>{children}</div>;
};
