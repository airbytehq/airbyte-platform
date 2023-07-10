import classNames from "classnames";
import { PropsWithChildren } from "react";

import styles from "./PageContainer.module.scss";

interface PageContainerProps {
  centered?: boolean;
}

export const PageContainer: React.FC<PropsWithChildren<PageContainerProps>> = ({ children, centered }) => {
  return (
    <div className={classNames(styles.pageContainer, { [styles["pageContainer--centered"]]: centered })}>
      {children}
    </div>
  );
};
