import classNames from "classnames";
import React, { PropsWithChildren } from "react";

import styles from "./Pre.module.scss";

interface PreProps {
  className?: string;
}

export const Pre: React.FC<PropsWithChildren<PreProps>> = ({ className, children }) => {
  return <pre className={classNames(className, styles.content)}>{children}</pre>;
};
