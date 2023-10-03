import classNames from "classnames";
import React, { PropsWithChildren } from "react";

import styles from "./Pre.module.scss";

interface PreProps {
  className?: string;
  longLines?: boolean;
}

export const Pre: React.FC<PropsWithChildren<PreProps>> = ({ className, longLines, children }) => {
  return <pre className={classNames(className, styles.content, { [styles.longLines]: longLines })}>{children}</pre>;
};
