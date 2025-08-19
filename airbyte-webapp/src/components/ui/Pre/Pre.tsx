import classNames from "classnames";
import React, { PropsWithChildren } from "react";

import styles from "./Pre.module.scss";

interface PreProps {
  className?: string;
  longLines?: boolean;
  wrapText?: boolean;
  "data-testid"?: string;
}

export const Pre: React.FC<PropsWithChildren<PreProps>> = ({
  className,
  longLines,
  wrapText,
  children,
  "data-testid": dataTestId,
}) => {
  return (
    <pre
      className={classNames(className, styles.content, { [styles.longLines]: longLines, [styles.wrapText]: wrapText })}
      data-testid={dataTestId}
    >
      {children}
    </pre>
  );
};
