import classNames from "classnames";
import React, { ReactNode } from "react";

import styles from "./Pre.module.scss";
import { CopyButton } from "../../CopyButton";

export const Pre = ({ children, className, ...props }: { children: ReactNode; className?: string }) => {
  return (
    <pre className={classNames(className, styles.codeBlock)} {...props}>
      {React.isValidElement(children) && children.type === "code" ? (
        <CopyButton className={styles.codeCopyButton} content={children.props.children} />
      ) : null}
      {children}
    </pre>
  );
};
