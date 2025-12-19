import classNames from "classnames";
import React from "react";

import styles from "./ChatInterfaceContainer.module.scss";

export interface ChatInterfaceContainerProps {
  children: React.ReactNode;
  className?: string;
}

export const ChatInterfaceContainer: React.FC<ChatInterfaceContainerProps> = ({ children, className }) => {
  return <div className={classNames(styles.container, className)}>{children}</div>;
};
