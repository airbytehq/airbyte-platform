import classNames from "classnames";
import React from "react";

import styles from "./ChatInterfaceHeader.module.scss";

export interface ChatInterfaceHeaderProps {
  children: React.ReactNode;
  className?: string;
}

export const ChatInterfaceHeader: React.FC<ChatInterfaceHeaderProps> = ({ children, className }) => {
  return <div className={classNames(styles.header, className)}>{children}</div>;
};
