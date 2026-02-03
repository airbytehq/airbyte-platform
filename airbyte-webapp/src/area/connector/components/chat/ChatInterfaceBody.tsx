import classNames from "classnames";
import React from "react";

import styles from "./ChatInterfaceBody.module.scss";

export interface ChatInterfaceBodyProps {
  children: React.ReactNode;
  className?: string;
}

export const ChatInterfaceBody: React.FC<ChatInterfaceBodyProps> = ({ children, className }) => {
  return <div className={classNames(styles.chatContainer, className)}>{children}</div>;
};
