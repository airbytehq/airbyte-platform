import classNames from "classnames";
import React from "react";

import styles from "./Toast.module.scss";
import { Message, MessageProps } from "../Message";

export type ToastProps = MessageProps;

export { MessageType as ToastType } from "../Message";

export const Toast: React.FC<ToastProps> = (props) => {
  return <Message {...props} className={classNames(props.className, styles.toastContainer)} />;
};
