import classNames from "classnames";
import React from "react";

import styles from "./Toast.module.scss";
import { Message, MessageProps } from "../Message";

export interface ToastProps extends MessageProps {
  timeout?: boolean;
}

export const Toast: React.FC<ToastProps> = ({ timeout, ...props }) => {
  return (
    <div onAnimationEnd={props.onClose}>
      <Message
        {...props}
        className={classNames(props.className, styles.toastContainer, {
          [styles["toastContainer--timeout"]]: timeout,
        })}
        textClassName={styles.toastText}
      />
    </div>
  );
};
