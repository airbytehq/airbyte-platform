import classNames from "classnames";
import React, { useEffect, useState } from "react";

import styles from "./Toast.module.scss";
import { Message, MessageProps } from "../Message";

export interface ToastProps extends MessageProps {
  timeout?: boolean;
}

export const Toast: React.FC<ToastProps> = ({ timeout, ...props }) => {
  // Delay the timeout animation until the user has focused the current tab
  const [isFocused, setIsFocused] = useState(document.hasFocus());
  useEffect(() => {
    const onFocus = () => setIsFocused(true);
    const onBlur = () => setIsFocused(false);

    window.addEventListener("focus", onFocus);
    window.addEventListener("blur", onBlur);

    return () => {
      window.removeEventListener("focus", onFocus);
      window.removeEventListener("blur", onBlur);
    };
  }, []);

  return (
    <div onAnimationEnd={props.onClose}>
      <Message
        {...props}
        className={classNames(props.className, styles.toastContainer, {
          [styles["toastContainer--timeout"]]: timeout,
          [styles["toastContainer--focused"]]: isFocused,
        })}
        textClassName={styles.toastText}
      />
    </div>
  );
};
