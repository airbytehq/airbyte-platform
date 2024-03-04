import React from "react";

import styles from "./GoogleAuthButton.module.scss";
import googleAuthButton from "./googleAuthButton.svg";

export const GoogleAuthButton: React.FC<React.PropsWithChildren<unknown>> = ({ children, ...restProps }) => (
  <button className={styles.button} {...restProps}>
    <img src={googleAuthButton} className={styles.image} alt="" />
    {children}
  </button>
);
