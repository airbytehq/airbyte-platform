import classnames from "classnames";
import React from "react";

import styles from "./AlertBanner.module.scss";

interface AlertBannerProps {
  color?: "default" | "warning" | "error";
  message: React.ReactNode;
}

export const AlertBanner: React.FC<AlertBannerProps> = ({ color = "default", message }) => {
  const bannerStyle = classnames(styles.alertBannerContainer, {
    [styles.default]: color === "default",
    [styles.yellow]: color === "warning",
    [styles.red]: color === "error",
  });

  return <div className={bannerStyle}>{message}</div>;
};
