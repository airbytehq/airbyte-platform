import classnames from "classnames";
import React from "react";

import styles from "./AlertBanner.module.scss";

interface AlertBannerProps {
  color?: "info" | "warning" | "error";
  message: React.ReactNode;
  "data-testid"?: string;
}

export const AlertBanner: React.FC<AlertBannerProps> = ({ color = "info", message, ...rest }) => {
  const bannerStyle = classnames(styles.alertBannerContainer, {
    [styles.info]: color === "info",
    [styles.yellow]: color === "warning",
    [styles.red]: color === "error",
  });

  return (
    <div className={bannerStyle} {...rest}>
      {message}
    </div>
  );
};
