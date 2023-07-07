import React from "react";

import Indicator from "components/Indicator";

import styles from "./NotificationIndicator.module.scss";

export const NotificationIndicator: React.FC = () => {
  return <Indicator className={styles.notification} />;
};
