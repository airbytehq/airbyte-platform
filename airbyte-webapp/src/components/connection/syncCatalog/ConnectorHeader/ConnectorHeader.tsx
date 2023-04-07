import React from "react";
import { FormattedMessage } from "react-intl";

import { getIcon } from "utils/imageUtils";

import styles from "./ConnectorHeader.module.scss";

interface ConnectorHeaderProps {
  type: "source" | "destination";
  icon?: string;
}

export const ConnectorHeader: React.FC<ConnectorHeaderProps> = ({ type, icon }) => {
  return (
    <span className={styles.container} data-testid={`connector-header-group-icon-container-${type}`}>
      <div className={styles.icon}>{getIcon(icon)}</div>
      <FormattedMessage id={`connector.${type}`} />
    </span>
  );
};
