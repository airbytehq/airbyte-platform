import React from "react";

import { Icon } from "components/ui/Icon";

import styles from "./InfoTooltip.module.scss";
import { Tooltip } from "./Tooltip";
import { InfoTooltipProps } from "./types";

export const InfoTooltip: React.FC<React.PropsWithChildren<InfoTooltipProps>> = ({ children, ...props }) => {
  return (
    <Tooltip
      {...props}
      control={
        <span className={styles.container}>
          <span className={styles.icon}>
            <Icon type="infoOutline" size="sm" />
          </span>
        </span>
      }
    >
      {children}
    </Tooltip>
  );
};
