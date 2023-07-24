import classNames from "classnames";
import React from "react";

import { SvgIcon } from "area/connector/utils";

import styles from "./ConnectorIcon.module.scss";

interface ConnectorIconProps {
  icon?: string;
  className?: string;
}

export const ConnectorIcon: React.FC<ConnectorIconProps> = ({ className, icon }) => (
  <div className={classNames(styles.content, className)} aria-hidden="true">
    <SvgIcon svg={icon} />
  </div>
);
