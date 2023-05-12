import classNames from "classnames";
import React from "react";

import { ClockIcon } from "components/icons/ClockIcon";
import { SuccessIcon } from "components/icons/SuccessIcon";
import { Icon } from "components/ui/Icon";

import styles from "./ConnectionStatusIndicator.module.scss";
import { ConnectionStatusLoadingSpinner } from "./ConnectionStatusLoadingSpinner";

export enum ConnectionStatusIndicatorStatus {
  OnTime = "onTime",
  OnTrack = "onTrack",
  Late = "late",
  Pending = "pending",
  Error = "error",
  ActionRequired = "actionRequired",
  Disabled = "disabled",
  Cancelled = "cancelled",
}

const ICON_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, JSX.Element>> = {
  onTime: <SuccessIcon />,
  onTrack: <SuccessIcon />,
  error: <Icon type="cross" />,
  disabled: <Icon type="pause" />,
  pending: <ClockIcon />,
  late: <ClockIcon />,
  actionRequired: <Icon type="cross" withBackground />,
  cancelled: <Icon type="minus" color="action" withBackground />,
};

const STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  onTime: styles["status--upToDate"],
  onTrack: styles["status--upToDate"],
  error: styles["status--error"],
  disabled: styles["status--disabled"],
  pending: styles["status--pending"],
  late: styles["status--late"],
  actionRequired: styles["status--actionRequired"],
  cancelled: styles["status--cancelled"],
};

const BOX_STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  onTime: styles["status--upToDate-withBox"],
  onTrack: styles["status--upToDate-withBox"],
  error: styles["status--error-withBox"],
  disabled: styles["status--disabled-withBox"],
  pending: styles["status--pending-withBox"],
  late: styles["status--late-withBox"],
  actionRequired: styles["status--actionRequired-withBox"],
  cancelled: styles["status--cancelled-withBox"],
};

interface ConnectionStatusIndicatorProps {
  status: ConnectionStatusIndicatorStatus;
  loading?: boolean;
  withBox?: boolean;
}

export const ConnectionStatusIndicator: React.FC<ConnectionStatusIndicatorProps> = ({ status, loading, withBox }) => (
  <div className={classNames(styles.status, STYLE_BY_STATUS[status], { [BOX_STYLE_BY_STATUS[status]]: withBox })}>
    <div className={styles.icon}>{ICON_BY_STATUS[status]}</div>
    {loading && <ConnectionStatusLoadingSpinner className={styles.spinner} />}
  </div>
);
