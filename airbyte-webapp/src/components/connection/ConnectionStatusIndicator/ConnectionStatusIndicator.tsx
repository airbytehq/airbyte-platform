import classNames from "classnames";
import React from "react";

import { ClockIcon } from "components/icons/ClockIcon";
import { SimpleCircleIcon } from "components/icons/SimpleCircleIcon";
import { SuccessIcon } from "components/icons/SuccessIcon";
import { WarningCircleIcon } from "components/icons/WarningCircleIcon";
import { Icon } from "components/ui/Icon";
import { LoadingSpinner } from "components/ui/LoadingSpinner";

import styles from "./ConnectionStatusIndicator.module.scss";

export enum ConnectionStatusIndicatorStatus {
  OnTime = "onTime",
  OnTrack = "onTrack",
  Late = "late",
  Pending = "pending",
  Error = "error",
  ActionRequired = "actionRequired",
  Disabled = "disabled",
}

const ICON_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, JSX.Element>> = {
  onTime: <SuccessIcon />,
  onTrack: <SuccessIcon />,
  error: <WarningCircleIcon />,
  disabled: <Icon type="pause" />,
  pending: <SimpleCircleIcon viewBox="2 2 20 20" />,
  late: <ClockIcon />,
  actionRequired: <Icon type="cross" withBackground size="sm" />,
};

const STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  onTime: styles["status--upToDate"],
  onTrack: styles["status--upToDate"],
  error: styles["status--error"],
  disabled: styles["status--disabled"],
  pending: styles["status--pending"],
  late: styles["status--late"],
  actionRequired: styles["status--actionRequired"],
};

const BOX_STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  onTime: styles["status--upToDate-withBox"],
  onTrack: styles["status--upToDate-withBox"],
  error: styles["status--error-withBox"],
  disabled: styles["status--disabled-withBox"],
  pending: styles["status--pending-withBox"],
  late: styles["status--late-withBox"],
  actionRequired: styles["status--actionRequired-withBox"],
};

interface ConnectionStatusIndicatorProps {
  status: ConnectionStatusIndicatorStatus;
  loading?: boolean;
  withBox?: boolean;
}

export const ConnectionStatusIndicator: React.FC<ConnectionStatusIndicatorProps> = ({ status, loading, withBox }) => (
  <div className={classNames(styles.status, STYLE_BY_STATUS[status], { [BOX_STYLE_BY_STATUS[status]]: withBox })}>
    <div className={styles.icon}>{ICON_BY_STATUS[status]}</div>
    {loading && <LoadingSpinner className={styles.spinner} />}
  </div>
);
