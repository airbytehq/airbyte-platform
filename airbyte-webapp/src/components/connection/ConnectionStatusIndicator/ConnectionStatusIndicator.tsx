import classNames from "classnames";
import React from "react";

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
  onTime: <Icon type="statusSuccess" size="lg" />,
  onTrack: <Icon type="statusSuccess" size="lg" />,
  error: <Icon type="statusWarning" size="lg" />,
  disabled: <Icon type="statusInactive" size="lg" />,
  pending: <Icon type="statusInactive" size="lg" />,
  late: <Icon type="clockFilled" size="lg" />,
  actionRequired: <Icon type="statusError" size="lg" />,
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
  <div
    className={classNames(styles.status, STYLE_BY_STATUS[status], { [BOX_STYLE_BY_STATUS[status]]: withBox })}
    data-loading={loading}
  >
    <div className={styles.icon}>{ICON_BY_STATUS[status]}</div>
    {loading && <LoadingSpinner className={styles.spinner} />}
  </div>
);
