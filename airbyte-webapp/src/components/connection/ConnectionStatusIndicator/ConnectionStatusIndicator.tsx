import classNames from "classnames";
import React from "react";

import { Icon } from "components/ui/Icon";
import { CircleLoader } from "components/ui/StatusIcon/CircleLoader";

import { useExperiment } from "hooks/services/Experiment";

import styles from "./ConnectionStatusIndicator.module.scss";
import { StreamStatusLoadingSpinner } from "../StreamStatusIndicator";

export enum ConnectionStatusIndicatorStatus {
  OnTime = "onTime",
  OnTrack = "onTrack",
  Late = "late",
  Pending = "pending",
  Syncing = "syncing",
  Queued = "queued",
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
  syncing: <CircleLoader />,
  queued: <CircleLoader />,
};

const STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  onTime: styles["status--upToDate"],
  onTrack: styles["status--upToDate"],
  error: styles["status--error"],
  disabled: styles["status--disabled"],
  pending: styles["status--pending"],
  late: styles["status--late"],
  actionRequired: styles["status--actionRequired"],
  syncing: styles["status--syncing"],
  queued: styles["status--syncing"],
};

const BOX_STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  onTime: styles["status--upToDate-withBox"],
  onTrack: styles["status--upToDate-withBox"],
  error: styles["status--error-withBox"],
  disabled: styles["status--disabled-withBox"],
  pending: styles["status--pending-withBox"],
  late: styles["status--late-withBox"],
  actionRequired: styles["status--actionRequired-withBox"],
  syncing: styles["status--syncing-withBox"],
  queued: styles["status--syncing-withBox"],
};

interface ConnectionStatusIndicatorProps {
  status: ConnectionStatusIndicatorStatus;
  loading?: boolean;
  withBox?: boolean;
}

export const ConnectionStatusIndicator: React.FC<ConnectionStatusIndicatorProps> = ({ status, loading, withBox }) => {
  const showSyncProgress = useExperiment("connection.syncProgress", false);

  return (
    <div
      className={classNames(styles.status, STYLE_BY_STATUS[status], { [BOX_STYLE_BY_STATUS[status]]: withBox })}
      data-loading={loading}
      data-testid="connection-status-indicator"
      data-status={status}
    >
      <div className={styles.icon}>{ICON_BY_STATUS[status]}</div>
      {!showSyncProgress && loading && <StreamStatusLoadingSpinner className={styles.spinner} />}
    </div>
  );
};
