import classNames from "classnames";
import React, { cloneElement } from "react";

import { Icon, IconProps } from "components/ui/Icon";
import { CircleLoader } from "components/ui/StatusIcon/CircleLoader";

import styles from "./ConnectionStatusIndicator.module.scss";

export enum ConnectionStatusIndicatorStatus {
  Synced = "synced",
  Pending = "pending",
  Paused = "paused",
  Syncing = "syncing",
  Queued = "queued",
  Incomplete = "incomplete",
  Failed = "failed",
  Disabled = "disabled",
  QueuedForNextSync = "queuedForNextSync",
  Clearing = "clearing",
  Refreshing = "refreshing",
  RateLimited = "rateLimited",
}

const ICON_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, JSX.Element>> = {
  synced: <Icon type="statusSuccess" size="md" />,
  incomplete: <Icon type="statusWarning" size="md" />,
  disabled: <Icon type="statusInactive" size="md" />,
  paused: <Icon type="statusInactive" size="md" />,
  pending: <Icon type="statusInactive" size="md" />,
  failed: <Icon type="statusError" size="md" />,
  syncing: <CircleLoader className={styles.circleLoader} />,
  clearing: <CircleLoader className={styles.circleLoader} />,
  refreshing: <CircleLoader className={styles.circleLoader} />,
  queued: <Icon type="statusQueued" title="Queued" size="md" />,
  queuedForNextSync: <Icon type="statusQueued" title="Queued" size="md" />,
  rateLimited: <CircleLoader className={styles.circleLoader} />,
};

const STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  synced: styles["status--upToDate"],
  incomplete: styles["status--incomplete"],
  disabled: styles["status--disabled"],
  paused: styles["status--disabled"],
  pending: styles["status--pending"],
  failed: styles["status--failed"],
  syncing: styles["status--syncing"],
  clearing: styles["status--syncing"],
  refreshing: styles["status--syncing"],
  queued: styles["status--syncing"],
  queuedForNextSync: styles["status--syncing"],
  rateLimited: styles["status--syncing"],
};

const BOX_STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  synced: styles["status--upToDate-withBox"],
  incomplete: styles["status--incomplete-withBox"],
  disabled: styles["status--disabled-withBox"],
  paused: styles["status--disabled-withBox"],
  pending: styles["status--pending-withBox"],
  failed: styles["status--failed-withBox"],
  syncing: styles["status--syncing-withBox"],
  clearing: styles["status--syncing-withBox"],
  refreshing: styles["status--syncing-withBox"],
  queued: styles["status--syncing-withBox"],
  queuedForNextSync: styles["status--syncing-withBox"],
  rateLimited: styles["status--syncing-withBox"],
};

interface ConnectionStatusIndicatorProps {
  status: ConnectionStatusIndicatorStatus;
  loading?: boolean;
  withBox?: boolean;
  size?: IconProps["size"];
}

export const ConnectionStatusIndicator: React.FC<ConnectionStatusIndicatorProps> = ({
  status,
  loading,
  withBox,
  size,
}) => {
  return (
    <div
      className={classNames(styles.status, STYLE_BY_STATUS[status], { [BOX_STYLE_BY_STATUS[status]]: withBox })}
      data-loading={loading}
      data-testid="connection-status-indicator"
      data-status={status}
    >
      <div className={styles.icon}>{cloneElement(ICON_BY_STATUS[status], { [size ? "size" : ""]: size })}</div>
    </div>
  );
};
