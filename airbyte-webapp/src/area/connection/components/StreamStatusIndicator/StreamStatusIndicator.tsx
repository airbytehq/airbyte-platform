import classNames from "classnames";
import React, { cloneElement } from "react";

import { Icon, IconProps } from "components/ui/Icon";
import { CircleLoader } from "components/ui/StatusIcon/CircleLoader";

import styles from "./StreamStatusIndicator.module.scss";

export enum StreamStatusType {
  Synced = "synced",
  Pending = "pending",
  Paused = "paused",
  Syncing = "syncing",
  Failed = "failed",
  Incomplete = "incomplete",
  Queued = "queued",
  QueuedForNextSync = "queuedForNextSync",
  Clearing = "clearing",
  Refreshing = "refreshing",
  RateLimited = "rateLimited",
}

const ICON_BY_STATUS: Readonly<Record<StreamStatusType, JSX.Element>> = {
  failed: <Icon type="errorFilled" title="error" />,
  paused: <Icon type="pauseFilled" title="paused" />,
  incomplete: <Icon type="warningFilled" title="warning" />,
  pending: <Icon type="pauseFilled" title="pending" />,
  synced: <Icon type="successFilled" title="synced" />,
  syncing: <CircleLoader title="syncing" className={styles.circleLoader} />,
  clearing: <CircleLoader title="clearing" className={styles.circleLoader} />,
  refreshing: <CircleLoader title="refreshing" className={styles.circleLoader} />,
  queued: <Icon type="statusQueued" title="queued" />,
  queuedForNextSync: <Icon type="statusQueued" title="queued" />,
  rateLimited: <CircleLoader title="rate limited" className={styles.circleLoader} />,
};

const STYLE_BY_STATUS: Readonly<Record<StreamStatusType, string>> = {
  failed: styles["status--failed"],
  paused: styles["status--paused"],
  incomplete: styles["status--incomplete"],
  pending: styles["status--pending"],
  synced: styles["status--upToDate"],
  syncing: styles["status--syncing"],
  clearing: styles["status--syncing"],
  refreshing: styles["status--syncing"],
  queued: styles["status--queued"],
  queuedForNextSync: styles["status--queued"],
  rateLimited: styles["status--syncing"],
};
interface StreamStatusIndicatorProps {
  status: StreamStatusType;
  size?: IconProps["size"];
}

export const StreamStatusIndicator: React.FC<StreamStatusIndicatorProps> = ({ status, size }) => {
  return (
    <div className={classNames(styles.status, STYLE_BY_STATUS[status])}>
      <div className={styles.icon}>{cloneElement(ICON_BY_STATUS[status], { [size ? "size" : ""]: size })}</div>
    </div>
  );
};
