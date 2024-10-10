import classNames from "classnames";
import React, { cloneElement } from "react";

import { Icon, IconProps } from "components/ui/Icon";
import { CircleLoader } from "components/ui/StatusIcon/CircleLoader";

import { ConnectionSyncStatus } from "core/api/types/AirbyteClient";

import styles from "./ConnectionStatusIndicator.module.scss";

const ICON_BY_STATUS: Readonly<Record<ConnectionSyncStatus, JSX.Element>> = {
  synced: <Icon type="statusSuccess" size="md" />,
  incomplete: <Icon type="statusWarning" size="md" />,
  paused: <Icon type="statusInactive" size="md" />,
  pending: <Icon type="statusInactive" size="md" />,
  failed: <Icon type="statusError" size="md" />,
  running: <CircleLoader className={styles.circleLoader} />,
};

const STYLE_BY_STATUS: Readonly<Record<ConnectionSyncStatus, string>> = {
  synced: styles["status--upToDate"],
  incomplete: styles["status--incomplete"],
  paused: styles["status--paused"],
  pending: styles["status--pending"],
  failed: styles["status--failed"],
  running: styles["status--syncing"],
};

const BOX_STYLE_BY_STATUS: Readonly<Record<ConnectionSyncStatus, string>> = {
  synced: styles["status--upToDate-withBox"],
  incomplete: styles["status--incomplete-withBox"],
  paused: styles["status--paused-withBox"],
  pending: styles["status--pending-withBox"],
  failed: styles["status--failed-withBox"],
  running: styles["status--syncing-withBox"],
};

interface ConnectionStatusIndicatorProps {
  status: ConnectionSyncStatus;
  withBox?: boolean;
  size?: IconProps["size"];
}

export const ConnectionStatusIndicator: React.FC<ConnectionStatusIndicatorProps> = ({ status, withBox, size }) => {
  const loading = status === ConnectionSyncStatus.running;
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
