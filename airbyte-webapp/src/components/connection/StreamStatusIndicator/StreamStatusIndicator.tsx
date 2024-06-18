import classNames from "classnames";
import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { CircleLoader } from "components/ui/StatusIcon/CircleLoader";

import { useExperiment } from "hooks/services/Experiment";

import styles from "./StreamStatusIndicator.module.scss";
import { StreamStatusLoadingSpinner } from "./StreamStatusLoadingSpinner";
import { ConnectionStatusIndicatorStatus } from "../ConnectionStatusIndicator";

const ICON_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, JSX.Element>> = {
  actionRequired: <Icon type="errorFilled" title="error" />,
  disabled: <Icon type="pauseFilled" title="paused" />,
  paused: <Icon type="pauseFilled" title="paused" />,
  error: <Icon type="warningFilled" title="warning" />,
  late: <Icon type="clockFilled" title="late" />,
  pending: <Icon type="pauseFilled" title="pending" />,
  onTime: <Icon type="successFilled" title="on-time" />,
  onTrack: <Icon type="successFilled" title="on-track" />,
  syncing: <CircleLoader title="syncing" className={styles.syncingIcon} />,
  clearing: <CircleLoader title="clearing" className={styles.syncingIcon} />,
  refreshing: <CircleLoader title="refreshing" className={styles.syncingIcon} />,
  queued: <Icon type="statusQueued" title="queued" />,
  queuedForNextSync: <Icon type="statusQueued" title="queued" />,
};

const STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  actionRequired: styles["status--actionRequired"],
  disabled: styles["status--disabled"],
  paused: styles["status--disabled"],
  error: styles["status--error"],
  late: styles["status--late"],
  pending: styles["status--pending"],
  onTime: styles["status--upToDate"],
  onTrack: styles["status--upToDate"],
  syncing: styles["status--syncing"],
  clearing: styles["status--syncing"],
  refreshing: styles["status--syncing"],
  queued: styles["status--queued"],
  queuedForNextSync: styles["status--queued"],
};

const BOX_STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  actionRequired: styles["status--actionRequired-withBox"],
  disabled: styles["status--disabled-withBox"],
  paused: styles["status--disabled-withBox"],
  error: styles["status--error-withBox"],
  late: styles["status--late-withBox"],
  pending: styles["status--pending-withBox"],
  onTime: styles["status--upToDate-withBox"],
  onTrack: styles["status--upToDate-withBox"],
  syncing: styles["status--syncing-withBox"],
  clearing: styles["status--syncing-withBox"],
  refreshing: styles["status--syncing-withBox"],
  queued: styles["status--queued-withBox"],
  queuedForNextSync: styles["status--queued-withBox"],
};

interface StreamStatusIndicatorProps {
  status: ConnectionStatusIndicatorStatus;
  // this prop can be removed when the sync progress feature is rolled out
  loading?: boolean;
  withBox?: boolean;
}

export const StreamStatusIndicator: React.FC<StreamStatusIndicatorProps> = ({ status, loading, withBox }) => {
  const showSyncProgress = useExperiment("connection.syncProgress", false);

  return (
    <div className={classNames(styles.status, STYLE_BY_STATUS[status], { [BOX_STYLE_BY_STATUS[status]]: withBox })}>
      <FlexContainer justifyContent="center" alignItems="center" className={styles.icon}>
        {ICON_BY_STATUS[status]}
        {!showSyncProgress && loading && <StreamStatusLoadingSpinner className={styles.spinner} />}
      </FlexContainer>
    </div>
  );
};
