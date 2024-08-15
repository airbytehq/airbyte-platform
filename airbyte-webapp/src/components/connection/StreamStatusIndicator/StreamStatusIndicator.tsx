import classNames from "classnames";
import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { CircleLoader } from "components/ui/StatusIcon/CircleLoader";

import styles from "./StreamStatusIndicator.module.scss";
import { ConnectionStatusIndicatorStatus } from "../ConnectionStatusIndicator";

const ICON_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, JSX.Element>> = {
  failed: <Icon type="errorFilled" title="error" />,
  paused: <Icon type="pauseFilled" title="paused" />,
  incomplete: <Icon type="warningFilled" title="warning" />,
  pending: <Icon type="pauseFilled" title="pending" />,
  synced: <Icon type="successFilled" title="synced" />,
  syncing: <CircleLoader title="syncing" className={styles.syncingIcon} />,
  clearing: <CircleLoader title="clearing" className={styles.syncingIcon} />,
  refreshing: <CircleLoader title="refreshing" className={styles.syncingIcon} />,
  queued: <Icon type="statusQueued" title="queued" />,
  queuedForNextSync: <Icon type="statusQueued" title="queued" />,
  rateLimited: <CircleLoader title="rate limited" className={styles.syncingIcon} />,
};

const STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
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

const BOX_STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  failed: styles["status--failed-withBox"],
  paused: styles["status--paused-withBox"],
  incomplete: styles["status--incomplete-withBox"],
  pending: styles["status--pending-withBox"],
  synced: styles["status--upToDate-withBox"],
  syncing: styles["status--syncing-withBox"],
  clearing: styles["status--syncing-withBox"],
  refreshing: styles["status--syncing-withBox"],
  queued: styles["status--queued-withBox"],
  queuedForNextSync: styles["status--queued-withBox"],
  rateLimited: styles["status--syncing-withBox"],
};

interface StreamStatusIndicatorProps {
  status: ConnectionStatusIndicatorStatus;
  // this prop can be removed when the sync progress feature is rolled out
  withBox?: boolean;
}

export const StreamStatusIndicator: React.FC<StreamStatusIndicatorProps> = ({ status, withBox }) => {
  return (
    <div className={classNames(styles.status, STYLE_BY_STATUS[status], { [BOX_STYLE_BY_STATUS[status]]: withBox })}>
      <FlexContainer justifyContent="center" alignItems="center" className={styles.icon}>
        {ICON_BY_STATUS[status]}
      </FlexContainer>
    </div>
  );
};
