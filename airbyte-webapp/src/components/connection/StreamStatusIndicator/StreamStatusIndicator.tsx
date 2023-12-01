import classNames from "classnames";
import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import styles from "./StreamStatusIndicator.module.scss";
import { StreamStatusLoadingSpinner } from "./StreamStatusLoadingSpinner";
import { ConnectionStatusIndicatorStatus } from "../ConnectionStatusIndicator";

const ICON_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, JSX.Element>> = {
  [ConnectionStatusIndicatorStatus.ActionRequired]: <Icon type="errorFilled" />,
  [ConnectionStatusIndicatorStatus.Disabled]: <Icon type="pauseFilled" />,
  [ConnectionStatusIndicatorStatus.Error]: <Icon type="warningFilled" />,
  [ConnectionStatusIndicatorStatus.Late]: <Icon type="clockFilled" />,
  [ConnectionStatusIndicatorStatus.Pending]: <Icon type="pauseFilled" />,
  [ConnectionStatusIndicatorStatus.OnTime]: <Icon type="successFilled" />,
  [ConnectionStatusIndicatorStatus.OnTrack]: <Icon type="successFilled" />,
};

const STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  [ConnectionStatusIndicatorStatus.ActionRequired]: styles["status--actionRequired"],
  [ConnectionStatusIndicatorStatus.Disabled]: styles["status--disabled"],
  [ConnectionStatusIndicatorStatus.Error]: styles["status--error"],
  [ConnectionStatusIndicatorStatus.Late]: styles["status--late"],
  [ConnectionStatusIndicatorStatus.Pending]: styles["status--pending"],
  [ConnectionStatusIndicatorStatus.OnTime]: styles["status--upToDate"],
  [ConnectionStatusIndicatorStatus.OnTrack]: styles["status--upToDate"],
};

const BOX_STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  [ConnectionStatusIndicatorStatus.ActionRequired]: styles["status--actionRequired-withBox"],
  [ConnectionStatusIndicatorStatus.Disabled]: styles["status--disabled-withBox"],
  [ConnectionStatusIndicatorStatus.Error]: styles["status--error-withBox"],
  [ConnectionStatusIndicatorStatus.Late]: styles["status--late-withBox"],
  [ConnectionStatusIndicatorStatus.Pending]: styles["status--pending-withBox"],
  [ConnectionStatusIndicatorStatus.OnTime]: styles["status--upToDate-withBox"],
  [ConnectionStatusIndicatorStatus.OnTrack]: styles["status--upToDate-withBox"],
};

interface StreamStatusIndicatorProps {
  status: ConnectionStatusIndicatorStatus;
  loading?: boolean;
  withBox?: boolean;
}

export const StreamStatusIndicator: React.FC<StreamStatusIndicatorProps> = ({ status, loading, withBox }) => (
  <div className={classNames(styles.status, STYLE_BY_STATUS[status], { [BOX_STYLE_BY_STATUS[status]]: withBox })}>
    <FlexContainer justifyContent="center" alignItems="center" className={styles.icon}>
      {ICON_BY_STATUS[status]}
      {loading && <StreamStatusLoadingSpinner className={styles.spinner} />}
    </FlexContainer>
  </div>
);
