import classNames from "classnames";
import React from "react";

import { ClockIcon } from "components/icons/ClockIcon";
import { ErrorIcon } from "components/icons/ErrorIcon";
import { SimpleCircleIcon } from "components/icons/SimpleCircleIcon";
import { SuccessIcon } from "components/icons/SuccessIcon";
import { WarningCircleIcon } from "components/icons/WarningCircleIcon";

import styles from "./StreamStatusIndicator.module.scss";
import { StreamStatusLoadingSpinner } from "./StreamStatusLoadingSpinner";
import { StreamStatusType } from "../StreamStatus/streamStatusUtils";

const ICON_BY_STATUS: Readonly<Record<StreamStatusType, JSX.Element>> = {
  [StreamStatusType.ActionRequired]: <ErrorIcon />,
  [StreamStatusType.Disabled]: <SimpleCircleIcon />,
  [StreamStatusType.Error]: <WarningCircleIcon />,
  [StreamStatusType.Late]: <ClockIcon />,
  [StreamStatusType.Pending]: <SimpleCircleIcon />,
  [StreamStatusType.UpToDate]: <SuccessIcon />,
};

const STYLE_BY_STATUS: Readonly<Record<StreamStatusType, string>> = {
  [StreamStatusType.ActionRequired]: styles["status--actionRequired"],
  [StreamStatusType.Disabled]: styles["status--disabled"],
  [StreamStatusType.Error]: styles["status--error"],
  [StreamStatusType.Late]: styles["status--late"],
  [StreamStatusType.Pending]: styles["status--pending"],
  [StreamStatusType.UpToDate]: styles["status--upToDate"],
};

const BOX_STYLE_BY_STATUS: Readonly<Record<StreamStatusType, string>> = {
  [StreamStatusType.ActionRequired]: styles["status--actionRequired-withBox"],
  [StreamStatusType.Disabled]: styles["status--disabled-withBox"],
  [StreamStatusType.Error]: styles["status--error-withBox"],
  [StreamStatusType.Late]: styles["status--late-withBox"],
  [StreamStatusType.Pending]: styles["status--pending-withBox"],
  [StreamStatusType.UpToDate]: styles["status--upToDate-withBox"],
};

interface StreamStatusIndicatorProps {
  status: StreamStatusType;
  loading?: boolean;
  withBox?: boolean;
}

export const StreamStatusIndicator: React.FC<StreamStatusIndicatorProps> = ({ status, loading, withBox }) => (
  <div className={classNames(styles.status, STYLE_BY_STATUS[status], { [BOX_STYLE_BY_STATUS[status]]: withBox })}>
    <div className={styles.icon}>{ICON_BY_STATUS[status]}</div>
    {loading && <StreamStatusLoadingSpinner className={styles.spinner} />}
  </div>
);
