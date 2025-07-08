import classNames from "classnames";
import React from "react";
import { FormattedMessage, FormattedRelativeTime } from "react-intl";

import { Text } from "components/ui/Text";

import styles from "./LastSync.module.scss";

interface LastSyncCellProps {
  timeInSeconds?: number | null;
  enabled?: boolean;
}

export const LastSyncCell: React.FC<LastSyncCellProps> = ({ timeInSeconds, enabled }) => {
  return (
    <Text className={classNames(styles.text, { [styles.enabled]: enabled })} size="sm">
      {timeInSeconds ? (
        <FormattedRelativeTime value={timeInSeconds - Date.now() / 1000} updateIntervalInSeconds={60} />
      ) : (
        <FormattedMessage id="general.dash" />
      )}
    </Text>
  );
};
