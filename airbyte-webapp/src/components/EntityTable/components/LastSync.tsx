import classNames from "classnames";
import React from "react";
import { FormattedRelativeTime } from "react-intl";

import { Text } from "components/ui/Text";

import styles from "./LastSync.module.scss";

interface LastSyncProps {
  timeInSeconds?: number | null;
  enabled?: boolean;
}

export const LastSync: React.FC<LastSyncProps> = ({ timeInSeconds, enabled }) => {
  return (
    <>
      {timeInSeconds ? (
        <Text className={classNames(styles.text, { [styles.enabled]: enabled })} size="sm">
          <FormattedRelativeTime value={timeInSeconds - Date.now() / 1000} updateIntervalInSeconds={60} />
        </Text>
      ) : null}
    </>
  );
};
