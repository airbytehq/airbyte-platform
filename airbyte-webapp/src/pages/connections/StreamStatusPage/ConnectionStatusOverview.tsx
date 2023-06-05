import { FormattedMessage } from "react-intl";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import {
  ConnectionStatusIndicatorStatus,
  ConnectionStatusIndicator,
} from "components/connection/ConnectionStatusIndicator";
import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { Status as ConnectionSyncStatus } from "components/EntityTable/types";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./ConnectionStatusOverview.module.scss";

const MESSAGE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  onTime: "connection.status.onTime",
  onTrack: "connection.status.onTrack",
  late: "connection.status.late",
  pending: "connection.status.pending",
  error: "connection.status.error",
  actionRequired: "connection.status.actionRequired",
  disabled: "connection.status.disabled",
};

export const ConnectionStatusOverview: React.FC = () => {
  const { nextSync, jobSyncRunning, jobResetRunning } = useConnectionSyncContext();

  const isLoading = jobSyncRunning || jobResetRunning;

  const status = useConnectionStatus();
  const { connectionStatus } = useConnectionSyncContext();

  return (
    <FlexContainer alignItems="center" gap="sm">
      <ConnectionStatusIndicator status={status} withBox loading={isLoading} />
      <Box ml="md">
        <FormattedMessage id={MESSAGE_BY_STATUS[status]} />
        {status === ConnectionStatusIndicatorStatus.OnTrack && (
          <Tooltip control={<Icon type="info" color="action" className={styles.onTrackInfo} />} placement="top">
            <FormattedMessage
              id={
                connectionStatus === ConnectionSyncStatus.FAILED
                  ? "connection.status.onTrack.failureDescription"
                  : "connection.status.onTrack.delayedDescription"
              }
            />
          </Tooltip>
        )}
        <Box as="span" ml="md">
          <Text color="grey" bold size="sm" as="span">
            {status === ConnectionStatusIndicatorStatus.OnTime && nextSync && (
              <FormattedMessage id="connection.stream.status.nextSync" values={{ sync: nextSync.fromNow() }} />
            )}
            {(status === ConnectionStatusIndicatorStatus.Late || status === ConnectionStatusIndicatorStatus.OnTrack) &&
              nextSync && (
                <FormattedMessage id="connection.stream.status.nextTry" values={{ sync: nextSync.fromNow() }} />
              )}
          </Text>
        </Box>
      </Box>
    </FlexContainer>
  );
};
