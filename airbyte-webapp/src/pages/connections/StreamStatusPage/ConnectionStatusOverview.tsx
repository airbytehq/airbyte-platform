import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import {
  ConnectionStatusIndicatorStatus,
  ConnectionStatusIndicator,
} from "components/connection/ConnectionStatusIndicator";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { JobStatus } from "core/api/types/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

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
  const { connection } = useConnectionEditService();
  const { isRunning, status, lastSyncJobStatus, nextSync } = useConnectionStatus(connection.connectionId);

  return (
    <FlexContainer alignItems="center" gap="sm">
      <ConnectionStatusIndicator status={status} withBox loading={isRunning} />
      <Box ml="md">
        <span data-testid="connection-status-text">
          <FormattedMessage id={MESSAGE_BY_STATUS[status]} />
        </span>
        {status === ConnectionStatusIndicatorStatus.OnTrack && (
          <Tooltip
            containerClassName={styles.onTrackInfo}
            control={<Icon type="infoOutline" color="action" size="sm" />}
            placement="top"
          >
            <FormattedMessage
              id={
                lastSyncJobStatus === JobStatus.failed
                  ? "connection.status.onTrack.failureDescription"
                  : "connection.status.onTrack.delayedDescription"
              }
            />
          </Tooltip>
        )}
        <Box as="span" ml="md">
          <Text color="grey" bold size="sm" as="span">
            {status === ConnectionStatusIndicatorStatus.OnTime && nextSync && (
              <FormattedMessage id="connection.stream.status.nextSync" values={{ sync: dayjs(nextSync).fromNow() }} />
            )}
            {(status === ConnectionStatusIndicatorStatus.Late || status === ConnectionStatusIndicatorStatus.OnTrack) &&
              nextSync && (
                <FormattedMessage id="connection.stream.status.nextTry" values={{ sync: dayjs(nextSync).fromNow() }} />
              )}
          </Text>
        </Box>
      </Box>
    </FlexContainer>
  );
};
