import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import { Text } from "components/ui/Text";

import { useExperiment } from "hooks/services/Experiment";

interface StreamsListSubtitleProps {
  connectionStatus: ConnectionStatusIndicatorStatus;
  nextSync?: number;
  recordsExtracted?: number;

  recordsLoaded?: number;
}
export const StreamsListSubtitle: React.FC<StreamsListSubtitleProps> = ({
  connectionStatus,
  nextSync,
  recordsExtracted,
  recordsLoaded,
}) => {
  const showSyncProgress = useExperiment("connection.syncProgress", false);
  return (
    <Text color="grey" bold size="sm" as="span">
      {connectionStatus === ConnectionStatusIndicatorStatus.OnTime && nextSync && (
        <FormattedMessage id="connection.stream.status.nextSync" values={{ sync: dayjs(nextSync).fromNow() }} />
      )}
      {(connectionStatus === ConnectionStatusIndicatorStatus.Late ||
        connectionStatus === ConnectionStatusIndicatorStatus.OnTrack) &&
        nextSync && (
          <FormattedMessage id="connection.stream.status.nextTry" values={{ sync: dayjs(nextSync).fromNow() }} />
        )}
      {((showSyncProgress && connectionStatus === ConnectionStatusIndicatorStatus.Syncing) ||
        connectionStatus === ConnectionStatusIndicatorStatus.Queued) &&
        (recordsLoaded ? (
          <FormattedMessage id="sources.countRecordsLoaded" values={{ count: recordsLoaded }} />
        ) : recordsExtracted ? (
          <FormattedMessage id="sources.countRecordsExtracted" values={{ count: recordsExtracted }} />
        ) : (
          <FormattedMessage id="connection.stream.status.table.syncStarting" />
        ))}
    </Text>
  );
};
