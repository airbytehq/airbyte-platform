import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { ConnectionSyncStatus } from "core/api/types/AirbyteClient";

interface StreamsListSubtitleProps {
  connectionStatus: ConnectionSyncStatus;
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
  return (
    <Text color="grey" bold size="sm" as="span" data-testid="streams-list-subtitle">
      {connectionStatus === ConnectionSyncStatus.synced && nextSync && (
        <FormattedMessage id="connection.stream.status.nextSync" values={{ sync: dayjs(nextSync).fromNow() }} />
      )}
      {connectionStatus === ConnectionSyncStatus.running &&
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
