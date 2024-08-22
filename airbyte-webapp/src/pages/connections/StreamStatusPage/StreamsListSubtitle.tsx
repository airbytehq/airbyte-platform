import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";

import { ConnectionStatusType } from "components/connection/ConnectionStatusIndicator";
import { Text } from "components/ui/Text";

interface StreamsListSubtitleProps {
  connectionStatus: ConnectionStatusType;
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
      {connectionStatus === ConnectionStatusType.Synced && nextSync && (
        <FormattedMessage id="connection.stream.status.nextSync" values={{ sync: dayjs(nextSync).fromNow() }} />
      )}
      {connectionStatus === ConnectionStatusType.Syncing &&
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
