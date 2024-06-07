import dayjs from "dayjs";
import { FormattedMessage, FormattedNumber } from "react-intl";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import { Text } from "components/ui/Text";

interface SyncProgressItemProps {
  recordsLoaded?: number;
  recordsExtracted?: number;
  syncStartedAt?: number;
  status: ConnectionStatusIndicatorStatus;
}

export const SyncProgressItem: React.FC<SyncProgressItemProps> = ({
  recordsLoaded,
  recordsExtracted,
  syncStartedAt,
  status,
}) => {
  const start = dayjs(syncStartedAt);
  const end = dayjs(Date.now());
  const hour = Math.abs(end.diff(start, "hour"));
  const minute = Math.abs(end.diff(start, "minute")) - hour * 60;

  if (status !== ConnectionStatusIndicatorStatus.Syncing && status !== ConnectionStatusIndicatorStatus.Queued) {
    return null;
  }

  return (
    <>
      <Text color="grey" as="span">
        {recordsLoaded && recordsLoaded > 0 ? (
          <FormattedMessage
            id="sources.countLoaded"
            values={{ count: <FormattedNumber value={recordsLoaded ?? 0} /> }}
          />
        ) : recordsExtracted ? (
          <FormattedMessage
            id="sources.countExtracted"
            values={{ count: <FormattedNumber value={recordsExtracted ?? 0} /> }}
          />
        ) : (
          <FormattedMessage id="sources.queued" />
        )}
      </Text>
      <Text color="grey" as="span">
        {" "}
        |{" "}
      </Text>
      <Text color="grey" as="span">
        {(hour > 0 || minute > 0) && recordsExtracted ? (
          <FormattedMessage
            id="sources.elapsed"
            values={{
              time: (
                <>
                  {hour ? <FormattedMessage id="sources.hour" values={{ hour }} /> : null}
                  {minute ? <FormattedMessage id="sources.minute" values={{ minute }} /> : null}
                </>
              ),
            }}
          />
        ) : (
          <FormattedMessage id="sources.starting" />
        )}
      </Text>
    </>
  );
};
