import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

interface SyncProgressItemProps {
  recordsLoaded?: number;
  recordsExtracted?: number;
  syncStartedAt?: number;
}

export const SyncProgressItem: React.FC<SyncProgressItemProps> = ({
  recordsLoaded,
  recordsExtracted,
  syncStartedAt,
}) => {
  if (!syncStartedAt) {
    return null;
  }

  const start = dayjs(syncStartedAt);
  const end = dayjs(Date.now());
  const hour = Math.abs(end.diff(start, "hour"));
  const minute = Math.abs(end.diff(start, "minute")) - hour * 60;

  return (
    <>
      <Text color="grey" as="span">
        {recordsLoaded && recordsLoaded > 0 ? (
          <FormattedMessage id="sources.countLoaded" values={{ count: recordsLoaded ?? 0 }} />
        ) : recordsExtracted ? (
          <FormattedMessage id="sources.countExtracted" values={{ count: recordsExtracted ?? 0 }} />
        ) : (
          <FormattedMessage id="sources.queued" />
        )}
      </Text>
      <Text color="grey" as="span">
        {" "}
        |{" "}
      </Text>
      <Text color="grey" as="span">
        {hour > 0 || minute > 0 || recordsExtracted ? (
          <FormattedMessage
            id="sources.elapsed"
            values={{
              time: (
                <>
                  {hour ? <FormattedMessage id="sources.hour" values={{ hour }} /> : null}
                  <FormattedMessage id="sources.minute" values={{ minute }} />
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
