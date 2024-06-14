import dayjs from "dayjs";
import { FormattedMessage, FormattedNumber } from "react-intl";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Text } from "components/ui/Text";

import { activeStatuses } from "area/connection/utils";

interface LatestSyncCellProps {
  recordsLoaded?: number;
  recordsExtracted?: number;
  syncStartedAt?: number;
  status: ConnectionStatusIndicatorStatus;
  isLoadingHistoricalData: boolean;
}

export const LatestSyncCell: React.FC<LatestSyncCellProps> = ({
  recordsLoaded,
  recordsExtracted,
  syncStartedAt,
  status,
  isLoadingHistoricalData,
}) => {
  const start = dayjs(syncStartedAt);
  const end = dayjs(Date.now());
  const hour = Math.abs(end.diff(start, "hour"));
  const minute = Math.abs(end.diff(start, "minute")) - hour * 60;

  if (activeStatuses.includes(status) && isLoadingHistoricalData) {
    return <LoadingSpinner />;
  }
  return (
    <>
      {!activeStatuses.includes(status) && (
        <Text color="grey" as="span">
          {recordsLoaded !== undefined ? (
            <FormattedMessage
              id="sources.countLoaded"
              values={{ count: <FormattedNumber value={recordsLoaded ?? 0} /> }}
            />
          ) : (
            <> -</>
          )}
        </Text>
      )}
      {activeStatuses.includes(status) && (
        <>
          <Text color="grey" as="span">
            {!!recordsLoaded && recordsLoaded > 0 ? (
              <FormattedMessage
                id="sources.countLoaded"
                values={{ count: <FormattedNumber value={recordsLoaded} /> }}
              />
            ) : recordsExtracted ? (
              <FormattedMessage
                id="sources.countExtracted"
                values={{ count: <FormattedNumber value={recordsExtracted} /> }}
              />
            ) : (
              <FormattedMessage id="sources.queued" />
            )}
          </Text>
          <Text color="grey" as="span">
            {" | "}
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
      )}
    </>
  );
};
