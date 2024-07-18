import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";

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
  const hours = Math.abs(end.diff(start, "hour"));
  const minutes = Math.abs(end.diff(start, "minute")) - hours * 60;

  if (!activeStatuses.includes(status) && isLoadingHistoricalData) {
    return <LoadingSpinner />;
  }
  return (
    <>
      {!activeStatuses.includes(status) && (
        <Text color="grey" as="span">
          {recordsLoaded !== undefined ? (
            <FormattedMessage id="sources.countLoaded" values={{ count: recordsLoaded }} />
          ) : (
            <>-</>
          )}
        </Text>
      )}
      {activeStatuses.includes(status) && (
        <>
          <Text color="grey" as="span">
            {!!recordsLoaded && recordsLoaded > 0 ? (
              <FormattedMessage id="sources.countLoaded" values={{ count: recordsLoaded }} />
            ) : recordsExtracted ? (
              <FormattedMessage id="sources.countExtracted" values={{ count: recordsExtracted }} />
            ) : (
              <FormattedMessage id="sources.starting" />
            )}
          </Text>
          {syncStartedAt && (
            <>
              <Text color="grey" as="span">
                {" | "}
              </Text>
              <Text color="grey" as="span">
                {hours || minutes ? (
                  <FormattedMessage
                    id="sources.elapsed"
                    values={{
                      hours,
                      minutes,
                    }}
                  />
                ) : (
                  <FormattedMessage id="sources.fewSecondsElapsed" />
                )}
              </Text>
            </>
          )}
        </>
      )}
    </>
  );
};
