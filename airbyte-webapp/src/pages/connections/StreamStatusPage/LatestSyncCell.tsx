import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";

import { StreamStatusType } from "components/connection/StreamStatusIndicator";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { activeStatuses } from "area/connection/utils";

interface LatestSyncCellProps {
  recordsLoaded?: number;
  recordsExtracted?: number;
  syncStartedAt?: number;
  status: StreamStatusType;
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
    return (
      <span data-testid="streams-list-latest-sync-cell-content" data-loading="true">
        <LoadingSpinner />
      </span>
    );
  }
  return (
    <span data-testid="streams-list-latest-sync-cell-content" data-loading="false">
      {!activeStatuses.includes(status) && (
        <Text color="grey" as="span">
          {recordsLoaded !== undefined ? (
            <Tooltip
              placement="top"
              control={<FormattedMessage id="sources.countLoaded" values={{ count: recordsLoaded }} />}
            >
              <FormattedMessage id="sources.sumOverAttempts" />
            </Tooltip>
          ) : (
            <>-</>
          )}
        </Text>
      )}
      {activeStatuses.includes(status) && (
        <>
          <Text color="grey" as="span">
            {!!recordsLoaded && recordsLoaded > 0 ? (
              <Tooltip
                placement="top"
                control={<FormattedMessage id="sources.countLoaded" values={{ count: recordsLoaded }} />}
              >
                <FormattedMessage id="sources.sumOverAttempts" />
              </Tooltip>
            ) : recordsExtracted ? (
              <Tooltip
                placement="top"
                control={<FormattedMessage id="sources.countExtracted" values={{ count: recordsExtracted }} />}
              >
                <FormattedMessage id="sources.sumOverAttempts" />
              </Tooltip>
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
    </span>
  );
};
