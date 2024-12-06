import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { StreamStatusType } from "components/connection/StreamStatusIndicator";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { activeStatuses } from "area/connection/utils";
import { formatBytes } from "core/utils/numberHelper";

interface LatestSyncCellProps {
  recordsLoaded?: number;
  recordsExtracted?: number;
  syncStartedAt?: number;
  status: StreamStatusType;
  isLoadingHistoricalData: boolean;
  showBytes: boolean;
  bytesLoaded?: number;
  bytesExtracted?: number;
}

export const LatestSyncCell: React.FC<LatestSyncCellProps> = ({
  recordsLoaded,
  recordsExtracted,
  syncStartedAt,
  status,
  isLoadingHistoricalData,
  showBytes,
  bytesLoaded,
  bytesExtracted,
}) => {
  const start = dayjs(syncStartedAt);
  const end = dayjs(Date.now());
  const hours = Math.abs(end.diff(start, "hour"));
  const minutes = Math.abs(end.diff(start, "minute")) - hours * 60;

  const valueToShow = useMemo(() => {
    if (activeStatuses.includes(status)) {
      // if we're showing bytes, show loaded bytes if they exist, otherwise show extracted bytes if they exist

      if (showBytes) {
        if (bytesLoaded && bytesLoaded > 0) {
          return <FormattedMessage id="sources.bytesLoaded" values={{ count: formatBytes(bytesLoaded) }} />;
        } else if (bytesExtracted && bytesExtracted > 0) {
          return <FormattedMessage id="sources.bytesExtracted" values={{ count: formatBytes(bytesExtracted) }} />;
        }
      } else if (!showBytes) {
        // if we're showing records, show loaded records if they exist, otherwise show extracted records if they exist
        if (recordsLoaded && recordsLoaded > 0) {
          return <FormattedMessage id="sources.countLoaded" values={{ count: recordsLoaded }} />;
        } else if (recordsExtracted && recordsExtracted > 0) {
          return <FormattedMessage id="sources.countExtracted" values={{ count: recordsExtracted }} />;
        }
      }
      // if none of them exist but the stream is active, show "starting"
      return <FormattedMessage id="sources.starting" />;
    }

    // if we're showing historical data, show the proper count or a placeholder if empty
    if (!activeStatuses.includes(status)) {
      if (showBytes && bytesLoaded !== undefined) {
        return <FormattedMessage id="sources.bytesLoaded" values={{ count: formatBytes(bytesLoaded) }} />;
      } else if (!showBytes && recordsLoaded !== undefined) {
        return <FormattedMessage id="sources.countLoaded" values={{ count: recordsLoaded }} />;
      }
    }

    return undefined;
  }, [showBytes, recordsLoaded, recordsExtracted, status, bytesLoaded, bytesExtracted]);

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
          {valueToShow ? (
            <Tooltip placement="top" control={valueToShow}>
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
            {valueToShow ? (
              <Tooltip placement="top" control={valueToShow}>
                <FormattedMessage id="sources.sumOverAttempts" />
              </Tooltip>
            ) : (
              <>-</>
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
