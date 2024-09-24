import { FormattedMessage, FormattedTime, useIntl } from "react-intl";
import { ContentType } from "recharts/types/component/Tooltip";

import { StreamStatusIndicator, StreamStatusType } from "components/connection/StreamStatusIndicator";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useFormatLengthOfTime } from "core/utils/time";

import styles from "./UptimeStatusGraphTooltip.module.scss";
import { ChartStream } from "./WaffleChart";

// What statuses we represent to users, other statuses must map to these
type PresentingStatuses = StreamStatusType.Synced | StreamStatusType.Incomplete | StreamStatusType.Failed;

const MESSAGE_BY_STATUS: Readonly<Record<PresentingStatuses, string>> = {
  synced: "connection.overview.graph.uptimeStatus.synced",
  incomplete: "connection.overview.graph.uptimeStatus.incomplete",
  failed: "connection.overview.graph.uptimeStatus.failed",
};

export const UptimeStatusGraphTooltip: ContentType<number, string> = ({ active, payload }) => {
  const { formatMessage } = useIntl();
  const jobRunTime: number = payload?.[0]?.payload?.runtimeMs;
  const formattedJobRunTime = useFormatLengthOfTime(jobRunTime);

  if (!active) {
    return null;
  }

  const date = payload?.[0]?.payload?.date;
  const recordsEmitted = payload?.[0]?.payload?.recordsEmitted;
  const recordsCommitted = payload?.[0]?.payload?.recordsCommitted;
  const streams: ChartStream[] = payload?.[0]?.payload?.streams;

  const statusesByCount = streams?.reduce<Record<PresentingStatuses, ChartStream[]>>(
    (acc, stream) => {
      const { status } = stream;

      if (
        status === StreamStatusType.Pending ||
        status === StreamStatusType.Syncing ||
        status === StreamStatusType.RateLimited ||
        status === StreamStatusType.Clearing ||
        status === StreamStatusType.Refreshing ||
        status === StreamStatusType.QueuedForNextSync ||
        status === StreamStatusType.Queued ||
        status === StreamStatusType.Paused
      ) {
        return acc;
      }
      acc[status].push(stream);
      return acc;
    },
    {
      // Order here determines the display order in the tooltip
      [StreamStatusType.Synced]: [],
      [StreamStatusType.Incomplete]: [],
      [StreamStatusType.Failed]: [],
    }
  );

  const showStreamStatusesSection =
    statusesByCount && Object.values(statusesByCount).some((streams) => streams.length > 0);

  return (
    <Card noPadding>
      <Box p="md">
        <FlexContainer direction="column">
          <Text size="md">
            <FormattedTime value={date} year="numeric" month="short" day="numeric" />
            &nbsp;
            <FormattedMessage id="general.unicodeBullet" />
            &nbsp;
            {formattedJobRunTime}
          </Text>

          <FlexContainer direction="column" gap="sm">
            <Text smallcaps bold color="grey">
              <FormattedMessage id="connection.overview.graph.volume" />
            </Text>
            <Text color="grey" size="sm">
              <FormattedMessage id="connection.overview.graph.recordsEmitted" values={{ value: recordsEmitted }} />
            </Text>
            <Text color="grey" size="sm">
              <FormattedMessage id="connection.overview.graph.recordsLoaded" values={{ value: recordsCommitted }} />
            </Text>
          </FlexContainer>

          {!!streams?.length && showStreamStatusesSection && (
            <FlexContainer direction="column" gap="sm">
              <Text smallcaps bold color="grey">
                <FormattedMessage id="connection.overview.graph.uptimeStatus" />
              </Text>
              {Object.entries(statusesByCount ?? []).map(([_status, streams]) => {
                const status = _status as PresentingStatuses;
                return streams.length === 0 ? null : (
                  <FlexContainer key={status} gap="sm" alignItems="baseline">
                    <StreamStatusIndicator size="sm" status={status} />
                    <FlexContainer direction="column" gap="none">
                      <Text color="grey" size="sm" className={styles.alignText} as="span">
                        {formatMessage({ id: MESSAGE_BY_STATUS[status] }, { count: streams.length })}
                      </Text>
                      {status === StreamStatusType.Incomplete && (
                        <>
                          {streams
                            .filter((_, idx) => idx < 3)
                            .map(({ streamName }) => (
                              <Text key={streamName} color="red" size="sm" className={styles.alignText}>
                                {streamName}
                              </Text>
                            ))}
                          {streams.length > 3 && (
                            <Text color="red" size="sm" className={styles.alignText}>
                              <FormattedMessage
                                id="connection.overview.graph.uptimeStatus.more"
                                values={{ moreCount: streams.length - 3 }}
                              />
                            </Text>
                          )}
                        </>
                      )}
                    </FlexContainer>
                  </FlexContainer>
                );
              })}
            </FlexContainer>
          )}

          <Text color="blue" size="sm">
            <FormattedMessage id="connection.overview.graph.clickThrough" />
          </Text>
        </FlexContainer>
      </Box>
    </Card>
  );
};
