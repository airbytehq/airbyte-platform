import { FormattedMessage, FormattedTime, useIntl } from "react-intl";
import { ContentType } from "recharts/types/component/Tooltip";

import {
  ConnectionStatusIndicator,
  ConnectionStatusIndicatorStatus,
} from "components/connection/ConnectionStatusIndicator";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useFormatLengthOfTime } from "core/utils/time";

import styles from "./UptimeStatusGraphTooltip.module.scss";
import { ChartStream } from "./WaffleChart";

// What statuses we represent to users, other statuses must map to these
type PresentingStatuses =
  | ConnectionStatusIndicatorStatus.OnTime
  | ConnectionStatusIndicatorStatus.Late
  | ConnectionStatusIndicatorStatus.Error
  | ConnectionStatusIndicatorStatus.ActionRequired;

const MESSAGE_BY_STATUS: Readonly<Record<PresentingStatuses, string>> = {
  onTime: "connection.overview.graph.uptimeStatus.onTime",
  late: "connection.overview.graph.uptimeStatus.late",
  error: "connection.overview.graph.uptimeStatus.error",
  actionRequired: "connection.overview.graph.uptimeStatus.actionRequired",
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
      let { status } = stream;

      if (status === ConnectionStatusIndicatorStatus.OnTrack) {
        status = ConnectionStatusIndicatorStatus.OnTime;
      } else if (
        status === ConnectionStatusIndicatorStatus.Pending ||
        status === ConnectionStatusIndicatorStatus.Syncing ||
        status === ConnectionStatusIndicatorStatus.Clearing ||
        status === ConnectionStatusIndicatorStatus.Refreshing ||
        status === ConnectionStatusIndicatorStatus.QueuedForNextSync ||
        status === ConnectionStatusIndicatorStatus.Queued ||
        status === ConnectionStatusIndicatorStatus.Disabled
      ) {
        return acc;
      }
      acc[status].push(stream);
      return acc;
    },
    {
      // Order here determines the display order in the tooltip
      [ConnectionStatusIndicatorStatus.OnTime]: [],
      [ConnectionStatusIndicatorStatus.Late]: [],
      [ConnectionStatusIndicatorStatus.Error]: [],
      [ConnectionStatusIndicatorStatus.ActionRequired]: [],
    }
  );

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

          {!!streams?.length && (
            <FlexContainer direction="column" gap="sm">
              <Text smallcaps bold color="grey">
                <FormattedMessage id="connection.overview.graph.uptimeStatus" />
              </Text>
              {Object.entries(statusesByCount ?? []).map(([_status, streams]) => {
                const status = _status as PresentingStatuses;
                return streams.length === 0 ? null : (
                  <FlexContainer key={status} gap="sm">
                    <Box mt="xs">
                      <ConnectionStatusIndicator size="xs" status={status} />
                    </Box>
                    <FlexContainer direction="column" gap="none">
                      <Text color="grey" size="sm" className={styles.alignText} as="span">
                        {formatMessage({ id: MESSAGE_BY_STATUS[status] }, { count: streams.length })}
                      </Text>
                      {status === ConnectionStatusIndicatorStatus.Error && (
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
