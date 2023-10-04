import { useIntl } from "react-intl";
import { ContentType } from "recharts/types/component/Tooltip";

import {
  ConnectionStatusIndicator,
  ConnectionStatusIndicatorStatus,
} from "components/connection/ConnectionStatusIndicator";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./UptimeStatusGraphTooltip.module.scss";
import { ChartStream } from "./WaffleChart";

// What statuses we represent to users, other statuses must map to these
type PresentingStatuses =
  | ConnectionStatusIndicatorStatus.OnTime
  | ConnectionStatusIndicatorStatus.Late
  | ConnectionStatusIndicatorStatus.Error
  | ConnectionStatusIndicatorStatus.ActionRequired;

const MESSAGE_BY_STATUS: Readonly<Record<PresentingStatuses, string>> = {
  onTime: "connection.uptimeStatus.onTime",
  late: "connection.uptimeStatus.late",
  error: "connection.uptimeStatus.error",
  actionRequired: "connection.uptimeStatus.actionRequired",
};

export const UptimeStatusGraphTooltip: ContentType<number, string> = ({ active, payload }) => {
  const { formatMessage } = useIntl();

  if (!active) {
    return null;
  }

  const date: number = payload?.[0]?.payload?.date;
  const streams: ChartStream[] = payload?.[0]?.payload?.streams;

  const statusesByCount = streams?.reduce<Record<PresentingStatuses, number>>(
    (acc, { status }) => {
      if (status === ConnectionStatusIndicatorStatus.OnTrack) {
        status = ConnectionStatusIndicatorStatus.OnTime;
      } else if (status === ConnectionStatusIndicatorStatus.Pending) {
        return acc;
      } else if (status === ConnectionStatusIndicatorStatus.Disabled) {
        return acc;
      }
      acc[status]++;
      return acc;
    },
    {
      // Order here determines the display order in the tooltip
      [ConnectionStatusIndicatorStatus.OnTime]: 0,
      [ConnectionStatusIndicatorStatus.Late]: 0,
      [ConnectionStatusIndicatorStatus.Error]: 0,
      [ConnectionStatusIndicatorStatus.ActionRequired]: 0,
    }
  );

  return (
    <Card>
      <Box p="lg">
        <Box pb="md">
          <Text size="md">{new Date(date).toLocaleDateString()}</Text>
        </Box>

        {Object.entries(statusesByCount ?? []).map(([_status, count]) => {
          const status = _status as PresentingStatuses;
          return count === 0 ? null : (
            <FlexContainer key={status} alignItems="center" gap="sm" className={styles.statusLine}>
              <FlexItem>
                <ConnectionStatusIndicator withBox status={status} />
              </FlexItem>
              <FlexItem>
                <Text size="lg">
                  <strong>{count}</strong>
                </Text>
              </FlexItem>
              <FlexItem>
                <Text size="md">{formatMessage({ id: MESSAGE_BY_STATUS[status] })}</Text>
              </FlexItem>
            </FlexContainer>
          );
        })}
      </Box>
    </Card>
  );
};
