import classNames from "classnames";
import { FormattedDate, FormattedMessage } from "react-intl";
import { ContentType } from "recharts/types/component/Tooltip";

import { FormattedTimeRange } from "components/FormattedTimeRange";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./ConnectionsGraph.module.scss";

type SyncType = "success" | "failure" | "partialSuccess" | "running";

export const ConnectionsGraphTooltip: ContentType<number, string> = ({ active, payload }) => {
  if (!active) {
    return null;
  }

  const startDate = payload?.[0]?.payload?.windowStart;
  const endDate = payload?.[0]?.payload?.windowEnd;
  const failureCount = payload?.[0]?.payload?.failure;
  const successCount = payload?.[0]?.payload?.success;
  const partialSuccessCount = payload?.[0]?.payload?.partialSuccess;
  const runningCount = payload?.[0]?.payload?.running;
  const lookbackUnit = payload?.[0]?.payload?.lookbackConfig.unit;
  const anySyncsHappened = !!successCount || !!failureCount || !!partialSuccessCount || !!runningCount;

  return (
    <Card noPadding>
      <Box p="md">
        <FlexContainer direction="column" gap="sm">
          {lookbackUnit === "day" ? (
            <Text bold>
              <FormattedDate
                value={startDate}
                weekday="short"
                day="numeric"
                month="long"
                year="numeric"
                hour="numeric"
                minute="numeric"
              />
            </Text>
          ) : (
            <FlexContainer direction="column" gap="xs">
              <Text bold>
                <FormattedDate value={startDate} day="numeric" month="long" year="numeric" />
              </Text>
              <Text color="grey">
                <FormattedTimeRange from={startDate} to={endDate} />
              </Text>
            </FlexContainer>
          )}
          {!!runningCount && runningCount > 0 && (
            <FlexContainer gap="sm">
              <TooltipColorIndicator syncType="running" />
              <Text color="grey">
                <FormattedMessage id="connections.graph.runningSyncsCount" values={{ count: runningCount }} />
              </Text>
            </FlexContainer>
          )}
          {!!failureCount && failureCount > 0 && (
            <FlexContainer gap="sm">
              <TooltipColorIndicator syncType="failure" />
              <Text color="grey">
                <FormattedMessage id="connections.graph.failedSyncsCount" values={{ count: failureCount }} />
              </Text>
            </FlexContainer>
          )}
          {!!partialSuccessCount && partialSuccessCount > 0 && (
            <FlexContainer gap="sm">
              <TooltipColorIndicator syncType="partialSuccess" />
              <Text color="grey">
                <FormattedMessage
                  id="connections.graph.partialSuccessfulSyncsCount"
                  values={{ count: partialSuccessCount }}
                />
              </Text>
            </FlexContainer>
          )}
          {!!successCount && successCount > 0 && (
            <FlexContainer gap="sm">
              <TooltipColorIndicator syncType="success" />
              <Text color="grey">
                <FormattedMessage id="connections.graph.successfulSyncsCount" values={{ count: successCount }} />
              </Text>
            </FlexContainer>
          )}
          {!anySyncsHappened && (
            <Text italicized color="grey">
              <FormattedMessage id="connections.graph.noSyncs" />
            </Text>
          )}
        </FlexContainer>
      </Box>
    </Card>
  );
};

const TooltipColorIndicator: React.FC<{ syncType: SyncType }> = ({ syncType }) => {
  return (
    <div
      className={classNames(
        styles.connectionsGraph__statusIndicator,
        styles[`connectionsGraph__statusIndicator--${syncType}`]
      )}
    />
  );
};
