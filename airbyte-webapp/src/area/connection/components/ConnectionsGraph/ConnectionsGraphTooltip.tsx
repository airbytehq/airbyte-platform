import classNames from "classnames";
import { FormattedDate, FormattedDateTimeRange, FormattedMessage } from "react-intl";
import { ContentType } from "recharts/types/component/Tooltip";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./ConnectionsGraph.module.scss";

type SyncType = "success" | "failure" | "partialSuccess";

export const ConnectionsGraphTooltip: ContentType<number, string> = ({ active, payload }) => {
  if (!active) {
    return null;
  }

  const startDate = payload?.[0]?.payload?.windowStart;
  const endDate = payload?.[0]?.payload?.windowEnd;
  const failureCount = payload?.[0]?.payload?.failure;
  const successCount = payload?.[0]?.payload?.success;
  const partialSuccessCount = payload?.[0]?.payload?.partialSuccess;
  const lookbackUnit = payload?.[0]?.payload?.lookbackConfig.unit;
  const anySyncsHappened = !!successCount || !!failureCount || !!partialSuccessCount;

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
                <FormattedDateTimeRange from={startDate} to={endDate} hour="numeric" minute="numeric" />
              </Text>
            </FlexContainer>
          )}
          {!!successCount && successCount > 0 && (
            <FlexContainer gap="sm">
              <TooltipColorIndicator syncType="success" />
              <Text color="grey">
                <FormattedMessage id="connections.graph.successfulJobsCount" values={{ count: successCount }} />
              </Text>
            </FlexContainer>
          )}
          {!!partialSuccessCount && partialSuccessCount > 0 && (
            <FlexContainer gap="sm">
              <TooltipColorIndicator syncType="partialSuccess" />
              <Text color="grey">
                <FormattedMessage
                  id="connections.graph.partialSuccessfulJobsCount"
                  values={{ count: partialSuccessCount }}
                />
              </Text>
            </FlexContainer>
          )}
          {!!failureCount && failureCount > 0 && (
            <FlexContainer gap="sm">
              <TooltipColorIndicator syncType="failure" />
              <Text color="grey">
                <FormattedMessage id="connections.graph.failedJobsCount" values={{ count: failureCount }} />
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
