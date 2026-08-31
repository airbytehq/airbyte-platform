import type { TooltipProps } from "recharts";

import dayjs from "dayjs";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

const formatWorkerUsageNumber = (value: number) => value.toFixed(2);

interface WorkspaceDataWorkerGraphTooltipProps extends TooltipProps<number, string> {
  granularity: "hour" | "day" | "week";
}

type ComparisonTimeRange = "1d" | "1w" | "1m" | "1q" | "1y";

interface SinglePeriodTooltipProps {
  comparison?: undefined;
  workspaceName: string;
}

interface ComparisonTooltipProps {
  comparison: {
    barColor: string;
    comparisonBarColor: string;
    selectedTimeRange: ComparisonTimeRange;
  };
  workspaceName?: never;
}

type TooltipPropsWithMode = WorkspaceDataWorkerGraphTooltipProps & (SinglePeriodTooltipProps | ComparisonTooltipProps);

const COMPARISON_HEADING_ID_BY_RANGE: Record<ComparisonTimeRange, string> = {
  "1d": "dataWorkerUsage.comparison.heading.1d",
  "1w": "dataWorkerUsage.comparison.heading.1w",
  "1m": "dataWorkerUsage.comparison.heading.1m",
  "1q": "dataWorkerUsage.comparison.heading.1q",
  "1y": "dataWorkerUsage.comparison.heading.1y",
};

export const WorkspaceDataWorkerGraphTooltip = (props: TooltipPropsWithMode) => {
  const { active, payload, granularity } = props;
  const { formatDate, formatMessage } = useIntl();

  if (!active || !payload?.length) {
    return null;
  }

  const graphData = payload[0]?.payload;

  if (props.comparison) {
    if (!graphData || !("currentUsage" in graphData)) {
      return null;
    }

    const formatComparisonDate = (date: string | undefined) => {
      if (!date) {
        return null;
      }

      return formatDate(
        dayjs(date).toDate(),
        granularity === "hour"
          ? {
              month: "short",
              day: "numeric",
              weekday: "short",
              hour: "numeric",
              minute: "2-digit",
              timeZoneName: "short",
            }
          : granularity === "week"
          ? { month: "short", day: "numeric", year: "numeric" }
          : { month: "short", day: "numeric", weekday: "short" }
      );
    };

    return (
      <Card noPadding>
        <Box p="md">
          <FlexContainer direction="column" gap="md">
            <Text bold>
              <FormattedMessage id={COMPARISON_HEADING_ID_BY_RANGE[props.comparison.selectedTimeRange]} />
            </Text>
            <FlexContainer direction="column" gap="md">
              <FlexContainer alignItems="center" justifyContent="space-between" gap="xl">
                <FlexContainer alignItems="center" gap="sm">
                  <Text as="span" size="xs" style={{ color: props.comparison.barColor }} aria-hidden="true">
                    ■
                  </Text>
                  <FlexContainer direction="column" gap="none">
                    <Text as="span">
                      <FormattedMessage id="dataWorkerUsage.comparison.current" />
                    </Text>
                    <Text as="span" color="grey" size="sm">
                      {formatComparisonDate(graphData.currentDate)}
                    </Text>
                  </FlexContainer>
                </FlexContainer>
                <Text as="span">
                  {graphData.currentUsage === null
                    ? formatMessage({ id: "dataWorkerUsage.comparison.unavailable" })
                    : formatMessage(
                        { id: "settings.organization.usage.graph.yAxisTick" },
                        { value: graphData.currentUsage.toFixed(2) }
                      )}
                </Text>
              </FlexContainer>
              <FlexContainer alignItems="center" justifyContent="space-between" gap="xl">
                <FlexContainer alignItems="center" gap="sm">
                  <Text as="span" size="xs" style={{ color: props.comparison.comparisonBarColor }} aria-hidden="true">
                    ■
                  </Text>
                  <FlexContainer direction="column" gap="none">
                    <Text as="span">
                      <FormattedMessage id="dataWorkerUsage.comparison.previous" />
                    </Text>
                    <Text as="span" color="grey" size="sm">
                      {formatComparisonDate(graphData.previousDate)}
                    </Text>
                  </FlexContainer>
                </FlexContainer>
                <Text as="span">
                  {graphData.previousUsage === null
                    ? formatMessage({ id: "dataWorkerUsage.comparison.unavailable" })
                    : formatMessage(
                        { id: "settings.organization.usage.graph.yAxisTick" },
                        { value: graphData.previousUsage.toFixed(2) }
                      )}
                </Text>
              </FlexContainer>
            </FlexContainer>
          </FlexContainer>
        </Box>
      </Card>
    );
  }

  const dateString = graphData?.date;
  const formattedDate = dateString
    ? formatDate(
        dayjs(dateString).toDate(),
        granularity === "hour"
          ? {
              month: "short",
              day: "numeric",
              weekday: "short",
              hour: "numeric",
              minute: "2-digit",
              timeZoneName: "short",
            }
          : granularity === "week"
          ? { month: "short", day: "numeric", year: "numeric" }
          : { month: "short", day: "numeric", weekday: "short" }
      )
    : undefined;
  const usageValue = formatWorkerUsageNumber(Number(payload[0]?.value ?? 0));

  return (
    <Card noPadding>
      <Box p="md">
        <FlexContainer direction="column" gap="xs">
          <Text bold>{formattedDate}</Text>
          <FlexContainer alignItems="center" justifyContent="space-between" gap="md">
            <Text>{props.workspaceName}</Text>
            <Text color="grey">{usageValue}</Text>
          </FlexContainer>
        </FlexContainer>
      </Box>
    </Card>
  );
};
