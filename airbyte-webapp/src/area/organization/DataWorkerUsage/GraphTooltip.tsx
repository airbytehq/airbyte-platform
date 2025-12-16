import dayjs from "dayjs";
import { FormattedDate, FormattedMessage } from "react-intl";
import { ContentType } from "recharts/types/component/Tooltip";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { WorkspaceLegendItem } from "./GraphLegend";
import styles from "./GraphTooltip.module.scss";

const formatWorkerUsageNumber = (value: number) => {
  return Number(value.toFixed(1));
};

const hasNonZeroUsage = (entry: { value?: number | string }) => {
  const formattedValue = entry.value ? formatWorkerUsageNumber(Number(entry.value)) : 0;
  return formattedValue > 0;
};

const sortByUsageDescendingThenByName = (
  a: { value?: number | string; name?: string },
  b: { value?: number | string; name?: string }
) => {
  // Primary sort: by usage value (descending)
  const valueDiff = (Number(b.value) || 0) - (Number(a.value) || 0);
  if (valueDiff !== 0) {
    return valueDiff;
  }
  // Secondary sort: if values are equal, sort by workspace name
  return (a.name ?? "").localeCompare(b.name ?? "");
};

export const GraphTooltip: ContentType<number, string> = ({ active, payload }) => {
  if (!active) {
    return null;
  }

  const totalWorkspaceUsage = formatWorkerUsageNumber(payload?.reduce((acc, { value }) => acc + Number(value), 0) ?? 0);

  // Filter out zero values and sort by usage for this date
  const workspacesSortedByUsage = [...(payload ?? [])].filter(hasNonZeroUsage).sort(sortByUsageDescendingThenByName);

  // Parse the date string as a local calendar date
  const dateString = payload?.[0]?.payload?.formattedDate;
  const localDate = dateString ? dayjs(dateString).toDate() : undefined;

  return (
    <Card noPadding>
      <Box p="md">
        <FlexContainer direction="column">
          <Text bold>
            {localDate && (
              <FormattedDate value={localDate} year="numeric" month="short" day="numeric" weekday="short" />
            )}
          </Text>
          {workspacesSortedByUsage && (
            <FlexContainer direction="column" gap="xs">
              {workspacesSortedByUsage?.map((entry) => {
                if (!entry.value || !entry.name) {
                  return null;
                }
                const fill = entry.payload && "fill" in entry ? (entry.fill as string) : "#000";
                return (
                  <WorkspaceLegendItem
                    color={fill}
                    key={entry.name}
                    name={entry.name}
                    usage={formatWorkerUsageNumber(entry.value)}
                  />
                );
              })}
            </FlexContainer>
          )}
          <FlexContainer alignItems="center" justifyContent="space-between" className={styles.tooltipTotal} gap="md">
            <Text>
              <FormattedMessage id="settings.organization.usage.graph.tooltip.total" />
            </Text>
            <Text color="grey">{totalWorkspaceUsage}</Text>
          </FlexContainer>
        </FlexContainer>
      </Box>
    </Card>
  );
};
