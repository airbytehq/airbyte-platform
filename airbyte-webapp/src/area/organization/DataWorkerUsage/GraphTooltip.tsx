import dayjs from "dayjs";
import { FormattedDate, FormattedMessage } from "react-intl";
import { ContentType } from "recharts/types/component/Tooltip";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { WorkspaceLegendItem } from "./GraphLegend";

const formatWorkerUsageNumber = (value: number) => {
  return Number(value.toFixed(1));
};

export const GraphTooltip: ContentType<number, string> = ({ active, payload }) => {
  if (!active) {
    return null;
  }

  const totalWorkspaceUsage = formatWorkerUsageNumber(payload?.reduce((acc, { value }) => acc + Number(value), 0) ?? 0);

  const workspacesSortedByUsage = [...(payload ?? [])].sort((a, b) => {
    return (b.value ?? 0) - (a.value ?? 0);
  });

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
          <Text>
            <FormattedMessage
              id="settings.organization.usage.graph.tooltip.total"
              values={{ value: totalWorkspaceUsage }}
            />
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
        </FlexContainer>
      </Box>
    </Card>
  );
};
