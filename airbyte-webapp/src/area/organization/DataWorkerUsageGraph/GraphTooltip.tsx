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

  return (
    <Card noPadding>
      <Box p="md">
        <FlexContainer direction="column">
          <Text bold>
            <FormattedDate
              value={payload?.[0]?.payload?.date}
              year="numeric"
              month="short"
              day="numeric"
              weekday="short"
            />
          </Text>
          <Text>
            <FormattedMessage
              id="settings.organization.usage.graph.tooltip.total"
              values={{ value: totalWorkspaceUsage }}
            />
          </Text>
          {payload && (
            <FlexContainer direction="column" gap="xs">
              {payload?.map((entry) => {
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
