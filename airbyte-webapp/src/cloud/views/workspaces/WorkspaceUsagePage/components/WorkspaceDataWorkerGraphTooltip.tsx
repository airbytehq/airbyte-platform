import type { TooltipProps } from "recharts";

import dayjs from "dayjs";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

const formatWorkerUsageNumber = (value: number) => {
  return Number(value.toFixed(1));
};

interface WorkspaceDataWorkerGraphTooltipProps extends TooltipProps<number, string> {
  workspaceName: string;
}

export const WorkspaceDataWorkerGraphTooltip = ({
  active,
  payload,
  workspaceName,
}: WorkspaceDataWorkerGraphTooltipProps) => {
  if (!active || !payload?.length) {
    return null;
  }

  const dateString = payload[0]?.payload?.date;
  const formattedDate = dateString ? dayjs(dateString).format("ddd, MMM D, h:mm A") : undefined;
  const usageValue = formatWorkerUsageNumber(Number(payload[0]?.value ?? 0));

  return (
    <Card noPadding>
      <Box p="md">
        <FlexContainer direction="column" gap="xs">
          <Text bold>{formattedDate}</Text>
          <FlexContainer alignItems="center" justifyContent="space-between" gap="md">
            <Text>{workspaceName}</Text>
            <Text color="grey">{usageValue}</Text>
          </FlexContainer>
        </FlexContainer>
      </Box>
    </Card>
  );
};
