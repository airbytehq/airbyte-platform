import type { TooltipProps } from "recharts";

import dayjs from "dayjs";
import { useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

const formatWorkerUsageNumber = (value: number) => {
  return Number(value.toFixed(1));
};

interface WorkspaceDataWorkerGraphTooltipProps extends TooltipProps<number, string> {
  workspaceName: string;
  granularity: "hour" | "day";
}

export const WorkspaceDataWorkerGraphTooltip = ({
  active,
  payload,
  workspaceName,
  granularity,
}: WorkspaceDataWorkerGraphTooltipProps) => {
  const { formatDate } = useIntl();

  if (!active || !payload?.length) {
    return null;
  }

  const dateString = payload[0]?.payload?.date;
  const formattedDate = dateString
    ? formatDate(
        dayjs(dateString).toDate(),
        granularity === "hour"
          ? { month: "short", day: "numeric", weekday: "short", hour: "numeric", minute: "2-digit" }
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
            <Text>{workspaceName}</Text>
            <Text color="grey">{usageValue}</Text>
          </FlexContainer>
        </FlexContainer>
      </Box>
    </Card>
  );
};
