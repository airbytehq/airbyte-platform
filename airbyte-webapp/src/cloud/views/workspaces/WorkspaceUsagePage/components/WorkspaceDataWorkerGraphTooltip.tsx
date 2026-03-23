import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";
import { ContentType } from "recharts/types/component/Tooltip";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

const formatWorkerUsageNumber = (value: number) => {
  return Number(value.toFixed(1));
};

export const WorkspaceDataWorkerGraphTooltip: ContentType<number, string> = ({ active, payload }) => {
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
            <Text>
              <FormattedMessage id="settings.workspace.usage.dataWorker.tooltipLabel" />
            </Text>
            <Text color="grey">{usageValue}</Text>
          </FlexContainer>
        </FlexContainer>
      </Box>
    </Card>
  );
};
