import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text, TextProps } from "components/ui/Text";

interface SyncCountProps {
  failedCount: number;
  successCount: number;
  partialSuccessCount: number;
}

export const SyncCount: React.FC<SyncCountProps> = ({
  failedCount,
  successCount,
  partialSuccessCount,
}: SyncCountProps) => {
  const parts: Array<{ id: string; textColor: TextProps["color"]; count: number }> = [
    { id: "connections.graph.failedSyncs", textColor: "red", count: failedCount },
    { id: "connections.graph.successfulSyncs", textColor: "green600", count: successCount },
    { id: "connections.graph.incompleteSyncs", textColor: "grey", count: partialSuccessCount },
  ];

  const nonZeroParts = parts.filter((part) => part.count > 0);

  return (
    <Box mb="sm">
      <FlexContainer gap="sm">
        {nonZeroParts.map(({ id, textColor, count }, index) => (
          <FlexContainer key={id} gap="sm">
            <Text color={textColor}>
              <FormattedMessage id={id} values={{ count }} />
            </Text>
            {index < nonZeroParts.length - 1 && <Text bold>&nbsp;&middot;&nbsp;</Text>}
          </FlexContainer>
        ))}
      </FlexContainer>
    </Box>
  );
};
