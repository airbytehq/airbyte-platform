import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { StreamCursorFieldDiff } from "core/api/types/AirbyteClient";

import styles from "./CursorFieldsDiffSection.module.scss";

interface CursorFieldsDiffSectionProps {
  cursorFields: StreamCursorFieldDiff[];
}

export const CursorFieldsDiffSection: React.FC<CursorFieldsDiffSectionProps> = ({ cursorFields }) => {
  if (cursorFields.length === 0) {
    return null;
  }

  return (
    <FlexContainer direction="column" gap="xs">
      <Text>
        <FormattedMessage
          id="connection.timeline.connection_schema_update.catalog_config_diff.cursorFieldsChanged"
          values={{
            count: cursorFields.length,
          }}
        />
      </Text>
      <Box pl="md">
        <FlexContainer direction="column" gap="xs">
          {cursorFields.map((cursorField) => (
            <Text key={cursorField.streamName}>
              <FormattedMessage
                id="connection.timeline.connection_schema_update.catalog_config_diff.cursorFieldsChanged.description"
                values={{
                  streamName: cursorField.streamName,
                  prevCursorField: cursorField.prev?.length ? (
                    <span className={styles.prevValue}>{cursorField.prev}</span>
                  ) : undefined,
                  currentCursorField: <span className={styles.currentValue}>{cursorField.current}</span>,
                }}
              />
            </Text>
          ))}
        </FlexContainer>
      </Box>
    </FlexContainer>
  );
};
