import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { FieldDataTypeDiff } from "core/api/types/AirbyteClient";

import styles from "./FieldsDataTypeDiffSection.module.scss";

interface FieldsDataTypeDiffSectionProps {
  fieldsDataType: FieldDataTypeDiff[];
}

export const FieldsDataTypeDiffSection: React.FC<FieldsDataTypeDiffSectionProps> = ({ fieldsDataType }) => {
  if (fieldsDataType.length === 0) {
    return null;
  }

  return (
    <FlexContainer direction="column" gap="xs">
      <Text>
        <FormattedMessage
          id="connection.timeline.connection_schema_update.catalog_config_diff.fieldsDataTypeChanged"
          values={{ count: fieldsDataType.length }}
        />
      </Text>
      <Box pl="md">
        <FlexContainer direction="column" gap="xs">
          {fieldsDataType.map((field) => (
            <Text key={`${field.streamName}-${field.fieldName}`}>
              <FormattedMessage
                id="connection.timeline.connection_schema_update.catalog_config_diff.fieldsDataTypeChanged.description"
                values={{
                  fieldName: `${field.streamName}.${field.fieldName}`,
                  prevDataType: <span className={styles.prevValue}>{field.prev}</span>,
                  currentDataType: <span className={styles.currentValue}>{field.current}</span>,
                }}
              />
            </Text>
          ))}
        </FlexContainer>
      </Box>
    </FlexContainer>
  );
};
