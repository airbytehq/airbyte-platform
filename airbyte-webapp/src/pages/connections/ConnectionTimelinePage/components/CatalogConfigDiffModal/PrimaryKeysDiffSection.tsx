import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { StreamConfigDiff } from "core/api/types/AirbyteClient";

import styles from "./PrimaryKeysDiffSection.module.scss";

interface PrimaryKeysDiffSectionProps {
  primaryKeys: StreamConfigDiff[];
}

export const PrimaryKeysDiffSection: React.FC<PrimaryKeysDiffSectionProps> = ({ primaryKeys }) => {
  if (primaryKeys.length === 0) {
    return null;
  }

  const getStyledPKs = (prev: string | undefined, current: string | undefined) => {
    const prevPKs = prev?.split(",") ?? [];
    const currentPKs = current?.split(",") ?? [];

    const prevPKsSet = new Set(prevPKs);
    const currentPKsSet = new Set(currentPKs);

    const removedPKs = [...prevPKsSet].filter((pk) => !currentPKsSet.has(pk));
    const unchangedPKs = [...prevPKsSet].filter((pk) => currentPKsSet.has(pk));
    const addedPKs = [...currentPKsSet].filter((pk) => !prevPKsSet.has(pk));

    return (
      <>
        {removedPKs.map((primaryKey) => {
          return (
            <>
              <span className={styles.prevValue}>{primaryKey}</span>
              {", "}
            </>
          );
        })}
        {unchangedPKs.map((primaryKey) => {
          return (
            <>
              <span className={styles.value}>{primaryKey}</span>
              {", "}
            </>
          );
        })}
        {addedPKs.map((primaryKey, index) => {
          return (
            <>
              <span className={styles.currentValue}>{primaryKey}</span>
              {index < addedPKs.length - 1 && ", "}
            </>
          );
        })}
      </>
    );
  };

  return (
    <FlexContainer direction="column" gap="xs">
      <Text>
        <FormattedMessage
          id="connection.timeline.connection_schema_update.catalog_config_diff.primaryKeysChanged"
          values={{ count: primaryKeys.length }}
        />
      </Text>
      <Box pl="md">
        <FlexContainer direction="column" gap="xs">
          {primaryKeys.map((primaryKey) => (
            <Text key={primaryKey.streamName}>
              {primaryKey.prev?.split(",").length === 1 && primaryKey.current?.split(",").length === 1 ? (
                <FormattedMessage
                  id="connection.timeline.connection_schema_update.catalog_config_diff.primaryKeysChanged.description.single"
                  values={{
                    streamName: primaryKey.streamName,
                    prevPrimaryKey: <span className={styles.prevValue}>{primaryKey.prev}</span>,
                    currentPrimaryKey: <span className={styles.currentValue}>{primaryKey.current}</span>,
                  }}
                />
              ) : (
                <FormattedMessage
                  id="connection.timeline.connection_schema_update.catalog_config_diff.primaryKeysChanged.description.multiple"
                  values={{
                    streamName: primaryKey.streamName,
                    changedPrimaryKeys: getStyledPKs(primaryKey.prev, primaryKey.current),
                  }}
                />
              )}
            </Text>
          ))}
        </FlexContainer>
      </Box>
    </FlexContainer>
  );
};
