import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { FieldName, StreamPrimaryKeyDiff } from "core/api/types/AirbyteClient";

import styles from "./PrimaryKeysDiffSection.module.scss";

/**
 *  transform nested PK array fields paths into dot-joined strings
 * @example [["a"], ["b", "c"]] -> ["a", "b.c"]
 */
const transformPKs = (pks: FieldName[] | undefined): string[] =>
  pks?.map((pk) => (Array.isArray(pk) ? pk.join(".") : pk)) ?? [];

interface PrimaryKeysDiffSectionProps {
  primaryKeys: StreamPrimaryKeyDiff[];
}

export const PrimaryKeysDiffSection: React.FC<PrimaryKeysDiffSectionProps> = ({ primaryKeys }) => {
  if (primaryKeys.length === 0) {
    return null;
  }

  const getStyledPKs = (prev: string[], current: string[] | undefined) => {
    const prevPKsSet = new Set(prev);
    const currentPKsSet = new Set(current);

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
              {addedPKs.length === 0 ? "" : ", "}
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
          {primaryKeys
            .map((pk) => ({
              streamName: pk.streamName,
              prev: transformPKs(pk.prev),
              current: transformPKs(pk.current),
            }))
            .map((primaryKey) => (
              <Text key={primaryKey.streamName}>
                {primaryKey.prev?.length === 1 && primaryKey.current?.length === 1 ? (
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
