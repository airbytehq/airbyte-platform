import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { StreamSyncModeDiff } from "core/api/types/AirbyteClient";

import styles from "./SyncModesDiffSection.module.scss";

interface SyncModesDiffSectionProps {
  syncModes: StreamSyncModeDiff[];
}

export const SyncModesDiffSection: React.FC<SyncModesDiffSectionProps> = ({ syncModes }) => {
  if (syncModes.length === 0) {
    return null;
  }

  return (
    <FlexContainer direction="column" gap="xs">
      <Text>
        <FormattedMessage
          id="connection.timeline.connection_schema_update.catalog_config_diff.syncModesChanged"
          values={{ count: syncModes.length }}
        />
      </Text>
      <Box pl="md">
        <FlexContainer direction="column" gap="xs">
          {syncModes.map((syncMode) => (
            <Text key={syncMode.streamName}>
              <FormattedMessage
                id="connection.timeline.connection_schema_update.catalog_config_diff.syncModesChanged.description"
                values={{
                  streamName: syncMode.streamName,
                  prevSyncMode: (
                    <span className={styles.syncMode}>
                      <FormattedMessage
                        id="connection.timeline.connection_schema_update.catalog_config_diff.syncModesChanged.mode"
                        values={{
                          sourceSyncMode: <FormattedMessage id={`syncMode.${syncMode.prevSourceSyncMode}`} />,
                          destinationSyncMode: (
                            <FormattedMessage id={`destinationSyncMode.${syncMode.prevDestinationSyncMode}`} />
                          ),
                        }}
                      />
                    </span>
                  ),
                  currentSyncMode: (
                    <span className={styles.syncMode}>
                      <FormattedMessage
                        id="connection.timeline.connection_schema_update.catalog_config_diff.syncModesChanged.mode"
                        values={{
                          sourceSyncMode: <FormattedMessage id={`syncMode.${syncMode.currentSourceSyncMode}`} />,
                          destinationSyncMode: (
                            <FormattedMessage id={`destinationSyncMode.${syncMode.currentDestinationSyncMode}`} />
                          ),
                        }}
                      />
                    </span>
                  ),
                }}
              />
            </Text>
          ))}
        </FlexContainer>
      </Box>
    </FlexContainer>
  );
};
