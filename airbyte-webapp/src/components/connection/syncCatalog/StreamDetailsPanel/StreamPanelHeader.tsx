import React, { ReactNode, useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { AirbyteStream, AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./StreamPanelHeader.module.scss";
import { SyncModeSelect, SyncModeValue } from "../SyncModeSelect";

interface StreamPanelHeaderProps {
  config?: AirbyteStreamConfiguration;
  disabled?: boolean;
  onClose: () => void;
  onSelectedChange: () => void;
  stream?: AirbyteStream;
  onSelectSyncMode: (option: SyncModeValue) => void;
  availableSyncModes: SyncModeValue[];
}

interface StreamPropertyProps {
  messageId: string;
  value?: string | ReactNode;
  "data-testid"?: string;
}

export const StreamProperty: React.FC<StreamPropertyProps> = ({ messageId, value, "data-testid": testId }) => (
  <div data-testid={testId}>
    <Text as="span" size="sm" className={styles.streamPropLabel} data-testid={testId ? `${testId}-label` : undefined}>
      <FormattedMessage id={messageId} />
    </Text>
    <Text as="span" size="md" className={styles.streamPropValue} data-testid={testId ? `${testId}-value` : undefined}>
      {value}
    </Text>
  </div>
);

const NamespaceProperty: React.FC<{ namespace?: string }> = ({ namespace }) => {
  const isSimplifiedCatalogRowEnabled = useExperiment("connection.syncCatalog.simplifiedCatalogRow", true);

  if (isSimplifiedCatalogRowEnabled) {
    return namespace ? (
      <StreamProperty messageId="form.sourceNamespace" value={namespace} data-testid="stream-details-namespace" />
    ) : null;
  }

  return (
    <StreamProperty
      messageId="form.namespace"
      value={namespace ?? <FormattedMessage id="form.noNamespace" />}
      data-testid="stream-details-namespace"
    />
  );
};

export const StreamPanelHeader: React.FC<StreamPanelHeaderProps> = ({
  config,
  disabled,
  onClose,
  onSelectedChange,
  stream,
  availableSyncModes,
  onSelectSyncMode,
}) => {
  const isSimplifiedCatalogRowEnabled = useExperiment("connection.syncCatalog.simplifiedCatalogRow", true);

  const syncSchema: SyncModeValue | undefined = useMemo(() => {
    if (!config) {
      return undefined;
    }
    const { syncMode, destinationSyncMode } = config;
    return { syncMode, destinationSyncMode };
  }, [config]);

  const syncMode = (
    <>
      {config?.syncMode && <FormattedMessage id={`syncMode.${config.syncMode}`} />}
      {` | `}
      {config?.destinationSyncMode && <FormattedMessage id={`destinationSyncMode.${config.destinationSyncMode}`} />}
    </>
  );

  return (
    <Box pt="lg" pb="md" pr="sm" pl="md" className={styles.container}>
      <FlexContainer justifyContent="space-between" alignItems="center" data-testid="stream-details-header">
        <FlexContainer gap="md" alignItems="center" className={styles.leftActions}>
          <div>
            <Switch
              size="sm"
              checked={config?.selected}
              onChange={onSelectedChange}
              disabled={disabled}
              data-testid="stream-details-sync-stream-switch"
            />
          </div>
          <Text color="grey300" size="xs">
            <FormattedMessage id="form.stream.sync" />
          </Text>
        </FlexContainer>
        <FlexContainer className={styles.properties} alignItems="center" justifyContent="center" gap="xl">
          <NamespaceProperty namespace={stream?.namespace} />
          <StreamProperty messageId="form.streamName" value={stream?.name} data-testid="stream-details-stream-name" />
          <FlexItem className={styles.syncModeProperty} alignSelf="center">
            {isSimplifiedCatalogRowEnabled ? (
              <SyncModeSelect
                options={availableSyncModes}
                onChange={onSelectSyncMode}
                value={syncSchema}
                variant="grey"
                disabled={disabled}
              />
            ) : (
              <StreamProperty messageId="form.syncMode" value={syncMode} data-testid="stream-details-sync-mode" />
            )}
          </FlexItem>
        </FlexContainer>
        <FlexContainer className={styles.rightActions} justifyContent="flex-end">
          <Button
            variant="clear"
            onClick={onClose}
            className={styles.crossIcon}
            icon={<Icon type="cross" />}
            data-testid="stream-details-close-button"
          />
        </FlexContainer>
      </FlexContainer>
    </Box>
  );
};
