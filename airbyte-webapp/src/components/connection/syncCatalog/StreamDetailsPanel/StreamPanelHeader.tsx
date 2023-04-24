import React, { ReactNode } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { AirbyteStream, AirbyteStreamConfiguration } from "core/request/AirbyteClient";

import styles from "./StreamPanelHeader.module.scss";

interface StreamPanelHeaderProps {
  config?: AirbyteStreamConfiguration;
  disabled?: boolean;
  onClose: () => void;
  onSelectedChange: () => void;
  stream?: AirbyteStream;
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

export const StreamPanelHeader: React.FC<StreamPanelHeaderProps> = ({
  config,
  disabled,
  onClose,
  onSelectedChange,
  stream,
}) => {
  const syncMode = (
    <>
      <FormattedMessage id={`syncMode.${config?.syncMode}`} />
      {` | `}
      <FormattedMessage id={`destinationSyncMode.${config?.destinationSyncMode}`} />
    </>
  );
  return (
    <FlexContainer
      className={styles.container}
      justifyContent="space-between"
      alignItems="center"
      data-testid="stream-details-header"
    >
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
      <FlexContainer className={styles.properties} justifyContent="center" gap="xl">
        <StreamProperty
          messageId="form.namespace"
          value={stream?.namespace ?? <FormattedMessage id="form.noNamespace" />}
          data-testid="stream-details-namespace"
        />
        <StreamProperty messageId="form.streamName" value={stream?.name} data-testid="stream-details-stream-name" />
        <StreamProperty messageId="form.syncMode" value={syncMode} data-testid="stream-details-sync-mode" />
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
  );
};
