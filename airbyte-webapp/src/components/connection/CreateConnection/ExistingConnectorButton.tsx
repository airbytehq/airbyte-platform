import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { ConnectorDefinitionBranding } from "components/ui/ConnectorDefinitionBranding";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { isSource } from "core/domain/connector/source";
import { DestinationRead, SourceRead } from "core/request/AirbyteClient";
import { useConnectionList } from "hooks/services/useConnectionHook";

import styles from "./ExistingConnectorButton.module.scss";

interface ExistingConnectorButtonProps<T extends SourceRead | DestinationRead> {
  connector: T;
  onClick: (sourceId: string) => void;
}

export const ExistingConnectorButton = <T extends SourceRead | DestinationRead>({
  connector,
  onClick,
}: ExistingConnectorButtonProps<T>) => {
  const { connections } = useConnectionList();

  const connectionCount = useMemo(
    () =>
      connections.filter((connection) =>
        isSource(connector)
          ? connection.source.sourceId === connector.sourceId
          : connection.destination.destinationId === connector.destinationId
      ).length,
    [connector, connections]
  );

  const onClickConnector = () => {
    if (isSource(connector)) {
      onClick(connector.sourceId);
    } else {
      onClick(connector.destinationId);
    }
  };

  return (
    <button onClick={onClickConnector} className={styles.existingConnectorButton}>
      <Text size="lg">{connector.name}</Text>
      {isSource(connector) ? (
        <ConnectorDefinitionBranding sourceDefinitionId={connector.sourceDefinitionId} />
      ) : (
        <ConnectorDefinitionBranding destinationDefinitionId={connector.destinationDefinitionId} />
      )}
      <Text>
        <FormattedMessage id="connectionForm.sourceExisting.connectionCount" values={{ count: connectionCount }} />
      </Text>
      <Icon type="chevronRight" size="lg" />
    </button>
  );
};
