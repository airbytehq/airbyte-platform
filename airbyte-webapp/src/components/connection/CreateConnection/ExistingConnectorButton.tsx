import { FormattedMessage } from "react-intl";

import { ConnectorDefinitionBranding } from "components/ui/ConnectorDefinitionBranding";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useConnectionList } from "core/api";
import { DestinationRead, SourceRead } from "core/api/types/AirbyteClient";
import { isSource } from "core/domain/connector/source";

import styles from "./ExistingConnectorButton.module.scss";

interface ExistingConnectorButtonProps<T extends SourceRead | DestinationRead> {
  connector: T;
  onClick: (sourceId: string) => void;
}

export const ExistingConnectorButton = <T extends SourceRead | DestinationRead>({
  connector,
  onClick,
}: ExistingConnectorButtonProps<T>) => {
  const connectionList = useConnectionList();
  const testId = `select-existing-${isSource(connector) ? "source" : "destination"}-${connector.name}`;

  const connectorId = isSource(connector) ? connector.sourceId : connector.destinationId;

  const connectionCount = connectionList?.connectionsByConnectorId?.get(connectorId)?.length ?? 0;

  return (
    <button onClick={() => onClick(connectorId)} className={styles.existingConnectorButton} data-testid={testId}>
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
