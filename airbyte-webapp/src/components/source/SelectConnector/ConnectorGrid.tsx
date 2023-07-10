import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";

import { BuilderConnectorButton, ConnectorButton } from "./ConnectorButton";
import styles from "./ConnectorGrid.module.scss";
import { RequestNewConnectorButton } from "./RequestNewConnectorButton";

interface ConnectorGridProps<T extends ConnectorDefinition> {
  connectorDefinitions: T[];
  onConnectorButtonClick: (definition: T) => void;
  onOpenRequestConnectorModal: () => void;
  showConnectorBuilderButton?: boolean;
  searchResultsHiddenByFilters: boolean;
}

export const ConnectorGrid = <T extends ConnectorDefinition>({
  connectorDefinitions,
  onConnectorButtonClick,
  onOpenRequestConnectorModal,
  showConnectorBuilderButton = false,
  searchResultsHiddenByFilters,
}: ConnectorGridProps<T>) => {
  return (
    <>
      {connectorDefinitions.length === 0 && (
        <div className={styles.connectorGrid__noMatches}>
          <Text size="lg" align="center">
            <FormattedMessage id="connector.noSearchResults" />
          </Text>
          {searchResultsHiddenByFilters && (
            <Text size="sm" align="center">
              <FormattedMessage id="connector.searchResultsHiddenByFilters" />
            </Text>
          )}
        </div>
      )}
      <div className={styles.connectorGrid}>
        {connectorDefinitions.map((definition) => {
          const key = isSourceDefinition(definition)
            ? definition.sourceDefinitionId
            : definition.destinationDefinitionId;
          return <ConnectorButton definition={definition} onClick={onConnectorButtonClick} key={key} />;
        })}
        {showConnectorBuilderButton && <BuilderConnectorButton />}
        <RequestNewConnectorButton onClick={onOpenRequestConnectorModal} />
      </div>
    </>
  );
};
