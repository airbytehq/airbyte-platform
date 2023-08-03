import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";

import { BuilderConnectorButton, ConnectorButton } from "./ConnectorButton";
import styles from "./ConnectorGrid.module.scss";
import { RequestNewConnectorButton } from "./RequestNewConnectorButton";

interface ConnectorGridProps<T extends ConnectorDefinition> {
  connectorDefinitions: T[];
  onConnectorButtonClick: (definition: T) => void;
  onShowAllResultsClick: () => void;
  onOpenRequestConnectorModal: () => void;
  showConnectorBuilderButton?: boolean;
  searchResultsHiddenByFilters: number;
}

export const ConnectorGrid = <T extends ConnectorDefinition>({
  connectorDefinitions,
  onConnectorButtonClick,
  onShowAllResultsClick,
  onOpenRequestConnectorModal,
  showConnectorBuilderButton = false,
  searchResultsHiddenByFilters,
}: ConnectorGridProps<T>) => {
  return (
    <>
      {connectorDefinitions.length === 0 && (
        <div className={styles.connectorGrid__noMatches}>
          <Text size="xl" align="center">
            <FormattedMessage id="connector.noSearchResults" />
          </Text>
          {searchResultsHiddenByFilters > 0 && (
            <Message
              text={
                <FormattedMessage
                  id="connector.searchResultsHiddenByFilters"
                  values={{ count: searchResultsHiddenByFilters }}
                />
              }
              actionBtnText={<FormattedMessage id="connector.showAllResults" />}
              onAction={onShowAllResultsClick}
              className={styles.connectorGrid__hiddenSearchResults}
            />
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
      {connectorDefinitions.length > 0 && searchResultsHiddenByFilters > 0 && (
        <Box pt="2xl">
          <Message
            text={
              <FormattedMessage
                id="connector.searchResultsHiddenByFilters"
                values={{ count: searchResultsHiddenByFilters }}
              />
            }
            actionBtnText={<FormattedMessage id="connector.showAllResults" />}
            onAction={onShowAllResultsClick}
            className={styles.connectorGrid__noMatches__message}
          />
        </Box>
      )}
    </>
  );
};
