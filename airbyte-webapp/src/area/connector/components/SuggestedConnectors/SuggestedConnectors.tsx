import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { ConnectorButton } from "components/source/SelectConnector/ConnectorButton";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useDestinationDefinitionList, useSourceDefinitionList } from "core/api";
import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { useLocalStorage } from "core/utils/useLocalStorage";

import styles from "./SuggestedConnectors.module.scss";

interface SuggestedConnectorsProps {
  definitionIds: string[];
  onConnectorButtonClick: (definition: ConnectorDefinition) => void;
}

export const SuggestedConnectorsUnmemoized: React.FC<SuggestedConnectorsProps> = ({
  definitionIds,
  onConnectorButtonClick,
}) => {
  const { formatMessage } = useIntl();
  const { sourceDefinitionMap } = useSourceDefinitionList();
  const { destinationDefinitionMap } = useDestinationDefinitionList();
  const [showSuggestedConnectors, setShowSuggestedConnectors] = useLocalStorage(
    "airbyte_connector-grid-show-suggested-connectors",
    true
  );

  const definitions = definitionIds
    .map((definitionId) => sourceDefinitionMap.get(definitionId) ?? destinationDefinitionMap.get(definitionId))
    .filter((definitionId): definitionId is ConnectorDefinition => Boolean(definitionId))
    // We want to display at most 3 suggested connectors
    .slice(0, 3);

  // If no valid suggested connectors are provided, don't render anything
  if (definitions.length === 0) {
    return null;
  }

  if (!showSuggestedConnectors) {
    return (
      <Box px="xl">
        <FlexContainer justifyContent="flex-end" alignItems="center">
          <Text>
            <Button variant="link" onClick={() => setShowSuggestedConnectors(true)} size="xs">
              <FormattedMessage id="connector.showSuggestedConnectors" />
            </Button>
          </Text>
        </FlexContainer>
      </Box>
    );
  }

  // It's a good enough proxy to check if the first definition is a source or destination
  const titleKey = isSourceDefinition(definitions[0])
    ? "sources.suggestedSources"
    : "destinations.suggestedDestinations";

  return (
    <div className={styles.suggestedConnectors}>
      <Box mb="xl">
        <div>
          <Heading as="h2" size="sm">
            <FormattedMessage id={titleKey} />
          </Heading>
          <Button
            variant="clear"
            onClick={() => setShowSuggestedConnectors(false)}
            className={styles.suggestedConnectors__dismiss}
            aria-label={formatMessage({ id: "connector.hideSuggestedConnectors" })}
          >
            <Icon type="cross" />
          </Button>
        </div>
      </Box>
      <div className={styles.suggestedConnectors__grid}>
        {definitions.map((definition) => (
          <ConnectorButton
            definition={definition}
            onClick={() => onConnectorButtonClick(definition)}
            key={isSourceDefinition(definition) ? definition.sourceDefinitionId : definition.destinationDefinitionId}
          />
        ))}
      </div>
    </div>
  );
};

export const SuggestedConnectors = React.memo(SuggestedConnectorsUnmemoized);
