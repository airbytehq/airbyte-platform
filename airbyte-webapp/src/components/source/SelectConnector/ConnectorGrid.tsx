import { useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { useExperiment } from "hooks/services/Experiment";

import { BuilderConnectorButton, ConnectorButton } from "./ConnectorButton";
import styles from "./ConnectorGrid.module.scss";
import { RequestNewConnectorButton } from "./RequestNewConnectorButton";

interface ConnectorGridProps<T extends ConnectorDefinition> {
  connectorDefinitions: T[];
  onConnectorButtonClick: (definition: T) => void;
  onOpenRequestConnectorModal: (searchTerm: string) => void;
}

export const ConnectorGrid = <T extends ConnectorDefinition>({
  connectorDefinitions,
  onConnectorButtonClick,
  onOpenRequestConnectorModal,
}: ConnectorGridProps<T>) => {
  const [searchTerm, setSearchTerm] = useState("");
  const showBuilderNavigationLinks = useExperiment("connectorBuilder.showNavigationLinks", false);

  const filteredDefinitions = useMemo(
    () =>
      connectorDefinitions.filter((definition) =>
        definition.name.toLowerCase().includes(searchTerm.toLocaleLowerCase())
      ),
    [searchTerm, connectorDefinitions]
  );

  return (
    <>
      <Box pb="xl">
        <Input
          placeholder="Search"
          value={searchTerm}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value || "")}
          light
        />
      </Box>
      {filteredDefinitions.length === 0 && (
        <div className={styles.connectorGrid__noMatches}>
          <Text align="center">
            <FormattedMessage id="connector.noSearchResults" />
          </Text>
        </div>
      )}
      <div className={styles.connectorGrid}>
        {filteredDefinitions.map((definition) => {
          const key = isSourceDefinition(definition)
            ? definition.sourceDefinitionId
            : definition.destinationDefinitionId;
          return <ConnectorButton definition={definition} onClick={onConnectorButtonClick} key={key} />;
        })}
        {showBuilderNavigationLinks && <BuilderConnectorButton />}
        <RequestNewConnectorButton onClick={() => onOpenRequestConnectorModal(searchTerm)} />
      </div>
    </>
  );
};
