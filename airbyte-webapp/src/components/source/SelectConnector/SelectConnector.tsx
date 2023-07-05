import classNames from "classnames";
import { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";
import { SearchInput } from "components/ui/SearchInput";

import { useCurrentWorkspace } from "core/api";
import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { useModalService } from "hooks/services/Modal";
import RequestConnectorModal from "views/Connector/RequestConnectorModal";

import { ConnectorGrid } from "./ConnectorGrid";
import styles from "./SelectConnector.module.scss";
import { useTrackSelectConnector } from "./useTrackSelectConnector";

interface SelectConnectorProps {
  connectorType: "source" | "destination";
  connectorDefinitions: ConnectorDefinition[];
  headingKey: string;
  onSelectConnectorDefinition: (id: string) => void;
}

export const SelectConnector: React.FC<SelectConnectorProps> = ({
  connectorType,
  connectorDefinitions,
  headingKey,
  onSelectConnectorDefinition,
}) => {
  const { formatMessage } = useIntl();
  const { email } = useCurrentWorkspace();
  const { openModal, closeModal } = useModalService();
  const trackSelectConnector = useTrackSelectConnector(connectorType);
  const [searchTerm, setSearchTerm] = useState("");

  const handleConnectorButtonClick = (definition: ConnectorDefinition) => {
    if (isSourceDefinition(definition)) {
      trackSelectConnector(definition.sourceDefinitionId, definition.name);
      onSelectConnectorDefinition(definition.sourceDefinitionId);
    } else {
      trackSelectConnector(definition.destinationDefinitionId, definition.name);
      onSelectConnectorDefinition(definition.destinationDefinitionId);
    }
  };

  const onOpenRequestConnectorModal = () =>
    openModal({
      title: formatMessage({ id: "connector.requestConnector" }),
      content: () => (
        <RequestConnectorModal
          connectorType={connectorType}
          workspaceEmail={email}
          searchedConnectorName={searchTerm}
          onClose={closeModal}
        />
      ),
      size: "sm",
    });

  const filteredDefinitions = useMemo(
    () =>
      connectorDefinitions.filter((definition) =>
        definition.name.toLowerCase().includes(searchTerm.toLocaleLowerCase())
      ),
    [searchTerm, connectorDefinitions]
  );

  return (
    <div className={styles.selectConnector}>
      <div className={classNames(styles.selectConnector__gutter, styles["selectConnector__gutter--left"])} />
      <div className={styles.selectConnector__header}>
        <Heading as="h2" size="lg">
          <FormattedMessage id={headingKey} />
        </Heading>
        <Box mt="lg">
          <SearchInput value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} />
        </Box>
      </div>
      <div className={classNames(styles.selectConnector__gutter, styles["selectConnector__gutter--right"])} />

      <div className={styles.selectConnector__grid}>
        <ConnectorGrid
          connectorDefinitions={filteredDefinitions}
          onConnectorButtonClick={handleConnectorButtonClick}
          onOpenRequestConnectorModal={onOpenRequestConnectorModal}
          showConnectorBuilderButton={connectorType === "source"}
        />
      </div>
    </div>
  );
};
