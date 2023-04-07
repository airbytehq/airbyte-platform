import classNames from "classnames";
import { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Heading } from "components/ui/Heading";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import { useTrackSelectConnector } from "core/analytics/useTrackSelectConnector";
import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { useModalService } from "hooks/services/Modal";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";
import RequestConnectorModal from "views/Connector/RequestConnectorModal";

import { ConnectorButton } from "./ConnectorButton";
import { RequestNewConnectorButton } from "./RequestNewConnectorButton";
import styles from "./SelectConnector.module.scss";

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
  const [searchTerm, setSearchTerm] = useState("");
  const trackSelectConnector = useTrackSelectConnector(connectorType);

  const filteredDefinitions = useMemo(
    () =>
      connectorDefinitions.filter((definition) =>
        definition.name.toLowerCase().includes(searchTerm.toLocaleLowerCase())
      ),
    [searchTerm, connectorDefinitions]
  );

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
    });

  return (
    <div className={styles.selectConnector}>
      <div className={classNames(styles.selectConnector__gutter, styles["selectConnector__gutter--left"])} />
      <div className={styles.selectConnector__header}>
        <Heading as="h2" size="lg">
          <FormattedMessage id={headingKey} />
        </Heading>
        <div className={styles.selectConnector__input}>
          <Input placeholder="Search" value={searchTerm} onChange={(e) => setSearchTerm(e.target.value || "")} light />
        </div>
      </div>
      <div className={classNames(styles.selectConnector__gutter, styles["selectConnector__gutter--right"])} />
      {filteredDefinitions.length === 0 && (
        <div className={styles.selectConnector__noMatches}>
          <Text align="center">
            <FormattedMessage id="connector.noSearchResults" />
          </Text>
        </div>
      )}
      <div className={styles.selectConnector__grid}>
        {filteredDefinitions.map((definition) => {
          const key = isSourceDefinition(definition)
            ? definition.sourceDefinitionId
            : definition.destinationDefinitionId;
          return <ConnectorButton definition={definition} onClick={handleConnectorButtonClick} key={key} />;
        })}
        <RequestNewConnectorButton onClick={onOpenRequestConnectorModal} />
      </div>
    </div>
  );
};
