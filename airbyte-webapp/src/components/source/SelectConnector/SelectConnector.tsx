import classNames from "classnames";
import { FormattedMessage, useIntl } from "react-intl";

import { Heading } from "components/ui/Heading";

import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { useModalService } from "hooks/services/Modal";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";
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

  const handleConnectorButtonClick = (definition: ConnectorDefinition) => {
    if (isSourceDefinition(definition)) {
      trackSelectConnector(definition.sourceDefinitionId, definition.name);
      onSelectConnectorDefinition(definition.sourceDefinitionId);
    } else {
      trackSelectConnector(definition.destinationDefinitionId, definition.name);
      onSelectConnectorDefinition(definition.destinationDefinitionId);
    }
  };

  const onOpenRequestConnectorModal = (searchTerm: string) =>
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
      </div>
      <div className={classNames(styles.selectConnector__gutter, styles["selectConnector__gutter--right"])} />

      <div className={styles.selectConnector__grid}>
        <ConnectorGrid
          connectorDefinitions={connectorDefinitions}
          onConnectorButtonClick={handleConnectorButtonClick}
          onOpenRequestConnectorModal={onOpenRequestConnectorModal}
        />
      </div>
    </div>
  );
};
