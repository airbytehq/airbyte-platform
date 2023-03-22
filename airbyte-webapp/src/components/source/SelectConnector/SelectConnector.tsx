import classNames from "classnames";
import { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { ReleaseStageBadge } from "components/ReleaseStageBadge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { useModalService } from "hooks/services/Modal";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";
import RequestConnectorModal from "views/Connector/RequestConnectorModal";

import styles from "./SelectConnector.module.scss";

interface SelectConnectorProps {
  connectorDefinitions: ConnectorDefinition[];
  headingKey: string;
  onSelectConnectorDefinition: (id: string) => void;
}

export const SelectConnector: React.FC<SelectConnectorProps> = ({
  connectorDefinitions,
  headingKey,
  onSelectConnectorDefinition,
}) => {
  const { formatMessage } = useIntl();
  const { email } = useCurrentWorkspace();
  const { openModal, closeModal } = useModalService();
  const [searchTerm, setSearchTerm] = useState("");

  const filteredDefinitions = useMemo(
    () =>
      connectorDefinitions.filter((definition) =>
        definition.name.toLowerCase().includes(searchTerm.toLocaleLowerCase())
      ),
    [searchTerm, connectorDefinitions]
  );

  const handleConnectorButtonClick = (definition: ConnectorDefinition) => {
    if (isSourceDefinition(definition)) {
      onSelectConnectorDefinition(definition.sourceDefinitionId);
    } else {
      onSelectConnectorDefinition(definition.destinationDefinitionId);
    }
  };

  const onOpenRequestConnectorModal = () =>
    openModal({
      title: formatMessage({ id: "connector.requestConnector" }),
      content: () => (
        <RequestConnectorModal
          connectorType="source"
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
      <div className={styles.selectConnector__grid}>
        {filteredDefinitions.map((definition) => {
          const key = isSourceDefinition(definition)
            ? definition.sourceDefinitionId
            : definition.destinationDefinitionId;
          return (
            <button
              className={styles.selectConnector__button}
              onClick={() => handleConnectorButtonClick(definition)}
              key={key}
            >
              <ConnectorIcon icon={definition.icon} className={styles.selectConnector__icon} />

              <span className={styles.selectConnector__text}>
                <Text size="lg" bold>
                  {definition.name}
                </Text>
              </span>

              <span className={styles.selectConnector__releaseStage}>
                <ReleaseStageBadge stage={definition.releaseStage} />
              </span>
            </button>
          );
        })}
        <div className={styles.selectConnector__noMatches}>
          {filteredDefinitions.length === 0 && (
            <Text centered>
              <FormattedMessage id="connector.noSearchResults" />
            </Text>
          )}
          <FlexContainer justifyContent="center">
            <Button onClick={onOpenRequestConnectorModal} variant="secondary">
              <FormattedMessage id="connector.requestConnector" />
            </Button>
          </FlexContainer>
        </div>
      </div>
    </div>
  );
};
