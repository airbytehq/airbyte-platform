import { FormattedMessage } from "react-intl";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { ReleaseStageBadge } from "components/ReleaseStageBadge";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { ConnectorDefinition } from "core/domain/connector";
import { DestinationRead, SourceRead, ActorDefinitionVersionRead } from "core/request/AirbyteClient";

import styles from "./ConnectorTitleBlock.module.scss";

type Connector = SourceRead | DestinationRead;

interface ConnectorTitleBlockProps<T extends Connector> {
  connector: T;
  connectorDefinition: ConnectorDefinition;
  actorDefinitionVersion: ActorDefinitionVersionRead;
}

export const ConnectorTitleBlock = <T extends Connector>({
  connector,
  connectorDefinition,
  actorDefinitionVersion,
}: ConnectorTitleBlockProps<T>) => {
  const titleInfo =
    connectorDefinition.releaseStage === "custom" ? (
      `${connectorDefinition.name}`
    ) : (
      <FormattedMessage
        id="connector.connectorNameAndVersion"
        values={{ connectorName: connectorDefinition.name, version: actorDefinitionVersion.dockerImageTag }}
      />
    );
  return (
    <FlexContainer direction="column" gap="lg">
      <FlexContainer alignItems="center">
        <ConnectorIcon icon={connector.icon} className={styles.icon} />
        <FlexContainer direction="column" gap="sm">
          <Heading as="h1" size="md">
            {connector.name}
          </Heading>
          <FlexContainer alignItems="center">
            <Text color="grey">{titleInfo}</Text>
            <ReleaseStageBadge stage={connectorDefinition.releaseStage} />
          </FlexContainer>
        </FlexContainer>
      </FlexContainer>
      {actorDefinitionVersion.breakingChanges && <Message type="warning" text="Test" />}
    </FlexContainer>
  );
};
