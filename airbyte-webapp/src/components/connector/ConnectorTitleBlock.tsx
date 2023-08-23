import { FormattedMessage } from "react-intl";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { ReleaseStageBadge } from "components/ReleaseStageBadge";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { ConnectorDefinition } from "core/domain/connector";
import { DestinationRead, SourceRead, ActorDefinitionVersionRead } from "core/request/AirbyteClient";

import { BreakingChangeBanner } from "./BreakingChangeBanner";
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
    <FlexContainer direction="column" gap="xl">
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
      {actorDefinitionVersion.breakingChanges &&
        actorDefinitionVersion.breakingChanges.upcomingBreakingChanges.length > 0 &&
        actorDefinitionVersion.isActorDefaultVersion && (
          <BreakingChangeBanner
            breakingChanges={actorDefinitionVersion.breakingChanges}
            supportState={actorDefinitionVersion.supportState}
            connectorId={"sourceId" in connector ? connector.sourceId : connector.destinationId}
            connectorName={connector.name}
            connectorType={"sourceDefinitionId" in connectorDefinition ? "source" : "destination"}
            connectorDefinitionId={
              "sourceDefinitionId" in connectorDefinition
                ? connectorDefinition.sourceDefinitionId
                : connectorDefinition.destinationDefinitionId
            }
          />
        )}
    </FlexContainer>
  );
};
