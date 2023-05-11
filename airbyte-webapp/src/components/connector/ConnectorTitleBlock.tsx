import { ConnectorIcon } from "components/common/ConnectorIcon";
import { ReleaseStageBadge } from "components/ReleaseStageBadge";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { ConnectorDefinition } from "core/domain/connector";
import { DestinationRead, SourceRead } from "core/request/AirbyteClient";

import styles from "./ConnectorTitleBlock.module.scss";

type Connector = SourceRead | DestinationRead;

interface ConnectorTitleBlockProps<T extends Connector> {
  connector: T;
  connectorDefinition: ConnectorDefinition;
}

export const ConnectorTitleBlock = <T extends Connector>({
  connector,
  connectorDefinition,
}: ConnectorTitleBlockProps<T>) => {
  return (
    <FlexContainer alignItems="center">
      <ConnectorIcon icon={connector.icon} className={styles.icon} />
      <FlexContainer direction="column" gap="sm">
        <Heading as="h1" size="md">
          {connector.name}
        </Heading>
        <FlexContainer alignItems="center">
          <Text color="grey">{connectorDefinition.name}</Text>
          <ReleaseStageBadge stage={connectorDefinition.releaseStage} />
        </FlexContainer>
      </FlexContainer>
    </FlexContainer>
  );
};
