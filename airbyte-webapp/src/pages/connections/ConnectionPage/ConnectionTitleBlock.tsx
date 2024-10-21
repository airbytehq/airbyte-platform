import { FormattedMessage } from "react-intl";
import { useParams } from "react-router-dom";

import { ConnectionHeaderControls } from "components/connection/ConnectionHeaderControls";
import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { ConnectionStatusIndicator } from "components/connection/ConnectionStatusIndicator";
import { ConnectorIcon } from "components/ConnectorIcon";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { SupportLevelBadge } from "components/ui/SupportLevelBadge";
import { Text } from "components/ui/Text";

import {
  useCurrentConnection,
  useDestinationDefinition,
  useDestinationDefinitionVersion,
  useSourceDefinition,
  useSourceDefinitionVersion,
} from "core/api";
import { ConnectionStatus, SupportLevel } from "core/api/types/AirbyteClient";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./ConnectionTitleBlock.module.scss";

interface ConnectorBlockProps {
  name: string;
  icon?: string;
  id: string;
  supportLevel?: SupportLevel;
  custom?: boolean;
  type: "source" | "destination";
  version: string;
}

const ConnectorBlock: React.FC<ConnectorBlockProps> = ({ name, icon, id, supportLevel, custom, type, version }) => {
  const params = useParams<{ workspaceId: string; connectionId: string; "*": ConnectionRoutePaths }>();
  const basePath = `/${RoutePaths.Workspaces}/${params.workspaceId}`;
  const connectorTypePath = type === "source" ? RoutePaths.Source : RoutePaths.Destination;
  const [connectionDetails] = useLocalStorage("airbyte_connection-additional-details", false);

  return (
    <Link to={`${basePath}/${connectorTypePath}/${id}`} className={styles.link}>
      <FlexContainer gap="sm" alignItems="center">
        <ConnectorIcon icon={icon} className={styles.connectorIcon} />
        <Text color="grey" size="sm" title={name} className={styles.connectorName}>
          {name}
          {connectionDetails && <> (v{version})</>}
        </Text>
        <SupportLevelBadge supportLevel={supportLevel} custom={custom} />
      </FlexContainer>
    </Link>
  );
};

export const ConnectionTitleBlock = () => {
  const connection = useCurrentConnection();
  const { name, source, destination, status: connectionStatus } = connection;
  const { status } = useConnectionStatus(connection.connectionId);
  const sourceDefinition = useSourceDefinition(connection.source.sourceDefinitionId);
  const sourceDefinitionVersion = useSourceDefinitionVersion(connection.source.sourceId);
  const destinationDefinition = useDestinationDefinition(connection.destination.destinationDefinitionId);
  const destinationDefinitionVersion = useDestinationDefinitionVersion(connection.destination.destinationId);

  return (
    <>
      {connectionStatus === ConnectionStatus.deprecated && (
        <Message type="warning" text={<FormattedMessage id="connection.connectionDeletedView" />} />
      )}
      <FlexContainer alignItems="center" justifyContent="space-between" wrap="wrap">
        <FlexContainer alignItems="center" className={styles.titleContainer}>
          <ConnectionStatusIndicator status={status} withBox />
          <FlexContainer direction="column" gap="xs" className={styles.textEllipsis}>
            <Heading as="h1" size="sm" title={name} className={styles.heading}>
              {name}
            </Heading>
            <FlexContainer alignItems="center" gap="sm">
              <ConnectorBlock
                name={source.name}
                icon={source.icon}
                id={source.sourceId}
                supportLevel={sourceDefinitionVersion.supportLevel}
                custom={sourceDefinition.custom}
                version={sourceDefinitionVersion.dockerImageTag}
                type="source"
              />
              <Icon type="arrowRight" color="disabled" />
              <ConnectorBlock
                name={destination.name}
                icon={destination.icon}
                id={destination.destinationId}
                supportLevel={destinationDefinitionVersion.supportLevel}
                custom={destinationDefinition.custom}
                version={destinationDefinitionVersion.dockerImageTag}
                type="destination"
              />
            </FlexContainer>
          </FlexContainer>
        </FlexContainer>
        <ConnectionHeaderControls />
      </FlexContainer>
    </>
  );
};
