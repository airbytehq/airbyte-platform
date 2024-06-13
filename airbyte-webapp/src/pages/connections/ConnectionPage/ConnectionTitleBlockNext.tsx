import { FormattedMessage } from "react-intl";
import { useParams } from "react-router-dom";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { ConnectionHeaderControls } from "components/connection/ConnectionHeaderControls";
import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { ConnectionStatusIndicator } from "components/connection/ConnectionStatusIndicator";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { SupportLevelBadge } from "components/ui/SupportLevelBadge";
import { Text } from "components/ui/Text";

import { ConnectionStatus, SupportLevel } from "core/api/types/AirbyteClient";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./ConnectionTitleBlockNext.module.scss";

interface ConnectorBlockProps {
  name: string;
  icon?: string;
  id: string;
  supportLevel?: SupportLevel;
  custom?: boolean;
  type: "source" | "destination";
  version: string;
}

const ConnectorBlockNext: React.FC<ConnectorBlockProps> = ({ name, icon, id, supportLevel, custom, type, version }) => {
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

export const ConnectionTitleBlockNext = () => {
  const { connection } = useConnectionEditService();
  const { name, source, destination, status: connectionStatus } = connection;
  const { isRunning, status } = useConnectionStatus(connection.connectionId);
  const { sourceDefinition, sourceDefinitionVersion, destDefinition, destDefinitionVersion } =
    useConnectionFormService();

  return (
    <>
      {connectionStatus === ConnectionStatus.deprecated && (
        <Message type="warning" text={<FormattedMessage id="connection.connectionDeletedView" />} />
      )}
      <FlexContainer alignItems="center" justifyContent="space-between">
        <FlexContainer alignItems="center" className={styles.titleContainer}>
          <ConnectionStatusIndicator status={status} withBox loading={isRunning} />
          <FlexContainer direction="column" gap="xs" className={styles.textEllipsis}>
            <Heading as="h1" size="sm" title={name} className={styles.heading}>
              {name}
            </Heading>
            <FlexContainer alignItems="center" gap="sm">
              <ConnectorBlockNext
                name={source.name}
                icon={source.icon}
                id={source.sourceId}
                supportLevel={sourceDefinitionVersion.supportLevel}
                custom={sourceDefinition.custom}
                version={sourceDefinitionVersion.dockerImageTag}
                type="source"
              />
              <Icon type="arrowRight" color="disabled" />
              <ConnectorBlockNext
                name={destination.name}
                icon={destination.icon}
                id={destination.destinationId}
                supportLevel={destDefinitionVersion.supportLevel}
                custom={destDefinition.custom}
                version={destDefinitionVersion.dockerImageTag}
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
