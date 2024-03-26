import { FormattedMessage } from "react-intl";
import { useParams } from "react-router-dom";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { EnabledControl } from "components/connection/EnabledControl";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { SupportLevelBadge } from "components/ui/SupportLevelBadge";
import { Text } from "components/ui/Text";

import { ConnectionStatus, SupportLevel } from "core/api/types/AirbyteClient";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
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
        <ConnectorIcon icon={icon} />
        <Text color="grey" size="lg">
          {name}
          {connectionDetails && <> (v{version})</>}
        </Text>
        <SupportLevelBadge supportLevel={supportLevel} custom={custom} />
      </FlexContainer>
    </Link>
  );
};

export const ConnectionTitleBlock = () => {
  const { connection } = useConnectionEditService();
  const { mode } = useConnectionFormService();
  const { name, source, destination, schemaChange, status } = connection;
  const { sourceDefinition, sourceDefinitionVersion, destDefinition, destDefinitionVersion } =
    useConnectionFormService();
  const { hasBreakingSchemaChange } = useSchemaChanges(schemaChange);

  return (
    <FlexContainer direction="column">
      <FlexContainer direction="row" justifyContent="space-between">
        <Heading as="h1" size="md">
          {name}
        </Heading>
        <EnabledControl disabled={hasBreakingSchemaChange || mode === "readonly"} />
      </FlexContainer>
      <FlexContainer>
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
          <Icon type="arrowRight" />
          <ConnectorBlock
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
      {status === ConnectionStatus.deprecated && (
        <Message type="warning" text={<FormattedMessage id="connection.connectionDeletedView" />} />
      )}
    </FlexContainer>
  );
};
