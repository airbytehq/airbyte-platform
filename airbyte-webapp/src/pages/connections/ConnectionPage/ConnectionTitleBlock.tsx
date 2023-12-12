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
}

const ConnectorBlock: React.FC<ConnectorBlockProps> = ({ name, icon, id, supportLevel, custom, type }) => {
  const params = useParams<{ workspaceId: string; connectionId: string; "*": ConnectionRoutePaths }>();
  const basePath = `/${RoutePaths.Workspaces}/${params.workspaceId}`;
  const connectorTypePath = type === "source" ? RoutePaths.Source : RoutePaths.Destination;

  return (
    <Link to={`${basePath}/${connectorTypePath}/${id}`} className={styles.link}>
      <FlexContainer gap="sm" alignItems="center">
        <ConnectorIcon icon={icon} />
        <Text color="grey" size="lg">
          {name}
        </Text>
        <SupportLevelBadge supportLevel={supportLevel} custom={custom} />
      </FlexContainer>
    </Link>
  );
};

export const ConnectionTitleBlock = () => {
  const { connection } = useConnectionEditService();
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
        <EnabledControl disabled={hasBreakingSchemaChange || status === ConnectionStatus.deprecated} />
      </FlexContainer>
      <FlexContainer>
        <FlexContainer alignItems="center" gap="sm">
          <ConnectorBlock
            name={source.name}
            icon={source.icon}
            id={source.sourceId}
            supportLevel={sourceDefinitionVersion.supportLevel}
            custom={sourceDefinition.custom}
            type="source"
          />
          <Icon type="arrowRight" />
          <ConnectorBlock
            name={destination.name}
            icon={destination.icon}
            id={destination.destinationId}
            supportLevel={destDefinitionVersion.supportLevel}
            custom={destDefinition.custom}
            type="destination"
          />
        </FlexContainer>
      </FlexContainer>
      {status === ConnectionStatus.deprecated && (
        <Message
          className={styles.connectionDeleted}
          type="warning"
          text={<FormattedMessage id="connection.connectionDeletedView" />}
        />
      )}
    </FlexContainer>
  );
};
