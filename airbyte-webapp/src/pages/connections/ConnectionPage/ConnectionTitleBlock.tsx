import { useParams } from "react-router-dom";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { EnabledControl } from "components/connection/ConnectionInfoCard/EnabledControl";
import { ReleaseStageBadge } from "components/ReleaseStageBadge";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { ReleaseStage } from "core/request/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { RoutePaths } from "pages/routePaths";

import styles from "./ConnectionTitleBlock.module.scss";
import { ConnectionRoutePaths } from "../types";

interface ConnectorBlockProps {
  name: string;
  icon?: string;
  id: string;
  stage?: ReleaseStage;
  type: "source" | "destination";
}

const ConnectorBlock: React.FC<ConnectorBlockProps> = ({ name, icon, id, stage, type }) => {
  const params = useParams<{ workspaceId: string; connectionId: string; "*": ConnectionRoutePaths }>();
  const basePath = `/${RoutePaths.Workspaces}/${params.workspaceId}`;
  const connectorTypePath = type === "source" ? RoutePaths.Source : RoutePaths.Destination;

  return (
    <Link to={`${basePath}/${connectorTypePath}/${id}`} className={styles.link}>
      <FlexContainer gap="sm" alignItems="center">
        <ConnectorIcon icon={icon} />
        <Text size="lg">{name}</Text>
        {stage && <ReleaseStageBadge stage={stage} />}
      </FlexContainer>
    </Link>
  );
};

export const ConnectionTitleBlock = () => {
  const {
    connection: { name, source, destination, schemaChange },
  } = useConnectionEditService();
  const { sourceDefinition, destDefinition } = useConnectionFormService();
  const { hasBreakingSchemaChange } = useSchemaChanges(schemaChange);

  return (
    <FlexContainer direction="column">
      <FlexContainer direction="row" justifyContent="space-between">
        <Heading as="h1" size="md">
          {name}
        </Heading>
        <EnabledControl disabled={hasBreakingSchemaChange} />
      </FlexContainer>
      <FlexContainer>
        <FlexContainer alignItems="center" gap="md">
          <ConnectorBlock
            name={source.name}
            icon={source.icon}
            id={source.sourceId}
            stage={sourceDefinition.releaseStage}
            type="source"
          />
          <Icon type="arrowRight" />
          <ConnectorBlock
            name={destination.name}
            icon={destination.icon}
            id={destination.destinationId}
            stage={destDefinition.releaseStage}
            type="destination"
          />
        </FlexContainer>
      </FlexContainer>
    </FlexContainer>
  );
};
