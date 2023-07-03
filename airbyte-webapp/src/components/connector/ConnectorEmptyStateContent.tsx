import classNames from "classnames";
import { FormattedMessage } from "react-intl";
import { createSearchParams, useNavigate } from "react-router-dom";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { useConnectorSpecificationMap } from "components/connection/ConnectionOnboarding/ConnectionOnboarding";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace } from "core/api";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./ConnectorEmptyStateContent.module.scss";

interface ConnectorEmptyStateContentProps {
  connectorType: "source" | "destination";
  connectorId: string;
  icon?: string;
  connectorName: string;
}

const ConnectorCard = ({ icon }: { icon?: string }) => {
  return (
    <Card className={styles.card}>
      <ConnectorIcon icon={icon} className={styles.icon} />
    </Card>
  );
};

const EmptyCard = () => {
  return <Card inset className={classNames(styles.card, styles.empty)} />;
};
export const ConnectorEmptyStateContent: React.FC<ConnectorEmptyStateContentProps> = ({
  icon,
  connectorId,
  connectorType,
  connectorName,
}) => {
  const { sourceDefinitions, destinationDefinitions } = useConnectorSpecificationMap();
  const navigate = useNavigate();
  const { workspaceId } = useCurrentWorkspace();

  const onButtonClick = () => {
    const basePath = `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`;

    const searchParams =
      connectorType === "source"
        ? createSearchParams({ sourceId: connectorId })
        : createSearchParams({ destinationId: connectorId });

    navigate({ pathname: basePath, search: `?${searchParams}` });
  };

  const roundConnectorCount = (): number => {
    if (connectorType === "source") {
      return Math.floor(Object.keys(destinationDefinitions).length / 10) * 10;
    } else if (connectorType === "destination") {
      return Math.floor(Object.keys(sourceDefinitions).length / 10) * 10;
    }
    return 0;
  };

  const intlValues =
    connectorType === "source" ? { has: "source", needs: "destination" } : { has: "destination", needs: "source" };

  return (
    <FlexContainer alignItems="center" justifyContent="center" direction="column" gap="xl" className={styles.container}>
      <FlexContainer alignItems="center" gap="xl">
        {connectorType === "source" ? <ConnectorCard icon={icon} /> : <EmptyCard />}
        <Icon type="arrowRight" />
        {connectorType === "destination" ? <ConnectorCard icon={icon} /> : <EmptyCard />}
      </FlexContainer>
      <FlexContainer direction="column" gap="xs" alignItems="center">
        <Heading as="h2" size="sm">
          <FormattedMessage
            id="connector.connections.empty"
            values={{ has: intlValues.has, needs: intlValues.needs }}
          />
        </Heading>
        <Text>
          {connectorType === "source" ? (
            <FormattedMessage
              id="connector.connections.empty.sourceCTA"
              values={{ name: connectorName, count: roundConnectorCount() }}
            />
          ) : (
            <FormattedMessage
              id="connector.connections.empty.destinationCTA"
              values={{ name: connectorName, count: roundConnectorCount() }}
            />
          )}
        </Text>
      </FlexContainer>
      <Button size="lg" onClick={onButtonClick} data-testid="create-connection">
        <Text inverseColor bold size="lg">
          <FormattedMessage id="connector.connections.empty.button" />
        </Text>
      </Button>
    </FlexContainer>
  );
};
