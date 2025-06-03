import { FormattedMessage } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { MainPageWithScroll } from "components";
import { ConnectorBuilderProjectTable } from "components/ConnectorBuilderProjectTable";
import { HeadTitle } from "components/HeadTitle";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { PageHeader } from "components/ui/PageHeader";

import { useCurrentWorkspace, useListBuilderProjects } from "core/api";
import { useIntent } from "core/utils/rbac";

import { ConnectorBuilderRoutePaths } from "../ConnectorBuilderRoutes";

export const ConnectorBuilderListPage: React.FC = () => {
  const navigate = useNavigate();
  const projects = useListBuilderProjects();
  const { workspaceId } = useCurrentWorkspace();
  const canCreateConnector = useIntent("CreateCustomConnector", { workspaceId });

  return projects.length ? (
    <MainPageWithScroll
      headTitle={<HeadTitle titles={[{ id: "connectorBuilder.title" }]} />}
      pageTitle={
        <PageHeader
          leftComponent={
            <Heading as="h1" size="lg">
              <FormattedMessage id="connectorBuilder.listPage.title" values={{ count: projects.length }} />
            </Heading>
          }
          endComponent={
            <Button
              disabled={!canCreateConnector}
              icon="plus"
              onClick={() => navigate(ConnectorBuilderRoutePaths.Create)}
              size="sm"
              data-testid="new-custom-connector"
            >
              <FormattedMessage id="connectorBuilder.listPage.newConnector" />
            </Button>
          }
        />
      }
    >
      <FlexContainer direction="column" gap="2xl">
        <ConnectorBuilderProjectTable projects={projects} />
      </FlexContainer>
    </MainPageWithScroll>
  ) : (
    <Navigate to={ConnectorBuilderRoutePaths.Create} />
  );
};
