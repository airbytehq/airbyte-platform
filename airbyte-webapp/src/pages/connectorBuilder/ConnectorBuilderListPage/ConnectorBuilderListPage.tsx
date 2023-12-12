import { FormattedMessage } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { MainPageWithScroll } from "components";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectorBuilderProjectTable } from "components/ConnectorBuilderProjectTable";
import { Button } from "components/ui/Button";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { PageHeader } from "components/ui/PageHeader";

import { useListBuilderProjects } from "core/api";

import { ConnectorBuilderRoutePaths } from "../ConnectorBuilderRoutes";

export const ConnectorBuilderListPage: React.FC = () => {
  const navigate = useNavigate();
  const projects = useListBuilderProjects();

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
              icon={<Icon type="plus" />}
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
      <ConnectorBuilderProjectTable projects={projects} />
    </MainPageWithScroll>
  ) : (
    <Navigate to={ConnectorBuilderRoutePaths.Create} />
  );
};
