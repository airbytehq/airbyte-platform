import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate, useParams } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { HeadTitle } from "components/common/HeadTitle";
import { FormPageContent } from "components/ConnectorBlocks";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { ConnectionConfiguration } from "area/connector/types";
import { useSourceDefinitionList, useCreateSource } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { SourcePaths, RoutePaths } from "pages/routePaths";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout/ConnectorDocumentationWrapper";

import { SourceForm } from "./SourceForm";

export const CreateSourcePage: React.FC = () => {
  const params = useParams<{ workspaceId: string }>();

  const { sourceDefinitionId } = useParams<{ sourceDefinitionId: string }>();
  const { clearAllFormChanges } = useFormChangeTrackerService();

  useTrackPage(PageTrackingCodes.SOURCE_NEW);
  const navigate = useNavigate();
  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Source}`;
  const { formatMessage } = useIntl();

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.sources" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: formatMessage({ id: "sources.newSource" }) },
  ];

  const { sourceDefinitions } = useSourceDefinitionList();
  const { mutateAsync: createSource } = useCreateSource();

  const onSubmitSourceStep = async (values: {
    name: string;
    serviceType: string;
    connectionConfiguration?: ConnectionConfiguration;
  }) => {
    const connector = sourceDefinitions.find((item) => item.sourceDefinitionId === values.serviceType);
    if (!connector) {
      // Unsure if this can happen, but the types want it defined
      throw new Error("No Connector Found");
    }
    const result = await createSource({ values, sourceConnector: connector });
    await new Promise((resolve) => setTimeout(resolve, 2000));
    clearAllFormChanges();
    navigate(`../${result.sourceId}/${SourcePaths.Connections}`);
  };

  const onGoBack = () => {
    navigate(`../${SourcePaths.SelectSourceNew}`);
  };

  return (
    <ConnectorDocumentationWrapper>
      <HeadTitle titles={[{ id: "sources.newSourceTitle" }]} />
      <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData} />
      <FormPageContent>
        <FlexContainer justifyContent="flex-start">
          <Box mb="md">
            <Button variant="clear" onClick={onGoBack} icon={<Icon type="chevronLeft" size="lg" />}>
              <FormattedMessage id="connectorBuilder.backButtonLabel" />
            </Button>
          </Box>
        </FlexContainer>
        <SourceForm
          onSubmit={onSubmitSourceStep}
          sourceDefinitions={sourceDefinitions}
          selectedSourceDefinitionId={sourceDefinitionId}
        />
        <CloudInviteUsersHint connectorType="source" />
      </FormPageContent>
    </ConnectorDocumentationWrapper>
  );
};
