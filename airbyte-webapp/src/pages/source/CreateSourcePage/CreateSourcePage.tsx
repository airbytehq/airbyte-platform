import React, { useEffect, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation, useNavigate, useParams } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { FormPageContent } from "components/ConnectorBlocks";
import { HeadTitle } from "components/HeadTitle";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { ConnectionConfiguration } from "area/connector/types";
import { useCreateSource, useSourceDefinitionList } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { clearConnectorChatBuilderStorage, CONNECTOR_CHAT_ACTIONS } from "core/utils/connectorChatBuilderStorage";
import { useExperiment } from "hooks/services/Experiment";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { RoutePaths, SourcePaths } from "pages/routePaths";
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
    connectionConfiguration: ConnectionConfiguration;
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

  const location = useLocation();
  // save previous path on mount so that it remains unchanged even if search params are added on this page
  const [prevPath] = useState<string>(location.state?.prevPath || `../${SourcePaths.SelectSourceNew}`);
  const onGoBack = () => {
    navigate(prevPath);
  };

  const isConnectorBuilderGenerateFromParamsEnabled = useExperiment("connectorBuilder.generateConnectorFromParams");
  useEffect(() => {
    if (isConnectorBuilderGenerateFromParamsEnabled) {
      clearConnectorChatBuilderStorage(CONNECTOR_CHAT_ACTIONS.SET_UP_NEW_CONNECTOR);
    }
  }, [isConnectorBuilderGenerateFromParamsEnabled]);

  return (
    <ConnectorDocumentationWrapper>
      <HeadTitle titles={[{ id: "sources.newSourceTitle" }]} />
      <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData} />
      <FormPageContent>
        <FlexContainer justifyContent="flex-start">
          <Box mb="md">
            <Button variant="clear" onClick={onGoBack} icon="chevronLeft" iconSize="lg">
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
