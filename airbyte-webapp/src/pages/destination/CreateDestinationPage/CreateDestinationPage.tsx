import React, { useEffect, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation, useNavigate, useParams } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { FormPageContent } from "components/ConnectorBlocks";
import { DestinationForm } from "components/destination/DestinationForm";
import { HeadTitle } from "components/HeadTitle";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { ConnectionConfiguration } from "area/connector/types";
import { useCreateDestination, useDestinationDefinitionList } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { clearConnectorChatBuilderStorage, CONNECTOR_CHAT_ACTIONS } from "core/utils/connectorChatBuilderStorage";
import { useExperiment } from "hooks/services/Experiment";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { DestinationPaths, RoutePaths } from "pages/routePaths";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

export const CreateDestinationPage: React.FC = () => {
  const { destinationDefinitionId, workspaceId } = useParams<{
    destinationDefinitionId: string;
    workspaceId: string;
  }>();
  useTrackPage(PageTrackingCodes.DESTINATION_NEW);

  const navigate = useNavigate();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const { destinationDefinitions } = useDestinationDefinitionList();
  const { mutateAsync: createDestination } = useCreateDestination();

  const onSubmitDestinationForm = async (values: {
    name: string;
    serviceType: string;
    connectionConfiguration: ConnectionConfiguration;
  }) => {
    const connector = destinationDefinitions.find((item) => item.destinationDefinitionId === values.serviceType);
    const result = await createDestination({
      values,
      destinationConnector: connector,
    });
    await new Promise((resolve) => setTimeout(resolve, 2000));
    clearAllFormChanges();
    navigate(`../${result.destinationId}/${DestinationPaths.Connections}`);
  };

  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Destination}`;
  const { formatMessage } = useIntl();

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.destinations" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: formatMessage({ id: "destinations.newDestination" }) },
  ];

  const location = useLocation();
  // save previous path on mount so that it remains unchanged even if search params are added on this page
  const [prevPath] = useState<string>(location.state?.prevPath || `../${DestinationPaths.SelectDestinationNew}`);
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
      <HeadTitle titles={[{ id: "destinations.newDestinationTitle" }]} />
      <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData} />
      <FormPageContent>
        <FlexContainer justifyContent="flex-start">
          <Box mb="md">
            <Button variant="clear" onClick={onGoBack} icon="chevronLeft" iconSize="lg">
              <FormattedMessage id="connectorBuilder.backButtonLabel" />
            </Button>
          </Box>
        </FlexContainer>
        <DestinationForm
          onSubmit={onSubmitDestinationForm}
          destinationDefinitions={destinationDefinitions}
          selectedDestinationDefinitionId={destinationDefinitionId}
        />
        <CloudInviteUsersHint connectorType="destination" />
      </FormPageContent>
    </ConnectorDocumentationWrapper>
  );
};
