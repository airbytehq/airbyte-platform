import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate, useParams } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { HeadTitle } from "components/common/HeadTitle";
import { FormPageContent } from "components/ConnectorBlocks";
import { DestinationForm } from "components/destination/DestinationForm";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { PageHeaderWithNavigation } from "components/ui/PageHeader";

import { ConnectionConfiguration } from "area/connector/types";
import { useDestinationDefinitionList, useCreateDestination } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
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
    connectionConfiguration?: ConnectionConfiguration;
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

  const onGoBack = () => {
    navigate(`../${DestinationPaths.SelectDestinationNew}`);
  };

  return (
    <ConnectorDocumentationWrapper>
      <HeadTitle titles={[{ id: "destinations.newDestinationTitle" }]} />
      <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData} />
      <FormPageContent>
        <FlexContainer justifyContent="flex-start">
          <Box mb="md">
            <Button variant="clear" onClick={onGoBack} icon={<Icon type="chevronLeft" size="lg" />}>
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
