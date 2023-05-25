import React from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate, useParams } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { HeadTitle } from "components/common/HeadTitle";
import { FormPageContent } from "components/ConnectorBlocks";
import { DestinationForm } from "components/destination/DestinationForm";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { PageHeader } from "components/ui/PageHeader";

import { ConnectionConfiguration } from "core/domain/connection";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAvailableDestinationDefinitions } from "hooks/domain/connector/useAvailableDestinationDefinitions";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useCreateDestination } from "hooks/services/useDestinationHook";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

export const CreateDestinationPage: React.FC = () => {
  const { destinationDefinitionId } = useParams<{ destinationDefinitionId: string }>();
  useTrackPage(PageTrackingCodes.DESTINATION_NEW);

  const navigate = useNavigate();
  const { hasFormChanges, clearAllFormChanges } = useFormChangeTrackerService();
  const destinationDefinitions = useAvailableDestinationDefinitions();
  const { mutateAsync: createDestination } = useCreateDestination();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

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
    navigate(`../${result.destinationId}`);
  };

  const onGoBack = () => {
    if (hasFormChanges) {
      openConfirmationModal({
        title: "form.discardChanges",
        text: "form.discardChangesConfirmation",
        submitButtonText: "form.discardChanges",
        onSubmit: () => {
          closeConfirmationModal();
          navigate(`../new-destination`);
        },
        onClose: () => {
          closeConfirmationModal();
        },
      });
    } else {
      navigate(`../new-destination`);
    }
  };

  return (
    <>
      <HeadTitle titles={[{ id: "destinations.newDestinationTitle" }]} />

      <ConnectorDocumentationWrapper>
        <FormPageContent>
          <PageHeader title={null} middleTitleBlock={<FormattedMessage id="destinations.newDestinationTitle" />} />
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
    </>
  );
};
