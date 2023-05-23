import { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { DestinationForm } from "components/destination/DestinationForm";
import { DestinationFormValues } from "components/destination/DestinationForm/DestinationForm";
import { ConnectorGrid } from "components/source/SelectConnector/ConnectorGrid";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { SearchInput } from "components/ui/SearchInput";

import { useAvailableDestinationDefinitions } from "hooks/domain/connector/useAvailableDestinationDefinitions";
import { AppActionCodes, useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useModalService } from "hooks/services/Modal";
import { useCreateDestination } from "hooks/services/useDestinationHook";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";
import RequestConnectorModal from "views/Connector/RequestConnectorModal";

interface CreateNewDestinationProps {
  onDestinationCreated: (destinationId: string) => void;
}

export const CreateNewDestination: React.FC<CreateNewDestinationProps> = ({ onDestinationCreated }) => {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedDestinationDefinitionId, setSelectedDestinationDefinitionId] = useState<string>();
  const destinationDefinitions = useAvailableDestinationDefinitions();
  const { trackAction } = useAppMonitoringService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { mutateAsync: createDestination } = useCreateDestination();
  const { openModal, closeModal } = useModalService();
  const { email } = useCurrentWorkspace();
  const { formatMessage } = useIntl();
  const { setDocumentationPanelOpen } = useDocumentationPanelContext();

  const { hasFormChanges, clearAllFormChanges } = useFormChangeTrackerService();

  const onSelectDestinationDefinitionId = (destinationDefinitionId: string | undefined) => {
    setSearchTerm("");
    setSelectedDestinationDefinitionId(destinationDefinitionId);
  };

  const onCreateDestination = async (values: DestinationFormValues) => {
    const destinationDefinition = destinationDefinitions.find(
      (item) => item.destinationDefinitionId === values.serviceType
    );
    if (!destinationDefinition) {
      trackAction(AppActionCodes.CONNECTOR_DEFINITION_NOT_FOUND, { destinationDefinitionId: values.serviceType });
      throw new Error(AppActionCodes.CONNECTOR_DEFINITION_NOT_FOUND);
    }
    const result = await createDestination({ values, destinationConnector: destinationDefinition });
    clearAllFormChanges();
    await new Promise((resolve) => setTimeout(resolve, 2000));
    onDestinationCreated(result.destinationId);
  };

  const onGoBack = () => {
    if (hasFormChanges) {
      openConfirmationModal({
        title: "form.discardChanges",
        text: "form.discardChangesConfirmation",
        submitButtonText: "form.discardChanges",
        onSubmit: () => {
          closeConfirmationModal();
          onSelectDestinationDefinitionId(undefined);
          setDocumentationPanelOpen(false);
        },
        onClose: () => {
          closeConfirmationModal();
        },
      });
    } else {
      onSelectDestinationDefinitionId(undefined);
      setDocumentationPanelOpen(false);
    }
  };

  const filteredDestinationDefinitions = useMemo(
    () =>
      destinationDefinitions.filter((item) => item.name.toLocaleLowerCase().includes(searchTerm.toLocaleLowerCase())),
    [destinationDefinitions, searchTerm]
  );

  const onOpenRequestConnectorModal = () =>
    openModal({
      title: formatMessage({ id: "connector.requestConnector" }),
      content: () => (
        <RequestConnectorModal
          connectorType="destination"
          workspaceEmail={email}
          searchedConnectorName={searchTerm}
          onClose={closeModal}
        />
      ),
    });

  if (selectedDestinationDefinitionId) {
    return (
      <>
        <FlexContainer justifyContent="flex-start">
          <Box mb="md">
            <Button variant="clear" onClick={onGoBack} icon={<Icon type="chevronLeft" size="lg" />}>
              <FormattedMessage id="connectorBuilder.backButtonLabel" />
            </Button>
          </Box>
        </FlexContainer>
        <DestinationForm
          selectedDestinationDefinitionId={selectedDestinationDefinitionId}
          destinationDefinitions={destinationDefinitions}
          onSubmit={onCreateDestination}
        />
      </>
    );
  }

  return (
    <FlexContainer gap="xl" direction="column">
      <SearchInput value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} />
      <ConnectorGrid
        connectorDefinitions={filteredDestinationDefinitions}
        onConnectorButtonClick={(destinationDefinition) =>
          onSelectDestinationDefinitionId(destinationDefinition.destinationDefinitionId)
        }
        onOpenRequestConnectorModal={onOpenRequestConnectorModal}
      />
    </FlexContainer>
  );
};
