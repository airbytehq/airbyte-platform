import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { DestinationForm, DestinationFormValues } from "components/destination/DestinationForm/DestinationForm";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import { useAvailableDestinationDefinitions } from "hooks/domain/connector/useAvailableDestinationDefinitions";
import { AppActionCodes, useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useCreateDestination, useDestinationList } from "hooks/services/useDestinationHook";
import ExistingEntityForm from "pages/connections/CreateConnectionPage/ExistingEntityForm";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

import { RadioButtonTiles } from "./RadioButtonTiles";

type DestinationType = "existing" | "new";

interface SelectDestinationProps {
  onSelectDestination: (destinationId: string) => void;
  initialSelectedDestinationType?: DestinationType;
}

export const SelectDestination: React.FC<SelectDestinationProps> = ({
  onSelectDestination,
  initialSelectedDestinationType = "existing",
}) => {
  const { destinations } = useDestinationList();
  const [selectedDestinationType, setSelectedDestinationType] = useState<DestinationType>(
    destinations.length === 0 ? "new" : initialSelectedDestinationType
  );
  const destinationDefinitions = useAvailableDestinationDefinitions();
  const { clearAllFormChanges, hasFormChanges } = useFormChangeTrackerService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { setDocumentationPanelOpen } = useDocumentationPanelContext();
  const { mutateAsync: createDestination } = useCreateDestination();
  const { trackAction } = useAppMonitoringService();

  const onCreateDestination = async (values: DestinationFormValues) => {
    const destinationConnector = destinationDefinitions.find(
      (item) => item.destinationDefinitionId === values.serviceType
    );
    if (!destinationConnector) {
      trackAction(AppActionCodes.CONNECTOR_NOT_FOUND, { destinationDefinitionId: values.serviceType });
      throw new Error(AppActionCodes.CONNECTOR_NOT_FOUND);
    }
    const result = await createDestination({ values, destinationConnector });
    clearAllFormChanges();
    await new Promise((resolve) => setTimeout(resolve, 2000));
    onSelectDestination(result.destinationId);
  };

  const onSelectDestinationType = (destinationType: DestinationType) => {
    if (hasFormChanges) {
      openConfirmationModal({
        title: "form.discardChanges",
        text: "form.discardChangesConfirmation",
        submitButtonText: "form.discardChanges",
        onSubmit: () => {
          closeConfirmationModal();
          setSelectedDestinationType(destinationType);
          if (destinationType === "existing") {
            setDocumentationPanelOpen(false);
          }
        },
        onClose: () => {
          setSelectedDestinationType(selectedDestinationType);
        },
      });
    } else {
      setSelectedDestinationType(destinationType);
      if (destinationType === "existing") {
        setDocumentationPanelOpen(false);
      }
    }
  };

  return (
    <>
      <Card withPadding>
        <Heading as="h2">
          <FormattedMessage id="connectionForm.defineDestination" />
        </Heading>
        <Box mt="md">
          <RadioButtonTiles
            name="destinationType"
            options={[
              {
                value: "existing",
                label: "connectionForm.destinationExisting",
                description: "connectionForm.destinationExistingDescription",
                disabled: destinations.length === 0,
              },
              {
                value: "new",
                label: "connectionForm.destinationNew",
                description: "connectionForm.destinationNewDescription",
              },
            ]}
            selectedValue={selectedDestinationType}
            onSelectRadioButton={(id) => onSelectDestinationType(id)}
          />
        </Box>
      </Card>
      {selectedDestinationType === "existing" && (
        <ExistingEntityForm type="destination" onSubmit={(destinationId) => onSelectDestination(destinationId)} />
      )}
      {selectedDestinationType === "new" && (
        <DestinationForm destinationDefinitions={destinationDefinitions} onSubmit={onCreateDestination} />
      )}
      <CloudInviteUsersHint connectorType="destination" />
    </>
  );
};
