import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useDestinationList } from "hooks/services/useDestinationHook";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

import { CreateNewDestination } from "./CreateNewDestination";
import { RadioButtonTiles } from "./RadioButtonTiles";
import { SelectExistingConnector } from "./SelectExistingConnector";

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
  const { hasFormChanges } = useFormChangeTrackerService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { setDocumentationPanelOpen } = useDocumentationPanelContext();

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
      <Box mt="xl">
        {selectedDestinationType === "existing" && (
          <SelectExistingConnector
            connectors={destinations}
            onSelectConnector={({ destinationId }) => onSelectDestination(destinationId)}
          />
        )}
        {selectedDestinationType === "new" && (
          <CreateNewDestination onDestinationCreated={(destinationId) => onSelectDestination(destinationId)} />
        )}
      </Box>
      <CloudInviteUsersHint connectorType="destination" />
    </>
  );
};
