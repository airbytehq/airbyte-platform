import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

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

const EXISTING_DESTINATION_TYPE = "existing";
const NEW_DESTINATION_TYPE = "new";
const DESTINATION_TYPE_PARAM = "destinationType";
const DESTINATION_ID_PARAM = "destinationId";

export const SelectDestination: React.FC = () => {
  const { destinations } = useDestinationList();
  const [searchParams, setSearchParams] = useSearchParams();

  const selectedDestinationType = useMemo(() => {
    return destinations.length === 0
      ? NEW_DESTINATION_TYPE
      : (searchParams.get(DESTINATION_TYPE_PARAM) as DestinationType) ?? EXISTING_DESTINATION_TYPE;
  }, [searchParams, destinations.length]);

  const { hasFormChanges } = useFormChangeTrackerService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { setDocumentationPanelOpen } = useDocumentationPanelContext();

  const selectDestinationType = (destinationType: DestinationType) => {
    searchParams.set(DESTINATION_TYPE_PARAM, destinationType);
    setSearchParams(searchParams);
  };

  const selectDestination = (destinationId: string) => {
    searchParams.delete(DESTINATION_TYPE_PARAM);
    searchParams.set(DESTINATION_ID_PARAM, destinationId);
    setSearchParams(searchParams);
  };

  const onSelectDestinationType = (destinationType: DestinationType) => {
    if (hasFormChanges) {
      openConfirmationModal({
        title: "form.discardChanges",
        text: "form.discardChangesConfirmation",
        submitButtonText: "form.discardChanges",
        onSubmit: () => {
          closeConfirmationModal();
          selectDestinationType(destinationType);
          if (destinationType === EXISTING_DESTINATION_TYPE) {
            setDocumentationPanelOpen(false);
          }
        },
        onClose: () => {
          selectDestinationType(destinationType);
        },
      });
    } else {
      selectDestinationType(destinationType);
      if (destinationType === EXISTING_DESTINATION_TYPE) {
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
                value: EXISTING_DESTINATION_TYPE,
                label: "connectionForm.destinationExisting",
                description: "connectionForm.destinationExistingDescription",
                disabled: destinations.length === 0,
              },
              {
                value: NEW_DESTINATION_TYPE,
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
        {selectedDestinationType === EXISTING_DESTINATION_TYPE && (
          <SelectExistingConnector connectors={destinations} selectConnector={selectDestination} />
        )}
        {selectedDestinationType === NEW_DESTINATION_TYPE && (
          <CreateNewDestination onDestinationCreated={(destinationId) => selectDestination(destinationId)} />
        )}
      </Box>
      <CloudInviteUsersHint connectorType="destination" />
    </>
  );
};
