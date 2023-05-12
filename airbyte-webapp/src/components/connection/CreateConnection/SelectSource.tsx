import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useSourceList } from "hooks/services/useSourceHook";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

import { CreateNewSource } from "./CreateNewSource";
import { RadioButtonTiles } from "./RadioButtonTiles";
import { SelectExistingConnector } from "./SelectExistingConnector";

type SourceType = "existing" | "new";

interface SelectSourceProps {
  onSelectSource: (sourceId: string) => void;
  initialSelectedSourceType?: SourceType;
}

export const SelectSource: React.FC<SelectSourceProps> = ({
  onSelectSource,
  initialSelectedSourceType = "existing",
}) => {
  const { sources } = useSourceList();
  const [selectedSourceType, setSelectedSourceType] = useState<SourceType>(
    sources.length === 0 ? "new" : initialSelectedSourceType
  );
  const { hasFormChanges } = useFormChangeTrackerService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { setDocumentationPanelOpen } = useDocumentationPanelContext();

  const onSelectSourceType = (sourceType: SourceType) => {
    if (hasFormChanges) {
      openConfirmationModal({
        title: "form.discardChanges",
        text: "form.discardChangesConfirmation",
        submitButtonText: "form.discardChanges",
        onSubmit: () => {
          closeConfirmationModal();
          setSelectedSourceType(sourceType);
          if (sourceType === "existing") {
            setDocumentationPanelOpen(false);
          }
        },
        onClose: () => {
          setSelectedSourceType(selectedSourceType);
        },
      });
    } else {
      setSelectedSourceType(sourceType);
      if (sourceType === "existing") {
        setDocumentationPanelOpen(false);
      }
    }
  };

  return (
    <>
      <Card withPadding>
        <Heading as="h2">
          <FormattedMessage id="connectionForm.defineSource" />
        </Heading>
        <Box mt="md">
          <RadioButtonTiles
            name="sourceType"
            options={[
              {
                value: "existing",
                label: "connectionForm.sourceExisting",
                description: "connectionForm.sourceExistingDescription",
                disabled: sources.length === 0,
              },
              {
                value: "new",
                label: "onboarding.sourceSetUp",
                description: "onboarding.sourceSetUp.description",
              },
            ]}
            selectedValue={selectedSourceType}
            onSelectRadioButton={(id) => onSelectSourceType(id)}
          />
        </Box>
      </Card>
      <Box mt="xl">
        {selectedSourceType === "existing" && (
          <SelectExistingConnector
            connectors={sources}
            onSelectConnector={({ sourceId }) => onSelectSource(sourceId)}
          />
        )}
        {selectedSourceType === "new" && <CreateNewSource onSourceCreated={(sourceId) => onSelectSource(sourceId)} />}
      </Box>
      <CloudInviteUsersHint connectorType="source" />
    </>
  );
};
