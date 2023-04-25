import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import { useAvailableSourceDefinitions } from "hooks/domain/connector/useAvailableSourceDefinitions";
import { AppActionCodes, useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useCreateSource, useSourceList } from "hooks/services/useSourceHook";
import ExistingEntityForm from "pages/connections/CreateConnectionPage/ExistingEntityForm";
import { SourceForm, SourceFormValues } from "pages/source/CreateSourcePage/SourceForm";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

import { RadioButtonTiles } from "./RadioButtonTiles";

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
  const sourceDefinitions = useAvailableSourceDefinitions();
  const { clearAllFormChanges, hasFormChanges } = useFormChangeTrackerService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { setDocumentationPanelOpen } = useDocumentationPanelContext();
  const { mutateAsync: createSource } = useCreateSource();
  const { trackAction } = useAppMonitoringService();

  const onCreateSource = async (values: SourceFormValues) => {
    const sourceConnector = sourceDefinitions.find((item) => item.sourceDefinitionId === values.serviceType);
    if (!sourceConnector) {
      trackAction(AppActionCodes.CONNECTOR_NOT_FOUND, { sourceDefinitionId: values.serviceType });
      throw new Error(AppActionCodes.CONNECTOR_NOT_FOUND);
    }
    const result = await createSource({ values, sourceConnector });
    clearAllFormChanges();
    await new Promise((resolve) => setTimeout(resolve, 2000));
    onSelectSource(result.sourceId);
  };

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
      {selectedSourceType === "existing" && (
        <ExistingEntityForm type="source" onSubmit={(sourceId) => onSelectSource(sourceId)} />
      )}
      {selectedSourceType === "new" && <SourceForm sourceDefinitions={sourceDefinitions} onSubmit={onCreateSource} />}
      <CloudInviteUsersHint connectorType="source" />
    </>
  );
};
