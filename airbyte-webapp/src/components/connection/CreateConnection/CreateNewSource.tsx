import { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { ConnectorGrid } from "components/source/SelectConnector/ConnectorGrid";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { SearchInput } from "components/ui/SearchInput";

import { useAvailableSourceDefinitions } from "hooks/domain/connector/useAvailableSourceDefinitions";
import { AppActionCodes, useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useModalService } from "hooks/services/Modal";
import { useCreateSource } from "hooks/services/useSourceHook";
import { SourceForm, SourceFormValues } from "pages/source/CreateSourcePage/SourceForm";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";
import RequestConnectorModal from "views/Connector/RequestConnectorModal";

interface CreateNewSourceProps {
  onSourceCreated: (sourceId: string) => void;
}

export const CreateNewSource: React.FC<CreateNewSourceProps> = ({ onSourceCreated }) => {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedSourceDefinitionId, setSelectedSourceDefinitionId] = useState<string>();
  const sourceDefinitions = useAvailableSourceDefinitions();
  const { trackAction } = useAppMonitoringService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { mutateAsync: createSource } = useCreateSource();
  const { openModal, closeModal } = useModalService();
  const { email } = useCurrentWorkspace();
  const { formatMessage } = useIntl();
  const { setDocumentationPanelOpen } = useDocumentationPanelContext();

  const { hasFormChanges, clearAllFormChanges } = useFormChangeTrackerService();

  const onSelectSourceDefinitionId = (sourceDefinitionId: string | undefined) => {
    setSearchTerm("");
    setSelectedSourceDefinitionId(sourceDefinitionId);
  };

  const onCreateSource = async (values: SourceFormValues) => {
    const sourceDefinition = sourceDefinitions.find((item) => item.sourceDefinitionId === values.serviceType);
    if (!sourceDefinition) {
      trackAction(AppActionCodes.CONNECTOR_DEFINITION_NOT_FOUND, { sourceDefinitionId: values.serviceType });
      throw new Error(AppActionCodes.CONNECTOR_DEFINITION_NOT_FOUND);
    }
    const result = await createSource({ values, sourceConnector: sourceDefinition });
    clearAllFormChanges();
    await new Promise((resolve) => setTimeout(resolve, 2000));
    onSourceCreated(result.sourceId);
    setDocumentationPanelOpen(false);
  };

  const onGoBack = () => {
    if (hasFormChanges) {
      openConfirmationModal({
        title: "form.discardChanges",
        text: "form.discardChangesConfirmation",
        submitButtonText: "form.discardChanges",
        onSubmit: () => {
          closeConfirmationModal();
          onSelectSourceDefinitionId(undefined);
          setDocumentationPanelOpen(false);
        },
        onClose: () => {
          closeConfirmationModal();
        },
      });
    } else {
      onSelectSourceDefinitionId(undefined);
      setDocumentationPanelOpen(false);
    }
  };

  const onOpenRequestConnectorModal = () =>
    openModal({
      title: formatMessage({ id: "connector.requestConnector" }),
      content: () => (
        <RequestConnectorModal
          connectorType="source"
          workspaceEmail={email}
          searchedConnectorName={searchTerm}
          onClose={closeModal}
        />
      ),
    });

  const filteredSourceDefinitions = useMemo(
    () => sourceDefinitions.filter((item) => item.name.toLocaleLowerCase().includes(searchTerm.toLocaleLowerCase())),
    [sourceDefinitions, searchTerm]
  );

  if (selectedSourceDefinitionId) {
    return (
      <>
        <FlexContainer justifyContent="flex-start">
          <Box mb="md">
            <Button variant="clear" onClick={onGoBack} icon={<Icon type="chevronLeft" size="lg" />}>
              <FormattedMessage id="connectorBuilder.backButtonLabel" />
            </Button>
          </Box>
        </FlexContainer>
        <SourceForm
          selectedSourceDefinitionId={selectedSourceDefinitionId}
          sourceDefinitions={sourceDefinitions}
          onSubmit={onCreateSource}
        />
      </>
    );
  }

  return (
    <FlexContainer gap="xl" direction="column">
      <SearchInput value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} />
      <ConnectorGrid
        connectorDefinitions={filteredSourceDefinitions}
        onConnectorButtonClick={(sourceDefinition) => onSelectSourceDefinitionId(sourceDefinition.sourceDefinitionId)}
        onOpenRequestConnectorModal={onOpenRequestConnectorModal}
      />
    </FlexContainer>
  );
};
