import React from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate, useParams } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { HeadTitle } from "components/common/HeadTitle";
import { FormPageContent } from "components/ConnectorBlocks";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { PageHeader } from "components/ui/PageHeader";

import { ConnectionConfiguration } from "core/domain/connection";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAvailableSourceDefinitions } from "hooks/domain/connector/useAvailableSourceDefinitions";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useCreateSource } from "hooks/services/useSourceHook";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout/ConnectorDocumentationWrapper";

import { SourceForm } from "./SourceForm";

export const CreateSourcePage: React.FC = () => {
  const { sourceDefinitionId } = useParams<{ sourceDefinitionId: string }>();
  const { hasFormChanges, clearAllFormChanges } = useFormChangeTrackerService();

  useTrackPage(PageTrackingCodes.SOURCE_NEW);
  const navigate = useNavigate();

  const sourceDefinitions = useAvailableSourceDefinitions();
  const { mutateAsync: createSource } = useCreateSource();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const onSubmitSourceStep = async (values: {
    name: string;
    serviceType: string;
    connectionConfiguration?: ConnectionConfiguration;
  }) => {
    const connector = sourceDefinitions.find((item) => item.sourceDefinitionId === values.serviceType);
    if (!connector) {
      // Unsure if this can happen, but the types want it defined
      throw new Error("No Connector Found");
    }
    const result = await createSource({ values, sourceConnector: connector });
    await new Promise((resolve) => setTimeout(resolve, 2000));
    clearAllFormChanges();
    navigate(`../${result.sourceId}`);
  };

  const onGoBack = () => {
    if (hasFormChanges) {
      openConfirmationModal({
        title: "form.discardChanges",
        text: "form.discardChangesConfirmation",
        submitButtonText: "form.discardChanges",
        onSubmit: () => {
          closeConfirmationModal();
          navigate(`../new-source`);
        },
        onClose: () => {
          closeConfirmationModal();
        },
      });
    } else {
      navigate(`../new-source`);
    }
  };

  return (
    <>
      <HeadTitle titles={[{ id: "sources.newSourceTitle" }]} />

      <ConnectorDocumentationWrapper>
        <FormPageContent>
          <PageHeader title={null} middleTitleBlock={<FormattedMessage id="sources.newSourceTitle" />} />
          <FlexContainer justifyContent="flex-start">
            <Box mb="md">
              <Button variant="clear" onClick={onGoBack} icon={<Icon type="chevronLeft" size="lg" />}>
                <FormattedMessage id="connectorBuilder.backButtonLabel" />
              </Button>
            </Box>
          </FlexContainer>
          <SourceForm
            onSubmit={onSubmitSourceStep}
            sourceDefinitions={sourceDefinitions}
            selectedSourceDefinitionId={sourceDefinitionId}
          />
          <CloudInviteUsersHint connectorType="source" />
        </FormPageContent>
      </ConnectorDocumentationWrapper>
    </>
  );
};
