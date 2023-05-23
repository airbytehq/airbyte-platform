import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { HeadTitle } from "components/common/HeadTitle";
import { FormPageContent } from "components/ConnectorBlocks";
import { SelectConnector } from "components/source/SelectConnector";
import { Box } from "components/ui/Box";
import { PageHeader } from "components/ui/PageHeader";

import { ConnectionConfiguration } from "core/domain/connection";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAvailableSourceDefinitions } from "hooks/domain/connector/useAvailableSourceDefinitions";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useCreateSource } from "hooks/services/useSourceHook";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout/ConnectorDocumentationWrapper";

import { SourceForm } from "./SourceForm";

export const CreateSourcePage: React.FC = () => {
  const [selectedSourceDefinitionId, setSelectedSourceDefinitionId] = useState("");
  useTrackPage(PageTrackingCodes.SOURCE_NEW);
  const navigate = useNavigate();

  const { clearAllFormChanges } = useFormChangeTrackerService();
  const sourceDefinitions = useAvailableSourceDefinitions();
  const { mutateAsync: createSource } = useCreateSource();

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

  return (
    <>
      <HeadTitle titles={[{ id: "sources.newSourceTitle" }]} />
      {!selectedSourceDefinitionId && (
        <Box pb="2xl">
          <SelectConnector
            connectorType="source"
            connectorDefinitions={sourceDefinitions}
            headingKey="sources.selectSourceTitle"
            onSelectConnectorDefinition={(id) => setSelectedSourceDefinitionId(id)}
          />
        </Box>
      )}

      {selectedSourceDefinitionId && (
        <ConnectorDocumentationWrapper>
          <FormPageContent>
            <PageHeader title={null} middleTitleBlock={<FormattedMessage id="sources.newSourceTitle" />} />
            <SourceForm
              onSubmit={onSubmitSourceStep}
              sourceDefinitions={sourceDefinitions}
              selectedSourceDefinitionId={selectedSourceDefinitionId}
            />
            <CloudInviteUsersHint connectorType="source" />
          </FormPageContent>
        </ConnectorDocumentationWrapper>
      )}
    </>
  );
};
