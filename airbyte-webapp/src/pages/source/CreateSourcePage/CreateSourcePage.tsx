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
import { useTrackPage, PageTrackingCodes } from "hooks/services/Analytics";
import { useExperiment } from "hooks/services/Experiment";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useCreateSource } from "hooks/services/useSourceHook";
import { useSourceDefinitionList } from "services/connector/SourceDefinitionService";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout/ConnectorDocumentationWrapper";

import { SourceForm } from "./SourceForm";

export const CreateSourcePage: React.FC = () => {
  const [selectedSourceDefinitionId, setSelectedSourceDefinitionId] = useState("");
  useTrackPage(PageTrackingCodes.SOURCE_NEW);
  const navigate = useNavigate();
  const newConnectorGridExperiment = useExperiment("connector.form.useSelectConnectorGrid", false);

  const { clearAllFormChanges } = useFormChangeTrackerService();
  const { sourceDefinitions } = useSourceDefinitionList();
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

  if (!newConnectorGridExperiment) {
    return (
      <>
        <HeadTitle titles={[{ id: "sources.newSourceTitle" }]} />{" "}
        <ConnectorDocumentationWrapper>
          <PageHeader title={null} middleTitleBlock={<FormattedMessage id="sources.newSourceTitle" />} />
          <FormPageContent>
            <SourceForm onSubmit={onSubmitSourceStep} sourceDefinitions={sourceDefinitions} />
            <CloudInviteUsersHint connectorType="source" />
          </FormPageContent>
        </ConnectorDocumentationWrapper>
      </>
    );
  }

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
