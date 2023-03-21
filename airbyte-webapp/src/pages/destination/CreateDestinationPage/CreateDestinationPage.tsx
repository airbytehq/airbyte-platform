import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { HeadTitle } from "components/common/HeadTitle";
import { FormPageContent } from "components/ConnectorBlocks";
import { DestinationForm } from "components/destination/DestinationForm";
import { SelectConnector } from "components/source/SelectConnector";
import { PageHeader } from "components/ui/PageHeader";

import { ConnectionConfiguration } from "core/domain/connection";
import { useTrackPage, PageTrackingCodes } from "hooks/services/Analytics";
import { useExperiment } from "hooks/services/Experiment";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useCreateDestination } from "hooks/services/useDestinationHook";
import { useDestinationDefinitionList } from "services/connector/DestinationDefinitionService";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

import styles from "./CreateDestinationPage.module.scss";
export const CreateDestinationPage: React.FC = () => {
  const [selectedDestinationDefinitionId, setSelectedDestinationDefinitionId] = useState("");
  useTrackPage(PageTrackingCodes.DESTINATION_NEW);
  const newConnectorGridExperiment = useExperiment("connector.form.useSelectConnectorGrid", false);

  const navigate = useNavigate();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const { destinationDefinitions } = useDestinationDefinitionList();
  const { mutateAsync: createDestination } = useCreateDestination();

  const onSubmitDestinationForm = async (values: {
    name: string;
    serviceType: string;
    connectionConfiguration?: ConnectionConfiguration;
  }) => {
    const connector = destinationDefinitions.find((item) => item.destinationDefinitionId === values.serviceType);
    const result = await createDestination({
      values,
      destinationConnector: connector,
    });
    await new Promise((resolve) => setTimeout(resolve, 2000));
    clearAllFormChanges();
    navigate(`../${result.destinationId}`);
  };

  if (!newConnectorGridExperiment) {
    return (
      <>
        <HeadTitle titles={[{ id: "destinations.newDestinationTitle" }]} />
        <ConnectorDocumentationWrapper>
          <PageHeader title={null} middleTitleBlock={<FormattedMessage id="destinations.newDestinationTitle" />} />
          <FormPageContent>
            <DestinationForm onSubmit={onSubmitDestinationForm} destinationDefinitions={destinationDefinitions} />
            <CloudInviteUsersHint connectorType="destination" />
          </FormPageContent>
        </ConnectorDocumentationWrapper>
      </>
    );
  }

  return (
    <>
      <HeadTitle titles={[{ id: "destinations.newDestinationTitle" }]} />
      {!selectedDestinationDefinitionId && (
        <div className={styles.selectDestinationWrapper}>
          <SelectConnector
            connectorDefinitions={destinationDefinitions}
            headingKey="destinations.selectDestinationTitle"
            onSelectConnectorDefinition={(id) => setSelectedDestinationDefinitionId(id)}
          />
        </div>
      )}

      {selectedDestinationDefinitionId && (
        <ConnectorDocumentationWrapper>
          <FormPageContent>
            <PageHeader title={null} middleTitleBlock={<FormattedMessage id="destinations.newDestinationTitle" />} />
            <DestinationForm
              onSubmit={onSubmitDestinationForm}
              destinationDefinitions={destinationDefinitions}
              selectedSourceDefinitionId={selectedDestinationDefinitionId}
            />
            <CloudInviteUsersHint connectorType="destination" />
          </FormPageContent>
        </ConnectorDocumentationWrapper>
      )}
    </>
  );
};
