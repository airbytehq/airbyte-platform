import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import { HeadTitle } from "components/common/HeadTitle";
import { FormPageContent } from "components/ConnectorBlocks";
import { DestinationForm } from "components/destination/DestinationForm";
import { SelectConnector } from "components/source/SelectConnector";
import { Box } from "components/ui/Box";
import { PageHeader } from "components/ui/PageHeader";

import { ConnectionConfiguration } from "core/domain/connection";
import { useAvailableDestinationDefinitions } from "hooks/domain/connector/useAvailableDestinationDefinitions";
import { useTrackPage, PageTrackingCodes } from "hooks/services/Analytics";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useCreateDestination } from "hooks/services/useDestinationHook";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";

export const CreateDestinationPage: React.FC = () => {
  const [selectedDestinationDefinitionId, setSelectedDestinationDefinitionId] = useState("");
  useTrackPage(PageTrackingCodes.DESTINATION_NEW);

  const navigate = useNavigate();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const destinationDefinitions = useAvailableDestinationDefinitions();
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

  return (
    <>
      <HeadTitle titles={[{ id: "destinations.newDestinationTitle" }]} />
      {!selectedDestinationDefinitionId && (
        <Box pb="2xl">
          <SelectConnector
            connectorType="destination"
            connectorDefinitions={destinationDefinitions}
            headingKey="destinations.selectDestinationTitle"
            onSelectConnectorDefinition={(id) => setSelectedDestinationDefinitionId(id)}
          />
        </Box>
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
