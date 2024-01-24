import React, { useCallback, useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { useGetDestinationFromParams } from "area/connector/utils";
import {
  useConnectionList,
  useDestinationDefinitionVersion,
  useGetDestinationDefinitionSpecification,
  useDestinationDefinition,
  useDeleteDestination,
  useInvalidateDestination,
  useUpdateDestination,
} from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useFormChangeTrackerService, useUniqueFormId } from "hooks/services/FormChangeTracker";
import { useDeleteModal } from "hooks/useDeleteModal";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ConnectorCardValues } from "views/Connector/ConnectorForm/types";

import styles from "./DestinationSettings.module.scss";

export const DestinationSettingsPage: React.FC = () => {
  const destination = useGetDestinationFromParams();
  const connectionList = useConnectionList({ destinationId: [destination.destinationId] });
  const connectionsWithDestination = useMemo(() => connectionList?.connections ?? [], [connectionList]);
  const destinationDefinition = useDestinationDefinition(destination.destinationDefinitionId);
  const destinationDefinitionVersion = useDestinationDefinitionVersion(destination.destinationId);
  const destinationSpecification = useGetDestinationDefinitionSpecification(
    destination.destinationDefinitionId,
    destination.destinationId
  );
  const reloadDestination = useInvalidateDestination(destination.destinationId);
  const { mutateAsync: updateDestination } = useUpdateDestination();
  const { mutateAsync: deleteDestination } = useDeleteDestination();
  const formId = useUniqueFormId();
  const { clearFormChange } = useFormChangeTrackerService();

  useTrackPage(PageTrackingCodes.DESTINATION_ITEM_SETTINGS);

  const onSubmitForm = async (values: ConnectorCardValues) => {
    await updateDestination({
      values,
      destinationId: destination.destinationId,
    });
  };

  const onDelete = useCallback(async () => {
    clearFormChange(formId);
    await deleteDestination({
      connectionsWithDestination,
      destination,
    });
  }, [clearFormChange, connectionsWithDestination, deleteDestination, destination, formId]);

  const modalAdditionalContent = useMemo<React.ReactNode>(() => {
    if (connectionsWithDestination.length === 0) {
      return null;
    }
    return (
      <Box pt="lg">
        <Text size="lg">
          <FormattedMessage
            id="tables.affectedConnectionsOnDeletion"
            values={{ count: connectionsWithDestination.length }}
          />
        </Text>
        <ul>
          {connectionsWithDestination.map((connection) => (
            <li key={connection.connectionId}>{connection.name}</li>
          ))}
        </ul>
      </Box>
    );
  }, [connectionsWithDestination]);

  const onDeleteClick = useDeleteModal("destination", onDelete, modalAdditionalContent);

  return (
    <div className={styles.content}>
      <ConnectorCard
        formType="destination"
        title={<FormattedMessage id="destination.destinationSettings" />}
        isEditMode
        formId={formId}
        availableConnectorDefinitions={[destinationDefinition]}
        selectedConnectorDefinitionSpecification={destinationSpecification}
        selectedConnectorDefinitionId={destinationSpecification.destinationDefinitionId}
        connector={destination}
        reloadConfig={reloadDestination}
        onSubmit={onSubmitForm}
        onDeleteClick={onDeleteClick}
        supportLevel={destinationDefinitionVersion.supportLevel}
      />
    </div>
  );
};
