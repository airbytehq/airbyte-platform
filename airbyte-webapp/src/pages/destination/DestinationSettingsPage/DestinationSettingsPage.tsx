import React, { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useEffectOnce } from "react-use";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { useGetDestinationFromParams } from "area/connector/utils";
import {
  useDestinationDefinitionVersion,
  useGetDestinationDefinitionSpecification,
  useDestinationDefinition,
  useDeleteDestination,
  useInvalidateDestination,
  useUpdateDestination,
} from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { trackTiming } from "core/utils/datadog";
import { useFormChangeTrackerService, useUniqueFormId } from "hooks/services/FormChangeTracker";
import { useDeleteModal } from "hooks/useDeleteModal";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ConnectorCardValues } from "views/Connector/ConnectorForm/types";

import styles from "./DestinationSettings.module.scss";

export const DestinationSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const destination = useGetDestinationFromParams();
  const destinationDefinition = useDestinationDefinition(destination.destinationDefinitionId);
  const destinationDefinitionVersion = useDestinationDefinitionVersion(destination.destinationId);
  const destinationSpecification = useGetDestinationDefinitionSpecification(destination.destinationId);
  const reloadDestination = useInvalidateDestination(destination.destinationId);
  const { mutateAsync: updateDestination } = useUpdateDestination();
  const { mutateAsync: deleteDestination } = useDeleteDestination();
  const formId = useUniqueFormId();
  const { clearFormChange } = useFormChangeTrackerService();

  useTrackPage(PageTrackingCodes.DESTINATION_ITEM_SETTINGS);

  useEffectOnce(() => {
    trackTiming("DestinationSettingsPage");
  });

  const onSubmitForm = async (values: ConnectorCardValues) => {
    await updateDestination({
      values,
      destinationId: destination.destinationId,
    });
  };

  const onDelete = useCallback(async () => {
    clearFormChange(formId);
    await deleteDestination({
      destination,
    });
  }, [clearFormChange, deleteDestination, destination, formId]);

  const onDeleteClick = useDeleteModal(
    "destination",
    onDelete,
    <Box mt="md">
      <Text bold>
        <FormattedMessage id="tables.deleteAssociatedConnectionsWarning" />
      </Text>
    </Box>,
    destination.name
  );

  return (
    <div className={styles.content}>
      <ConnectorCard
        formType="destination"
        title={formatMessage({ id: "destination.destinationSettings" })}
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
