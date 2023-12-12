import React, { useCallback, useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { useGetSourceFromParams } from "area/connector/utils";
import {
  useConnectionList,
  useSourceDefinitionVersion,
  useGetSourceDefinitionSpecification,
  useSourceDefinition,
  useDeleteSource,
  useInvalidateSource,
  useUpdateSource,
} from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useFormChangeTrackerService, useUniqueFormId } from "hooks/services/FormChangeTracker";
import { useDeleteModal } from "hooks/useDeleteModal";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ConnectorCardValues } from "views/Connector/ConnectorForm";

import styles from "./SourceSettingsPage.module.scss";

export const SourceSettingsPage: React.FC = () => {
  const source = useGetSourceFromParams();
  const connectionList = useConnectionList({ sourceId: [source.sourceId] });
  const connectionsWithSource = useMemo(() => connectionList?.connections ?? [], [connectionList]);
  const sourceDefinition = useSourceDefinition(source.sourceDefinitionId);
  const sourceDefinitionVersion = useSourceDefinitionVersion(source.sourceId);
  const sourceDefinitionSpecification = useGetSourceDefinitionSpecification(source.sourceDefinitionId, source.sourceId);

  const reloadSource = useInvalidateSource(source.sourceId);
  const { mutateAsync: updateSource } = useUpdateSource();
  const { mutateAsync: deleteSource } = useDeleteSource();
  const formId = useUniqueFormId();
  const { clearFormChange } = useFormChangeTrackerService();

  useTrackPage(PageTrackingCodes.SOURCE_ITEM_SETTINGS);

  const onSubmit = async (values: ConnectorCardValues) => {
    await updateSource({
      values,
      sourceId: source.sourceId,
    });
  };

  const onDelete = useCallback(async () => {
    clearFormChange(formId);
    await deleteSource({ connectionsWithSource, source });
  }, [clearFormChange, formId, deleteSource, connectionsWithSource, source]);

  const modalAdditionalContent = useMemo<React.ReactNode>(() => {
    if (connectionsWithSource.length === 0) {
      return null;
    }

    return (
      <Box pt="lg">
        <Text size="lg">
          <FormattedMessage
            id="tables.affectedConnectionsOnDeletion"
            values={{ count: connectionsWithSource.length }}
          />
        </Text>

        <ul>
          {connectionsWithSource.map((connection) => (
            <li key={connection.connectionId}>{`${connection.name}`}</li>
          ))}
        </ul>
      </Box>
    );
  }, [connectionsWithSource]);

  const onDeleteClick = useDeleteModal("source", onDelete, modalAdditionalContent);

  return (
    <div className={styles.content}>
      <ConnectorCard
        formType="source"
        title={<FormattedMessage id="sources.sourceSettings" />}
        isEditMode
        formId={formId}
        availableConnectorDefinitions={[sourceDefinition]}
        selectedConnectorDefinitionSpecification={sourceDefinitionSpecification}
        selectedConnectorDefinitionId={sourceDefinitionSpecification.sourceDefinitionId}
        connector={source}
        reloadConfig={reloadSource}
        onSubmit={onSubmit}
        onDeleteClick={onDeleteClick}
        supportLevel={sourceDefinitionVersion.supportLevel}
      />
    </div>
  );
};
