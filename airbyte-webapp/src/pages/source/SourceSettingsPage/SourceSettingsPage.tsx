import React, { useCallback, useEffect, useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { ConnectionConfiguration } from "core/domain/connection";
import { useTrackPage, PageTrackingCodes } from "hooks/services/Analytics";
import { useFormChangeTrackerService, useUniqueFormId } from "hooks/services/FormChangeTracker";
import { useDeleteSource, useInvalidateSource, useUpdateSource } from "hooks/services/useSourceHook";
import { useDeleteModal } from "hooks/useDeleteModal";
import { useGetSourceDefinitionSpecification } from "services/connector/SourceDefinitionSpecificationService";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

import styles from "./SourceSettingsPage.module.scss";
import { useSourceOverviewContext } from "../SourceOverviewPage/sourceOverviewContext";

export const SourceSettingsPage: React.FC = () => {
  const { mutateAsync: updateSource } = useUpdateSource();
  const { mutateAsync: deleteSource } = useDeleteSource();
  const { setDocumentationPanelOpen } = useDocumentationPanelContext();
  const formId = useUniqueFormId();
  const { clearFormChange } = useFormChangeTrackerService();

  const { source, sourceDefinition, connections } = useSourceOverviewContext();

  useTrackPage(PageTrackingCodes.SOURCE_ITEM_SETTINGS);
  useEffect(() => {
    return () => {
      setDocumentationPanelOpen(false);
    };
  }, [setDocumentationPanelOpen]);

  const sourceDefinitionSpecification = useGetSourceDefinitionSpecification(source.sourceDefinitionId);

  const reloadSource = useInvalidateSource(source.sourceId);

  const onSubmit = async (values: {
    name: string;
    serviceType: string;
    connectionConfiguration?: ConnectionConfiguration;
  }) => {
    await updateSource({
      values,
      sourceId: source.sourceId,
    });
  };

  const onDelete = useCallback(async () => {
    clearFormChange(formId);
    await deleteSource({ connectionsWithSource: connections, source });
  }, [clearFormChange, connections, source, deleteSource, formId]);

  const modalAdditionalContent = useMemo<React.ReactNode>(() => {
    if (connections.length === 0) {
      return null;
    }
    return (
      <p>
        <FormattedMessage id="tables.affectedConnectionsOnDeletion" values={{ count: connections.length }} />
        {connections.map((connection) => (
          <React.Fragment key={connection.connectionId}>
            - <strong>{`${connection.name}\n`}</strong>
          </React.Fragment>
        ))}
      </p>
    );
  }, [connections]);

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
      />
    </div>
  );
};
