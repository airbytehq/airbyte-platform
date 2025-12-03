import React, { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useEffectOnce } from "react-use";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { useGetSourceFromParams } from "area/connector/utils";
import {
  useSourceDefinitionVersion,
  useGetSourceDefinitionSpecification,
  useSourceDefinition,
  useDeleteSource,
  useInvalidateSource,
  useUpdateSource,
} from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { trackTiming } from "core/utils/datadog";
import { useFormChangeTrackerService, useUniqueFormId } from "hooks/services/FormChangeTracker";
import { useDeleteModal } from "hooks/useDeleteModal";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ConnectorCardValues } from "views/Connector/ConnectorForm";

import styles from "./SourceSettingsPage.module.scss";

export const SourceSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const source = useGetSourceFromParams();
  const sourceDefinition = useSourceDefinition(source.sourceDefinitionId);
  const sourceDefinitionVersion = useSourceDefinitionVersion(source.sourceId);
  const sourceDefinitionSpecification = useGetSourceDefinitionSpecification(source.sourceId);

  const reloadSource = useInvalidateSource(source.sourceId);
  const { mutateAsync: updateSource } = useUpdateSource();
  const { mutateAsync: deleteSource } = useDeleteSource();
  const formId = useUniqueFormId();
  const { clearFormChange } = useFormChangeTrackerService();

  useTrackPage(PageTrackingCodes.SOURCE_ITEM_SETTINGS);

  useEffectOnce(() => {
    trackTiming("SourceSettingsPage");
  });

  const onSubmit = async (values: ConnectorCardValues) => {
    await updateSource({
      values,
      sourceId: source.sourceId,
    });
  };

  const onDelete = useCallback(async () => {
    clearFormChange(formId);
    await deleteSource({ source });
  }, [clearFormChange, formId, deleteSource, source]);

  const onDeleteClick = useDeleteModal(
    "source",
    onDelete,
    <Box mt="md">
      <Text bold>
        <FormattedMessage id="tables.deleteAssociatedConnectionsWarning" />
      </Text>
    </Box>,
    source.name
  );

  return (
    <div className={styles.content}>
      <ConnectorCard
        formType="source"
        title={formatMessage({ id: "sources.sourceSettings" })}
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
