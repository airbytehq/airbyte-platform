import isBoolean from "lodash/isBoolean";
import pick from "lodash/pick";
import React, { useCallback, useEffect } from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation } from "react-router-dom";
import { useUnmount } from "react-use";

import {
  FormConnectionFormValues,
  useConnectionValidationSchema,
  useInitialFormValues,
} from "components/connection/ConnectionForm/formConfig";
import { useRefreshSourceSchemaWithConfirmationOnDirty } from "components/connection/ConnectionForm/refreshSourceSchemaWithConfirmationOnDirty";
import { SchemaChangeBackdrop } from "components/connection/ConnectionForm/SchemaChangeBackdrop";
import { SchemaRefreshing } from "components/connection/ConnectionForm/SchemaRefreshing";
import { SyncCatalogCard } from "components/connection/ConnectionForm/SyncCatalogCard";
import { SyncCatalogTable } from "components/connection/ConnectionForm/SyncCatalogTable";
import { UpdateConnectionFormControls } from "components/connection/ConnectionForm/UpdateConnectionFormControls";
import { SchemaError } from "components/connection/CreateConnectionForm/SchemaError";
import { Form } from "components/forms";
import LoadingSchema from "components/LoadingSchema";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message/Message";
import { ScrollParent } from "components/ui/ScrollParent";

import { ConnectionValues, useDestinationDefinitionVersion, useGetStateTypeQuery } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { trackError } from "core/utils/datadog";
import { useConfirmCatalogDiff } from "hooks/connection/useConfirmCatalogDiff";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";
import { ModalResult, useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";

import { ClearDataWarningModal } from "./ClearDataWarningModal";
import styles from "./ConnectionReplicationPage.module.scss";
import { recommendActionOnConnectionUpdate } from "./connectionUpdateHelpers";
import { RecommendRefreshModal } from "./RecommendRefreshModal";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";

const SchemaChangeMessage: React.FC = () => {
  const { isDirty } = useFormState<FormConnectionFormValues>();
  const refreshWithConfirm = useRefreshSourceSchemaWithConfirmationOnDirty(isDirty);

  const { refreshSchema } = useConnectionFormService();
  const { connection, schemaHasBeenRefreshed, schemaRefreshing } = useConnectionEditService();
  const { hasNonBreakingSchemaChange, hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);

  if (schemaHasBeenRefreshed) {
    return null;
  }

  if (hasNonBreakingSchemaChange && !schemaRefreshing) {
    return (
      <Message
        type="info"
        text={<FormattedMessage id="connection.schemaChange.nonBreaking" />}
        actionBtnText={<FormattedMessage id="connection.schemaChange.reviewAction" />}
        onAction={refreshSchema}
        data-testid="schemaChangesDetected"
      />
    );
  }

  if (hasBreakingSchemaChange && !schemaRefreshing) {
    return (
      <Message
        type="error"
        text={<FormattedMessage id="connection.schemaChange.breaking" />}
        actionBtnText={<FormattedMessage id="connection.schemaChange.reviewAction" />}
        onAction={refreshWithConfirm}
        data-testid="schemaChangesDetected"
      />
    );
  }
  return null;
};

const relevantConnectionKeys = [
  "syncCatalog" as const,
  "namespaceDefinition" as const,
  "namespaceFormat" as const,
  "prefix" as const,
];

export const ConnectionReplicationPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_REPLICATION);
  const isSyncCatalogV2Enabled = useExperiment("connection.syncCatalogV2");
  const isSyncCatalogV2Allowed = useFeature(FeatureItem.SyncCatalogV2);
  const useSyncCatalogV2 = isSyncCatalogV2Enabled && isSyncCatalogV2Allowed;
  const { trackSchemaEdit } = useAnalyticsTrackFunctions();

  const getStateType = useGetStateTypeQuery();

  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { openModal } = useModalService();

  const { connection, schemaRefreshing, updateConnection, discardRefreshedSchema } = useConnectionEditService();
  const { schemaError, setSubmitError, refreshSchema, mode } = useConnectionFormService();
  const initialValues = useInitialFormValues(connection, mode);

  const { supportsRefreshes: destinationSupportsRefreshes } = useDestinationDefinitionVersion(
    connection.destination.destinationId
  );

  type RelevantConnectionValues = Pick<ConnectionValues, (typeof relevantConnectionKeys)[number]>;
  const validationSchema = useConnectionValidationSchema().pick(relevantConnectionKeys);

  const saveConnection = useCallback(
    async (values: Partial<ConnectionValues>, skipReset: boolean) => {
      await updateConnection({
        connectionId: connection.connectionId,
        ...(pick(values, relevantConnectionKeys) as RelevantConnectionValues),
        // required to update the catalog if a schema change w/transforms exists
        sourceCatalogId: connection.catalogId,
        skipReset,
      });
    },
    [connection, updateConnection]
  );

  const onFormSubmit = useCallback(
    async (values: RelevantConnectionValues) => {
      setSubmitError(null);

      /**
       * - determine whether to recommend a reset / refresh
       * - if yes, give the user the option to opt out
       * - save the connection (unless the user cancels the action via the recommendation modal)
       */

      const { shouldTrackAction, shouldRecommendRefresh } = recommendActionOnConnectionUpdate({
        catalogDiff: connection.catalogDiff,
        formSyncCatalog: values.syncCatalog,
        storedSyncCatalog: connection.syncCatalog,
      });

      // handler for modal -- saves connection w/ modal result taken into account
      async function handleModalResult(
        result: ModalResult<boolean>,
        values: RelevantConnectionValues,
        saveConnection: (values: RelevantConnectionValues, skipReset: boolean) => Promise<void>
      ) {
        if (result.type === "completed" && isBoolean(result.reason)) {
          // Save the connection taking into account the correct skipReset value from the dialog choice.
          return await saveConnection(values, !result.reason /* skipReset */);
        }
        // We don't want to set saved to true or schema has been refreshed to false.
        return Promise.reject();
      }

      try {
        if (shouldRecommendRefresh) {
          // if the destination doesn't support refreshes, we need to clear data instead
          if (!destinationSupportsRefreshes) {
            // recommend clearing data
            const stateType = await getStateType(connection.connectionId);
            const result = await openModal<boolean>({
              title: formatMessage({ id: "connection.clearDataRecommended" }),
              size: "md",
              content: (props) => <ClearDataWarningModal {...props} stateType={stateType} />,
            });
            await handleModalResult(result, values, saveConnection);
          } else {
            // recommend refreshing data
            const result = await openModal<boolean>({
              title: formatMessage({ id: "connection.refreshDataRecommended" }),
              size: "md",
              content: ({ onCancel, onComplete }) => (
                <RecommendRefreshModal onCancel={onCancel} onComplete={onComplete} />
              ),
            });
            await handleModalResult(result, values, saveConnection);
          }
        } else {
          // do not recommend a refresh or clearing data, just save
          await saveConnection(values, true /* skipReset */);
        }

        /* analytics */
        if (shouldTrackAction) {
          trackSchemaEdit(connection);
        }

        return Promise.resolve();
      } catch (e) {
        setSubmitError(e);
        throw new Error(e); // we _do_ need this to throw in order for isSubmitSuccessful to be false
      }
    },
    [
      connection,
      setSubmitError,
      destinationSupportsRefreshes,
      getStateType,
      openModal,
      formatMessage,
      saveConnection,
      trackSchemaEdit,
    ]
  );

  useConfirmCatalogDiff();

  useUnmount(() => {
    discardRefreshedSchema();
  });

  const { state } = useLocation();
  useEffect(() => {
    if (typeof state === "object" && state && "triggerRefreshSchema" in state && state.triggerRefreshSchema) {
      refreshSchema();
    }
  }, [refreshSchema, state]);

  const onSuccess = () => {
    registerNotification({
      id: "connection_settings_change_success",
      text: formatMessage({ id: "form.changesSaved" }),
      type: "success",
    });
  };

  const onError = (e: Error) => {
    trackError(e, { connectionName: connection.name });
    registerNotification({
      id: "connection_settings_change_error",
      text: formatMessage({ id: "connection.updateFailed" }),
      type: "error",
    });
  };

  const newSyncCatalogV2Form = connection && (
    <ScrollParent props={{ className: styles.scrollableContainer }}>
      <Form<RelevantConnectionValues>
        defaultValues={initialValues}
        reinitializeDefaultValues
        schema={validationSchema}
        onSubmit={onFormSubmit}
        trackDirtyChanges
        disabled={mode === "readonly"}
        onError={onError}
        onSuccess={onSuccess}
      >
        <FlexContainer direction="column">
          <SchemaChangeMessage />
          <SchemaChangeBackdrop>
            <SchemaRefreshing>
              <Card noPadding title={formatMessage({ id: "connection.schema" })}>
                <Box mb="xl" data-testid="catalog-tree-table-body">
                  <SyncCatalogTable />
                </Box>
              </Card>
            </SchemaRefreshing>
          </SchemaChangeBackdrop>
        </FlexContainer>
      </Form>
    </ScrollParent>
  );

  const oldSyncCatalogForm =
    schemaError && !schemaRefreshing ? (
      <ScrollParent>
        <SchemaError schemaError={schemaError} refreshSchema={refreshSchema} />
      </ScrollParent>
    ) : !schemaRefreshing && connection ? (
      <Form<RelevantConnectionValues>
        defaultValues={initialValues}
        schema={validationSchema}
        onSubmit={onFormSubmit}
        trackDirtyChanges
      >
        <div className={styles.formContainer}>
          <ScrollParent props={{ className: styles.scrollableContainer }}>
            <FlexContainer direction="column">
              <SchemaChangeMessage />
              <SchemaChangeBackdrop>
                <SyncCatalogCard />
              </SchemaChangeBackdrop>
            </FlexContainer>
          </ScrollParent>
          <Box pb="xl" px="xl" pt="lg" className={styles.editControlsContainer}>
            <UpdateConnectionFormControls onCancel={discardRefreshedSchema} />
          </Box>
        </div>
      </Form>
    ) : (
      <LoadingSchema />
    );

  return <div className={styles.container}>{useSyncCatalogV2 ? newSyncCatalogV2Form : oldSyncCatalogForm}</div>;
};
