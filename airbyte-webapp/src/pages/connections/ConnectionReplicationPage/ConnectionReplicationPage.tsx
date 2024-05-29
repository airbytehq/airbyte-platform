import isBoolean from "lodash/isBoolean";
import React, { useCallback, useEffect } from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation } from "react-router-dom";
import { useUnmount } from "react-use";

import { ConnectionConfigurationCard } from "components/connection/ConnectionForm/ConnectionConfigurationCard";
import {
  FormConnectionFormValues,
  useConnectionValidationSchema,
} from "components/connection/ConnectionForm/formConfig";
import { useRefreshSourceSchemaWithConfirmationOnDirty } from "components/connection/ConnectionForm/refreshSourceSchemaWithConfirmationOnDirty";
import { SchemaChangeBackdrop } from "components/connection/ConnectionForm/SchemaChangeBackdrop";
import { SyncCatalogCard } from "components/connection/ConnectionForm/SyncCatalogCard";
import { SyncCatalogCardNext } from "components/connection/ConnectionForm/SyncCatalogCardNext";
import { UpdateConnectionFormControls } from "components/connection/ConnectionForm/UpdateConnectionFormControls";
import { SchemaError } from "components/connection/CreateConnectionForm/SchemaError";
import { Form } from "components/forms";
import LoadingSchema from "components/LoadingSchema";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message/Message";

import { ConnectionValues, useDestinationDefinition, useGetStateTypeQuery } from "core/api";
import { WebBackendConnectionRead, WebBackendConnectionUpdate } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useConfirmCatalogDiff } from "hooks/connection/useConfirmCatalogDiff";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";
import { ModalResult, useModalService } from "hooks/services/Modal";

import { ClearDataWarningModal } from "./ClearDataWarningModal";
import styles from "./ConnectionReplicationPage.module.scss";
import { recommendActionOnConnectionUpdate } from "./connectionUpdateHelpers";
import { RecommendRefreshModal } from "./RecommendRefreshModal";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";
import { SchemaRefreshing } from "../../../components/connection/ConnectionForm/SchemaRefreshing";

const toWebBackendConnectionUpdate = (connection: WebBackendConnectionRead): WebBackendConnectionUpdate => ({
  name: connection.name,
  connectionId: connection.connectionId,
  namespaceDefinition: connection.namespaceDefinition,
  namespaceFormat: connection.namespaceFormat,
  prefix: connection.prefix,
  syncCatalog: connection.syncCatalog,
  scheduleData: connection.scheduleData,
  scheduleType: connection.scheduleType,
  status: connection.status,
  resourceRequirements: connection.resourceRequirements,
  operations: connection.operations,
  sourceCatalogId: connection.catalogId,
});

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

export const ConnectionReplicationPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_REPLICATION);
  const isSyncCatalogV2Enabled = useExperiment("connection.syncCatalogV2", false);
  const { trackSchemaEdit } = useAnalyticsTrackFunctions();
  const isRefreshConnectionEnabled = useExperiment("platform.activate-refreshes", false);

  const getStateType = useGetStateTypeQuery();

  const { formatMessage } = useIntl();
  const { openModal } = useModalService();

  const { connection, schemaRefreshing, updateConnection, discardRefreshedSchema } = useConnectionEditService();
  const { initialValues, schemaError, setSubmitError, refreshSchema, mode } = useConnectionFormService();
  const { supportRefreshes: destinationSupportsRefreshes } = useDestinationDefinition(
    connection.destination.destinationDefinitionId
  );

  const validationSchema = useConnectionValidationSchema();

  const saveConnection = useCallback(
    async (values: ConnectionValues, skipReset: boolean) => {
      const connectionAsUpdate = toWebBackendConnectionUpdate(connection);

      await updateConnection({
        ...connectionAsUpdate,
        ...values,
        connectionId: connection.connectionId,
        skipReset,
      });
    },
    [connection, updateConnection]
  );

  const onFormSubmit = useCallback(
    async (values: FormConnectionFormValues) => {
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
        values: FormConnectionFormValues,
        saveConnection: (values: ConnectionValues, skipReset: boolean) => Promise<void>
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
          if (
            !isRefreshConnectionEnabled ||
            (isRefreshConnectionEnabled && !destinationSupportsRefreshes) // if the destination doesn't support refreshes, we need to clear data instead
          ) {
            // recommend clearing data
            const stateType = await getStateType(connection.connectionId);
            const result = await openModal<boolean>({
              title: formatMessage({ id: "connection.streamConfigurationChanged" }),
              size: "md",
              content: (props) => <ClearDataWarningModal {...props} stateType={stateType} />,
            });
            await handleModalResult(result, values, saveConnection);
          } else {
            // recommend refreshing data
            const result = await openModal<boolean>({
              title: formatMessage({ id: "connection.streamConfigurationChanged" }),
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
      isRefreshConnectionEnabled,
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

  const isSimplifiedCreation = useExperiment("connection.simplifiedCreation", true);

  const newSyncCatalogV2Form = connection && (
    <Form<FormConnectionFormValues>
      defaultValues={initialValues}
      reinitializeDefaultValues
      schema={validationSchema}
      onSubmit={onFormSubmit}
      trackDirtyChanges
      disabled={mode === "readonly"}
    >
      <FlexContainer direction="column">
        <SchemaChangeMessage />
        <SchemaChangeBackdrop>
          {!isSimplifiedCreation && <ConnectionConfigurationCard />}
          <SchemaRefreshing>
            <SyncCatalogCardNext />
          </SchemaRefreshing>
        </SchemaChangeBackdrop>
      </FlexContainer>
    </Form>
  );

  const oldSyncCatalogForm =
    schemaError && !schemaRefreshing ? (
      <SchemaError schemaError={schemaError} refreshSchema={refreshSchema} />
    ) : !schemaRefreshing && connection ? (
      <Form<FormConnectionFormValues>
        defaultValues={initialValues}
        schema={validationSchema}
        onSubmit={onFormSubmit}
        trackDirtyChanges
        disabled={mode === "readonly"}
      >
        <FlexContainer direction="column">
          <SchemaChangeMessage />
          <SchemaChangeBackdrop>
            {!isSimplifiedCreation && <ConnectionConfigurationCard />}
            <SyncCatalogCard />
            <div className={styles.editControlsContainer}>
              <UpdateConnectionFormControls onCancel={discardRefreshedSchema} />
            </div>
          </SchemaChangeBackdrop>
        </FlexContainer>
      </Form>
    ) : (
      <LoadingSchema />
    );

  return (
    <FlexContainer direction="column" className={styles.content}>
      {isSyncCatalogV2Enabled ? newSyncCatalogV2Form : oldSyncCatalogForm}
    </FlexContainer>
  );
};
