import isBoolean from "lodash/isBoolean";
import React, { useCallback, useEffect, useState } from "react";
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
import { ScrollableContainer } from "components/ScrollableContainer";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message/Message";

import { ConnectionValues, useDestinationDefinitionVersion, useGetStateTypeQuery } from "core/api";
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
  const [scrollElement, setScrollElement] = useState<HTMLDivElement | undefined>();
  const isSyncCatalogV2Enabled = useExperiment("connection.syncCatalogV2", false);
  const { trackSchemaEdit } = useAnalyticsTrackFunctions();

  const getStateType = useGetStateTypeQuery();

  const { formatMessage } = useIntl();
  const { openModal } = useModalService();

  const { connection, schemaRefreshing, updateConnection, discardRefreshedSchema } = useConnectionEditService();
  const { schemaError, setSubmitError, refreshSchema, mode } = useConnectionFormService();
  const initialValues = useInitialFormValues(connection, mode);

  const { supportsRefreshes: destinationSupportsRefreshes } = useDestinationDefinitionVersion(
    connection.destination.destinationId
  );

  const validationSchema = useConnectionValidationSchema().pick(["syncCatalog"]);

  const saveConnection = useCallback(
    async (values: Partial<ConnectionValues>, skipReset: boolean) => {
      await updateConnection({
        syncCatalog: values.syncCatalog,
        connectionId: connection.connectionId,
        // required to update the catalog if a schema change w/transforms exists
        sourceCatalogId: connection.catalogId,
        skipReset,
      });
    },
    [connection, updateConnection]
  );

  const onFormSubmit = useCallback(
    async (values: Pick<FormConnectionFormValues, "syncCatalog">) => {
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
        values: Pick<FormConnectionFormValues, "syncCatalog">,
        saveConnection: (values: Pick<ConnectionValues, "syncCatalog">, skipReset: boolean) => Promise<void>
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

  const setScrollableContainer = (ref: HTMLDivElement | null) => {
    if (ref === null) {
      return;
    }
    setScrollElement(ref);
  };

  const newSyncCatalogV2Form = connection && (
    <ScrollableContainer ref={setScrollableContainer} className={styles.scrollableContainer}>
      <Form<Pick<FormConnectionFormValues, "syncCatalog">>
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
            <SchemaRefreshing>
              <Card noPadding title={formatMessage({ id: "connection.schema" })}>
                <Box mb="xl" data-testid="catalog-tree-table-body">
                  <SyncCatalogTable scrollParentContainer={scrollElement} />
                </Box>
              </Card>
            </SchemaRefreshing>
          </SchemaChangeBackdrop>
        </FlexContainer>
      </Form>
    </ScrollableContainer>
  );

  const oldSyncCatalogForm =
    schemaError && !schemaRefreshing ? (
      <ScrollableContainer>
        <SchemaError schemaError={schemaError} refreshSchema={refreshSchema} />
      </ScrollableContainer>
    ) : !schemaRefreshing && connection ? (
      <Form<Pick<FormConnectionFormValues, "syncCatalog">>
        defaultValues={initialValues}
        schema={validationSchema}
        onSubmit={onFormSubmit}
        trackDirtyChanges
      >
        <div className={styles.formContainer}>
          <ScrollableContainer ref={setScrollableContainer}>
            <FlexContainer direction="column">
              <SchemaChangeMessage />
              <SchemaChangeBackdrop>
                <SyncCatalogCard scrollParentContainer={scrollElement} />
              </SchemaChangeBackdrop>
            </FlexContainer>
          </ScrollableContainer>
          <Box pb="xl" px="xl" pt="lg" className={styles.editControlsContainer}>
            <UpdateConnectionFormControls onCancel={discardRefreshedSchema} />
          </Box>
        </div>
      </Form>
    ) : (
      <LoadingSchema />
    );

  return <div className={styles.container}>{isSyncCatalogV2Enabled ? newSyncCatalogV2Form : oldSyncCatalogForm}</div>;
};
