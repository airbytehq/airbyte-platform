import { useQueryClient } from "@tanstack/react-query";
import React, { Suspense, useCallback, useEffect } from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Form } from "components/forms";
import LoadingSchema from "components/LoadingSchema";

import { useGetDestinationFromSearchParams, useGetSourceFromSearchParams } from "area/connector/utils";
import { connectionsKeys, useCreateConnection, useDiscoverSchema } from "core/api";
import { ConnectionScheduleType } from "core/api/types/AirbyteClient";
import {
  ConnectionFormServiceProvider,
  useConnectionFormService,
} from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperimentContext } from "hooks/services/Experiment";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./CreateConnectionForm.module.scss";
import { SchemaError } from "./SchemaError";
import { SimplifiedConnectionConfiguration } from "./SimplifiedConnectionCreation/SimplifiedConnectionConfiguration";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";
import { ScrollableContainer } from "../../ScrollableContainer";
import {
  FormConnectionFormValues,
  useConnectionValidationSchema,
  useInitialFormValues,
} from "../ConnectionForm/formConfig";

export const CREATE_CONNECTION_FORM_ID = "create-connection-form";

const CreateConnectionFormInner: React.FC = () => {
  const navigate = useNavigate();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const { mutateAsync: createConnection } = useCreateConnection();
  const { connection, mode, setSubmitError } = useConnectionFormService();
  const initialValues = useInitialFormValues(connection, mode);
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  useExperimentContext("source-definition", connection.source?.sourceDefinitionId);
  const queryClient = useQueryClient();

  const validationSchema = useConnectionValidationSchema();

  const onSubmit = useCallback(
    async ({ ...restFormValues }: FormConnectionFormValues) => {
      try {
        const createdConnection = await createConnection({
          values: {
            ...restFormValues,
          },
          source: connection.source,
          destination: connection.destination,
          sourceDefinition: {
            sourceDefinitionId: connection.source?.sourceDefinitionId ?? "",
          },
          destinationDefinition: {
            name: connection.destination?.name ?? "",
            destinationDefinitionId: connection.destination?.destinationDefinitionId ?? "",
          },
          sourceCatalogId: connection.catalogId,
        });
        clearAllFormChanges();
        navigate(`../../connections/${createdConnection.connectionId}`);

        const willSyncAfterCreation = restFormValues.scheduleType === ConnectionScheduleType.basic;
        if (willSyncAfterCreation) {
          registerNotification({
            id: "onboarding.firstSyncStarted",
            text: formatMessage({ id: "onboarding.firstSyncStarted" }),
            type: "success",
          });

          // 2s is above the 90th percentile of the time it takes for a sync job to be created after connection is created
          // on 2024-10-16 the 90th percentile is 1,842ms for connections created last 30 days
          setTimeout(() => {
            queryClient.invalidateQueries(connectionsKeys.statuses([createdConnection.connectionId]));
          }, 2000);
        }
      } catch (e) {
        setSubmitError(e);
      }
    },
    [
      clearAllFormChanges,
      connection.catalogId,
      connection.destination,
      connection.source,
      createConnection,
      navigate,
      setSubmitError,
      registerNotification,
      formatMessage,
      queryClient,
    ]
  );

  return (
    <div className={styles.container}>
      <Suspense fallback={<LoadingSchema />}>
        <Form<FormConnectionFormValues>
          defaultValues={initialValues}
          schema={validationSchema}
          onSubmit={onSubmit}
          trackDirtyChanges
          formTrackerId={CREATE_CONNECTION_FORM_ID}
        >
          <div className={styles.formContainer}>
            <SimplifiedConnectionConfiguration />
          </div>
        </Form>
      </Suspense>
    </div>
  );
};

export const CreateConnectionForm: React.FC = () => {
  const source = useGetSourceFromSearchParams();
  const destination = useGetDestinationFromSearchParams();
  const { trackFailure } = useAnalyticsTrackFunctions();

  const { schema, isLoading, schemaErrorStatus, catalogId, onDiscoverSchema } = useDiscoverSchema(
    source.sourceId,
    true
  );

  useEffect(() => {
    if (schemaErrorStatus) {
      trackFailure(source, destination, schemaErrorStatus);
    }
    // we need to track the schemaErrorStatus changes only
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [schemaErrorStatus]);

  if (schemaErrorStatus) {
    return (
      <ScrollableContainer>
        <SchemaError schemaError={schemaErrorStatus} refreshSchema={onDiscoverSchema} />
      </ScrollableContainer>
    );
  }
  if (!schema) {
    return <LoadingSchema />;
  }

  const partialConnection = {
    syncCatalog: schema,
    destination,
    source,
    catalogId,
  };

  return (
    <ConnectionFormServiceProvider
      connection={partialConnection}
      mode="create"
      refreshSchema={onDiscoverSchema}
      schemaError={schemaErrorStatus}
    >
      {isLoading ? <LoadingSchema /> : <CreateConnectionFormInner />}
    </ConnectionFormServiceProvider>
  );
};
