import { useQueryClient } from "@tanstack/react-query";
import React, { Suspense, useCallback, useState } from "react";
import { UseFormReturn } from "react-hook-form";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Form } from "components/ui/forms";
import { ExternalLink } from "components/ui/Link";
import LoadingSchema from "components/ui/LoadingSchema";
import { ScrollParent } from "components/ui/ScrollParent";

import {
  ConnectionFormServiceProvider,
  useConnectionFormService,
} from "area/connection/utils/ConnectionForm/ConnectionFormService";
import { useGetDestinationFromSearchParams, useGetSourceFromSearchParams } from "area/connector/utils";
import {
  connectionsKeys,
  HttpError,
  HttpProblem,
  useCreateConnection,
  useDestinationDefinitionVersion,
  useDiscoverSchemaQuery,
  useDiscoverSourceSchemaMutation,
} from "core/api";
import { AirbyteCatalog, ConnectionScheduleType } from "core/api/types/AirbyteClient";
import { useExperiment, useExperimentContext } from "core/services/Experiment";
import { useFormChangeTrackerService } from "core/services/FormChangeTracker";
import { useNotificationService } from "core/services/Notification";
import { FormModeProvider, useFormMode } from "core/services/ui/FormModeContext";
import { useCheckSubHourlySchedule } from "core/utils/checkSubHourlySchedule";
import { useProFeaturesModal } from "core/utils/useProFeaturesModal";

import styles from "./CreateConnectionForm.module.scss";
import { SchemaError } from "./SchemaError";
import { SimplifiedConnectionConfiguration } from "./SimplifiedConnectionCreation/SimplifiedConnectionConfiguration";
import { I18N_KEY_UNDER_ONE_HOUR_NOT_ALLOWED } from "./SimplifiedConnectionCreation/SimplifiedConnectionScheduleFormField";
import { FormConnectionFormValues, useInitialFormValues } from "../ConnectionForm/formConfig";
import { useConnectionValidationZodSchema } from "../ConnectionForm/schemas/connectionSchema";

export const CREATE_CONNECTION_FORM_ID = "create-connection-form";

const CreateConnectionFormInner: React.FC = () => {
  const navigate = useNavigate();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const { mutateAsync: createConnection } = useCreateConnection();
  const { connection, setSubmitError } = useConnectionFormService();
  const { mode } = useFormMode();
  const destinationDefinitionVersion = useDestinationDefinitionVersion(connection.destination.destinationId);
  const initialValues = useInitialFormValues(connection, mode, destinationDefinitionVersion.supportsFileTransfer);
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const { formatMessage } = useIntl();
  useExperimentContext("source-definition", connection.source?.sourceDefinitionId);
  const queryClient = useQueryClient();
  const checkSubHourlySchedule = useCheckSubHourlySchedule();
  const { showProFeatureModalIfNeeded } = useProFeaturesModal("sub-hourly-sync");

  const zodValidationSchema = useConnectionValidationZodSchema();

  const onSubmit = useCallback(
    async ({ ...restFormValues }: FormConnectionFormValues) => {
      try {
        // Check for sub-hourly schedule and show modal if needed
        const isSubHourly = await checkSubHourlySchedule({
          scheduleType: restFormValues.scheduleType,
          scheduleData: restFormValues.scheduleData,
        });

        if (isSubHourly) {
          await showProFeatureModalIfNeeded();
        }

        // Proceed with connection creation
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
      } catch (error) {
        if (
          !(error instanceof HttpError && HttpProblem.isType(error, "error:connection-conflicting-destination-stream"))
        ) {
          setSubmitError(error);
        }

        // Needs to be re-thrown so react-hook-form can handle the error. We should probably get rid of setSubmitError
        // entirely and just use react-hook-form to handle errors.
        throw error;
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
      checkSubHourlySchedule,
      showProFeatureModalIfNeeded,
    ]
  );

  const onError = useCallback(
    (error: Error, _values: FormConnectionFormValues, methods: UseFormReturn<FormConnectionFormValues>) => {
      if (error instanceof HttpError && HttpProblem.isType(error, "error:cron-validation/under-one-hour-not-allowed")) {
        methods.setError("scheduleData.cron.cronExpression", {
          message: I18N_KEY_UNDER_ONE_HOUR_NOT_ALLOWED,
        });
      }
      if (error instanceof HttpError && HttpProblem.isType(error, "error:connection-conflicting-destination-stream")) {
        registerNotification({
          id: "connection.conflictingDestinationStream",
          text: formatMessage(
            {
              id: "connectionForm.conflictingDestinationStream",
            },
            {
              stream: error.response?.data?.streams?.[0]?.streamName,
              moreCount:
                (error.response?.data?.streams?.length ?? 0) > 1 ? (error.response?.data?.streams?.length ?? 1) - 1 : 0,
              lnk: (...lnk: React.ReactNode[]) => (
                <ExternalLink href={error.response.documentationUrl ?? ""}>{lnk}</ExternalLink>
              ),
            }
          ),
          actionBtnText: formatMessage({ id: "connectionForm.conflictingDestinationStream.action" }),
          onAction: () => {
            // Generate a random 6-character string with underscore suffix
            const randomPrefix = `${Math.random().toString(36).substring(2, 8)}_`;
            // Update the form values with the new prefix and resubmit
            methods.setValue("prefix", randomPrefix);
            unregisterNotificationById("connection.conflictingDestinationStream");
            methods.handleSubmit(onSubmit)();
          },
          type: "error",
        });
      }
    },
    [formatMessage, onSubmit, registerNotification, unregisterNotificationById]
  );

  return (
    <div className={styles.container}>
      <Suspense fallback={<LoadingSchema />}>
        <Form<FormConnectionFormValues>
          defaultValues={initialValues}
          zodSchema={zodValidationSchema}
          onSubmit={onSubmit}
          onError={onError}
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
  const asyncSchemaDiscoveryEnabled = useExperiment("asyncSchemaDiscovery");

  const { data, error, isFetching, refetch } = useDiscoverSchemaQuery(source, { useErrorBoundary: false });
  const { mutateAsync: discoverSchemaMutation, isLoading: isMutationLoading } = useDiscoverSourceSchemaMutation(source);
  const [refreshedSchema, setRefreshedSchema] = useState<{ catalog: AirbyteCatalog; catalogId: string } | null>(null);

  const schema = refreshedSchema?.catalog ?? data?.catalog;
  const catalogId = refreshedSchema?.catalogId ?? data?.catalogId;
  const isLoading = isFetching || isMutationLoading;

  const refreshSchema = useCallback(async () => {
    if (asyncSchemaDiscoveryEnabled) {
      const result = await discoverSchemaMutation();
      setRefreshedSchema({ catalog: result.catalog, catalogId: result.catalogId });
    } else {
      await refetch();
    }
  }, [asyncSchemaDiscoveryEnabled, discoverSchemaMutation, refetch]);

  if (error && error instanceof Error) {
    return (
      <ScrollParent>
        <SchemaError schemaError={error} refreshSchema={refreshSchema} />
      </ScrollParent>
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
    <FormModeProvider mode="create">
      <ConnectionFormServiceProvider
        connection={partialConnection}
        refreshSchema={refreshSchema}
        schemaError={error instanceof Error ? error : null}
      >
        {isLoading ? <LoadingSchema /> : <CreateConnectionFormInner key={catalogId} />}
      </ConnectionFormServiceProvider>
    </FormModeProvider>
  );
};
