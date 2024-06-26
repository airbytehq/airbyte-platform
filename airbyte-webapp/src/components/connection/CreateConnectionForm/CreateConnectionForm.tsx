import React, { Suspense, useCallback, useEffect } from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Form } from "components/forms";
import LoadingSchema from "components/LoadingSchema";
import { FlexContainer } from "components/ui/Flex";

import { useGetDestinationFromSearchParams, useGetSourceFromSearchParams } from "area/connector/utils";
import { useCreateConnection, useDiscoverSchema } from "core/api";
import { ConnectionScheduleType } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import {
  ConnectionFormServiceProvider,
  useConnectionFormService,
} from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment, useExperimentContext } from "hooks/services/Experiment";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useNotificationService } from "hooks/services/Notification";

import { ConnectionNameCard } from "./ConnectionNameCard";
import styles from "./CreateConnectionForm.module.scss";
import { DataResidencyCard } from "./DataResidencyCard";
import { SchemaError } from "./SchemaError";
import { SimplifiedConnectionConfiguration } from "./SimplifiedConnectionCreation/SimplifiedConnectionConfiguration";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";
import { ConnectionConfigurationCard } from "../ConnectionForm/ConnectionConfigurationCard";
import { CreateConnectionFormControls } from "../ConnectionForm/CreateConnectionFormControls";
import { FormConnectionFormValues, useConnectionValidationSchema } from "../ConnectionForm/formConfig";
import { SyncCatalogCard } from "../ConnectionForm/SyncCatalogCard";
import { SyncCatalogCardNext } from "../ConnectionForm/SyncCatalogCardNext";

export const CREATE_CONNECTION_FORM_ID = "create-connection-form";

const CreateConnectionFormInner: React.FC = () => {
  const isSyncCatalogV2Enabled = useExperiment("connection.syncCatalogV2", false);
  const navigate = useNavigate();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const { mutateAsync: createConnection } = useCreateConnection();
  const { connection, initialValues, setSubmitError } = useConnectionFormService();
  const canEditDataGeographies = useFeature(FeatureItem.AllowChangeDataGeographies);
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  useExperimentContext("source-definition", connection.source?.sourceDefinitionId);

  const validationSchema = useConnectionValidationSchema();

  const isSimplifiedCreation = useExperiment("connection.simplifiedCreation", true);

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
        if (isSimplifiedCreation && willSyncAfterCreation) {
          registerNotification({
            id: "onboarding.firstSyncStarted",
            text: formatMessage({ id: "onboarding.firstSyncStarted" }),
            type: "success",
          });
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
      isSimplifiedCreation,
      registerNotification,
      formatMessage,
    ]
  );

  return (
    <Suspense fallback={<LoadingSchema />}>
      <Form<FormConnectionFormValues>
        defaultValues={initialValues}
        schema={validationSchema}
        onSubmit={onSubmit}
        trackDirtyChanges
        formTrackerId={CREATE_CONNECTION_FORM_ID}
      >
        <FlexContainer direction="column" className={styles.formContainer}>
          {isSimplifiedCreation ? (
            <SimplifiedConnectionConfiguration />
          ) : (
            <>
              <ConnectionNameCard />
              {canEditDataGeographies && <DataResidencyCard />}
              <ConnectionConfigurationCard />
              {isSyncCatalogV2Enabled ? <SyncCatalogCardNext /> : <SyncCatalogCard />}
              <CreateConnectionFormControls />
            </>
          )}
        </FlexContainer>
      </Form>
    </Suspense>
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
    return <SchemaError schemaError={schemaErrorStatus} refreshSchema={onDiscoverSchema} />;
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
