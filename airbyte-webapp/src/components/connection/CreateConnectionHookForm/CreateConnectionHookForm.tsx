import React, { Suspense, useCallback, useEffect } from "react";
import { useNavigate } from "react-router-dom";

import { Form } from "components/forms";
import LoadingSchema from "components/LoadingSchema";
import { FlexContainer } from "components/ui/Flex";

import { useGetDestinationFromSearchParams, useGetSourceFromSearchParams } from "area/connector/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCreateConnection } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import {
  ConnectionFormServiceProvider,
  useConnectionFormService,
} from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperimentContext } from "hooks/services/Experiment";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useDiscoverSchema } from "hooks/services/useSourceHook";

import { ConnectionNameHookFormCard } from "./ConnectionNameHookFormCard";
import styles from "./CreateConnectionHookForm.module.scss";
import { ConnectionConfigurationHookFormCard } from "../ConnectionForm/ConnectionConfigurationHookFormCard";
import { CreateControlsHookForm } from "../ConnectionForm/CreateControlsHookForm";
import { HookFormConnectionFormValues, useConnectionHookFormValidationSchema } from "../ConnectionForm/hookFormConfig";
import { OperationsSectionHookForm } from "../ConnectionForm/OperationsSectionHookForm";
import { SyncCatalogHookFormField } from "../ConnectionForm/SyncCatalogHookFormField";
import { mapFormValuesToOperations } from "../ConnectionForm/utils";
import { DataResidencyHookFormCard } from "../CreateConnectionForm/DataResidencyHookFormCard";
import { SchemaError } from "../CreateConnectionForm/SchemaError";
import { useAnalyticsTrackFunctions } from "../CreateConnectionForm/useAnalyticsTrackFunctions";

const CreateConnectionFormInner: React.FC = () => {
  const navigate = useNavigate();
  const workspaceId = useCurrentWorkspaceId();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const { mutateAsync: createConnection } = useCreateConnection();
  const { connection, initialValues, setSubmitError } = useConnectionFormService();
  const canEditDataGeographies = useFeature(FeatureItem.AllowChangeDataGeographies);
  useExperimentContext("source-definition", connection.source?.sourceDefinitionId);

  const validationSchema = useConnectionHookFormValidationSchema();

  const onSubmit = useCallback(
    async ({ normalization, transformations, ...restFormValues }: HookFormConnectionFormValues) => {
      try {
        const createdConnection = await createConnection({
          values: {
            ...restFormValues,
            // don't add operations if normalization and transformations are undefined
            ...((normalization !== undefined || transformations !== undefined) && {
              // combine the normalization and transformations into operations[]
              operations: mapFormValuesToOperations(workspaceId, normalization, transformations),
            }),
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
      workspaceId,
    ]
  );

  return (
    <Suspense fallback={<LoadingSchema />}>
      <Form<HookFormConnectionFormValues>
        defaultValues={initialValues}
        schema={validationSchema}
        onSubmit={onSubmit}
        trackDirtyChanges
      >
        <FlexContainer direction="column" className={styles.formContainer}>
          <ConnectionNameHookFormCard />
          {canEditDataGeographies && <DataResidencyHookFormCard />}
          <ConnectionConfigurationHookFormCard />
          <SyncCatalogHookFormField />
          <OperationsSectionHookForm />
          <CreateControlsHookForm />
        </FlexContainer>
      </Form>
    </Suspense>
  );
};

/**
 * react-hook-form version of the CreateConnectionForm
 */
export const CreateConnectionHookForm: React.FC = () => {
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
      {isLoading ? (
        <LoadingSchema />
      ) : schemaErrorStatus ? (
        <SchemaError schemaError={schemaErrorStatus} />
      ) : (
        <CreateConnectionFormInner />
      )}
    </ConnectionFormServiceProvider>
  );
};
