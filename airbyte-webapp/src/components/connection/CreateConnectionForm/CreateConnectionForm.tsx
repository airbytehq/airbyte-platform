import React, { Suspense, useCallback, useEffect } from "react";
import { useNavigate } from "react-router-dom";

import { Form } from "components/forms";
import LoadingSchema from "components/LoadingSchema";
import { FlexContainer } from "components/ui/Flex";

import { useGetDestinationFromSearchParams, useGetSourceFromSearchParams } from "area/connector/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCreateConnection, useDiscoverSchema } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import {
  ConnectionFormServiceProvider,
  useConnectionFormService,
} from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperimentContext } from "hooks/services/Experiment";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";

import { ConnectionNameCard } from "./ConnectionNameCard";
import styles from "./CreateConnectionForm.module.scss";
import { DataResidencyCard } from "./DataResidencyCard";
import { SchemaError } from "./SchemaError";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";
import { ConnectionConfigurationCard } from "../ConnectionForm/ConnectionConfigurationCard";
import { CreateConnectionFormControls } from "../ConnectionForm/CreateConnectionFormControls";
import { FormConnectionFormValues, useConnectionValidationSchema } from "../ConnectionForm/formConfig";
import { OperationsSectionCard } from "../ConnectionForm/OperationsSectionCard";
import { SyncCatalogCard } from "../ConnectionForm/SyncCatalogCard";
import { mapFormValuesToOperations } from "../ConnectionForm/utils";

const CreateConnectionFormInner: React.FC = () => {
  const navigate = useNavigate();
  const workspaceId = useCurrentWorkspaceId();
  const { clearAllFormChanges } = useFormChangeTrackerService();
  const { mutateAsync: createConnection } = useCreateConnection();
  const { connection, initialValues, setSubmitError } = useConnectionFormService();
  const canEditDataGeographies = useFeature(FeatureItem.AllowChangeDataGeographies);
  useExperimentContext("source-definition", connection.source?.sourceDefinitionId);

  const validationSchema = useConnectionValidationSchema();

  const onSubmit = useCallback(
    async ({ normalization, transformations, ...restFormValues }: FormConnectionFormValues) => {
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
      <Form<FormConnectionFormValues>
        defaultValues={initialValues}
        schema={validationSchema}
        onSubmit={onSubmit}
        trackDirtyChanges
      >
        <FlexContainer direction="column" className={styles.formContainer}>
          <ConnectionNameCard />
          {canEditDataGeographies && <DataResidencyCard />}
          <ConnectionConfigurationCard />
          <SyncCatalogCard />
          <OperationsSectionCard />
          <CreateConnectionFormControls />
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
