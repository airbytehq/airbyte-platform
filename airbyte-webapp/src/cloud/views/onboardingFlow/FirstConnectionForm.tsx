import React, { useCallback, useRef } from "react";
import { useFormContext, useFormState } from "react-hook-form";
import { useEffectOnce } from "react-use";

import { Form } from "components/ui/forms";
import LoadingSchema from "components/ui/LoadingSchema";
import { Text } from "components/ui/Text";

import { FormConnectionFormValues, useInitialFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { useConnectionValidationZodSchema } from "area/connection/components/ConnectionForm/schemas/connectionSchema";
import { SchemaError } from "area/connection/components/CreateConnectionForm/SchemaError";
import { SyncCatalogTable } from "area/connection/components/SyncCatalogTable";
import {
  ConnectionFormServiceProvider,
  ConnectionOrPartialConnection,
  useConnectionFormService,
} from "area/connection/utils/ConnectionForm/ConnectionFormService";
import { useCreateConnection, useDestinationDefinitionVersion, useDiscoverSchemaQuery } from "core/api";
import { ConnectionScheduleType } from "core/api/types/AirbyteClient";
import { useFormatError } from "core/errors";
import { useNotificationService } from "core/services/Notification";
import { FormModeProvider, useFormMode } from "core/services/ui/FormModeContext";
import { trackError } from "core/utils/datadog";

import { OnboardingPair } from "./useOnboardingPair";

export const FIRST_CONNECTION_FORM_ID = "onboarding-first-connection-form";

/**
 * Shows the stream errors before a submit, as the create page does: a source can select no stream by default, and a
 * default sync mode can need a primary key that the stream does not have.
 */
const StreamSelectionError: React.FC = () => {
  const { isValid, errors } = useFormState<FormConnectionFormValues>();
  const { trigger } = useFormContext<FormConnectionFormValues>();
  const { getErrorMessage } = useConnectionFormService();

  useEffectOnce(() => {
    trigger("syncCatalog.streams");
  });

  return (
    <Text color="red" size="lg" aria-live="polite">
      {getErrorMessage(isValid, errors)}
    </Text>
  );
};

interface FirstConnectionFormProps {
  pair: OnboardingPair;
  /** The screen owns the mutation, so its action bar shows the saving state. */
  createConnection: ReturnType<typeof useCreateConnection>["mutateAsync"];
  onCreated: () => void;
}

/**
 * The stream table of the create connection page, with its defaults. The flow has no settings step, so the connection
 * takes the default settings of the partial connection.
 */
const StreamSelectionForm: React.FC<FirstConnectionFormProps> = ({ pair, createConnection, onCreated }) => {
  const { source, destination } = pair;
  const { connection } = useConnectionFormService();
  const { mode } = useFormMode();
  const { supportsFileTransfer } = useDestinationDefinitionVersion(destination.destinationId);
  const initialValues = useInitialFormValues(connection, mode, supportsFileTransfer);
  const zodSchema = useConnectionValidationZodSchema();
  const { registerNotification } = useNotificationService();
  const formatError = useFormatError();
  // Continue sits outside the form and does not wait for the async validation, so a second submit during a create is
  // ignored.
  const isCreatingRef = useRef(false);

  const onSubmit = useCallback(
    async (values: FormConnectionFormValues) => {
      if (isCreatingRef.current) {
        return;
      }
      isCreatingRef.current = true;
      try {
        await createConnection({
          values,
          source,
          destination,
          sourceDefinition: { sourceDefinitionId: source.sourceDefinitionId },
          destinationDefinition: {
            name: destination.name,
            destinationDefinitionId: destination.destinationDefinitionId,
          },
          sourceCatalogId: connection.catalogId,
        });
      } finally {
        isCreatingRef.current = false;
      }
      onCreated();
    },
    [connection.catalogId, createConnection, destination, onCreated, source]
  );

  const onError = useCallback(
    (error: Error) => {
      trackError(error);
      registerNotification({
        id: "onboardingFlow/create-connection-failure",
        text: formatError(error),
        type: "error",
      });
    },
    [formatError, registerNotification]
  );

  return (
    <Form<FormConnectionFormValues>
      id={FIRST_CONNECTION_FORM_ID}
      defaultValues={initialValues}
      zodSchema={zodSchema}
      onSubmit={onSubmit}
      onError={onError}
    >
      <SyncCatalogTable variant="compact" />
      <StreamSelectionError />
    </Form>
  );
};

/**
 * Discovers the source schema and shows the stream table, with the same pieces as `CreateConnectionForm`. That form
 * reads its connectors from the URL and leads to the normal app's connection pages, so the flow cannot use it as is.
 */
export const FirstConnectionForm: React.FC<FirstConnectionFormProps> = ({ pair, createConnection, onCreated }) => {
  const { data, error, isFetching, refetch } = useDiscoverSchemaQuery(pair.source, { useErrorBoundary: false });
  const refreshSchema = async () => {
    await refetch();
  };

  if (error instanceof Error) {
    return <SchemaError schemaError={error} refreshSchema={refreshSchema} />;
  }
  if (!data?.catalog || isFetching) {
    return <LoadingSchema />;
  }

  const connection: ConnectionOrPartialConnection = {
    syncCatalog: data.catalog,
    source: pair.source,
    destination: pair.destination,
    catalogId: data.catalogId,
    // The manual schedule keeps the connection from starting a sync on creation, so "Run your first sync" starts it.
    scheduleType: ConnectionScheduleType.manual,
  };

  return (
    <FormModeProvider mode="create">
      <ConnectionFormServiceProvider connection={connection} refreshSchema={refreshSchema} schemaError={null}>
        <StreamSelectionForm
          key={data.catalogId}
          pair={pair}
          createConnection={createConnection}
          onCreated={onCreated}
        />
      </ConnectionFormServiceProvider>
    </FormModeProvider>
  );
};
