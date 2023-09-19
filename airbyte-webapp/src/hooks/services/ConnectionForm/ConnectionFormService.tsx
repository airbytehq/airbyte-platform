import { FormikErrors } from "formik";
import React, { createContext, useCallback, useContext, useState } from "react";
import { useIntl } from "react-intl";

import {
  ConnectionFormValues,
  ConnectionValidationSchema,
  FormikConnectionFormValues,
  mapFormPropsToOperation,
  useInitialValues,
} from "components/connection/ConnectionForm/formConfig";

import { ConnectionValues, useDestinationDefinitionVersion, useSourceDefinitionVersion } from "core/api";
import {
  ActorDefinitionVersionRead,
  ConnectionScheduleType,
  DestinationDefinitionRead,
  DestinationDefinitionSpecificationRead,
  OperationRead,
  SourceDefinitionRead,
  SourceDefinitionSpecificationRead,
  WebBackendConnectionRead,
} from "core/api/types/AirbyteClient";
import { FormError, generateMessageFromError } from "core/utils/errorStatusMessage";
import { useDestinationDefinition } from "services/connector/DestinationDefinitionService";
import { useGetDestinationDefinitionSpecification } from "services/connector/DestinationDefinitionSpecificationService";
import { useSourceDefinition } from "services/connector/SourceDefinitionService";
import { useGetSourceDefinitionSpecification } from "services/connector/SourceDefinitionSpecificationService";

import { ConnectionHookFormContext } from "./ConnectionHookFormService";
import { useExperiment } from "../Experiment";
import { useUniqueFormId } from "../FormChangeTracker";
import { SchemaError } from "../useSourceHook";

export type ConnectionFormMode = "create" | "edit" | "readonly";

export type ConnectionOrPartialConnection =
  | WebBackendConnectionRead
  | (Partial<WebBackendConnectionRead> & Pick<WebBackendConnectionRead, "syncCatalog" | "source" | "destination">);

interface ConnectionServiceProps {
  connection: ConnectionOrPartialConnection;
  mode: ConnectionFormMode;
  schemaError?: SchemaError | null;
  refreshSchema: () => Promise<void>;
}

export const tidyConnectionFormValues = (
  values: FormikConnectionFormValues,
  workspaceId: string,
  validationSchema: ConnectionValidationSchema,
  operations?: OperationRead[]
): ConnectionValues => {
  // TODO (https://github.com/airbytehq/airbyte/issues/17279): We should try to fix the types so we don't need the casting.
  const formValues: ConnectionFormValues = validationSchema.cast(values, {
    context: { isRequest: true },
  }) as unknown as ConnectionFormValues;

  formValues.operations = mapFormPropsToOperation(values, operations, workspaceId);

  if (formValues.scheduleType === ConnectionScheduleType.manual) {
    // Have to set this to undefined to override the existing scheduleData
    formValues.scheduleData = undefined;
  }
  return formValues;
};

interface ConnectionFormHook {
  connection: ConnectionOrPartialConnection;
  mode: ConnectionFormMode;
  sourceDefinition: SourceDefinitionRead;
  sourceDefinitionSpecification: SourceDefinitionSpecificationRead;
  sourceDefinitionVersion: ActorDefinitionVersionRead;
  destDefinition: DestinationDefinitionRead;
  destDefinitionVersion: ActorDefinitionVersionRead;
  destDefinitionSpecification: DestinationDefinitionSpecificationRead;
  initialValues: FormikConnectionFormValues;
  schemaError?: SchemaError;
  formId: string;
  setSubmitError: (submitError: FormError | null) => void;
  getErrorMessage: (
    formValid: boolean,
    errors?: FormikErrors<FormikConnectionFormValues>
  ) => string | JSX.Element | null;
  refreshSchema: () => Promise<void>;
}

const useConnectionForm = ({
  connection,
  mode,
  schemaError,
  refreshSchema,
}: ConnectionServiceProps): ConnectionFormHook => {
  const {
    source: { sourceId, sourceDefinitionId },
    destination: { destinationId, destinationDefinitionId },
  } = connection;

  const sourceDefinition = useSourceDefinition(sourceDefinitionId);
  const sourceDefinitionVersion = useSourceDefinitionVersion(sourceId);
  const sourceDefinitionSpecification = useGetSourceDefinitionSpecification(sourceDefinitionId, connection.sourceId);

  const destDefinition = useDestinationDefinition(destinationDefinitionId);
  const destDefinitionVersion = useDestinationDefinitionVersion(destinationId);
  const destDefinitionSpecification = useGetDestinationDefinitionSpecification(
    destinationDefinitionId,
    connection.destinationId
  );

  const initialValues = useInitialValues(
    connection,
    destDefinitionVersion,
    destDefinitionSpecification,
    mode !== "create"
  );
  const { formatMessage } = useIntl();
  const [submitError, setSubmitError] = useState<FormError | null>(null);
  const formId = useUniqueFormId();

  const getErrorMessage = useCallback<ConnectionFormHook["getErrorMessage"]>(
    (formValid, errors) => {
      if (submitError) {
        return generateMessageFromError(submitError);
      }

      // There is a case when some fields could be dropped in the database. We need to validate the form without property dirty
      const hasValidationError = !formValid;

      if (hasValidationError) {
        const hasNoStreamsSelectedError = errors?.syncCatalog?.streams === "connectionForm.streams.required";
        return formatMessage({
          id: hasNoStreamsSelectedError ? "connectionForm.streams.required" : "connectionForm.validation.error",
        });
      }

      return null;
    },
    [formatMessage, submitError]
  );

  return {
    connection,
    mode,
    sourceDefinition,
    sourceDefinitionVersion,
    sourceDefinitionSpecification,
    destDefinition,
    destDefinitionVersion,
    destDefinitionSpecification,
    initialValues,
    schemaError,
    formId,
    setSubmitError,
    getErrorMessage,
    refreshSchema,
  };
};

const ConnectionFormContext = createContext<ConnectionFormHook | null>(null);

export const ConnectionFormServiceProvider: React.FC<React.PropsWithChildren<ConnectionServiceProps>> = ({
  children,
  ...props
}) => {
  const data = useConnectionForm(props);
  return <ConnectionFormContext.Provider value={data}>{children}</ConnectionFormContext.Provider>;
};

export const useConnectionFormService = () => {
  /**
   *TODO: remove conditional context after successful CreateConnectionForm migration
   *https://github.com/airbytehq/airbyte-platform-internal/issues/8639
   */
  const doUseCreateConnectionHookForm = useExperiment("form.createConnectionHookForm", false);
  const contextType = doUseCreateConnectionHookForm ? ConnectionHookFormContext : ConnectionFormContext;

  const context = useContext(contextType as React.Context<ConnectionFormHook | null>);
  if (context === null) {
    throw new Error("useConnectionFormService must be used within a ConnectionFormProvider");
  }
  return context;
};
