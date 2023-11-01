/* eslint-disable check-file/filename-blocklist */
// temporary disable eslint rule for this file during form migration
import React, { createContext, useCallback, useContext, useState } from "react";
import { FieldErrors } from "react-hook-form/dist/types/errors";
import { useIntl } from "react-intl";

import {
  useSourceDefinitionVersion,
  useDestinationDefinitionVersion,
  useGetSourceDefinitionSpecification,
  useGetDestinationDefinitionSpecification,
} from "core/api";
import {
  ActorDefinitionVersionRead,
  DestinationDefinitionRead,
  DestinationDefinitionSpecificationRead,
  SourceDefinitionRead,
  SourceDefinitionSpecificationRead,
  WebBackendConnectionRead,
} from "core/request/AirbyteClient";
import { FormError, generateMessageFromError } from "core/utils/errorStatusMessage";
import { useDestinationDefinition } from "services/connector/DestinationDefinitionService";
import { useSourceDefinition } from "services/connector/SourceDefinitionService";

import {
  HookFormConnectionFormValues,
  useInitialHookFormValues,
} from "../../../components/connection/ConnectionForm/hookFormConfig";
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

interface ConnectionHookFormHook {
  connection: ConnectionOrPartialConnection;
  mode: ConnectionFormMode;
  sourceDefinition: SourceDefinitionRead;
  sourceDefinitionVersion: ActorDefinitionVersionRead;
  sourceDefinitionSpecification: SourceDefinitionSpecificationRead;
  destDefinition: DestinationDefinitionRead;
  destDefinitionVersion: ActorDefinitionVersionRead;
  destDefinitionSpecification: DestinationDefinitionSpecificationRead;
  initialValues: HookFormConnectionFormValues;
  schemaError?: SchemaError;
  refreshSchema: () => Promise<void>;
  setSubmitError: (submitError: FormError | null) => void;
  getErrorMessage: (
    formValid: boolean,
    errors?: FieldErrors<HookFormConnectionFormValues>
  ) => string | JSX.Element | null;
}

const useConnectionHookForm = ({
  connection,
  mode,
  schemaError,
  refreshSchema,
}: ConnectionServiceProps): ConnectionHookFormHook => {
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

  const initialValues = useInitialHookFormValues(
    connection,
    destDefinitionVersion,
    destDefinitionSpecification,
    mode !== "create"
  );
  const { formatMessage } = useIntl();
  const [submitError, setSubmitError] = useState<FormError | null>(null);

  const getErrorMessage = useCallback<ConnectionHookFormHook["getErrorMessage"]>(
    (formValid, errors) => {
      if (submitError) {
        return generateMessageFromError(submitError);
      }

      if (!formValid) {
        const hasNoStreamsSelectedError = errors?.syncCatalog?.streams?.message === "connectionForm.streams.required";
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
    refreshSchema,
    setSubmitError,
    getErrorMessage,
  };
};

export const ConnectionHookFormContext = createContext<ConnectionHookFormHook | null>(null);

export const ConnectionHookFormServiceProvider: React.FC<React.PropsWithChildren<ConnectionServiceProps>> = ({
  children,
  ...props
}) => {
  const data = useConnectionHookForm(props);
  return <ConnectionHookFormContext.Provider value={data}>{children}</ConnectionHookFormContext.Provider>;
};

export const useConnectionHookFormService = () => {
  const context = useContext(ConnectionHookFormContext);
  if (context === null) {
    throw new Error("useConnectionHookFormService must be used within a ConnectionHookFormProvider");
  }
  return context;
};
