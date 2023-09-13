/* eslint-disable check-file/filename-blocklist */
// temporary disable eslint rule for this file during form migration
import React, { createContext, useContext } from "react";

import { useDestinationDefinitionVersion } from "core/api";
import {
  ActorDefinitionVersionRead,
  DestinationDefinitionRead,
  DestinationDefinitionSpecificationRead,
  SourceDefinitionRead,
  SourceDefinitionSpecificationRead,
  WebBackendConnectionRead,
} from "core/request/AirbyteClient";
import { useDestinationDefinition } from "services/connector/DestinationDefinitionService";
import { useGetDestinationDefinitionSpecification } from "services/connector/DestinationDefinitionSpecificationService";
import { useSourceDefinition } from "services/connector/SourceDefinitionService";
import { useGetSourceDefinitionSpecification } from "services/connector/SourceDefinitionSpecificationService";

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

// not sure this one should be here, consider moving it to the another file or utils file
// export const tidyConnectionHookFormValues = (
//   values: HookFormConnectionFormValues,
//   workspaceId: string,
//   validationSchema: ConnectionValidationSchema,
//   operations?: OperationRead[]
// ): ConnectionValues => {
//   // TODO (https://github.com/airbytehq/airbyte/issues/17279): We should try to fix the types so we don't need the casting.
//   const formValues: ConnectionFormValues = validationSchema.cast(values, {
//     context: { isRequest: true },
//   }) as unknown as ConnectionFormValues;
//
//   formValues.operations = mapFormPropsToOperation(values, operations, workspaceId);
//
//   if (formValues.scheduleType === ConnectionScheduleType.manual) {
//     // Have to set this to undefined to override the existing scheduleData
//     formValues.scheduleData = undefined;
//   }
//   return formValues;
// };

interface ConnectionHookFormHook {
  connection: ConnectionOrPartialConnection;
  mode: ConnectionFormMode;
  sourceDefinition: SourceDefinitionRead;
  sourceDefinitionSpecification: SourceDefinitionSpecificationRead;
  destDefinition: DestinationDefinitionRead;
  destDefinitionVersion: ActorDefinitionVersionRead;
  destDefinitionSpecification: DestinationDefinitionSpecificationRead;
  initialValues: HookFormConnectionFormValues;
  schemaError?: SchemaError;
  refreshSchema: () => Promise<void>;
  // formId: string;
  // setSubmitError: (submitError: FormError | null) => void;
  // getErrorMessage: (
  //   formValid: boolean,
  //   errors?: FormikErrors<FormikConnectionFormValues>
  // ) => string | JSX.Element | null;
}

const useConnectionHookForm = ({
  connection,
  mode,
  schemaError,
  refreshSchema,
}: ConnectionServiceProps): ConnectionHookFormHook => {
  const {
    source: { sourceDefinitionId },
    destination: { destinationId, destinationDefinitionId },
  } = connection;

  const sourceDefinition = useSourceDefinition(sourceDefinitionId);
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
    mode !== "create"
    // destDefinitionSpecification,
  );
  // const { formatMessage } = useIntl();
  // const [submitError, setSubmitError] = useState<FormError | null>(null);

  /**
   * not sure we need this in react-hook-form, since we have change tracker inside thr base form
   */

  // const formId = useUniqueFormId();

  /**
   * commented out because need to figure out how to manage errors with react-hook-form
   */
  // const getErrorMessage = useCallback<ConnectionHookFormHook["getErrorMessage"]>(
  //   (formValid, errors) => {
  //     if (submitError) {
  //       return generateMessageFromError(submitError);
  //     }
  //
  //     // There is a case when some fields could be dropped in the database. We need to validate the form without property dirty
  //     const hasValidationError = !formValid;
  //
  //     if (hasValidationError) {
  //       const hasNoStreamsSelectedError = errors?.syncCatalog?.streams === "connectionForm.streams.required";
  //       return formatMessage({
  //         id: hasNoStreamsSelectedError ? "connectionForm.streams.required" : "connectionForm.validation.error",
  //       });
  //     }
  //
  //     return null;
  //   },
  //   [formatMessage, submitError]
  // );

  return {
    connection,
    mode,
    sourceDefinition,
    sourceDefinitionSpecification,
    destDefinition,
    destDefinitionVersion,
    destDefinitionSpecification,
    initialValues,
    schemaError,
    refreshSchema,
    // formId,
    // setSubmitError,
    // getErrorMessage,
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
