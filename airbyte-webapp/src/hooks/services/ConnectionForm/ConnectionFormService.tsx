import React, { createContext, useCallback, useContext, useState } from "react";
import { FieldErrors } from "react-hook-form";
import { useIntl } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { ExternalLink } from "components/ui/Link";

import { HttpProblem } from "core/api";
import { WebBackendConnectionRead } from "core/api/types/AirbyteClient";
import { useFormatError } from "core/errors";
import { FormError } from "core/utils/errorStatusMessage";
import { links } from "core/utils/links";

export type ConnectionFormMode = "create" | "edit" | "readonly";

export type ConnectionOrPartialConnection =
  | WebBackendConnectionRead
  | (Partial<WebBackendConnectionRead> & Pick<WebBackendConnectionRead, "syncCatalog" | "source" | "destination">);

interface ConnectionServiceProps {
  connection: ConnectionOrPartialConnection;
  schemaError?: Error | null;
  refreshSchema: () => Promise<void>;
}

interface ConnectionFormHook {
  connection: ConnectionOrPartialConnection;
  schemaError?: Error | null;
  refreshSchema: () => Promise<void>;
  setSubmitError: (submitError: FormError | null) => void;
  getErrorMessage: (formValid: boolean, errors?: FieldErrors<FormConnectionFormValues>) => React.ReactNode;
}

const useConnectionForm = ({ connection, schemaError, refreshSchema }: ConnectionServiceProps): ConnectionFormHook => {
  const formatError = useFormatError();
  const { formatMessage } = useIntl();
  const [submitError, setSubmitError] = useState<FormError | null>(null);

  const getErrorMessage = useCallback<ConnectionFormHook["getErrorMessage"]>(
    (formValid, errors) => {
      if (submitError) {
        if (HttpProblem.isTypeOrSubtype(submitError, "error:cron-validation") && submitError.i18nType !== "exact") {
          // Handle cron expression errors (that don't have an explicit translation already) with a more detailed error
          return formatMessage(
            { id: "form.cronExpression.invalid" },
            { lnk: (btnText: React.ReactNode) => <ExternalLink href={links.cronReferenceLink}>{btnText}</ExternalLink> }
          ) as string;
        }

        return formatError(submitError);
      }

      if (!formValid) {
        const hasNoStreamsSelectedError = errors?.syncCatalog?.streams?.message === "connectionForm.streams.required";
        const hasHashCollisionError =
          errors?.syncCatalog?.streams?.message === "connectionForm.streams.hashFieldCollision";

        const hasPrimaryKeyOrCursorError = (field: "primaryKey" | "cursorField") =>
          errors?.syncCatalog?.streams &&
          Object.entries(
            errors?.syncCatalog?.streams as FieldErrors<FormConnectionFormValues["syncCatalog"]["streams"]>
          ).some(([_, streamNode]) => streamNode?.config?.[field]?.message === `connectionForm.${field}.required`);

        const pkError = hasPrimaryKeyOrCursorError("primaryKey");
        const cursorError = hasPrimaryKeyOrCursorError("cursorField");

        const validationErrorMessage =
          pkError && cursorError
            ? "form.error.pkAndCursor.required"
            : pkError
            ? "form.error.pk.missing"
            : cursorError
            ? "form.error.cursor.missing"
            : "connectionForm.validation.creationError";
        return formatMessage({
          id: hasNoStreamsSelectedError
            ? "connectionForm.streams.required"
            : hasHashCollisionError
            ? "connectionForm.streams.hashFieldCollision"
            : validationErrorMessage,
        });
      }

      return null;
    },
    [submitError, formatError, formatMessage]
  );

  return {
    connection,
    schemaError,
    refreshSchema,
    setSubmitError,
    getErrorMessage,
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
  const context = useContext(ConnectionFormContext);
  if (context === null) {
    throw new Error("useConnectionFormService must be used within a ConnectionFormProvider");
  }
  return context;
};
