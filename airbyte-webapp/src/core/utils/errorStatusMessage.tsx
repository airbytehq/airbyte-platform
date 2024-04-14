import type { useIntl } from "react-intl";

import { FormattedMessage } from "react-intl";

import { FailureOrigin, FailureReason } from "core/api/types/AirbyteClient";

export class FormError extends Error {
  status?: number;
}

export const generateMessageFromError = (error: FormError): JSX.Element | string | null => {
  if (error.message) {
    return error.message;
  }

  if (!error.status || error.status === 0) {
    return null;
  }

  return error.status === 400 ? (
    <FormattedMessage id="form.validationError" />
  ) : (
    <FormattedMessage id="form.someError" />
  );
};

interface FailureUiDetails {
  type: "error" | "warning";
  typeLabel: string;
  origin: FailureReason["failureOrigin"];
  message: string;
  secondaryMessage?: string;
}
export const failureUiDetailsFromReason = <
  T extends FailureReason | undefined | null,
  RetVal = T extends FailureReason ? FailureUiDetails : null,
>(
  reason: T,
  formatMessage: ReturnType<typeof useIntl>["formatMessage"]
): RetVal => {
  if (!reason) {
    return null as RetVal;
  }

  const isConfigError = reason.failureType === "config_error";
  const isSourceError = reason.failureOrigin === FailureOrigin.source;
  const isDestinationError = reason.failureOrigin === FailureOrigin.destination;

  const origin = reason.failureOrigin;
  const type = isConfigError && (isSourceError || isDestinationError) ? "error" : "warning";
  const typeLabel = formatMessage(
    { id: type === "error" ? "failureMessage.type.error" : "failureMessage.type.warning" },
    { origin }
  );
  const message = reason.externalMessage ?? formatMessage({ id: "errorView.unknown" });
  const secondaryMessage =
    type === "error" && reason.externalMessage !== reason.internalMessage ? undefined : reason.internalMessage;

  const result: FailureUiDetails = { type, typeLabel, origin, message, secondaryMessage };
  return result as RetVal;
};
