import type { useIntl } from "react-intl";

import { FormattedMessage } from "react-intl";

import { FailureOrigin, FailureReason } from "core/api/types/AirbyteClient";
import { I18nError } from "core/errors";
import { TimelineFailureReason } from "pages/connections/ConnectionTimelinePage/types";

export class FormError extends Error {
  status?: number;
}

/**
 * @deprecated Use the `useFormatError` hook from `core/errors` instead.
 */
export const generateMessageFromError = (
  error: FormError,
  formatMessage: ReturnType<typeof useIntl>["formatMessage"]
): React.ReactNode => {
  if (error instanceof I18nError) {
    return error.translate(formatMessage);
  }

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

export interface FailureUiDetails {
  type: "error" | "warning";
  typeLabel: string;
  origin: FailureReason["failureOrigin"];
  message: string;
  secondaryMessage?: string;
}

export const getFailureType = (failure: FailureReason | TimelineFailureReason): "error" | "warning" => {
  const isConfigError = failure.failureType === "config_error";
  const isSourceError = failure.failureOrigin === FailureOrigin.source;
  const isDestinationError = failure.failureOrigin === FailureOrigin.destination;

  return isConfigError && (isSourceError || isDestinationError) ? "error" : "warning";
};

export const failureUiDetailsFromReason = <
  T extends FailureReason | TimelineFailureReason | undefined | null,
  RetVal = T extends FailureReason ? FailureUiDetails : null,
>(
  reason: T,
  formatMessage: ReturnType<typeof useIntl>["formatMessage"]
): RetVal => {
  if (!reason) {
    return null as RetVal;
  }

  const type = getFailureType(reason);
  const origin = reason.failureOrigin;

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
