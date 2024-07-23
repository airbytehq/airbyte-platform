import { useCallback } from "react";
import { useIntl } from "react-intl";

import { I18nError } from "./I18nError";

type FormatErrorFn = (error: Error) => ReturnType<ReturnType<typeof useIntl>["formatMessage"]>;

/**
 * Utility hook that returns a method to format an error to be displayed to the user.
 * This will take into account whether the error is an I18nError and translate it if so.
 */
export const useFormatError = (): FormatErrorFn => {
  const { formatMessage } = useIntl();
  return useCallback(
    (error?: Error) => {
      return error instanceof I18nError ? error.translate(formatMessage) : error?.message;
    },
    [formatMessage]
  );
};
