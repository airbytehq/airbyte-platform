import { useIntl } from "react-intl";

type FormatMessageFn = ReturnType<typeof useIntl>["formatMessage"];

/**
 * An error that can be thrown or extended to have an i18n message been rendered in the error view.
 * By default the error view will show the error message as is. For I18nError, the error view will
 * translate the message/i18n key with the specified i18nParams via react-intl.
 */
export class I18nError extends Error {
  constructor(
    public readonly i18nKey: string,
    public readonly i18nParams?: Parameters<FormatMessageFn>[1]
  ) {
    super(i18nKey);
    this.name = "I18nError";
  }

  translate(
    formatMessage: ReturnType<typeof useIntl>["formatMessage"]
  ): ReturnType<ReturnType<typeof useIntl>["formatMessage"]> {
    return formatMessage({ id: this.i18nKey }, this.i18nParams);
  }
}
