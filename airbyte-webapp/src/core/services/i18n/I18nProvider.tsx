import type { IntlConfig } from "react-intl";

import isEqual from "lodash/isEqual";
import React, { useContext, useMemo, useState } from "react";
import { IntlProvider } from "react-intl";

import errorMessages from "locales/en.errors.json";
import messages from "locales/en.json";

type Messages = IntlConfig["messages"];

interface I18nContext {
  setMessageOverwrite: (messages: Messages) => void;
}

const i18nContext = React.createContext<I18nContext>({ setMessageOverwrite: () => null });

export const useI18nContext = () => {
  return useContext(i18nContext);
};

interface I18nProviderProps {
  /**
   * The locale to use for internationalization. If not provided, the browser locale will be used.
   */
  locale?: string;
}

const getBrowserLocale = () => new Intl.DateTimeFormat().resolvedOptions().locale ?? "en";

export const I18nProvider: React.FC<React.PropsWithChildren<I18nProviderProps>> = ({ children, locale }) => {
  const [overwrittenMessages, setOvewrittenMessages] = useState<Messages>({});

  const i18nOverwriteContext = useMemo<I18nContext>(
    () => ({
      setMessageOverwrite: (messages) => {
        setOvewrittenMessages((prevMessages) => (isEqual(prevMessages, messages) ? prevMessages : messages));
      },
    }),
    []
  );

  const mergedMessages = useMemo(
    () => ({
      ...messages,
      ...Object.fromEntries(Object.entries(errorMessages).map(([key, value]) => [`error:${key}`, value])),
      ...(overwrittenMessages ?? {}),
    }),
    [overwrittenMessages]
  );

  // Silence all warnings and errors during unit tests
  const logger = process.env.NODE_ENV === "test" ? () => {} : undefined;

  return (
    <i18nContext.Provider value={i18nOverwriteContext}>
      <IntlProvider
        locale={locale ?? getBrowserLocale()}
        messages={mergedMessages}
        defaultRichTextElements={{
          b: (chunk) => <strong>{chunk}</strong>,
        }}
        onWarn={logger}
        onError={logger}
      >
        {children}
      </IntlProvider>
    </i18nContext.Provider>
  );
};
