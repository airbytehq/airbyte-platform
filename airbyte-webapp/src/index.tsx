import React, { lazy, Suspense } from "react";
import { createRoot } from "react-dom/client";

import { WebappConfigContextProvider, loadConfig } from "core/config";
import { ErrorDetails } from "core/errors/components/ErrorDetails";
import { I18nProvider } from "core/services/i18n";
import { CLOUD_EDITION } from "core/utils/app";
import { loadDatadog } from "core/utils/datadog";
import { loadOsano } from "core/utils/dataPrivacy";
import { AirbyteThemeProvider } from "hooks/theme/useAirbyteTheme";

import "react-reflex/styles.css";
import "./dayjs-setup";
import "./scss/global.scss";

const CloudApp = lazy(() => import(`packages/cloud/App`));
const App = lazy(() => import(`./App`));

loadConfig()
  .then((config) => {
    loadDatadog(config);

    // In Cloud load the Osano script (GDPR consent tool before anything else)
    if (config.edition === CLOUD_EDITION) {
      loadOsano();
    }

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const root = createRoot(document.getElementById("root")!);
    root.render(
      <React.StrictMode>
        <WebappConfigContextProvider config={config}>
          <Suspense fallback={null}>{config.edition === CLOUD_EDITION ? <CloudApp /> : <App />}</Suspense>
        </WebappConfigContextProvider>
      </React.StrictMode>
    );
  })
  .catch((error) => {
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const root = createRoot(document.getElementById("root")!);
    root.render(
      <AirbyteThemeProvider>
        <I18nProvider>
          <ErrorDetails error={error} />
        </I18nProvider>
      </AirbyteThemeProvider>
    );
  });
