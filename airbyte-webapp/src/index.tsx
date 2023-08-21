import { lazy, Suspense } from "react";
import { createRoot } from "react-dom/client";

import "react-reflex/styles.css";
import { isCloudApp } from "core/utils/app";
import { loadDatadog } from "core/utils/datadog";
import { loadOsano } from "core/utils/dataPrivacy";

import "./dayjs-setup";
import "./scss/global.scss";

// We do not follow default config approach since we want to init datadog asap
loadDatadog();

// In Cloud load the Osano script (GDPR consent tool before anything else)
if (isCloudApp()) {
  loadOsano();
}

const CloudApp = lazy(() => import(`packages/cloud/App`));
const App = lazy(() => import(`./App`));

// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
const root = createRoot(document.getElementById("root")!);
root.render(<Suspense fallback={null}>{isCloudApp() ? <CloudApp /> : <App />}</Suspense>);
