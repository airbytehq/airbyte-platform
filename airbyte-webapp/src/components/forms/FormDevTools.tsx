import React, { Suspense } from "react";

import { isDevelopment } from "utils/isDevelopment";

const FormDevToolsInternal = React.lazy(() => import("./FormDevToolsInternal"));

export const FormDevTools = isDevelopment()
  ? () => (
      <Suspense fallback={null}>
        <FormDevToolsInternal />
      </Suspense>
    )
  : () => null;
